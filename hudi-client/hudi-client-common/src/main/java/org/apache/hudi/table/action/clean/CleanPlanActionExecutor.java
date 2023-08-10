/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.action.clean;

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanFileInfo;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.CleanFileInfo;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CleanPlanActionExecutor<T extends HoodieRecordPayload, I, K, O> extends BaseActionExecutor<T, I, K, O, Option<HoodieCleanerPlan>> {

  private static final Logger LOG = LogManager.getLogger(CleanPlanner.class);

  private final Option<Map<String, String>> extraMetadata;

  public CleanPlanActionExecutor(HoodieEngineContext context,
                                 HoodieWriteConfig config,
                                 HoodieTable<T, I, K, O> table,
                                 String instantTime,
                                 Option<Map<String, String>> extraMetadata) {
    super(context, config, table, instantTime);
    this.extraMetadata = extraMetadata;
  }

  private int getCommitsSinceLastCleaning() {
    Option<HoodieInstant> lastCleanInstant = table.getActiveTimeline().getCleanerTimeline().filterCompletedInstants().lastInstant();
    HoodieTimeline commitTimeline = table.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();

    int numCommits;
    if (lastCleanInstant.isPresent() && !table.getActiveTimeline().isEmpty(lastCleanInstant.get())) {
      try {
        HoodieCleanMetadata cleanMetadata = TimelineMetadataUtils
            .deserializeHoodieCleanMetadata(table.getActiveTimeline().getInstantDetails(lastCleanInstant.get()).get());
        String lastCompletedCommitTimestamp = cleanMetadata.getLastCompletedCommitTimestamp();
        numCommits = commitTimeline.findInstantsAfter(lastCompletedCommitTimestamp).countInstants();
      } catch (IOException e) {
        throw new HoodieIOException("Parsing of last clean instant " + lastCleanInstant.get() + " failed", e);
      }
    } else {
      numCommits = commitTimeline.countInstants();
    }

    return numCommits;
  }

  private boolean needsCleaning(CleaningTriggerStrategy strategy) {
    if (strategy == CleaningTriggerStrategy.NUM_COMMITS) {
      int numberOfCommits = getCommitsSinceLastCleaning();
      int maxInlineCommitsForNextClean = config.getCleaningMaxCommits();// 1
      return numberOfCommits >= maxInlineCommitsForNextClean;
    } else {
      throw new HoodieException("Unsupported cleaning trigger strategy: " + config.getCleaningTriggerStrategy());
    }
  }

  /**
   * Generates List of files to be cleaned.
   *
   * @param context HoodieEngineContext
   * @return Cleaner Plan
   */
  HoodieCleanerPlan requestClean(HoodieEngineContext context) {
    try {
      CleanPlanner<T, I, K, O> planner = new CleanPlanner<>(context, table, config);
      Option<HoodieInstant> earliestInstant = planner.getEarliestCommitToRetain();//根据保存策略（commit数或者时间以及有没有没完成的commit来确认具体清理截止位置）
      context.setJobStatus(this.getClass().getSimpleName(), "Obtaining list of partitions to be cleaned: " + config.getTableName());//flink侧目前可能啥都没做
      List<String> partitionsToClean = planner.getPartitionPathsToClean(earliestInstant); //去重后的所有相关分区 这里首先引入了第一次范围

      if (partitionsToClean.isEmpty()) {
        LOG.info("Nothing to clean here. It is already clean");
        return HoodieCleanerPlan.newBuilder().setPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS.name()).build();
      }
      LOG.info("Total Partitions to clean : " + partitionsToClean.size() + ", with policy " + config.getCleanerPolicy());
      int cleanerParallelism = Math.min(partitionsToClean.size(), config.getCleanerParallelism());
      LOG.info("Using cleanerParallelism: " + cleanerParallelism);

      context.setJobStatus(this.getClass().getSimpleName(), "Generating list of file slices to be cleaned: " + config.getTableName());

      //重点 除了relpace file groups外基本都已明确
      Map<String, Pair<Boolean, List<CleanFileInfo>>> cleanOpsWithPartitionMeta = context//进一步会确认数据文件版本 ，map<分区,pair<boolean,list<CleanFileInfo>>> 即map<分区名，Pair<分区是否完全删除，具体待clean文件信息>>
          .map(partitionsToClean, partitionPathToClean -> Pair.of(partitionPathToClean, planner.getDeletePaths(partitionPathToClean)), cleanerParallelism)//难点 生成压缩前计划最后的疑问
          .stream()
          .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

      Map<String, List<HoodieCleanFileInfo>> cleanOps = cleanOpsWithPartitionMeta.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey,//e就是map的一个元素 e.getkey是分区 values 2次后就是CleanFileInfo，在被转为HoodieCleanFileInfo
              e -> CleanerUtils.convertToHoodieCleanFileInfoList(e.getValue().getValue())));

      //根据上方的结果 找出需要删除整个分区的分区 （而不是只clean分区下部分fileslice）
      List<String> partitionsToDelete = cleanOpsWithPartitionMeta.entrySet().stream().filter(entry -> entry.getValue().getKey()).map(Map.Entry::getKey)//filter只要true的外层map的key也就是分区list
          .collect(Collectors.toList());

      //filePathsToBeDeletedPerPartition
      return new HoodieCleanerPlan(earliestInstant//earliestInstant要不为empty 要不就一个instant 不是多个instant
          .map(x -> new HoodieActionInstant(x.getTimestamp(), x.getAction(), x.getState().name())).orElse(null),
          planner.getLastCompletedCommitTimestamp(),//最后一个完成commit或者deltacommit
          config.getCleanerPolicy().name(), CollectionUtils.createImmutableMap(),
          CleanPlanner.LATEST_CLEAN_PLAN_VERSION, cleanOps, partitionsToDelete);//最后两个是纯粹要清理的文件Map<String, List<HoodieCleanFileInfo>>（filePathsToBeDeletedPerPartition） 和 要清理的分区 List<String>
    } catch (IOException e) {
      throw new HoodieIOException("Failed to schedule clean operation", e);
    }
  }

  /**
   * Creates a Cleaner plan if there are files to be cleaned and stores them in instant file.
   * Cleaner Plan contains absolute file paths.
   *
   * @param startCleanTime Cleaner Instant Time
   * @return Cleaner Plan if generated
   */
  protected Option<HoodieCleanerPlan> requestClean(String startCleanTime) {
    final HoodieCleanerPlan cleanerPlan = requestClean(context);//生成clean 计划
    if ((cleanerPlan.getFilePathsToBeDeletedPerPartition() != null)
        && !cleanerPlan.getFilePathsToBeDeletedPerPartition().isEmpty()
        && cleanerPlan.getFilePathsToBeDeletedPerPartition().values().stream().mapToInt(List::size).sum() > 0) {
      // Only create cleaner plan which does some work
      final HoodieInstant cleanInstant = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, startCleanTime);//生成承载压缩计划的instant文件
      // Save to both aux and timeline folder
      try {
        table.getActiveTimeline().saveToCleanRequested(cleanInstant, TimelineMetadataUtils.serializeCleanerPlan(cleanerPlan));//这一步把压缩计划写入对应clean的元数据文件
        LOG.info("Requesting Cleaning with instant time " + cleanInstant);
      } catch (IOException e) {
        LOG.error("Got exception when saving cleaner requested file", e);
        throw new HoodieIOException(e.getMessage(), e);
      }
      return Option.of(cleanerPlan);
    }
    return Option.empty();
  }

  @Override
  public Option<HoodieCleanerPlan> execute() {
    if (!needsCleaning(config.getCleaningTriggerStrategy())) {//num_commit
      return Option.empty();
    }
    // Plan a new clean action
    return requestClean(instantTime); //当前带clean commit数大于等于1时
  }

}
