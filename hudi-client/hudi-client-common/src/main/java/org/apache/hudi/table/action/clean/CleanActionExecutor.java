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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.CleanFileInfo;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.io.FileBasedInternalSchemaStorageManager;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CleanActionExecutor<T extends HoodieRecordPayload, I, K, O> extends BaseActionExecutor<T, I, K, O, HoodieCleanMetadata> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(CleanActionExecutor.class);
  private final TransactionManager txnManager;
  private final boolean skipLocking;

  public CleanActionExecutor(HoodieEngineContext context, HoodieWriteConfig config, HoodieTable<T, I, K, O> table, String instantTime) {
    this(context, config, table, instantTime, false);
  }

  public CleanActionExecutor(HoodieEngineContext context, HoodieWriteConfig config, HoodieTable<T, I, K, O> table, String instantTime, boolean skipLocking) {
    super(context, config, table, instantTime);
    this.txnManager = new TransactionManager(config, table.getMetaClient().getFs());
    this.skipLocking = skipLocking;
  }

  private static Boolean deleteFileAndGetResult(FileSystem fs, String deletePathStr) throws IOException {
    Path deletePath = new Path(deletePathStr);
    LOG.debug("Working on delete path :" + deletePath);
    try {
      boolean isDirectory = fs.isDirectory(deletePath);
      boolean deleteResult = fs.delete(deletePath, isDirectory);
      if (deleteResult) {
        LOG.debug("Cleaned file at path :" + deletePath);
      }
      return deleteResult;
    } catch (FileNotFoundException fio) {
      // With cleanPlan being used for retried cleaning operations, its possible to clean a file twice
      return false;
    }
  }

  private static Stream<Pair<String, PartitionCleanStat>> deleteFilesFunc(Iterator<Pair<String, CleanFileInfo>> cleanFileInfo, HoodieTable table) {
    Map<String, PartitionCleanStat> partitionCleanStatMap = new HashMap<>();//PartitionCleanStat这个对象中有一个保存单个文件清理结果状态的list对象
    FileSystem fs = table.getMetaClient().getFs();

    cleanFileInfo.forEachRemaining(partitionDelFileTuple -> {
      String partitionPath = partitionDelFileTuple.getLeft();
      Path deletePath = new Path(partitionDelFileTuple.getRight().getFilePath());
      String deletePathStr = deletePath.toString();
      Boolean deletedFileResult = null;
      try {
        deletedFileResult = deleteFileAndGetResult(fs, deletePathStr);//删除其中一个pair的真实文件

      } catch (IOException e) {
        LOG.error("Delete file failed: " + deletePathStr);
      }
      final PartitionCleanStat partitionCleanStat =
          partitionCleanStatMap.computeIfAbsent(partitionPath, k -> new PartitionCleanStat(partitionPath));
      boolean isBootstrapBasePathFile = partitionDelFileTuple.getRight().isBootstrapBaseFile();

      //实际已经更新了map的元素 即add后的value还是更新到了map里
      if (isBootstrapBasePathFile) {
        // For Bootstrap Base file deletions, store the full file path.
        partitionCleanStat.addDeleteFilePatterns(deletePath.toString(), true);
        partitionCleanStat.addDeletedFileResult(deletePath.toString(), deletedFileResult, true);
      } else {
        partitionCleanStat.addDeleteFilePatterns(deletePath.getName(), false);
        partitionCleanStat.addDeletedFileResult(deletePath.getName(), deletedFileResult, false);
      }
    });
    return partitionCleanStatMap.entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue()));
  }

  /**
   * Performs cleaning of partition paths according to cleaning policy and returns the number of files cleaned. Handles
   * skews in partitions to clean by making files to clean as the unit of task distribution.
   *
   * @throws IllegalArgumentException if unknown cleaning policy is provided
   */
  List<HoodieCleanStat> clean(HoodieEngineContext context, HoodieCleanerPlan cleanerPlan) {
    int cleanerParallelism = Math.min(//初步判断这里是个所有分区的所有总待清理数
        (int) (cleanerPlan.getFilePathsToBeDeletedPerPartition().values().stream().mapToInt(List::size).count()),
        config.getCleanerParallelism());//并行度取clean运行配置和所有list所有元素合 最小值
    LOG.info("Using cleanerParallelism: " + cleanerParallelism);

    context.setJobStatus(this.getClass().getSimpleName(), "Perform cleaning of partitions: " + config.getTableName());

    //getFilePathsToBeDeletedPerPartition()返回的是map<partition,list<HoodieCleanFileInfo>>
    Stream<Pair<String, CleanFileInfo>> filesToBeDeletedPerPartition =
        cleanerPlan.getFilePathsToBeDeletedPerPartition().entrySet().stream()
            .flatMap(x -> x.getValue().stream().map(y -> new ImmutablePair<>(x.getKey(),
                new CleanFileInfo(y.getFilePath(), y.getIsBootstrapBaseFile()))));

    Stream<ImmutablePair<String, PartitionCleanStat>> partitionCleanStats =
        context.mapPartitionsToPairAndReduceByKey(filesToBeDeletedPerPartition,
            iterator -> deleteFilesFunc(iterator, table), PartitionCleanStat::merge, cleanerParallelism);//这里clean执行

    Map<String, PartitionCleanStat> partitionCleanStatsMap = partitionCleanStats
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    //删除那些需要整个删除分区路径的
    List<String> partitionsToBeDeleted = cleanerPlan.getPartitionsToBeDeleted() != null ? cleanerPlan.getPartitionsToBeDeleted() : new ArrayList<>();
    partitionsToBeDeleted.forEach(entry -> {
      try {
        deleteFileAndGetResult(table.getMetaClient().getFs(), table.getMetaClient().getBasePath() + "/" + entry);
      } catch (IOException e) {
        LOG.warn("Partition deletion failed " + entry);
      }
    });

    // Return PartitionCleanStat for each partition passed.
    return cleanerPlan.getFilePathsToBeDeletedPerPartition().keySet().stream().map(partitionPath -> {
      PartitionCleanStat partitionCleanStat = partitionCleanStatsMap.containsKey(partitionPath)
          ? partitionCleanStatsMap.get(partitionPath)
          : new PartitionCleanStat(partitionPath);
      HoodieActionInstant actionInstant = cleanerPlan.getEarliestInstantToRetain();
      return HoodieCleanStat.newBuilder().withPolicy(config.getCleanerPolicy()).withPartitionPath(partitionPath)
          .withEarliestCommitRetained(Option.ofNullable(
              actionInstant != null
                  ? new HoodieInstant(HoodieInstant.State.valueOf(actionInstant.getState()),
                  actionInstant.getAction(), actionInstant.getTimestamp())
                  : null))
          .withLastCompletedCommitTimestamp(cleanerPlan.getLastCompletedCommitTimestamp())
          .withDeletePathPattern(partitionCleanStat.deletePathPatterns())
          .withSuccessfulDeletes(partitionCleanStat.successDeleteFiles())
          .withFailedDeletes(partitionCleanStat.failedDeleteFiles())
          .withDeleteBootstrapBasePathPatterns(partitionCleanStat.getDeleteBootstrapBasePathPatterns())
          .withSuccessfulDeleteBootstrapBaseFiles(partitionCleanStat.getSuccessfulDeleteBootstrapBaseFiles())
          .withFailedDeleteBootstrapBaseFiles(partitionCleanStat.getFailedDeleteBootstrapBaseFiles())
          .isPartitionDeleted(partitionsToBeDeleted.contains(partitionPath))
          .build();
    }).collect(Collectors.toList());
  }


  /**
   * Executes the Cleaner plan stored in the instant metadata.
   */
  HoodieCleanMetadata runPendingClean(HoodieTable<T, I, K, O> table, HoodieInstant cleanInstant) {
    try {
      HoodieCleanerPlan cleanerPlan = CleanerUtils.getCleanerPlan(table.getMetaClient(), cleanInstant);
      return runClean(table, cleanInstant, cleanerPlan);//
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }

  private HoodieCleanMetadata runClean(HoodieTable<T, I, K, O> table, HoodieInstant cleanInstant, HoodieCleanerPlan cleanerPlan) {
    ValidationUtils.checkArgument(cleanInstant.getState().equals(HoodieInstant.State.REQUESTED)
        || cleanInstant.getState().equals(HoodieInstant.State.INFLIGHT));//

    HoodieInstant inflightInstant = null;
    try {
      final HoodieTimer timer = new HoodieTimer();
      timer.startTimer();
      if (cleanInstant.isRequested()) {//request的clean变为inflight
        inflightInstant = table.getActiveTimeline().transitionCleanRequestedToInflight(cleanInstant,
            TimelineMetadataUtils.serializeCleanerPlan(cleanerPlan));
      } else {
        inflightInstant = cleanInstant;
      }

      List<HoodieCleanStat> cleanStats = clean(context, cleanerPlan);//待确认这里的clean和外层的区别，需要知道filePathsToBeDeletedPerPartition当初怎么来的//再生成clean计划时已明确
      if (cleanStats.isEmpty()) {
        return HoodieCleanMetadata.newBuilder().build();
      }

      table.getMetaClient().reloadActiveTimeline();
      HoodieCleanMetadata metadata = CleanerUtils.convertCleanMetadata(//？
          inflightInstant.getTimestamp(),
          Option.of(timer.endTimer()),
          cleanStats
      );
      if (!skipLocking) {
        this.txnManager.beginTransaction(Option.of(inflightInstant), Option.empty());
      }
      writeTableMetadata(metadata, inflightInstant.getTimestamp());//metadata表方面更新 暂时不深入
      table.getActiveTimeline().transitionCleanInflightToComplete(inflightInstant,
          TimelineMetadataUtils.serializeCleanMetadata(metadata));//inflight转为complete
      LOG.info("Marked clean started on " + inflightInstant.getTimestamp() + " as complete");
      return metadata;
    } catch (IOException e) {
      throw new HoodieIOException("Failed to clean up after commit", e);
    } finally {
      if (!skipLocking) {
        this.txnManager.endTransaction(Option.of(inflightInstant));
      }
    }
  }

  @Override
  public HoodieCleanMetadata execute() {
    List<HoodieCleanMetadata> cleanMetadataList = new ArrayList<>();
    // If there are inflight(failed) or previously requested clean operation, first perform them
    List<HoodieInstant> pendingCleanInstants = table.getCleanTimeline()//获取所有没完成的clean instant,具体方法里给instant 获得元数据文件然后读出byte[]
        .filterInflightsAndRequested().getInstants().collect(Collectors.toList());
    if (pendingCleanInstants.size() > 0) {//clean timeline里确认找到所有未完成的clean 动作
      // try to clean old history schema.
      try {
        FileBasedInternalSchemaStorageManager fss = new FileBasedInternalSchemaStorageManager(table.getMetaClient());
        fss.cleanOldFiles(pendingCleanInstants.stream().map(is -> is.getTimestamp()).collect(Collectors.toList()));//删除.hoodie/.schema下文件（oldfile）但是该目录下长期为空 目前还没有用到clean计划
      } catch (Exception e) {
        // we should not affect original clean logic. Swallow exception and log warn.
        LOG.warn("failed to clean old history schema");
      }
      pendingCleanInstants.forEach(hoodieInstant -> {
        if (table.getCleanTimeline().isEmpty(hoodieInstant)) {
          table.getActiveTimeline().deleteEmptyInstantIfExists(hoodieInstant);//清理clean.request
        } else {
          LOG.info("Finishing previously unfinished cleaner instant=" + hoodieInstant);
          try {
            cleanMetadataList.add(runPendingClean(table, hoodieInstant));//这里清理对应元数据 并把结果写入HoodieCleanMetadata 20230716
          } catch (Exception e) {
            LOG.warn("Failed to perform previous clean operation, instant: " + hoodieInstant, e);
          }
        }
        table.getMetaClient().reloadActiveTimeline();//刷新instant
        if (config.isMetadataTableEnabled()) {
          table.getHoodieView().sync();
        }
      });
    }

    // return the last clean metadata for now
    // TODO (NA) : Clean only the earliest pending clean just like how we do for other table services
    // This requires the CleanActionExecutor to be refactored as BaseCommitActionExecutor
    return cleanMetadataList.size() > 0 ? cleanMetadataList.get(cleanMetadataList.size() - 1) : null;
  }
}
