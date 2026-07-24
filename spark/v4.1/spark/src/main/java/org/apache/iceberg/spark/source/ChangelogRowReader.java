/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.source;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.AddedRowsScanTask;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeletedDataFileScanTask;
import org.apache.iceberg.DeletedRowsScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.UTF8String;

class ChangelogRowReader extends BaseRowReader<ChangelogScanTask>
    implements PartitionReader<InternalRow> {

  ChangelogRowReader(SparkInputPartition partition) {
    this(
        partition.table(),
        partition.io(),
        partition.taskGroup(),
        partition.projection(),
        partition.isCaseSensitive(),
        partition.cacheDeleteFilesOnExecutors());
  }

  ChangelogRowReader(
      Table table,
      FileIO fileIO,
      ScanTaskGroup<ChangelogScanTask> taskGroup,
      Schema expectedSchema,
      boolean caseSensitive,
      boolean cacheDeleteFilesOnExecutors) {
    super(table, fileIO, taskGroup, expectedSchema, caseSensitive, cacheDeleteFilesOnExecutors);
  }

  @Override
  protected CloseableIterator<InternalRow> open(ChangelogScanTask task) {
    return openChangelogScanTask(task).iterator();
  }

  private CloseableIterable<InternalRow> openChangelogScanTask(ChangelogScanTask task) {
    if (task instanceof AddedRowsScanTask) {
      return openAddedRowsScanTask((AddedRowsScanTask) task);

    } else if (task instanceof DeletedRowsScanTask) {
      throw new UnsupportedOperationException("Deleted rows scan task is not supported yet");

    } else if (task instanceof DeletedDataFileScanTask) {
      return openDeletedDataFileScanTask((DeletedDataFileScanTask) task);

    } else {
      throw new IllegalArgumentException(
          "Unsupported changelog scan task type: " + task.getClass().getName());
    }
  }

  CloseableIterable<InternalRow> openAddedRowsScanTask(AddedRowsScanTask task) {
    String filePath = task.file().location();
    SparkDeleteFilter deletes = new SparkDeleteFilter(filePath, task.deletes(), counter(), true);
    return deletes.filter(rows(task, task, deletes.requiredSchema()));
  }

  private CloseableIterable<InternalRow> openDeletedDataFileScanTask(DeletedDataFileScanTask task) {
    String filePath = task.file().location();
    SparkDeleteFilter deletes =
        new SparkDeleteFilter(filePath, task.existingDeletes(), counter(), true);
    return deletes.filter(rows(task, task, deletes.requiredSchema()));
  }

  private CloseableIterable<InternalRow> rows(
      ContentScanTask<DataFile> task, ChangelogScanTask changeTask, Schema readSchema) {
    Map<Integer, ?> idToConstant = changelogConstantsMap(task, changeTask, readSchema);

    String filePath = task.file().location();

    // update the current file for Spark's filename() function
    InputFileBlockHolder.set(filePath, task.start(), task.length());

    InputFile location = getInputFile(filePath);
    Preconditions.checkNotNull(location, "Could not find InputFile");
    return newIterable(
        location,
        task.file().format(),
        task.start(),
        task.length(),
        task.residual(),
        readSchema,
        idToConstant);
  }

  /**
   * Extends the regular task constants with the changelog metadata values. The changelog columns
   * are constant per {@link ChangelogScanTask}, so resolving them like other metadata constants
   * places them at their projected positions for any column order, including projections where
   * Spark appends metadata columns such as {@code _row_id} after the changelog columns.
   */
  private Map<Integer, ?> changelogConstantsMap(
      ContentScanTask<DataFile> task, ChangelogScanTask changeTask, Schema readSchema) {
    Map<Integer, Object> idToConstant = Maps.newHashMap();
    idToConstant.putAll(constantsMap(task, readSchema));
    idToConstant.put(
        MetadataColumns.CHANGE_TYPE.fieldId(),
        UTF8String.fromString(changeTask.operation().name()));
    idToConstant.put(MetadataColumns.CHANGE_ORDINAL.fieldId(), changeTask.changeOrdinal());
    idToConstant.put(MetadataColumns.COMMIT_SNAPSHOT_ID.fieldId(), changeTask.commitSnapshotId());
    return idToConstant;
  }

  @Override
  protected Stream<ContentFile<?>> referencedFiles(ChangelogScanTask task) {
    if (task instanceof AddedRowsScanTask) {
      return addedRowsScanTaskFiles((AddedRowsScanTask) task);

    } else if (task instanceof DeletedRowsScanTask) {
      throw new UnsupportedOperationException("Deleted rows scan task is not supported yet");

    } else if (task instanceof DeletedDataFileScanTask) {
      return deletedDataFileScanTaskFiles((DeletedDataFileScanTask) task);

    } else {
      throw new IllegalArgumentException(
          "Unsupported changelog scan task type: " + task.getClass().getName());
    }
  }

  private static Stream<ContentFile<?>> deletedDataFileScanTaskFiles(DeletedDataFileScanTask task) {
    DataFile file = task.file();
    List<DeleteFile> existingDeletes = task.existingDeletes();
    return Stream.concat(Stream.of(file), existingDeletes.stream());
  }

  private static Stream<ContentFile<?>> addedRowsScanTaskFiles(AddedRowsScanTask task) {
    DataFile file = task.file();
    List<DeleteFile> deletes = task.deletes();
    return Stream.concat(Stream.of(file), deletes.stream());
  }
}
