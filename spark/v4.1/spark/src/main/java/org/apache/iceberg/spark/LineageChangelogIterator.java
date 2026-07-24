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
package org.apache.iceberg.spark;

import java.util.Iterator;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.StructType;

/**
 * An iterator that uses row lineage to remove carry-over rows and, optionally, to convert
 * delete/insert pairs into update records within a single Spark task. It assumes that rows are
 * partitioned by {@code _row_id} and sorted by ({@code _row_id}, {@code _change_ordinal}, {@code
 * _change_type}).
 *
 * <p>Format version 3 assigns every row a stable {@code _row_id} that writers must preserve across
 * rewrites, and a {@code _last_updated_sequence_number} that changes only when the row is modified.
 * A delete and an insert of the same {@code _row_id} within the same change ordinal therefore
 * represent the same logical row: if the last updated sequence numbers match, the pair is a
 * carry-over produced by a copy-on-write rewrite and is removed; if they differ, the pair is an
 * update. Unlike {@link ComputeUpdateIterator}, no identifier columns are required, and unlike
 * {@link RemoveCarryoverIterator}, carry-over detection compares two longs instead of entire rows.
 */
class LineageChangelogIterator extends ChangelogIterator {

  private final boolean computeUpdates;
  private final int rowIdIndex;
  private final int lastUpdatedIndex;
  private final int changeOrdinalIndex;

  private Row cachedRow = null;

  LineageChangelogIterator(Iterator<Row> rowIterator, StructType rowType, boolean computeUpdates) {
    super(rowIterator, rowType);
    this.computeUpdates = computeUpdates;
    this.rowIdIndex = rowType.fieldIndex(MetadataColumns.ROW_ID.name());
    this.lastUpdatedIndex = rowType.fieldIndex(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name());
    this.changeOrdinalIndex = rowType.fieldIndex(MetadataColumns.CHANGE_ORDINAL.name());
  }

  @Override
  public boolean hasNext() {
    return cachedRow != null || rowIterator().hasNext();
  }

  @Override
  public Row next() {
    Row currentRow = currentRow();

    // deletes sort before inserts, so a pair can only start at a delete row
    if (changeType(currentRow).equals(DELETE) && rowIterator().hasNext()) {
      Row nextRow = rowIterator().next();

      if (samePairedRow(currentRow, nextRow)) {
        Preconditions.checkState(
            changeType(nextRow).equals(INSERT),
            "Cannot pair rows by lineage: %s row and %s row share _row_id %s within change"
                + " ordinal %s",
            DELETE,
            changeType(nextRow),
            rowId(currentRow),
            currentRow.getInt(changeOrdinalIndex));

        if (lastUpdatedSequenceNumber(currentRow) == lastUpdatedSequenceNumber(nextRow)) {
          // the row was rewritten unchanged: a carry-over, drop both sides
          return null;
        } else if (computeUpdates) {
          this.cachedRow = modify(nextRow, changeTypeIndex(), UPDATE_AFTER);
          return modify(currentRow, changeTypeIndex(), UPDATE_BEFORE);
        } else {
          this.cachedRow = nextRow;
          return currentRow;
        }
      } else {
        this.cachedRow = nextRow;
        return currentRow;
      }
    }

    return currentRow;
  }

  private Row currentRow() {
    Row row;
    if (cachedRow != null) {
      row = cachedRow;
      this.cachedRow = null;
    } else {
      row = rowIterator().next();
    }

    return row;
  }

  private boolean samePairedRow(Row currentRow, Row nextRow) {
    return !nextRow.isNullAt(rowIdIndex)
        && rowId(currentRow) == nextRow.getLong(rowIdIndex)
        && currentRow.getInt(changeOrdinalIndex) == nextRow.getInt(changeOrdinalIndex);
  }

  private long rowId(Row row) {
    Preconditions.checkState(
        !row.isNullAt(rowIdIndex),
        "Cannot use row lineage to compute the changelog: null _row_id found. Data files written"
            + " before the table was upgraded to format version 3 do not carry row lineage."
            + " Provide identifier_columns to fall back to identifier-based computation.");
    return row.getLong(rowIdIndex);
  }

  private long lastUpdatedSequenceNumber(Row row) {
    Preconditions.checkState(
        !row.isNullAt(lastUpdatedIndex),
        "Cannot use row lineage to compute the changelog: null _last_updated_sequence_number"
            + " found. Provide identifier_columns to fall back to identifier-based computation.");
    return row.getLong(lastUpdatedIndex);
  }

  private Row modify(Row row, int valueIndex, Object value) {
    if (row instanceof GenericRow) {
      GenericRow genericRow = (GenericRow) row;
      genericRow.values()[valueIndex] = value;
      return genericRow;
    } else {
      Object[] values = new Object[row.size()];
      for (int index = 0; index < row.size(); index++) {
        values[index] = row.get(index);
      }
      values[valueIndex] = value;
      return RowFactory.create(values);
    }
  }
}
