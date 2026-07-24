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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

public class TestLineageChangelogIterator extends SparkTestHelperBase {
  private static final String DELETE = ChangelogOperation.DELETE.name();
  private static final String INSERT = ChangelogOperation.INSERT.name();
  private static final String UPDATE_BEFORE = ChangelogOperation.UPDATE_BEFORE.name();
  private static final String UPDATE_AFTER = ChangelogOperation.UPDATE_AFTER.name();

  private static final StructType SCHEMA =
      new StructType(
          new StructField[] {
            new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("data", DataTypes.StringType, true, Metadata.empty()),
            new StructField(
                MetadataColumns.CHANGE_TYPE.name(), DataTypes.StringType, false, Metadata.empty()),
            new StructField(
                MetadataColumns.CHANGE_ORDINAL.name(),
                DataTypes.IntegerType,
                false,
                Metadata.empty()),
            new StructField(
                MetadataColumns.COMMIT_SNAPSHOT_ID.name(),
                DataTypes.LongType,
                false,
                Metadata.empty()),
            new StructField(
                MetadataColumns.ROW_ID.name(), DataTypes.LongType, true, Metadata.empty()),
            new StructField(
                MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(),
                DataTypes.LongType,
                true,
                Metadata.empty())
          });

  @Test
  public void testCarryoverPairIsRemoved() {
    // rows sorted by (_row_id, _change_ordinal, _change_type); the iterators may modify rows in
    // place, so each run gets a fresh copy
    List<Object[]> expected = ImmutableList.of(new Object[] {2, "b", DELETE, 1, 2L, 101L, 1L});

    validate(
        ChangelogIterator.computeUpdatesWithLineage(carryoverRows().iterator(), SCHEMA), expected);
    validate(
        ChangelogIterator.removeCarryoversWithLineage(carryoverRows().iterator(), SCHEMA),
        expected);
  }

  private List<Row> carryoverRows() {
    return ImmutableList.of(
        row(1, "a", DELETE, 1, 2L, 100L, 1L),
        row(1, "a", INSERT, 1, 2L, 100L, 1L),
        row(2, "b", DELETE, 1, 2L, 101L, 1L));
  }

  @Test
  public void testUpdatePairing() {
    validate(
        ChangelogIterator.computeUpdatesWithLineage(updateRows().iterator(), SCHEMA),
        ImmutableList.of(
            new Object[] {1, "a", UPDATE_BEFORE, 1, 2L, 100L, 1L},
            new Object[] {1, "a-new", UPDATE_AFTER, 1, 2L, 100L, 2L},
            new Object[] {3, "c", INSERT, 1, 2L, 102L, 2L}));

    // without computing updates, the delete/insert pair is kept but carry-over detection applies
    validate(
        ChangelogIterator.removeCarryoversWithLineage(updateRows().iterator(), SCHEMA),
        ImmutableList.of(
            new Object[] {1, "a", DELETE, 1, 2L, 100L, 1L},
            new Object[] {1, "a-new", INSERT, 1, 2L, 100L, 2L},
            new Object[] {3, "c", INSERT, 1, 2L, 102L, 2L}));
  }

  private List<Row> updateRows() {
    return ImmutableList.of(
        row(1, "a", DELETE, 1, 2L, 100L, 1L),
        row(1, "a-new", INSERT, 1, 2L, 100L, 2L),
        row(3, "c", INSERT, 1, 2L, 102L, 2L));
  }

  @Test
  public void testTouchUpdateIsNotCarryover() {
    // identical values but a bumped last updated sequence number: the writer declared a
    // modification, so lineage reports an update instead of removing the pair
    List<Row> rows =
        ImmutableList.of(
            row(1, "a", DELETE, 1, 2L, 100L, 1L), row(1, "a", INSERT, 1, 2L, 100L, 2L));

    validate(
        ChangelogIterator.computeUpdatesWithLineage(rows.iterator(), SCHEMA),
        ImmutableList.of(
            new Object[] {1, "a", UPDATE_BEFORE, 1, 2L, 100L, 1L},
            new Object[] {1, "a", UPDATE_AFTER, 1, 2L, 100L, 2L}));
  }

  @Test
  public void testRowsAcrossOrdinalsAreNotPaired() {
    // the same row id deleted in one commit and inserted in another must not be paired
    List<Row> rows =
        ImmutableList.of(
            row(1, "a", DELETE, 1, 2L, 100L, 1L), row(1, "a", INSERT, 2, 3L, 100L, 1L));

    validate(
        ChangelogIterator.computeUpdatesWithLineage(rows.iterator(), SCHEMA),
        ImmutableList.of(
            new Object[] {1, "a", DELETE, 1, 2L, 100L, 1L},
            new Object[] {1, "a", INSERT, 2, 3L, 100L, 1L}));
  }

  @Test
  public void testUpdatesInMultipleOrdinals() {
    List<Row> rows =
        ImmutableList.of(
            row(1, "a", DELETE, 1, 2L, 100L, 1L),
            row(1, "b", INSERT, 1, 2L, 100L, 2L),
            row(1, "b", DELETE, 2, 3L, 100L, 2L),
            row(1, "c", INSERT, 2, 3L, 100L, 3L));

    validate(
        ChangelogIterator.computeUpdatesWithLineage(rows.iterator(), SCHEMA),
        ImmutableList.of(
            new Object[] {1, "a", UPDATE_BEFORE, 1, 2L, 100L, 1L},
            new Object[] {1, "b", UPDATE_AFTER, 1, 2L, 100L, 2L},
            new Object[] {1, "b", UPDATE_BEFORE, 2, 3L, 100L, 2L},
            new Object[] {1, "c", UPDATE_AFTER, 2, 3L, 100L, 3L}));
  }

  @Test
  public void testLoneRowsPassThrough() {
    List<Row> rows =
        ImmutableList.of(
            row(1, "a", DELETE, 1, 2L, 100L, 1L),
            row(2, "b", INSERT, 1, 2L, 101L, 2L),
            row(3, "c", INSERT, 1, 2L, 102L, 2L));

    List<Object[]> expected =
        ImmutableList.of(
            new Object[] {1, "a", DELETE, 1, 2L, 100L, 1L},
            new Object[] {2, "b", INSERT, 1, 2L, 101L, 2L},
            new Object[] {3, "c", INSERT, 1, 2L, 102L, 2L});

    validate(ChangelogIterator.computeUpdatesWithLineage(rows.iterator(), SCHEMA), expected);
    validate(ChangelogIterator.removeCarryoversWithLineage(rows.iterator(), SCHEMA), expected);
  }

  @Test
  public void testNullRowIdFails() {
    List<Row> rows =
        ImmutableList.of(
            row(1, "a", DELETE, 1, 2L, null, 1L), row(1, "a", INSERT, 1, 2L, 100L, 1L));

    Iterator<Row> iterator = ChangelogIterator.computeUpdatesWithLineage(rows.iterator(), SCHEMA);
    assertThatThrownBy(() -> Lists.newArrayList(iterator))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("null _row_id")
        .hasMessageContaining("identifier_columns");
  }

  @Test
  public void testDuplicateChangeTypeFails() {
    // two deletes of the same row id within one commit cannot come from a compliant writer
    List<Row> rows =
        ImmutableList.of(
            row(1, "a", DELETE, 1, 2L, 100L, 1L), row(1, "a", DELETE, 1, 2L, 100L, 1L));

    Iterator<Row> iterator = ChangelogIterator.computeUpdatesWithLineage(rows.iterator(), SCHEMA);
    assertThatThrownBy(() -> Lists.newArrayList(iterator))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot pair rows by lineage");
  }

  private Row row(
      Integer id,
      String data,
      String changeType,
      int changeOrdinal,
      Long commitSnapshotId,
      Long rowId,
      Long lastUpdated) {
    return new GenericRowWithSchema(
        new Object[] {id, data, changeType, changeOrdinal, commitSnapshotId, rowId, lastUpdated},
        SCHEMA);
  }

  private void validate(Iterator<Row> iterator, List<Object[]> expected) {
    List<Row> result = Lists.newArrayList(iterator);
    assertEquals("Rows should match", expected, rowsToJava(result));
  }
}
