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

import static org.apache.spark.sql.functions.expr;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import java.util.Locale;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * A benchmark that compares the identifier-based and row-lineage-based strategies of the {@code
 * create_changelog_view} procedure.
 *
 * <p>Two tables with identical data and identical copy-on-write MERGE history are created, one on
 * format version 2 (identifier-based changelog computation) and one on format version 3
 * (row-lineage-based changelog computation). Each MERGE pass rewrites every data file, so most
 * changelog rows are carry-overs, which is the dominant cost the two strategies handle differently:
 * the identifier-based strategy shuffles on every data column and compares entire rows, while the
 * lineage-based strategy shuffles on {@code (_row_id, _change_ordinal)} and compares {@code
 * _last_updated_sequence_number}.
 *
 * <p>To run this benchmark for spark-4.1: <code>
 *   ./gradlew -DsparkVersions=4.1 :iceberg-spark:iceberg-spark-extensions-4.1_2.13:jmh
 *       -PjmhIncludeRegex=CreateChangelogViewBenchmark
 *       -PjmhOutputPath=benchmark/iceberg-create-changelog-view-benchmark.txt
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
public class CreateChangelogViewBenchmark {

  private static final String TABLE_V2 = "changelog_v2";
  private static final String TABLE_V3 = "changelog_v3";
  private static final String VIEW_NAME = "clv";
  private static final int NUM_FILES = 5;
  private static final int NUM_ROWS_PER_FILE = 400_000;
  private static final int NUM_MERGES = 2;
  private static final int NUM_INSERTED_ROWS_PER_MERGE = 20_000;

  private final Configuration hadoopConf = new Configuration();
  private SparkSession spark;

  @Setup
  public void setupBenchmark() throws NoSuchTableException {
    setupSpark();
    initTable(TABLE_V2, 2);
    initTable(TABLE_V3, 3);
    appendData(TABLE_V2);
    appendData(TABLE_V3);
    for (int mergeNum = 0; mergeNum < NUM_MERGES; mergeNum++) {
      runMerge(TABLE_V2, mergeNum);
      runMerge(TABLE_V3, mergeNum);
    }
  }

  @TearDown
  public void tearDownBenchmark() {
    sql("DROP TABLE IF EXISTS %s PURGE", TABLE_V2);
    sql("DROP TABLE IF EXISTS %s PURGE", TABLE_V3);
    spark.stop();
  }

  @Benchmark
  @Threads(1)
  public void removeCarryoversV2() {
    sql(
        "CALL system.create_changelog_view(table => '%s', changelog_view => '%s')",
        TABLE_V2, VIEW_NAME);
    materializeView();
  }

  @Benchmark
  @Threads(1)
  public void removeCarryoversV3Lineage() {
    sql(
        "CALL system.create_changelog_view(table => '%s', changelog_view => '%s')",
        TABLE_V3, VIEW_NAME);
    materializeView();
  }

  @Benchmark
  @Threads(1)
  public void computeUpdatesV2Identifiers() {
    sql(
        "CALL system.create_changelog_view(table => '%s', changelog_view => '%s', "
            + "identifier_columns => array('id'))",
        TABLE_V2, VIEW_NAME);
    materializeView();
  }

  @Benchmark
  @Threads(1)
  public void computeUpdatesV3Identifiers() {
    sql(
        "CALL system.create_changelog_view(table => '%s', changelog_view => '%s', "
            + "identifier_columns => array('id'))",
        TABLE_V3, VIEW_NAME);
    materializeView();
  }

  @Benchmark
  @Threads(1)
  public void computeUpdatesV3Lineage() {
    sql(
        "CALL system.create_changelog_view(table => '%s', changelog_view => '%s', "
            + "compute_updates => true)",
        TABLE_V3, VIEW_NAME);
    materializeView();
  }

  private void materializeView() {
    spark.table(VIEW_NAME).write().format("noop").mode("overwrite").save();
  }

  private void setupSpark() {
    this.spark =
        SparkSession.builder()
            .config(TestBase.DISABLE_UI)
            .config("spark.sql.extensions", IcebergSparkSessionExtensions.class.getName())
            .config("spark.sql.catalog.spark_catalog", SparkSessionCatalog.class.getName())
            .config("spark.sql.catalog.spark_catalog.type", "hadoop")
            .config("spark.sql.catalog.spark_catalog.warehouse", newWarehouseDir())
            .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "false")
            .config(SQLConf.SHUFFLE_PARTITIONS().key(), "4")
            .master("local[4]")
            .getOrCreate();
  }

  private void initTable(String tableName, int formatVersion) {
    sql(
        "CREATE TABLE %s ( "
            + " id LONG, intCol INT, floatCol FLOAT, doubleCol DOUBLE, "
            + " decimalCol DECIMAL(20, 5), dateCol DATE, timestampCol TIMESTAMP, "
            + " stringCol1 STRING, stringCol2 STRING, stringCol3 STRING, stringCol4 STRING)"
            + "USING iceberg "
            + "TBLPROPERTIES ("
            + " '%s' '%s',"
            + " '%s' '%s',"
            + " '%s' '%d',"
            + " '%s' '%d')",
        tableName,
        TableProperties.MERGE_MODE,
        RowLevelOperationMode.COPY_ON_WRITE.modeName(),
        TableProperties.MERGE_DISTRIBUTION_MODE,
        DistributionMode.NONE.modeName(),
        TableProperties.SPLIT_OPEN_FILE_COST,
        Integer.MAX_VALUE,
        TableProperties.FORMAT_VERSION,
        formatVersion);
  }

  private void appendData(String tableName) throws NoSuchTableException {
    for (int fileNum = 0; fileNum < NUM_FILES; fileNum++) {
      long start = (long) fileNum * NUM_ROWS_PER_FILE;
      Dataset<Row> inputDF = generateRows(start, start + NUM_ROWS_PER_FILE, "v0");
      // ensure the schema is precise (including nullability)
      StructType sparkSchema = spark.table(tableName).schema();
      spark.createDataFrame(inputDF.rdd(), sparkSchema).coalesce(1).writeTo(tableName).append();
    }
  }

  private void runMerge(String tableName, int mergeNum) {
    // update 10% of existing rows and insert new rows past the current id range; in
    // copy-on-write mode every data file is rewritten, so most changelog rows are carry-overs
    long tableRows = (long) NUM_FILES * NUM_ROWS_PER_FILE;
    long insertStart = tableRows + ((long) mergeNum * NUM_INSERTED_ROWS_PER_MERGE);
    Dataset<Row> updates =
        generateRows(0, tableRows, "m" + mergeNum)
            .where(String.format(Locale.ROOT, "id %% 10 = %d", mergeNum));
    Dataset<Row> inserts =
        generateRows(insertStart, insertStart + NUM_INSERTED_ROWS_PER_MERGE, "m" + mergeNum);
    updates.union(inserts).createOrReplaceTempView("source");

    sql(
        "MERGE INTO %s t USING source s "
            + "ON t.id = s.id "
            + "WHEN MATCHED THEN "
            + " UPDATE SET * "
            + "WHEN NOT MATCHED THEN "
            + " INSERT *",
        tableName);
  }

  private Dataset<Row> generateRows(long start, long end, String version) {
    // four ~40-char string columns model the shuffle cost of realistic wide rows
    return spark
        .range(start, end, 1)
        .withColumn("intCol", expr("CAST(id AS INT)"))
        .withColumn("floatCol", expr("CAST(id AS FLOAT)"))
        .withColumn("doubleCol", expr("CAST(id AS DOUBLE)"))
        .withColumn("decimalCol", expr("CAST(id AS DECIMAL(20, 5))"))
        .withColumn("dateCol", expr("DATE_ADD(CURRENT_DATE(), CAST(id % 30 AS INT))"))
        .withColumn("timestampCol", expr("TO_TIMESTAMP(dateCol)"))
        .withColumn("stringCol1", versionedString(version, 1))
        .withColumn("stringCol2", versionedString(version, 2))
        .withColumn("stringCol3", versionedString(version, 3))
        .withColumn("stringCol4", versionedString(version, 4));
  }

  private static Column versionedString(String version, int colNum) {
    return expr(
        String.format(
            Locale.ROOT, "CONCAT('%s-', MD5(CAST(id AS STRING)), '-col%d')", version, colNum));
  }

  private String newWarehouseDir() {
    return hadoopConf.get("hadoop.tmp.dir") + UUID.randomUUID();
  }

  @FormatMethod
  private void sql(@FormatString String query, Object... args) {
    spark.sql(String.format(query, args));
  }
}
