package com.smartgrid.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.to_timestamp;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryStartedEvent;
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryProgressEvent;
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryTerminatedEvent;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnalyticsEngine {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsEngine.class);

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
            .appName("SmartGridAnalytics")
            .master(System.getenv().getOrDefault("SPARK_MASTER", "local[*]"))
            .getOrCreate();

        spark.streams().addListener(new StreamingQueryListener() {
            @Override
            public void onQueryStarted(QueryStartedEvent event) {
                log.info("Query started: id={} name={}", event.id(), event.name());
            }

            @Override
            public void onQueryProgress(QueryProgressEvent event) {
                var p = event.progress();
                log.info("Query progress: name={} batchId={} inputRows={} rowsPerSecond={} durationMs={}",
                        p.name(), p.batchId(), p.numInputRows(), p.inputRowsPerSecond(), p.durationMs());
            }

            @Override
            public void onQueryTerminated(QueryTerminatedEvent event) {
                log.warn("Query terminated: id={} exception={}", event.id(), event.exception());
            }
        });

        StructType schema = new StructType()
                .add("eventType", DataTypes.StringType)
                .add("nodeId", DataTypes.StringType)
                .add("districtId", DataTypes.StringType)
                .add("type", DataTypes.StringType)
                .add("energyValue", DataTypes.DoubleType)
                .add("timestamp", DataTypes.StringType);

        Dataset<Row> raw = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
                .option("subscribe", System.getenv().getOrDefault("INPUT_TOPIC", "measurement-events"))
                .load();

        Dataset<Row> parsed = raw
                .selectExpr("CAST(value AS STRING) as json")
                .select(from_json(col("json"), schema).as("data"))
                .select("data.*")
                .withColumn("ts", to_timestamp(col("timestamp")));

        Dataset<Row> adjusted = parsed
                .withColumn("adjusted_value",
                        expr("CASE WHEN type = 'producer' THEN energyValue WHEN type = 'consumer' THEN -energyValue ELSE 0 END"))
                .withColumn("soc_delta",
                        expr("CASE WHEN type = 'accumulator' THEN energyValue ELSE 0 END"))
                .withWatermark("ts", "5 minutes");

        Dataset<Row> windowed = adjusted
                .groupBy(
                        org.apache.spark.sql.functions.window(col("ts"), System.getenv().getOrDefault("WINDOW_DURATION", "10 minutes"), System.getenv().getOrDefault("WINDOW_SLIDE", "5 minutes")),
                        col("districtId")
                )
                .agg(org.apache.spark.sql.functions.avg("adjusted_value").as("avg_district_balance"));

        StreamingQuery q1 = windowed
                .selectExpr("CAST(districtId AS STRING) AS key", "to_json(struct(*)) AS value")
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
                .option("topic", System.getenv().getOrDefault("OUTPUT_WINDOW_TOPIC", "district-window-stats"))
                .option("checkpointLocation", System.getenv().getOrDefault("CHECKPOINT_DIR", "analytics_checkpoint") + "/windowed")
                .start();

        Dataset<Row> soc = adjusted
                .groupBy(col("districtId"))
                .agg(org.apache.spark.sql.functions.sum("soc_delta").as("current_SOC"));

        StreamingQuery q2 = soc
                .selectExpr("CAST(districtId AS STRING) AS key", "to_json(struct(*)) AS value")
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
                .option("topic", System.getenv().getOrDefault("OUTPUT_SOC_TOPIC", "district-soc-state"))
                .option("checkpointLocation", System.getenv().getOrDefault("CHECKPOINT_DIR", "analytics_checkpoint") + "/soc")
                .outputMode("complete")
                .start();

        q1.awaitTermination();
        q2.awaitTermination();
    }
}
