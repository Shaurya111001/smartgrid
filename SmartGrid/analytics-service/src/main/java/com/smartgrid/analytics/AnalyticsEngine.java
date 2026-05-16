package com.smartgrid.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.to_timestamp;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class AnalyticsEngine {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
            .appName("SmartGridAnalytics")
            .master(System.getenv().getOrDefault("SPARK_MASTER", "local[*]"))
            .getOrCreate();

        // Schema matching MeasurementEvent emitted by Measurement Service
        StructType schema = new StructType()
                .add("eventType", DataTypes.StringType)
                .add("nodeId", DataTypes.StringType)
                .add("districtId", DataTypes.StringType)
                .add("type", DataTypes.StringType)
                .add("energyValue", DataTypes.DoubleType)
                .add("timestamp", DataTypes.StringType);

        Dataset<Row> raw = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092"))
                .option("subscribe", System.getenv().getOrDefault("INPUT_TOPIC", "measurement-events"))
                .load();

        Dataset<Row> parsed = raw
                .selectExpr("CAST(value AS STRING) as json")
                .select(from_json(col("json"), schema).as("data"))
                .select("data.*")
                // convert ISO-8601 string timestamp to actual timestamp type for event-time
                .withColumn("ts", to_timestamp(col("timestamp")));

        // adjusted value: producers positive, consumers negative
        Dataset<Row> adjusted = parsed.withColumn("adjusted_value",
                expr("CASE WHEN type = 'producer' THEN energyValue WHEN type = 'consumer' THEN -energyValue ELSE 0 END"))
                .withWatermark("ts", "5 minutes");

        // Windowed aggregate: net district balance per window
        Dataset<Row> windowed = adjusted
                .groupBy(
                        org.apache.spark.sql.functions.window(col("ts"), System.getenv().getOrDefault("WINDOW_DURATION", "10 minutes"), System.getenv().getOrDefault("WINDOW_SLIDE", "5 minutes")),
                        col("districtId")
                )
                .agg(org.apache.spark.sql.functions.sum("adjusted_value").as("net_district_balance"));

        StreamingQuery q1 = windowed
                .selectExpr("CAST(districtId AS STRING) AS key", "to_json(struct(*)) AS value")
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092"))
                .option("topic", System.getenv().getOrDefault("OUTPUT_WINDOW_TOPIC", "district-window-stats"))
                .option("checkpointLocation", "analytics_checkpoint/windowed")
                .start();

        // State of Charge per district (simple cumulative sum since job start)
        Dataset<Row> soc = adjusted
                .groupBy(col("districtId"))
                .agg(org.apache.spark.sql.functions.sum("adjusted_value").as("current_SOC"));

        StreamingQuery q2 = soc
                .selectExpr("CAST(districtId AS STRING) AS key", "to_json(struct(*)) AS value")
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092"))
                .option("topic", System.getenv().getOrDefault("OUTPUT_SOC_TOPIC", "district-soc-state"))
                .option("checkpointLocation", "analytics_checkpoint/soc")
                .outputMode("complete")
                .start();

        q1.awaitTermination();
        q2.awaitTermination();
    }
}
