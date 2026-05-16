package com.example;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.window;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;



public class AnalyticsEngine {
        public static void main(String[] args) throws Exception {

            SparkSession spark = SparkSession.builder()
                .appName("SmartGridAnalytics")
                .master("local[*]")
                .config("spark.driver.extraJavaOptions",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED")
                .getOrCreate();

            // 1. Define Schema to match the JSON
            StructType schema = new StructType()
                    .add("nodeId", DataTypes.StringType)
                    .add("type", DataTypes.StringType)
                    .add("districtId", DataTypes.StringType)
                    .add("value", DataTypes.DoubleType)
                    .add("timestamp", DataTypes.TimestampType);

            // 2. Read from Kafka
            Dataset<Row> rawStream = spark.readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("subscribe", "energy-measurement")
                    .load();

            // 3. Parse JSON and apply Watermark (for out-of-order data)
            Dataset<Row> cleanData = rawStream
                    .selectExpr("CAST(value AS STRING) as json")
                    .select(from_json(col("json"), schema).as("data"))
                    .select("data.*")
                    .withWatermark("timestamp", "10 minutes");

            Dataset<Row> processedData = cleanData.withColumn("adjusted_value", 
                 expr("CASE WHEN type = 'producer' THEN value WHEN type = 'consumer' THEN -value ELSE 0 END")
                    );

            // 4. Windowed Aggregation (e.g., 5 min windows, sliding every 1 min)
            Dataset<Row> windowedStats = processedData
                .groupBy(
                    window(col("timestamp"), "10 minutes", "5 minutes"), 
                    col("districtId")
                )
                .agg(sum("adjusted_value").as("net_district_balance"));
            // 5. Output to Console (For testing)
            StreamingQuery query = windowedStats
                .selectExpr("CAST(districtId AS STRING) AS key", "to_json(struct(*)) AS value")
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("topic", "district-window-stats") // Kafka topic for graphs
                .option("checkpointLocation", "checkpoint_dir")
                .start();

            Dataset<Row> stateOfCharge = processedData
                .groupBy(col("districtId"))
                .agg(sum("adjusted_value").as("current_SOC"));

            StreamingQuery query2 = stateOfCharge
                .selectExpr("CAST(districtId AS STRING) AS key", "to_json(struct(*)) AS value")
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("topic", "district-soc-state") // Kafka topic for status dials
                .option("checkpointLocation", "checkpoint_dir_soc")
                .outputMode("complete")
                .start();

        query.awaitTermination();
        query2.awaitTermination();
    }
}
