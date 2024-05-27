package com.example;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.glue.GlueContext;
import com.amazonaws.services.glue.util.JsonOptions;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.model.Record;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class GlueJobKCL {
    public static void main(String[] args) {
        // Initialize Spark session and Glue context
        SparkSession spark = SparkSession.builder().appName("GlueJobKCL").getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        GlueContext glueContext = new GlueContext(jsc);

        // Kinesis stream configuration
        String kinesisStreamName = "your-kinesis-stream-name";
        String regionName = "your-region";
        String applicationName = "your-application-name";
        
        // DynamoDB table configuration
        String dynamodbTableName = "your-dynamodb-table-name";
        String dynamodbRegionName = "your-region";

        // Create Kinesis client configuration
        KinesisClientLibConfiguration kclConfig = new KinesisClientLibConfiguration(
                applicationName,
                kinesisStreamName,
                null,
                UUID.randomUUID().toString()
        ).withRegionName(regionName)
         .withInitialPositionInStream(InitialPositionInStream.LATEST);

        // Create and start Kinesis worker
        Worker worker = new Worker.Builder()
                .recordProcessorFactory(new RecordProcessorFactory(spark, dynamodbTableName, dynamodbRegionName))
                .config(kclConfig)
                .build();

        worker.run();
    }

    private static class RecordProcessorFactory implements IRecordProcessorFactory {
        private SparkSession spark;
        private String dynamodbTableName;
        private String dynamodbRegionName;

        public RecordProcessorFactory(SparkSession spark, String dynamodbTableName, String dynamodbRegionName) {
            this.spark = spark;
            this.dynamodbTableName = dynamodbTableName;
            this.dynamodbRegionName = dynamodbRegionName;
        }

        @Override
        public IRecordProcessor createProcessor() {
            return new RecordProcessor(spark, dynamodbTableName, dynamodbRegionName);
        }
    }

    private static class RecordProcessor implements IRecordProcessor {
        private SparkSession spark;
        private String dynamodbTableName;
        private String dynamodbRegionName;

        public RecordProcessor(SparkSession spark, String dynamodbTableName, String dynamodbRegionName) {
            this.spark = spark;
            this.dynamodbTableName = dynamodbTableName;
            this.dynamodbRegionName = dynamodbRegionName;
        }

        @Override
        public void initialize(String shardId) {
            // Initialization logic if needed
        }

        @Override
        public void processRecords(ProcessRecordsInput input) {
            List<Record> records = input.getRecords();
            List<String> jsonRecords = records.stream()
                    .map(record -> new String(record.getData().array(), StandardCharsets.UTF_8))
                    .collect(Collectors.toList());

            Dataset<Row> dataFrame = spark.read().json(jsonRecords);
            
            // Example transformation
            Dataset<Row> transformedDataFrame = dataFrame.filter(dataFrame.col("column").equalTo("value"));

            // Write to DynamoDB
            AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                    .withRegion(dynamodbRegionName)
                    .build();
            DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);
            Table table = dynamoDB.getTable(dynamodbTableName);

            transformedDataFrame.foreach(row -> {
                Map<String, Object> map = row.getValuesMap(row.schema().fieldNames());
                Item item = new Item();
                map.forEach(item::with);
                PutItemOutcome outcome = table.putItem(item);
            });
        }

        @Override
        public void shutdown(ShutdownReason reason) {
            // Shutdown logic if needed
        }
    }
}
