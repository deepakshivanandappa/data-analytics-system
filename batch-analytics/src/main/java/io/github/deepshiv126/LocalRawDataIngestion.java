package io.github.deepshiv126;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class LocalRawDataIngestion {

    public static void main(String[] args) {
        //1: Initialize Spark Session with Iceberg's Hadoop Catalog.
        SparkSession spark = SparkSession.builder()
                .appName("LocalRawDataIngestion")
                .master("local[*]")
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "hadoop")
                .config("spark.sql.catalog.spark_catalog.warehouse",
                        "file://" + new File("batch-analytics/src/main/resources/iceberg-catalog").getAbsolutePath())
                .getOrCreate();

        //2: Set up Iceberg catalog
        Configuration hadoopConf = new Configuration();
        String metadataPath = "file://" + new File("batch-analytics/src/main/resources/iceberg-catalog").getAbsolutePath();
        hadoopConf.set("fs.defaultFS", metadataPath);
        Catalog catalog = new HadoopCatalog(hadoopConf, "batch-analytics/src/main/resources/iceberg-catalog");

        //3: Define table names
        Map<String, String> tableToJsonFileMap = new HashMap<>();
        tableToJsonFileMap.put("JIRA_EVENTS_T", "jira_events.json");
        tableToJsonFileMap.put("GITHUB_EVENTS_T", "github_events.json");
        tableToJsonFileMap.put("CI_EVENTS_T", "ci_events.json");
        tableToJsonFileMap.put("CD_EVENTS_T", "cd_events.json");

        //4: Process each table
        //TODO: Reading files is to mimic the reading data from MQ - Topic/Partition
        for (Map.Entry<String, String> entry : tableToJsonFileMap.entrySet()) {
            String tableName = entry.getKey();
            String jsonFile = entry.getValue();

            try {
                String jsonFilePath = "batch-analytics/src/main/resources/external/" + jsonFile;
                Dataset<Row> jsonData = spark.read()
                        .option("multiLine", true)
                        .json(jsonFilePath);

                // Log the schema and data only to demo purpose.
                System.out.println("Processing " + jsonFile + ":");
                jsonData.printSchema();
                jsonData.show();

                // Table identifier
                TableIdentifier tableIdentifier = TableIdentifier.of("customer1", "db", tableName);
                PartitionSpec spec = PartitionSpec.unpartitioned();
                Map<String, String> tableProperties = new HashMap<>();
                tableProperties.put("write.format.default", "parquet");

                // Convert Spark schema to Iceberg schema
                // TODO: On the fly schema needs to be changed to concrete schema definition to conform the data integrity.
                Schema icebergSchema = SparkSchemaUtil.convert(jsonData.schema());

                // Create or load table
                try {
                    catalog.loadTable(tableIdentifier);
                    System.out.println("Table " + tableName + " already exists, loaded existing table.");
                } catch (NoSuchTableException e) {
                    catalog.createTable(tableIdentifier, icebergSchema, spec, tableProperties);
                    System.out.println("Table " + tableName + " created successfully.");
                }

                // Write data to Iceberg table
                jsonData.write()
                        .format("iceberg")
                        .mode("overwrite")
                        .save("customer1.db." + tableName);

                System.out.println("Data successfully written to the Iceberg table: " + tableName);

            } catch (Exception e) {
                System.err.println("Error processing " + jsonFile + ": " + e.getMessage());
                e.printStackTrace();
            }
        }

        //5: Stop Spark session
        spark.stop();
        System.out.println("Spark session stopped. All data processing completed.");
    }
}
