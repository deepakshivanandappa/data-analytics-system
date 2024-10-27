package io.github.deepshiv126;

//import org.apache.iceberg.Catalog;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class LocalRawDataIngestionTest {

    private SparkSession spark;
    private HadoopCatalog catalog;

    @BeforeEach
    public void setUp() {
        spark = SparkSession.builder()
                .appName("LocalRawDataIngestionTest")
                .master("local[*]")
                .getOrCreate();

        // Mock the Iceberg catalog
        catalog = mock(HadoopCatalog.class);
    }

    @AfterEach
    public void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    public void testProcessJsonData() {
        // Mock JSON schema
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true)
        });

        // Mock DataFrame
        Dataset<Row> mockData = spark.createDataFrame(
                spark.sparkContext().parallelize(
                        java.util.Arrays.asList(
                                RowFactory.create(1, "test1"),
                                RowFactory.create(2, "test2")
                        )
                ),
                schema
        );

        // Verify the mock DataFrame
        assertEquals(2, mockData.count());
        assertEquals("id", mockData.schema().fields()[0].name());
        assertEquals("name", mockData.schema().fields()[1].name());

        // Define table parameters
        String tableName = "JIRA_ISSUES_T";
        TableIdentifier tableIdentifier = TableIdentifier.of("customer1", "db", tableName);
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("write.format.default", "parquet");

        // Mocking Iceberg table creation
        try {
            doThrow(NoSuchTableException.class).when(catalog).loadTable(tableIdentifier);
            catalog.createTable(tableIdentifier, SparkSchemaUtil.convert(mockData.schema()), spec, tableProperties);
        } catch (Exception e) {
            Assertions.fail("Failed to mock Iceberg Catalog interactions: " + e.getMessage());
        }

        // Verify if the createTable was called once
        verify(catalog, times(1)).createTable(any(), any(), any(), any());

        // Write mock data to the Iceberg table
        mockData.write()
                .format("iceberg")
                .mode("overwrite")
                .save("customer1.db." + tableName);

        // Mocking DataFrame
        Dataset<Row> writtenData = spark.read().format("iceberg").load("customer1.db." + tableName);
        assertEquals(2, writtenData.count());

        // Validate the data schema
        assertEquals("id", writtenData.schema().fields()[0].name());
        assertEquals("name", writtenData.schema().fields()[1].name());
    }
}
