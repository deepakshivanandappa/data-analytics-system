package io.github.deepshiv126;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class RealTimeLeadTime {

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        // 1. Initialize Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("RealTimeLeadTime")
                .master("local[*]")
                .getOrCreate();

        // 2. Kafka source
        Dataset<Row> kafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "lead_time_topic")
                .option("startingOffsets", "latest")
                .option("kafka.security.protocol", "SASL_PLAINTEXT")
                .option("kafka.sasl.mechanism", "PLAIN")
                .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user1\" password=\"rkeFjT058P\";")
                .load();


        // 3. Define schemas
        StructType jiraSchema = new StructType()
                .add("issue_id", "string")
                .add("issue_created_time", "string")
                .add("issue_resolved_time", "string")
                .add("status", "string");

        StructType githubSchema = new StructType()
                .add("commit_id", "string")
                .add("issue_id", "string")
                .add("commit_time", "string")
                .add("branch", "string");

        StructType ciSchema = new StructType()
                .add("build_id", "string")
                .add("commit_id", "string")
                .add("build_start_time", "string")
                .add("build_end_time", "string")
                .add("build_status", "string");

        StructType cdSchema = new StructType()
                .add("deployment_id", "string")
                .add("issue_id", "string")
                .add("deploy_start_time", "string")
                .add("deploy_end_time", "string")
                .add("deploy_status", "string");

        // 4. Parse Kafka stream
        Dataset<Row> parsedStream = kafkaStream
                .selectExpr("CAST(value AS STRING) AS json_string")
                .select(
                        functions.expr("json_string").alias("raw_json"),
                        functions.from_json(functions.col("json_string"), jiraSchema).alias("jira_data"),
                        functions.from_json(functions.col("json_string"), githubSchema).alias("github_data"),
                        functions.from_json(functions.col("json_string"), ciSchema).alias("ci_data"),
                        functions.from_json(functions.col("json_string"), cdSchema).alias("cd_data")
                )
                .selectExpr(
                        "COALESCE(jira_data.issue_id, github_data.issue_id, ci_data.issue_id, cd_data.issue_id) AS issue_id",
                        "jira_data.issue_created_time",
                        "jira_data.issue_resolved_time",
                        "github_data.commit_time",
                        "ci_data.build_end_time",
                        "cd_data.deploy_end_time"
                );

        // 8. Temporary view for SQL processing
        parsedStream.createOrReplaceTempView("incoming_events");

        // 9. SQL to calcualte lead time
        String sqlQuery =
                "SELECT " +
                        "  issue_id, " +
                        "  MAX(CAST(issue_created_time AS TIMESTAMP)) AS jira_created_time, " +
                        "  MAX(CAST(commit_time AS TIMESTAMP)) AS github_commit_time, " +
                        "  MAX(CAST(build_end_time AS TIMESTAMP)) AS ci_build_completion_time, " +
                        "  MAX(CAST(deploy_end_time AS TIMESTAMP)) AS cd_deployed_time, " +
                        "  ROUND((UNIX_TIMESTAMP(MAX(CAST(deploy_end_time AS TIMESTAMP))) - " +
                        "         UNIX_TIMESTAMP(MIN(CAST(issue_created_time AS TIMESTAMP)))) / 3600, 2) AS total_lead_time_hours " +
                        "FROM incoming_events " +
                        "GROUP BY issue_id";

        // 10. Execute SQL
        Dataset<Row> leadTimeStream = spark.sql(sqlQuery);

        // 11. Output the results to console
        StreamingQuery query = leadTimeStream.writeStream()
                .outputMode(OutputMode.Update())
                .format("console")
                .start();
        query.awaitTermination();
    }

}

