package io.github.deepshiv126;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

public class LeadTimeMetric {

    public static void main(String[] args) {
        // 1. Initialize Spark Session with Iceberg's Hadoop Catalog
        SparkSession spark = SparkSession.builder()
                .appName("LeadTimeCalculatorUsingSimpleSQL")
                .master("local[*]")
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "hadoop")
                .config("spark.sql.catalog.spark_catalog.warehouse",
                        "file://" + new File("batch-analytics/src/main/resources/iceberg-catalog").getAbsolutePath())
                .getOrCreate();

        // 2. Read all event tables from Iceberg
        Dataset<Row> jiraEvents = spark.read()
                .format("iceberg")
                .load("customer1.db.JIRA_EVENTS_T");

        Dataset<Row> githubEvents = spark.read()
                .format("iceberg")
                .load("customer1.db.GITHUB_EVENTS_T");

        Dataset<Row> ciEvents = spark.read()
                .format("iceberg")
                .load("customer1.db.CI_EVENTS_T");

        Dataset<Row> cdEvents = spark.read()
                .format("iceberg")
                .load("customer1.db.CD_EVENTS_T");

        // 3. Create temporary views for SQL queries
        jiraEvents.createOrReplaceTempView("jira_events");
        githubEvents.createOrReplaceTempView("github_events");
        ciEvents.createOrReplaceTempView("ci_events");
        cdEvents.createOrReplaceTempView("cd_events");

        // 4. Run simplified Spark SQL query
        String leadTimeQuery = "SELECT " +
                "j.issue_id AS issue_id, " +
                "CAST(j.issue_created_time AS TIMESTAMP) AS jira_created_time, " +
                "CAST(g.commit_time AS TIMESTAMP) AS github_commit_time, " +
                "CAST(ci.build_end_time AS TIMESTAMP) AS ci_build_completion_time, " +
                "CAST(cd.deploy_end_time AS TIMESTAMP) AS cd_deployed_time, " +
                "ROUND((UNIX_TIMESTAMP(CAST(cd.deploy_end_time AS TIMESTAMP)) - " +
                "UNIX_TIMESTAMP(CAST(j.issue_created_time AS TIMESTAMP))) / 3600, 2) AS total_lead_time_hours " +
                "FROM jira_events j " +
                "JOIN github_events g ON j.issue_id = g.issue_id " +
                "JOIN ci_events ci ON g.commit_id = ci.commit_id " +
                "JOIN cd_events cd ON j.issue_id = cd.issue_id";


        Dataset<Row> leadTimeData = spark.sql(leadTimeQuery);

        // 5. Display lead time metrics
        System.out.println("Lead Time Metrics :");
        leadTimeData.show();

        // 6. Stop Spark session
        spark.stop();
        System.out.println("Spark session stopped. Lead time calculation completed.");
    }
}
