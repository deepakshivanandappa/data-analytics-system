## Java Version

```bash
$ java --version
openjdk 11.0.25 2024-10-15
OpenJDK Runtime Environment Homebrew (build 11.0.25+0)
OpenJDK 64-Bit Server VM Homebrew (build 11.0.25+0, mixed mode)
```

## Maven Version

```bash
$ mvn --version                                                                                                                                                                                  ─╯
Apache Maven 3.9.9 (8e8579a9e76f7d015ee5ec7bfcdc97d260186937)
Maven home: /usr/local/Cellar/maven/3.9.9/libexec
Java version: 11.0.25, vendor: Homebrew, runtime: /usr/local/Cellar/openjdk@11/11.0.25/libexec/openjdk.jdk/Contents/Home
Default locale: en_US, platform encoding: UTF-8
OS name: "mac os x", version: "15.0.1", arch: "x86_64", family: "mac"

```

## Run the Spark app

```bash
mvn clean install ; 
╰─ java --add-opens java.base/sun.nio.ch=ALL-UNNAMED -cp  batch-analytics/target/batch-analytics-1.0-SNAPSHOT.jar:/Users/deepakshivanandappa/.m2/repository/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.3.1/iceberg-spark-runtime-3.4_2.12-1.3.1.jar io.github.deepshiv126.LocalRawDataIngestion        ─╯
╰─ java --add-opens java.base/sun.nio.ch=ALL-UNNAMED -cp  batch-analytics/target/batch-analytics-1.0-SNAPSHOT.jar:/Users/deepakshivanandappa/.m2/repository/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.3.1/iceberg-spark-runtime-3.4_2.12-1.3.1.jar io.github.deepshiv126.LeadTimeMetric               ─╯
```
