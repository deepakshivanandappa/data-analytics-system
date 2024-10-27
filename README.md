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


## Run the Kafka 
```bash
minikube start --cpus=4 --memory=4192  
helm install kafka bitnami/kafka --version 30.1.6

client.properties 
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
    username="user1" \
    password="$(kubectl get secret kafka-user-passwords --namespace default -o jsonpath='{.data.client-passwords}' | base64 -d | cut -d , -f 1)";
    


##  Kafka client run the following commands:
kubectl run kafka-client --restart='Never' --image docker.io/bitnami/kafka:3.8.0-debian-12-r5 --namespace default --command -- sleep infinity
kubectl cp --namespace default /path/to/client.properties kafka-client:/tmp/client.properties
kubectl exec --tty -i kafka-client --namespace default -- bash 
PRODUCER:
    kafka-console-producer.sh \
        --producer.config /tmp/client.properties \
        --bootstrap-server kafka.default.svc.cluster.local:9092 \
        --topic test

CONSUMER:
    kafka-console-consumer.sh \
        --consumer.config /tmp/client.properties \
        --bootstrap-server kafka.default.svc.cluster.local:9092 \
        --topic test \
        --from-beginning

```