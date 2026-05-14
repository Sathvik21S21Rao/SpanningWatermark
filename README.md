# Capped Drift-Aware Watermarks for Bi-signal Spanning Events

This project implements a custom Apache Flink Watermark Generator based on continuous Root Mean Square Deviation (RMSD). It is designed to handle "spanning events" (events with a duration, separated into START and END signals) that arrive out-of-order due to network delays.

Instead of using a fixed time window, this implementation uses an **Exponential Moving Average (EMA)** to dynamically calculate the network delay budget and smoothly advance the Flink watermark.

## Prerequisites
To run this project locally, you need the following installed:
- Java 17
- Apache Maven
- Apache Flink (Version 1.17.2 or compatible)
- Apache Kafka & Zookeeper
- Python 3.x (with `confluent-kafka` and `pyyaml` installed)

## How to Run the Project

Running this project requires opening 4 separate terminal windows to start the infrastructure, deploy the Flink job, and simulate the data stream.

### Terminal 1: Start Kafka Infrastructure
Navigate to your Kafka installation directory and start Zookeeper and Kafka:
```bash
# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka
bin/kafka-server-start.sh config/server.properties
```
**Note:** Before running the job for the first time, ensure the topic exists:
```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic spanning-events --partitions 1 --replication-factor 1
```
### Terminal 2: Start Flink Cluster
Start your local Apache Flink cluster:
```bash
$FLINK_HOME/bin/start-cluster.sh
```
### Terminal 3: Watch the Output Logs (Optional but Recommended)
Because Flink runs in the background, you can track the watermark updates live by tailing the Flink TaskManager logs:
```bash
tail -f $FLINK_HOME/log/*.out | grep "WATERMARK"
```
### Terminal 4: Build and Deploy
Navigate to the root of this Java project.
1. Build the Fat JAR:
This packages the code, the Kafka connector, and the JSON parser into one JAR.
```bash
mvn clean package -U
```
2. Submit the Job to Flink:
```bash
$FLINK_HOME/bin/flink run -c org.spanning.MainJob target/rmsd-watermark-1.0-SNAPSHOT.jar
```
### Terminal 5: Generate the Simulated Data
Once the Flink job is running and waiting for data, navigate to your Python data generator folder and run the simulation script:
```bash
def main.py