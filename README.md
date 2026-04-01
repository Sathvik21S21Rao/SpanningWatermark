# Watermark Generator - Flink Streaming Job

## Setup

### 1. Add ADWIN dependency (MOA)
Download a MOA release JAR and place it in `dependencies/` (so `run.sh` picks it up). For example:
```bash
curl -L -o dependencies/moa.jar https://repo1.maven.org/maven2/nz/ac/waikato/cms/moa/moa/2023.07.1/moa-2023.07.1.jar
```

### 2. Compile the Java files
```bash
javac -cp "/opt/flink/lib/*:/opt/flink/opt/*:/opt/kafka/libs/*:dependencies/*" -d classes \
  StreamingJob.java \
  WatermarkGen/PeriodicWatermarkGenerator.java \
  WatermarkGen/AdwinWatermarkGenerator.java
```

### 3. Create JAR file
```bash
jar cvf span.jar -C classes/ .
```

### 4. Start Flink cluster
```bash
$FLINK_HOME/bin/start-cluster.sh
```
View the Flink dashboard on `localhost:8081`

### 5. Submit job to Flink
```bash
chmod +x run.sh
./run.sh
```

### 6. Kafka setup (for testing)
Create the input topic (default `span-events`):
```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic span-events --partitions 1 --replication-factor 1
```

## Data Generation

Install requirements:
```bash
pip install -r requirements.txt
```

Run the Kafka data generator:
```bash
cd DataGen
python KafkaGen.py
```
Ensure the kafka topics mentioned in `DataGen/config.yaml` are created