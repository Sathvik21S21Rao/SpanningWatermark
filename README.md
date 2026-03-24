# Watermark Generator - Flink Streaming Job

## Setup

### 1. Compile the Java files
```bash
javac -cp "/opt/flink/lib/*:/opt/flink/opt/*:/opt/kafka/libs/*" -d classes \
  StreamingJob.java \
  WatermarkGen/PeriodicWatermarkGenerator.java \
  WatermarkGen/AdwinWatermarkGenerator.java
```

### 2. Create JAR file
```bash
jar cvf span.jar -C classes/ .
```

### 3. Start Flink cluster
```bash
$FLINK_HOME/bin/start-cluster.sh
```
View the Flink dashboard on `localhost:8081`

### 4. Submit job to Flink
```bash
chmod +x run.sh
./run.sh
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