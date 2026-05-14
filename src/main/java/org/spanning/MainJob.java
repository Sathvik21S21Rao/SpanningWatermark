package org.spanning;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;

public class MainJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Set how often onPeriodicEmit is called (e.g., every 500ms)
        env.getConfig().setAutoWatermarkInterval(500);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("spanning-events")
                .setGroupId("flink-test-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Custom Strategy using your RMSD Generator
        WatermarkStrategy<SpanningEvent> strategy = WatermarkStrategy
                .<SpanningEvent>forGenerator(ctx -> new RMSDWatermarkGenerator())
                .withTimestampAssigner((event, recordTimestamp) -> event.getTimestampEpoch())
                // Allow events to be idle if traffic stops
                .withIdleness(Duration.ofSeconds(5));

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
            // Parse JSON string to Java Object
            .map(jsonString -> {
                ObjectMapper mapper = new ObjectMapper();
                return mapper.readValue(jsonString, SpanningEvent.class);
            })
            // Apply the custom watermark math
            .assignTimestampsAndWatermarks(strategy)
            .print();

        env.execute("RMSD Watermark Test Job");
    }
}