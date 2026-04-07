// import org.apache.flink.api.common.eventtime.WatermarkGenerator;
// import org.apache.flink.api.common.eventtime.WatermarkOutput;
// import org.apache.flink.api.common.eventtime.WatermarkStrategy;
// import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
// import org.apache.flink.api.common.serialization.SimpleStringSchema;
// import org.apache.flink.configuration.Configuration;
// import org.apache.flink.connector.kafka.source.KafkaSource;
// import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
// import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
// import org.apache.kafka.clients.consumer.OffsetResetStrategy;

// import com.fasterxml.jackson.databind.JsonNode;
// import com.fasterxml.jackson.databind.ObjectMapper;

// import java.io.File;
// import java.io.FileInputStream;
// import java.io.IOException;
// import java.io.InputStream;
// import java.time.Instant;
// import java.util.Properties;

// /**
//  * Streaming pipeline for processing span events with watermarks
//  * Equivalent of the Python StreamingJob.py
//  */
// public class StreamingJob {
//     private Properties properties;

//     private static class JsonTimestampAssigner implements SerializableTimestampAssigner<String> {
//         private static final long serialVersionUID = 1L;
//         private transient ObjectMapper mapper;

//         @Override
//         public long extractTimestamp(String event, long recordTimestamp) {
//             try {
//                 if (mapper == null) {
//                     mapper = new ObjectMapper();
//                 }
//                 JsonNode json = mapper.readTree(event);
//                 String ts = json.get("timestamp").asText();
//                 return Instant.parse(ts).toEpochMilli();
//             } catch (Exception e) {
//                 return recordTimestamp;
//             }
//         }
//     }

//     public StreamingJob(String configPath) throws IOException {
//         loadConfig(configPath);
//     }

//     /**
//      * Load configuration from properties file
//      */
//     private void loadConfig(String configPath) throws IOException {
//         properties = new Properties();
//         try (InputStream inputStream = new FileInputStream(new File(configPath))) {
//             properties.load(inputStream);
//         }
//     }

//     /**
//      * Main execution method for the pipeline
//      */
//     public void run() throws Exception {
//         // Create configuration
//         Configuration flinkConfig = new Configuration();
//         String restBindAddress = properties.getProperty("flink.rest.bind.address", "0.0.0.0");
//         String restPortStr = properties.getProperty("flink.rest.port", "8081");
//         int restPort = Integer.parseInt(restPortStr);

//         flinkConfig.setString("rest.bind-address", restBindAddress);
//         flinkConfig.setString("rest.port", String.valueOf(restPort));

//         // Create execution environment
//         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);

//         // Set parallelism
//         String parallelismStr = properties.getProperty("flink.parallelism", "1");
//         int parallelism = Integer.parseInt(parallelismStr);
//         env.setParallelism(parallelism);

//         // Get watermark strategy and configuration
//         String strategy = properties.getProperty("watermark.strategy", "adwin");
//         String watermarkAutoIntervalStr = properties.getProperty("watermark.auto.interval", "200");
//         int watermarkAutoInterval = Integer.parseInt(watermarkAutoIntervalStr);
//         String maxOutOfOrdernessStr = properties.getProperty("watermark.max.out.of.orderness.ms", "1000");
//         int maxOutOfOrderness = Integer.parseInt(maxOutOfOrdernessStr);

//         env.getConfig().setAutoWatermarkInterval(watermarkAutoInterval);

//         // Create appropriate watermark generator
//         WatermarkGenerator<String> watermarkGenerator;
//         if ("periodic".equals(strategy)) {
//             watermarkGenerator = new PeriodicWatermarkGenerator(maxOutOfOrderness);
//         } else if ("adwin".equals(strategy)) {
//             watermarkGenerator = new AdwinWatermarkGenerator(maxOutOfOrderness);
//         } else {
//             throw new IllegalArgumentException("Unknown watermark strategy: " + strategy);
//         }

//         // Create watermark strategy
//         WatermarkStrategy<String> wmStrategy = WatermarkStrategy
//                 .forGenerator((ctx) -> watermarkGenerator)
//                 .withTimestampAssigner(new JsonTimestampAssigner());

//         // Build Kafka source
//         String bootstrapServers = properties.getProperty("kafka.bootstrap.servers", "localhost:9092");
//         String groupId = properties.getProperty("kafka.group.id", "watermark-processor");
//         String inputTopic = properties.getProperty("topics.input", "span-events");

//         KafkaSource<String> source = KafkaSource.<String>builder()
//                 .setBootstrapServers(bootstrapServers)
//                 .setTopics(inputTopic)
//                 .setGroupId(groupId)
//                 .setValueOnlyDeserializer(new SimpleStringSchema())
//                 .setStartingOffsets(OffsetsInitializer.earliest())
//                 .build();

//         // Create source and apply watermark strategy
//         var stream = env.fromSource(source, wmStrategy, "KafkaSource");

//         // count the number of complete spans per some configured time window and push to output topic
//         /*"event_id": e["event_id"],
//             "signal_type": e["signal_type"],
//             "timestamp": e["timestamp"].isoformat(timespec="milliseconds")+"Z",
//             "payload": e["payload"],*/
        

//         stream.print();
        

//         env.execute("SpanPipeline");
//     }

//     public static void main(String[] args) throws Exception {
//         String configPath;
//         if (args.length > 0) {
//             configPath = args[0];
//         } else {
//             // Default configuration path
//             configPath = System.getProperty("user.dir") + File.separator + "config.properties";
//         }

//         StreamingJob job = new StreamingJob(configPath);
//         job.run();
//     }
// }

import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.time.Instant;
import java.time.Duration;
import java.util.Properties;

/**
 * Full Streaming Pipeline:
 * Kafka → Watermark (custom) → event_id ordering → window aggregation → Kafka sink
 */
public class StreamingJob {

    private Properties properties;

    // ---------------- Timestamp Assigner (UNCHANGED) ----------------
    private static class JsonTimestampAssigner implements SerializableTimestampAssigner<String> {
        private transient ObjectMapper mapper;

        @Override
        public long extractTimestamp(String event, long recordTimestamp) {
            try {
                if (mapper == null) mapper = new ObjectMapper();
                JsonNode json = mapper.readTree(event);
                String ts = json.get("timestamp").asText();
                return Instant.parse(ts).toEpochMilli();
            } catch (Exception e) {
                return recordTimestamp;
            }
        }
    }

    public StreamingJob(String configPath) throws Exception {
        properties = new Properties();
        try (InputStream inputStream = new FileInputStream(new File(configPath))) {
            properties.load(inputStream);
        }
    }

    public void run() throws Exception {

        // ---------------- Flink Config ----------------
        Configuration flinkConfig = new Configuration();
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);

        env.setParallelism(Integer.parseInt(properties.getProperty("flink.parallelism", "1")));

        // Optional but recommended
        env.enableCheckpointing(5000);

        // ---------------- Watermark Setup (YOUR GENERATOR) ----------------
        String strategy = properties.getProperty("watermark.strategy", "adwin");
        int watermarkAutoInterval = Integer.parseInt(
                properties.getProperty("watermark.auto.interval", "200"));
        int maxOutOfOrderness = Integer.parseInt(
                properties.getProperty("watermark.max.out.of.orderness.ms", "1000"));

        env.getConfig().setAutoWatermarkInterval(watermarkAutoInterval);

        WatermarkGenerator<String> watermarkGenerator;
        if ("periodic".equals(strategy)) {
            watermarkGenerator = new PeriodicWatermarkGenerator(maxOutOfOrderness);
        } else if ("adwin".equals(strategy)) {
            watermarkGenerator = new AdwinWatermarkGenerator(maxOutOfOrderness);
        } else {
            throw new IllegalArgumentException("Unknown watermark strategy: " + strategy);
        }

        WatermarkStrategy<String> wmStrategy = WatermarkStrategy
                .forGenerator(ctx -> watermarkGenerator)
                .withTimestampAssigner(new JsonTimestampAssigner());

        // ---------------- Kafka Source ----------------
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(properties.getProperty("kafka.bootstrap.servers"))
                .setTopics(properties.getProperty("topics.input", "span-events"))
                .setGroupId(properties.getProperty("kafka.group.id", "span-group"))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        var stream = env.fromSource(source, wmStrategy, "KafkaSource");

        // ---------------- Map: JSON → (event_id, +1/-1) ----------------
        var mapped = stream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            private transient ObjectMapper mapper;

            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                if (mapper == null) mapper = new ObjectMapper();

                JsonNode json = mapper.readTree(value);

                String eventId = json.get("event_id").asText();
                int signalType = json.get("signal_type").asInt();

                return Tuple2.of(eventId, (signalType == 1) ? 1 : -1);
            }
        });

        // ---------------- Ensure per-event ordering ----------------
        var perEvent = mapped
                .keyBy(t -> t.f0)
                .map(t -> t.f1);

        // ---------------- Windowed Aggregation ----------------
        var windowedCounts = perEvent
                .keyBy(v -> 0)
                .window(TumblingEventTimeWindows.of(
                        Duration.ofSeconds(
                                Integer.parseInt(properties.getProperty("window.size.seconds", "10"))
                        )
                ))
                .process(new ProcessWindowFunction<Integer, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer key,
                                        Context context,
                                        Iterable<Integer> elements,
                                        Collector<String> out) {

                        int sum = 0;
                        for (Integer v : elements) sum += v;

                        String result = String.format(
                                "{\"window_start\":\"%s\",\"window_end\":\"%s\",\"count\":%d}",
                                Instant.ofEpochMilli(context.window().getStart()),
                                Instant.ofEpochMilli(context.window().getEnd()),
                                sum
                        );

                        out.collect(result);
                    }
                });

        // ---------------- Kafka Sink (AT LEAST ONCE) ----------------
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(properties.getProperty("kafka.bootstrap.servers"))
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(properties.getProperty("topics.output", "span-counts"))
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        windowedCounts.sinkTo(sink);

        env.execute("Span Pipeline (Custom Watermark + Kafka Sink)");
    }

    public static void main(String[] args) throws Exception {
        String configPath = (args.length > 0)
                ? args[0]
                : System.getProperty("user.dir") + File.separator + "config.properties";

        new StreamingJob(configPath).run();
    }
}