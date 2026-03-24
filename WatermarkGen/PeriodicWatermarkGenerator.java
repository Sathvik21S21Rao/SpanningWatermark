import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

public class PeriodicWatermarkGenerator implements WatermarkGenerator<String>, Serializable {

    private static final long serialVersionUID = 1L;
    private long W_s = Long.MIN_VALUE;  // Watermark for start events (signal_type 0)
    private long W_e = Long.MIN_VALUE;  // Watermark for end events (signal_type 1)
    private long W = Long.MIN_VALUE;    // Overall watermark
    private long maxOutOfOrderness;
    private long lastEmittedWm = Long.MIN_VALUE;
    private transient ObjectMapper objectMapper;
    private Map<Long, Long> activeSpans = new HashMap<>();
    private Map<Long,Long> minStartTimestamps = new HashMap<>(); // Multiset to track min start timestamps of active spans
    private Long maxEndTimestamp = Long.MIN_VALUE; // No need for a multiset for max end timestamps since it is monotonically increasing

    public PeriodicWatermarkGenerator(long maxOutOfOrdernessMs) {
        this.maxOutOfOrderness = maxOutOfOrdernessMs;
    }

    @Override
    public void onEvent(String event, long eventTimestamp, WatermarkOutput output) {
        try {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
            }
            JsonNode jsonNode = objectMapper.readTree(event);
            long event_id = jsonNode.get("event_id").asLong();
            int signalType = jsonNode.get("signal_type").asInt();
            // Parse ISO 8601 timestamp string and convert to milliseconds since epoch
            String timestampStr = jsonNode.get("timestamp").asText();
            long timestamp = Instant.parse(timestampStr).toEpochMilli();

            if (signalType == 0) {
                // Start event
                activeSpans.put(event_id, timestamp);
                System.out.println("Received start event: " + event_id + " with timestamp: " + timestamp);
                if (timestamp > W_s) {
                    W_s = timestamp;
                }
                minStartTimestamps.put(timestamp, minStartTimestamps.getOrDefault(timestamp, 0L) + 1);
            } else if (signalType == 1) {
                // End event
                maxEndTimestamp = Math.max(maxEndTimestamp, timestamp);
                
                minStartTimestamps.put(activeSpans.get(event_id), minStartTimestamps.get(activeSpans.get(event_id)) - 1);
                if (minStartTimestamps.get(activeSpans.get(event_id)) == 0) {
                    minStartTimestamps.remove(activeSpans.get(event_id));
                }
                if (!activeSpans.isEmpty()) {
                    W_e = Math.min(minStartTimestamps.keySet().iterator().next(),
                                   maxEndTimestamp);
                } else {
                    W_e = maxEndTimestamp;
                }
                activeSpans.remove(event_id);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        

        W = Math.min(W_s, W_e);
        if(W == Long.MIN_VALUE) {
            return;
        }
        long candidateWm = W - maxOutOfOrderness;

        if (candidateWm > lastEmittedWm) {
            System.out.println("Emitting watermark: " + candidateWm + " based on W_s: " + W_s + " and W_e: " + W_e);
            output.emitWatermark(new Watermark(candidateWm));
            lastEmittedWm = candidateWm;
        }
    }
}
