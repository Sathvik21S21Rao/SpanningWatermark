import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;

public class PeriodicWatermarkGenerator implements WatermarkGenerator<String>, Serializable {

    private static final long serialVersionUID = 1L;
    private long W_s = Long.MIN_VALUE;  // Watermark for start events (signal_type 0)
    private long W_e = Long.MIN_VALUE;  // Watermark for end events (signal_type 1)
    private long W = Long.MIN_VALUE;    // Overall watermark
    private long maxOutOfOrderness;
    private long lastEmittedWm = Long.MIN_VALUE;
    private transient ObjectMapper objectMapper;

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
            int signalType = jsonNode.get("signal_type").asInt();
            long timestamp = jsonNode.get("timestamp").asLong();

            if (signalType == 0) {
                // Start event
                if (timestamp > W_s) {
                    W_s = timestamp;
                }
            } else if (signalType == 1) {
                // End event
                if (timestamp > W_e) {
                    W_e = timestamp;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        if (W_s == Long.MIN_VALUE && W_e == Long.MIN_VALUE) {
            return;
        }

        W = Math.min(W_s, W_e);
        long candidateWm = W - maxOutOfOrderness;

        if (candidateWm > lastEmittedWm) {
            output.emitWatermark(new Watermark(candidateWm));
            lastEmittedWm = candidateWm;
        }
    }
}
