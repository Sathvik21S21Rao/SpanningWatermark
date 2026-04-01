import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import moa.classifiers.core.driftdetection.ADWIN;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class AdwinWatermarkGenerator implements WatermarkGenerator<String>, Serializable {

    private static final long serialVersionUID = 1L;

    //Watermarks
    private long Wcs = Long.MIN_VALUE;
    private long Wce = Long.MIN_VALUE;
    private long W = Long.MIN_VALUE;
    private long lastEmittedWm = Long.MIN_VALUE;

    //Delay buffers
    private long m_s = 1;
    private long m_e = 1;

    //Max timestamps
    private long Smax = Long.MIN_VALUE;
    private long Emax = Long.MIN_VALUE;

    //Incomplete set U(tp)
    private Map<Long, Long> activeSpans = new HashMap<>();
    private TreeMap<Long, Long> minStartTimestamps = new TreeMap<>();

    //ADWIN detectors
    private ADWIN A_s;
    private ADWIN A_e;

    //Sensitivity
    private double sensitivity = 0.01;
    private final double SENSITIVITY_RATE = 1.2;

    //Late ratio stats
    private long total_s = 0, late_s = 0;
    private long total_e = 0, late_e = 0;

    //Chunk max delay
    private long maxDelay_s = Long.MIN_VALUE;
    private long maxDelay_e = Long.MIN_VALUE;

    //Config
    private final double LATE_THRESHOLD = 0.1;
    private final long WARMUP = 1000;

    private long count_s = 0;
    private long count_e = 0;

    private transient ObjectMapper objectMapper;
    private final long maxOutOfOrderness;

    public AdwinWatermarkGenerator(long maxOutOfOrdernessMs) {
        this.maxOutOfOrderness = maxOutOfOrdernessMs;
        A_s = new ADWIN(sensitivity);
        A_e = new ADWIN(sensitivity);
    }

    @Override
    public void onEvent(String event, long eventTimestamp, WatermarkOutput output) {
        // System.out.println("Emitting adwin watermark: " + lastEmittedWm);
        try {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
            }

            JsonNode json = objectMapper.readTree(event);

            long id = json.get("event_id").asLong();
            int type = json.get("signal_type").asInt();
            long t = Instant.parse(json.get("timestamp").asText()).toEpochMilli();

            long tp = System.currentTimeMillis();
            long d = Math.max(0, tp - t);   // delay

            if (type == 0) {
                processStart(id, t, d);
            } else {
                processEnd(id, t, d);
            }

            System.out.println("[onEvent] type=" + type 
            + " count_s=" + count_s + " count_e=" + count_e
            + " lastEmittedWm=" + lastEmittedWm
            + " Wcs=" + Wcs + " Wce=" + Wce);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void processStart(long id, long t, long d) {

        count_s++;
        total_s++;

        // Update Smax
        Smax = Math.max(Smax, t);

        // Add to incomplete set U(tp)
        activeSpans.put(id, t);
        minStartTimestamps.put(t, minStartTimestamps.getOrDefault(t, 0L) + 1);

        // Warmup
        if (count_s <= WARMUP) {
            m_s = Math.max(m_s, d);
            return;
        }

        // Normalize delay
        double z = (double) d / Math.max(m_s, 1);

        boolean drift = A_s.setInput(z, sensitivity);

        // Track lateness
        if (t < Wcs) {
            late_s++;
        }

        maxDelay_s = Math.max(maxDelay_s, d);

        if (drift) {
            handleDrift();
        }
    }

    private void processEnd(long id, long t, long d) {

        count_e++;
        total_e++;

        // Update Emax
        Emax = Math.max(Emax, t);

        // Remove from U(tp)
        if (activeSpans.containsKey(id)) {
            long startTs = activeSpans.get(id);

            long c = minStartTimestamps.get(startTs) - 1;
            if (c == 0) minStartTimestamps.remove(startTs);
            else minStartTimestamps.put(startTs, c);

            activeSpans.remove(id);
        }

        // Warmup
        if (count_e <= WARMUP) {
            m_e = Math.max(m_e, d);
            return;
        }

        double z = (double) d / Math.max(m_e, 1);

        boolean drift = A_e.setInput(z, sensitivity);

        if (t < Wce) {
            late_e++;
        }

        maxDelay_e = Math.max(maxDelay_e, d);

        if (drift) {
            handleDrift();
        }
    }

    private void handleDrift() {

        System.out.println("[handleDrift] rho_s=" + ((double)late_s/Math.max(total_s,1))
        + " rho_e=" + ((double)late_e/Math.max(total_e,1))
        + " m_s=" + m_s + " m_e=" + m_e
        + " Smax=" + Smax + " Emax=" + Emax);

        double rho_s = (double) late_s / Math.max(total_s, 1);
        double rho_e = (double) late_e / Math.max(total_e, 1);

        //Late-ratio gate
        if (Math.max(rho_s, rho_e) < LATE_THRESHOLD) {

            //Compute SUmin
            long SUmin = minStartTimestamps.isEmpty()
                    ? Long.MAX_VALUE
                    : minStartTimestamps.firstKey();

            //Core formulas (paper exact)
            Wcs = Smax - m_s;
            Wce = Math.min(Emax - m_e, SUmin);
            W = Math.min(Wcs, Wce);

            if (W > lastEmittedWm) {
                lastEmittedWm = W;
            }

            //Increase sensitivity (stable regime)
            sensitivity = Math.max(sensitivity / SENSITIVITY_RATE, 1e-6);

        } else {
            //Increase delay budgets (unstable regime)
            m_s = Math.max(m_s, maxDelay_s);
            m_e = Math.max(m_e, maxDelay_e);

            //Reduce sensitivity
            sensitivity = Math.min(sensitivity * SENSITIVITY_RATE, 1.0);
        }

        //Reset chunk stats
        total_s = late_s = 0;
        total_e = late_e = 0;
        maxDelay_s = Long.MIN_VALUE;
        maxDelay_e = Long.MIN_VALUE;
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {

        if (lastEmittedWm == Long.MIN_VALUE) return;
        System.out.println("Emitting adwin watermark: " + lastEmittedWm);
        output.emitWatermark(new Watermark(lastEmittedWm));
    }
}