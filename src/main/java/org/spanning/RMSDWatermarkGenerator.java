package org.spanning;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

import java.util.HashMap;
import java.util.Map;

public class RMSDWatermarkGenerator implements WatermarkGenerator<SpanningEvent> {

    private final Map<String, Long> incompleteEvents = new HashMap<>();
    private final Map<String, Long> startDelays = new HashMap<>(); // Tracks Delta_s
    
    private long maxCompletedEndTime = Long.MIN_VALUE;
    private long currentWatermark = Long.MIN_VALUE;
    
    // Adaptive Tracking Variables
    private double emaRMSD = 0.0;
    private final double MAX_EXPECTED_DELAY = 3000.0; // From config.yaml
    private final double EMA_ALPHA = 0.2; // How fast it adapts to new network conditions

    @Override
    public void onEvent(SpanningEvent event, long eventTimestamp, WatermarkOutput output) {
        if (event.signal_type.equals("START")) {
            incompleteEvents.put(event.event_id, eventTimestamp);
            startDelays.put(event.event_id, event.simulated_delay); // Save Delta_s
            
        } else if (event.signal_type.equals("END")) {
            incompleteEvents.remove(event.event_id);
            if (eventTimestamp > maxCompletedEndTime) {
                maxCompletedEndTime = eventTimestamp;
            }
            
            // --- THE PAPER'S RMSD MATH ---
            if (startDelays.containsKey(event.event_id)) {
                long delay_s = startDelays.remove(event.event_id);
                long delay_e = event.simulated_delay;
                
                // Closed form continuous RMSD: sqrt( (ds^2 + ds*de + de^2) / 3 )
                double ds2 = Math.pow(delay_s, 2);
                double de2 = Math.pow(delay_e, 2);
                double ds_de = delay_s * delay_e;
                
                double currentRmsd = Math.sqrt((ds2 + ds_de + de2) / 3.0);
                
                // Smooth the RMSD using Exponential Moving Average
                emaRMSD = (EMA_ALPHA * currentRmsd) + ((1.0 - EMA_ALPHA) * emaRMSD);
            }
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        long minIncompleteStartTime = Long.MAX_VALUE;
        for (Long startTs : incompleteEvents.values()) {
            if (startTs < minIncompleteStartTime) {
                minIncompleteStartTime = startTs;
            }
        }

        long idealWatermark = Math.min(maxCompletedEndTime, minIncompleteStartTime);
        
        if (idealWatermark == Long.MAX_VALUE || idealWatermark == Long.MIN_VALUE) {
            return;
        }

        // Initialize safely on the very first tick
        if (currentWatermark == Long.MIN_VALUE) {
            currentWatermark = idealWatermark - (long)MAX_EXPECTED_DELAY;
        }

        // --- EQUATION 11: DELAY-AWARE WATERMARK UPDATE ---
        // Normalize RMSD to a weight between 0 and 1
        double d = Math.min(emaRMSD / MAX_EXPECTED_DELAY, 1.0); 
        
        // W_new = W_prev + (1 - d) * (W* - W_prev)
        long calculatedWatermark = currentWatermark + (long)((1.0 - d) * (idealWatermark - currentWatermark));

        if (calculatedWatermark > currentWatermark) {
            currentWatermark = calculatedWatermark;
            System.out.println("[WATERMARK UPDATE] Emitting: " + currentWatermark + " | Current Normalized RMSD (d): " + String.format("%.3f", d));
            output.emitWatermark(new Watermark(currentWatermark));
        }
    }
}