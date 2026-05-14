package org.spanning;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SpanningEvent {
    public String event_id;
    public String signal_type; // "START" or "END"
    public String timestamp;   // ISO string from Python
    public long simulated_delay; // so that we can simulate network congestion
    
    // We will parse the ISO string into a long epoch timestamp
    public long getTimestampEpoch() {
        return java.time.Instant.parse(timestamp).toEpochMilli();
    }
}

// bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic spanning-events