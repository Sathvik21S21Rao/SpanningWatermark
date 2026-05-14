import random
from datetime import timedelta

class SpanDataGen:
    def __init__(self, num_events, start_time, min_span_ms, max_span_ms, delayed, events_per_sec, seed=42):
        self.num_events = num_events
        self.start_time = start_time
        self.min_span_ms = min_span_ms
        self.max_span_ms = max_span_ms
        self.max_delay_ms = delayed 
        self.events_per_sec = events_per_sec
        
        # 1. THE SEED: This guarantees reproducible randomness. 
        if seed is not None:
            random.seed(seed)

    def generate(self):
        events = []
        current_event_time = self.start_time
        
        ms_between_events = 1000.0 / self.events_per_sec if self.events_per_sec > 0 else 100

        for i in range(self.num_events):
            # True Event Times
            start_ts = current_event_time
            span = random.randint(self.min_span_ms, self.max_span_ms)
            end_ts = start_ts + timedelta(milliseconds=span)

            # Simulated Network Delays
            start_delay = random.randint(0, self.max_delay_ms)
            end_delay = random.randint(0, self.max_delay_ms)

            # The time the system will actually receive the signals
            start_arrival_time = start_ts + timedelta(milliseconds=start_delay)
            end_arrival_time = end_ts + timedelta(milliseconds=end_delay)

            # START signal
            events.append({
                "event_id": f"evt_{i}",
                "signal_type": "START",
                "timestamp": start_ts,
                "payload": {"value": random.randint(1, 100)},
                "_arrival_time": start_arrival_time, 
                "_delay_ms": start_delay 
            })

            # END signal
            events.append({
                "event_id": f"evt_{i}",
                "signal_type": "END",
                "timestamp": end_ts,
                "payload": {"value": random.randint(1, 100)},
                "_arrival_time": end_arrival_time, 
                "_delay_ms": end_delay
            })

            current_event_time += timedelta(milliseconds=ms_between_events)

        # Sort entirely by arrival time to simulate real-world out-of-order network ingestion
        events.sort(key=lambda x: x["_arrival_time"])
        
        # Print a tracking table before returning so you can debug Flink later
        self._print_tracking_table(events)

        return events

    def _print_tracking_table(self, events):
        """
        Prints a neat ground-truth table to your console.
        You can use this to manually trace if your Flink watermark is behaving correctly.
        """
        print("\n" + "="*80)
        print(" GROUND TRUTH: INGESTION ORDER (Simulating Kafka Arrival)")
        print("="*80)
        print(f"{'Arrival Time':<25} | {'Event ID':<8} | {'Type':<6} | {'Event Time (True)':<25} | {'Delay'}")
        print("-" * 80)
        
        for e in events:
            arr_str = e["_arrival_time"].strftime("%H:%M:%S.%f")[:-3]
            ev_str = e["timestamp"].strftime("%H:%M:%S.%f")[:-3]
            print(f"{arr_str:<25} | {e['event_id']:<8} | {e['signal_type']:<6} | {ev_str:<25} | +{e['_delay_ms']}ms")
        print("="*80 + "\n")