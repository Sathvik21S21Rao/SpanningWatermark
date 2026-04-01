import csv
import random
import string
from datetime import datetime, timedelta
import time


class SpanDataGen:
    def __init__(
        self,
        num_events: int,
        start_time: datetime,
        min_span_ms: int,
        max_span_ms: int,
        events_per_sec: int,
        delayed: int = 0,
        seed: int = None,
    ):
        """
        num_events  : number of spanning events
        start_time  : base timestamp
        min_span_ms : minimum duration of span
        max_span_ms : maximum duration of span
        delayed     : max out-of-orderness in milliseconds
        """

        self.num_events = num_events
        self.start_time = start_time
        self.min_span_ms = min_span_ms
        self.max_span_ms = max_span_ms
        self.delayed = delayed  # bounded disorder window
        self.events_per_sec = events_per_sec
        if seed is not None:
            random.seed(seed)

    def _random_payload(self, length=8):
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

    def _generate_span(self, event_id: int):
        """
        Generate span based on event-time throughput
        """

        # events_per_sec → ms per event
        ms_per_event = 1000 / self.events_per_sec

        # base progression
        base_offset_ms = event_id * ms_per_event
        start_ts = timedelta(milliseconds=base_offset_ms) + self.start_time
        duration_ms = random.randint(self.min_span_ms, self.max_span_ms)
        end_ts = start_ts + timedelta(milliseconds=duration_ms)

        payload = self._random_payload()

        return (
        {
            "event_id": event_id,
            "signal_type": 0,
            "timestamp": start_ts,
            "payload": payload,
        },
        {
            "event_id": event_id,
            "signal_type": 1,
            "timestamp": end_ts,
            "payload": payload,
        }
    )

    def _apply_controlled_disorder(self, events):
        """
        Reorder events with bounded out-of-orderness.

        Guarantee:
        Any event can only move ahead of events within +delayed ms
        """

        if self.delayed == 0:
            return sorted(events, key=lambda x: x["timestamp"])

        delay = timedelta(milliseconds=self.delayed)

        # Step 1: sort by event-time
        events = sorted(events, key=lambda x: x["timestamp"])

        result = []
        buffer = []
        i = 0
        n = len(events)

        while i < n or buffer:

            # Fill buffer if empty
            if not buffer:
                current_time = events[i]["timestamp"]
                buffer.append(events[i])
                i += 1

                while i < n and events[i]["timestamp"] <= current_time + delay:
                    buffer.append(events[i])
                    i += 1

            # Randomly emit one event from buffer
            idx = random.randint(0, len(buffer) - 1)
            chosen = buffer.pop(idx)
            result.append(chosen)

            # Expand buffer window based on last emitted event
            if i < n:
                current_time = chosen["timestamp"]
                while i < n and events[i]["timestamp"] <= current_time + delay:
                    buffer.append(events[i])
                    i += 1

        return result



    def generate(self):
        """
        Generate full dataset with controlled disorder
        """

        events = []

        for event_id in range(self.num_events):
            start_event, end_event = self._generate_span(event_id)
            events.append(start_event)
            events.append(end_event)

        # Apply bounded out-of-order logic
        events = self._apply_controlled_disorder(events)

        return events

    def write_csv(self, filepath: str):
        """
        Write events to CSV
        """

        events = self.generate()

        with open(filepath, mode='w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["event_id", "signal_type", "timestamp", "payload"])

            for e in events:
                writer.writerow([
                    e["event_id"],
                    e["signal_type"],
                    e["timestamp"].isoformat(),
                    e["payload"],
                ])

if __name__=='__main__':

    gen = SpanDataGen(
        num_events=10,
        start_time=datetime.now(),
        min_span_ms=10000,
        max_span_ms=20000,
        delayed=5000,
        seed=42,
        events_per_sec=10000
    )
    gen.write_csv("span_events.csv")
