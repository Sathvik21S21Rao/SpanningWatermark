import json
import time
import yaml
from datetime import datetime
from confluent_kafka import Producer

from DataGen import SpanDataGen



def load_config(path="config.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def create_producer(kafka_conf):
    return Producer(kafka_conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] {err}")

def create_generator(config):
    gen_cfg = config["generator"]

    start_time_str = gen_cfg.get("start_time")
    start_time = datetime.fromisoformat(start_time_str) if start_time_str else datetime.now()

    return SpanDataGen(
        num_events=gen_cfg["num_events"],
        start_time=start_time,  # ← now uses current time if not specified
        min_span_ms=gen_cfg["min_span_ms"],
        max_span_ms=gen_cfg["max_span_ms"],
        delayed=gen_cfg.get("delayed", 0),
        events_per_sec=gen_cfg["events_per_sec"],
        seed=gen_cfg.get("seed", None),
    )


def stream_events(generator, producer, topic, data_conf):
    events = generator.generate()

    rate = data_conf["generator"]["events_per_sec"]

    delay = 1.0 / rate if rate > 0 else 0

    sent = 0

    print(f"Streaming {len(events)} events at ~{rate} signals/sec")

    for i,e in enumerate(events):
        
        payload = {
            "event_id": e["event_id"],
            "signal_type": e["signal_type"],
            "timestamp": e["timestamp"].isoformat(timespec="milliseconds")+"Z",
            "payload": e["payload"],
        }

        producer.produce(
            topic=topic,
            key=str(e["event_id"]),   # ensures span consistency
            value=json.dumps(payload),
            callback=delivery_report
        )

        producer.poll(0)

        sent += 1
        
        # Throughput control (per-event pacing)
        if delay > 0:
            time.sleep(delay)

    producer.flush()
    print(f"Done. Sent {sent} events.")


def main():
    config = load_config()

    topic = config["kafka"]["topic"]
    kafka_conf = config["kafka"]["config"]
    data_conf = config["data"]

    producer = create_producer(kafka_conf)
    generator = create_generator(data_conf)

    stream_events(generator, producer, topic, data_conf)


if __name__ == "__main__":
    main()
