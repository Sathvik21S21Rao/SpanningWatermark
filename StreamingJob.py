from datetime import datetime
import yaml
import os

from pyflink.common import Types, Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from WatermarkGen.WatermarkGen import *
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
)


class SpanPipeline:
    """Pipeline for processing span events with watermarks"""
    
    def __init__(self, config_path=None):
        """
        Initialize SpanPipeline with configuration
        
        Args:
            config_path: Path to config.yaml file. If None, loads from default location
        """
        if config_path is None:
            config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
        
        with open(config_path, "r") as f:
            cfg = yaml.safe_load(f)
        
        self.flink_cfg = cfg["flink"]
        self.kafka_cfg = cfg["kafka"]
        self.topics_cfg = cfg["topics"]
        self.watermark_cfg = cfg["watermark"]
        
        self.flink_config = Configuration()
        self.flink_config.set_string("rest.bind-address", self.flink_cfg["rest_bind_address"])
        self.flink_config.set_string("rest.port", str(self.flink_cfg["rest_port"]))
        
        self.completed_events = set()
        self.uncompleted_events = set()
    
    def run(self):
        env = StreamExecutionEnvironment.get_execution_environment(self.flink_config)
        env.set_parallelism(self.flink_cfg["parallelism"])

        env.add_jars(*self.flink_cfg["jars"])

        watermark_strategy = PeriodicWatermark if self.watermark_cfg["strategy"] == "periodic" else Adwin

        source = KafkaSource.builder() \
            .set_bootstrap_servers(self.kafka_cfg["bootstrap_servers"]) \
            .set_topics(self.topics_cfg["input"]) \
            .set_group_id(self.kafka_cfg["group_id"]) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()

        stream = env.from_source(source, watermark_strategy, "KafkaSource")
        
        env.execute("SpanPipeline")


if __name__ == "__main__":
    pipeline = SpanPipeline()
    pipeline.run()


