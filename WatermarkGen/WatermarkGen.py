from pyflink.common import WatermarkStrategy
from pyflink.common.watermark_strategy import Watermark
from pyflink.datastream import WatermarkGenerator


class PeriodicWatermark(WatermarkGenerator):

    def __init__(self, max_out_of_orderness_ms: int):
        self.max_ts = float('-inf')
        self.max_out_of_orderness = max_out_of_orderness_ms
        self.last_emitted_wm = float('-inf')

    def on_event(self, event, event_timestamp: int, output):
        pass
    def on_periodic_emit(self, output): 
        pass

class Adwin(WatermarkGenerator):
    
    def __init__(self, max_out_of_orderness_ms: int, **kwargs):
        self.max_ts = float('-inf')
        self.max_out_of_orderness = max_out_of_orderness_ms
        self.last_emitted_wm = float('-inf')

    def on_event(self, event, event_timestamp: int, output): # all logic here and we need to use emit watermark in on_event
        pass

    def on_periodic_emit(self, output): # empty for adwin
        pass

# Have to set env.get_config().set_auto_watermark_interval(0) for adwin
# output.emit_watermark(Watermark(candidate_wm)), output is passed as an argument
# for Adwin logic we can use a separate class and call the function within on_event here
