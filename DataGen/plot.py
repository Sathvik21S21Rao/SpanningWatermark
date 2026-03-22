# Plot Event Time vs Processing Time and connect points with same event_id

import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import csv
import random
# Input data
fh = open("span_events.csv")
reader = csv.reader(fh)
next(reader,None)
data = [line for line in reader]
# Parse events
events = []
for eid, sig, ts, payload in data:
    events.append({
        "event_id": eid,
        "signal_type": sig,
        "event_time": datetime.fromisoformat(ts)
    })

# Assign processing-time (arrival order)
base_ptime = datetime.now()
start_events = 0
incomplete_events = {}
for i, e in enumerate(events):
    if e.get("signal_type")=='0':
        e["processing_time"] = base_ptime + timedelta(milliseconds=start_events * 4000)
        start_events+=1
        incomplete_events[e["event_id"]]=(e["event_time"],e["processing_time"])
    else:
        e["processing_time"]=incomplete_events[e["event_id"]][1]+(e["event_time"]-incomplete_events[e["event_id"]][0])+timedelta(milliseconds=random.randint(0,7000))
        incomplete_events.pop(e["event_id"])

# Group by event_id
grouped = {}
for e in events:
    eid = e["event_id"]
    if eid not in grouped:
        grouped[eid] = []
    grouped[eid].append(e)

# Plot
plt.figure()

for eid, group in grouped.items():
    
    pts = [e["processing_time"] for e in group]
    ets = [e["event_time"] for e in group]
    
    plt.plot(pts, ets, marker='o')

plt.xlabel("Processing Time")
plt.ylabel("Event Time")
plt.title("Event vs Processing Time (Connected by Event ID)")

plt.tight_layout()
plt.show()
