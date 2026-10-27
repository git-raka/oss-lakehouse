"""Electric-bus telemetry simulator -> MQTT (estate/bus/<bus_id>/telemetry) every 5 seconds."""
import json
import os
import random
import time
from datetime import datetime

import paho.mqtt.client as mqtt

BUSES = [f"BUS-{i:02d}" for i in range(1, 9)]
INTERVAL = float(os.getenv("IOT_INTERVAL_SEC", "5"))


def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="bus-simulator")
    while True:
        try:
            client.connect(os.getenv("MQTT_HOST", "mosquitto"), 1883)
            break
        except OSError:
            time.sleep(3)
    client.loop_start()
    state = {b: {"soc": random.uniform(60, 100), "odo": 20000 + i * 3500,
                 "lat": -6.93 + random.uniform(-0.01, 0.01), "lon": 109.85 + random.uniform(-0.01, 0.01)}
             for i, b in enumerate(BUSES)}
    print(f"Bus simulator running: {len(BUSES)} buses, interval {INTERVAL}s", flush=True)
    while True:
        for bus, s in state.items():
            speed = max(0.0, random.gauss(22, 9)) if random.random() > 0.2 else 0.0
            km = speed * INTERVAL / 3600
            s["odo"] += km
            s["soc"] = s["soc"] - km * random.uniform(0.35, 0.55)
            if s["soc"] < 15:
                s["soc"] = 98.0  # back from charging
            s["lat"] += random.uniform(-0.0003, 0.0003)
            s["lon"] += random.uniform(-0.0003, 0.0003)
            client.publish(f"estate/bus/{bus}/telemetry", json.dumps({
                "bus_id": bus, "ts": datetime.now().isoformat(timespec="seconds"),
                "lat": round(s["lat"], 6), "lon": round(s["lon"], 6), "speed_kmh": round(speed, 1),
                "soc_pct": round(s["soc"], 1), "odometer_km": round(s["odo"], 3),
                "energy_kwh": round(km * random.uniform(0.9, 1.3), 4),
                "boarding": random.choices([0, 1, 2, 3], [85, 9, 4, 2])[0],
            }), qos=1)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
