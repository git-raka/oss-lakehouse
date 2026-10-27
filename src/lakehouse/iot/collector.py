"""Edge IoT consumer: buffers MQTT messages and flushes them to the landing zone every 60 seconds.
If S3 is unreachable (uplink down), the buffer is kept and shipped on the next flush."""
import os
import threading
import time
from datetime import datetime

import paho.mqtt.client as mqtt

from lakehouse.common import LANDING_BUCKET, ensure_buckets, s3

FLUSH_SEC = int(os.getenv("IOT_FLUSH_SEC", "60"))
buffer: list[str] = []
lock = threading.Lock()


def on_message(_client, _userdata, msg):
    with lock:
        buffer.append(msg.payload.decode())


def main():
    while True:
        try:
            ensure_buckets()
            break
        except Exception as e:  # storage not ready yet
            print("waiting for storage:", e, flush=True)
            time.sleep(5)
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="edge-collector")
    client.on_message = on_message
    client.on_connect = lambda c, *_: c.subscribe("estate/bus/+/telemetry", qos=1)
    while True:
        try:
            client.connect(os.getenv("MQTT_HOST", "mosquitto"), 1883)
            break
        except OSError:
            time.sleep(3)
    client.loop_start()
    store = s3()
    print(f"Collector running, flushing every {FLUSH_SEC}s to s3://{LANDING_BUCKET}/iot_bus/", flush=True)
    while True:
        time.sleep(FLUSH_SEC)
        with lock:
            batch = buffer[:]
        if not batch:
            continue
        key = f"iot_bus/live_{datetime.now():%Y%m%dT%H%M%S}.jsonl"
        try:
            store.put_object(Bucket=LANDING_BUCKET, Key=key, Body=("\n".join(batch) + "\n").encode())
            with lock:
                del buffer[:len(batch)]
            print(f"flushed {len(batch)} messages -> {key}", flush=True)
        except Exception as e:
            print(f"upload failed, keeping buffer ({len(batch)} messages): {e}", flush=True)


if __name__ == "__main__":
    main()
