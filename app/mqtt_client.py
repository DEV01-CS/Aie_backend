import os, json, asyncio, logging
import paho.mqtt.client as mqtt

class MQTTIngestor:
    def __init__(self, queue: asyncio.Queue):
        self.queue = queue
        self.client = None
        self.loop = asyncio.get_event_loop()
        self.host = os.getenv("MQTT_HOST", "mqtt")
        self.port = int(os.getenv("MQTT_PORT", "1883"))
        self.topic = os.getenv("MQTT_TOPIC", "telemetry/data")

    def on_connect(self, client, userdata, flags, reason_code, properties=None):
        logging.info(f"MQTT connected rc={reason_code}, subscribing {self.topic}")
        client.subscribe(self.topic)

    def on_message(self, client, userdata, msg):
        # payload_str = msg.payload.decode(errors="ignore")
        # # Push raw payload to async queue; consumer validates + writes
        # self.loop.call_soon_threadsafe(self.queue.put_nowait, (payload_str, msg.topic))
        # Push raw bytes to async queue; consumer decodes + validates  #Apply CBOR here
        payload_bytes = msg.payload
        self.loop.call_soon_threadsafe(self.queue.put_nowait, (payload_bytes, msg.topic))


    def start(self):
        self.client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect(self.host, self.port, 60)
        self.client.loop_start()

    def stop(self):
        try:
            self.client.loop_stop()
            self.client.disconnect()
        except Exception:
            pass
