from paho.mqtt import client as mqtt_client
import os


class MQTTResource:
    def __init__(self):
        self.client: mqtt_client.Client | None = None

    async def connect(self):
        if self.client is None:
            self.client = mqtt_client.Client()
            broker = os.environ.get("MQTT_BROKER", "localhost")
            port = int(os.environ.get("MQTT_PORT", 1883))
            topic = os.environ.get("MQTT_TOPIC", "bitswan/topology")

            def on_connect(client, userdata, flags, rc):
                if rc == 0:
                    print("Connected to MQTT Broker!")
                    client.subscribe(topic)
                else:
                    print(f"Failed to connect, return code {rc}")

            self.client.on_connect = on_connect
            self.client.connect(broker, port)
            self.client.loop_start()
            print("MQTT client connected and loop started")

    async def disconnect(self):
        if self.client is not None:
            self.client.loop_stop()
            self.client.disconnect()
            self.client = None
            print("MQTT client disconnected")

    def get_client(self) -> mqtt_client.Client:
        if self.client is None:
            raise RuntimeError("MQTT client is not initialized")
        return self.client


mqtt_resource = MQTTResource()


async def get_mqtt_client() -> mqtt_client.Client:
    await mqtt_resource.connect()
    return mqtt_resource.get_client()
