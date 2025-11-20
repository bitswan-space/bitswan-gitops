from paho.mqtt import client as mqtt_client
import os


class MQTTResource:
    def __init__(self):
        self.client: mqtt_client.Client | None = None

    async def connect(self) -> bool:
        if self.client is None:
            self.client = mqtt_client.Client(transport="websockets")
            broker = os.environ.get("MQTT_BROKER")
            if not broker:
                print("There is no MQTT_BROKER env var..skipping connection to MQTT")
                return False
            port = int(os.environ.get("MQTT_PORT", 443))

            if port == 443:
                # external communication with the public MQTT broker
                self.client.tls_set(cert_reqs=mqtt_client.ssl.CERT_REQUIRED)
            else:
                # internal communication between bitswan services
                self.client.tls_set(cert_reqs=mqtt_client.ssl.CERT_NONE)

            def on_connect(client, userdata, flags, rc):
                if rc == 0:
                    print("Connected to MQTT Broker!")
                else:
                    print(f"Failed to connect, return code {rc}")

            self.client.on_connect = on_connect
            username, password = (
                os.environ.get("MQTT_USERNAME"),
                os.environ.get("MQTT_PASSWORD"),
            )
            if username and password:
                self.client.username_pw_set(username, password)
            self.client.connect(broker, port)
            self.client.loop_start()
            print("MQTT client connected and loop started")
            return True

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
