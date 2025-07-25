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
                print("ERROR: MQTT_BROKER environment variable is not set - skipping MQTT connection")
                return False
            
            try:
                port = int(os.environ.get("MQTT_PORT", 443))
            except ValueError:
                print("ERROR: MQTT_PORT must be a valid integer")
                return False

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
                    print(f"Failed to connect to MQTT Broker, return code {rc}")

            self.client.on_connect = on_connect
            username, password = os.environ.get("MQTT_USERNAME"), os.environ.get(
                "MQTT_PASSWORD"
            )
            if username and password:
                self.client.username_pw_set(username, password)
            else:
                print("WARNING: MQTT_USERNAME or MQTT_PASSWORD not set - attempting anonymous connection")
            
            try:
                print(f"Attempting to connect to MQTT broker: {broker}:{port}")
                self.client.connect(broker, port)
                self.client.loop_start()
                print("MQTT client connected and loop started")
                return True
            except Exception as e:
                print(f"ERROR: Failed to connect to MQTT broker: {e}")
                return False

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
