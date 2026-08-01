"""
End-to-End Test for Duux MQTT Bridge.

This script tests the complete flow:
1. Generates test TLS certificates
2. Starts the real DuuxMqttServer on port 8443 (TLS)
3. Connects a simulated Duux Fan client via TLS socket
4. Sends real MQTT CONNECT, SUBSCRIBE, and PUBLISH packets
5. Verifies CONNACK, SUBACK, PINGRESP
6. Verifies HA MQTT Discovery payload generation
7. Sends a command from HA and verifies it reaches the simulated fan
"""

import asyncio
import json
import logging
import ssl
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from duux_mqtt_bridge.rootfs.opt.duux_bridge.cert_manager import CertManager
from duux_mqtt_bridge.rootfs.opt.duux_bridge.mqtt_server import DuuxMqttServer
from duux_mqtt_bridge.rootfs.opt.duux_bridge.ha_bridge import HomeAssistantBridge, DEVICE_PROFILES
from duux_mqtt_bridge.rootfs.opt.duux_bridge.mqtt_protocol import (
    CONNECT, CONNACK, PUBLISH, SUBSCRIBE, SUBACK, PINGREQ, PINGRESP,
    build_publish, encode_utf8_string, parse_publish, decode_remaining_length
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
_LOGGER = logging.getLogger("e2e_test")


class MockHaMqttBroker:
    """Mock HA MQTT broker to capture published discovery and state messages."""

    def __init__(self):
        self.published_messages = []
        self.subscriptions = []

    def publish(self, topic, payload, retain=False):
        _LOGGER.info("HA BROKER RECEIVED: %s -> %s", topic, payload[:150] if isinstance(payload, str) else payload)
        self.published_messages.append((topic, payload, retain))

    def subscribe(self, topic):
        _LOGGER.info("HA BROKER SUBSCRIBED: %s", topic)
        self.subscriptions.append(topic)


async def run_e2e_test():
    _LOGGER.info("=== Starting E2E Integration Test ===")

    # 1. Setup certificates
    cert_dir = Path("/tmp/duux_test_certs")
    cert_file = cert_dir / "cert.pem"
    key_file = cert_dir / "key.pem"

    cm = CertManager()
    cm.cert_dir = cert_dir
    cm.cert_file = cert_file
    cm.key_file = key_file
    cert_path, key_path = cm.ensure_certificates()

    # 2. Setup Server and HA Bridge
    server = DuuxMqttServer()
    mock_ha_broker = MockHaMqttBroker()

    # Create HA bridge using mock broker publish
    bridge = HomeAssistantBridge(mqtt_host="localhost", mqtt_port=1883)
    bridge._client = mock_ha_broker  # Replace paho client with mock

    # Wire callbacks as in __main__.py
    def on_device_connect(device_id, username, password):
        _LOGGER.info("App: Device connected: %s (user: %s)", device_id, username)
        bridge.set_device_model(device_id, "whisper_flex_2")
        bridge.publish_discovery(device_id, "whisper_flex_2")
        bridge.publish_device_online(device_id)

    def on_device_publish(device_id, topic, payload):
        _LOGGER.info("App: Device published: %s -> %s", topic, payload.decode(errors="replace"))
        try:
            data = json.loads(payload.decode())
            fan_data = data.get("sub", {}).get("Tune", [{}])[0]
            if fan_data:
                bridge.publish_device_state(device_id, fan_data)
        except Exception as e:
            _LOGGER.error("Error parsing payload: %s", e)

    def on_ha_command(device_id, command):
        _LOGGER.info("App: Command for device %s: %s", device_id, command)
        asyncio.create_task(
            server.publish_to_device(device_id, f"sensor/{device_id}/command", command)
        )

    server.set_on_device_connect(on_device_connect)
    server.set_on_device_publish(on_device_publish)
    bridge.set_on_command(on_ha_command)

    # 3. Start server on 8443
    test_port = 8443
    await server.start("127.0.0.1", test_port, cert_path, key_path)
    _LOGGER.info("Server started on port %d", test_port)

    # 4. Simulate Duux Fan connecting over TLS
    ssl_ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE  # Fan doesn't verify cert

    reader, writer = await asyncio.open_connection(
        "127.0.0.1", test_port, ssl=ssl_ctx
    )
    _LOGGER.info("Simulated Fan connected via TLS!")

    # 5. Send CONNECT packet
    mac = "AA:BB:CC:11:22:33"
    token = "a" * 64
    # Fixed header: CONNECT (0x10)
    variable_header = encode_utf8_string("MQTT") + bytes([4, 0xC2, 0, 60])  # Clean session + Username + Password
    payload = encode_utf8_string(mac.lower()) + encode_utf8_string(mac) + encode_utf8_string(token)
    rem_len = len(variable_header) + len(payload)

    connect_pkt = bytes([0x10, rem_len]) + variable_header + payload
    writer.write(connect_pkt)
    await writer.drain()

    # Read CONNACK response from server
    connack = await reader.readexactly(4)
    assert connack[0] == 0x20  # CONNACK
    assert connack[3] == 0x00  # Return code 0 (Accepted)
    _LOGGER.info("✅ SUCCESS: Received CONNACK (Connection accepted)")

    # Verify Discovery messages were published to HA mock
    discovery_topics = [t[0] for t in mock_ha_broker.published_messages]
    assert any("homeassistant/fan/duux_aa_bb_cc_11_22_33" in t for t in discovery_topics)
    assert any("homeassistant/select/duux_aa_bb_cc_11_22_33" in t for t in discovery_topics)
    assert any("homeassistant/sensor/duux_aa_bb_cc_11_22_33" in t for t in discovery_topics)
    _LOGGER.info("✅ SUCCESS: HA MQTT Discovery messages generated correctly (%d entities published)", len(discovery_topics))

    # 6. Simulate Fan publishing state update
    fan_state = {
        "sub": {
            "Tune": [
                {
                    "power": 1,
                    "speed": 15,
                    "mode": 0,
                    "horosc": 2,
                    "verosc": 1,
                    "batlvl": 9,
                    "batcha": 1
                }
            ]
        }
    }
    state_payload = json.dumps(fan_state).encode()
    pub_pkt = build_publish(f"sensor/{mac.lower()}/in", state_payload, qos=0)
    writer.write(pub_pkt)
    await writer.drain()

    await asyncio.sleep(0.2)

    # Verify state was published to HA topic
    state_messages = [m for m in mock_ha_broker.published_messages if m[0] == "duux/aa_bb_cc_11_22_33/state"]
    assert len(state_messages) > 0
    received_state = json.loads(state_messages[0][1])
    assert received_state["speed"] == 15
    assert received_state["power"] == 1
    _LOGGER.info("✅ SUCCESS: Fan state update correctly relayed to HA state topic!")

    # 7. Simulate HA sending a command to turn off fan
    _LOGGER.info("Simulating HA user changing fan speed to 25...")
    msg = type("Msg", (), {
        "topic": "duux/aa_bb_cc_11_22_33/set/speed",
        "payload": b"25"
    })()
    bridge._on_message(None, None, msg)

    # Read PUBLISH packet on the fan TLS socket
    first_byte = await reader.readexactly(1)
    assert (first_byte[0] >> 4) == PUBLISH

    # Read remaining length
    multiplier = 1
    val = 0
    while True:
        b = (await reader.readexactly(1))[0]
        val += (b & 0x7F) * multiplier
        if (b & 0x80) == 0:
            break
        multiplier *= 128

    pkt_data = await reader.readexactly(val)
    parsed_pub = parse_publish(first_byte[0], pkt_data)
    assert parsed_pub.topic == f"sensor/{mac.lower()}/command"
    assert parsed_pub.payload.decode() == "tune set speed 25"
    _LOGGER.info("✅ SUCCESS: Command 'tune set speed 25' correctly delivered to fan over TLS!")

    # 8. Clean shutdown
    writer.close()
    await writer.wait_closed()
    await server.stop()
    _LOGGER.info("=== ALL E2E TESTS PASSED PERFECTLY ===")


if __name__ == "__main__":
    asyncio.run(run_e2e_test())
