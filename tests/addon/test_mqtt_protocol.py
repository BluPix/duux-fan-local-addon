"""
Tests for the MQTT 3.1.1 protocol parser and builder.
"""

import struct
import pytest

from duux_mqtt_bridge.rootfs.opt.duux_bridge.mqtt_protocol import (
    CONNECT,
    CONNACK,
    PUBLISH,
    PUBACK,
    SUBSCRIBE,
    SUBACK,
    PINGREQ,
    PINGRESP,
    DISCONNECT,
    ConnectPacket,
    PublishPacket,
    SubscribeRequest,
    decode_remaining_length,
    encode_remaining_length,
    decode_utf8_string,
    encode_utf8_string,
    parse_connect,
    build_connack,
    parse_subscribe,
    build_suback,
    parse_publish,
    build_publish,
    build_pingresp,
)


class TestRemainingLength:
    """Tests for MQTT remaining length encoding/decoding."""

    def test_single_byte_length(self):
        """Length 0-127 should be encoded as a single byte."""
        for length in [0, 1, 64, 127]:
            encoded = encode_remaining_length(length)
            decoded, consumed = decode_remaining_length(encoded, 0)
            assert decoded == length
            assert consumed == 1

    def test_two_byte_length(self):
        """Length 128-16383 should be encoded as two bytes."""
        for length in [128, 256, 16383]:
            encoded = encode_remaining_length(length)
            decoded, consumed = decode_remaining_length(encoded, 0)
            assert decoded == length
            assert consumed == 2

    def test_three_byte_length(self):
        """Length 16384-2097151 should be encoded as three bytes."""
        length = 16384
        encoded = encode_remaining_length(length)
        decoded, consumed = decode_remaining_length(encoded, 0)
        assert decoded == length
        assert consumed == 3

    def test_zero_length(self):
        """Zero should encode to a single zero byte."""
        encoded = encode_remaining_length(0)
        assert encoded == b"\x00"
        decoded, consumed = decode_remaining_length(encoded, 0)
        assert decoded == 0
        assert consumed == 1

    def test_offset_decode(self):
        """Decoding should work with a non-zero offset."""
        prefix = b"\xFF\xFF"
        encoded = encode_remaining_length(200)
        data = prefix + encoded
        decoded, consumed = decode_remaining_length(data, 2)
        assert decoded == 200


class TestUtf8String:
    """Tests for MQTT UTF-8 string encoding/decoding."""

    def test_encode_simple_string(self):
        """A simple ASCII string should be length-prefixed."""
        result = encode_utf8_string("hello")
        assert result == b"\x00\x05hello"

    def test_encode_empty_string(self):
        """An empty string should have zero length prefix."""
        result = encode_utf8_string("")
        assert result == b"\x00\x00"

    def test_decode_simple_string(self):
        """Should decode a length-prefixed string."""
        data = b"\x00\x05hello"
        string, new_offset = decode_utf8_string(data, 0)
        assert string == "hello"
        assert new_offset == 7

    def test_roundtrip(self):
        """Encode then decode should return the original string."""
        test_strings = ["", "hello", "AA:BB:CC:DD:EE:FF", "collector3.cloudgarden.nl"]
        for s in test_strings:
            encoded = encode_utf8_string(s)
            decoded, _ = decode_utf8_string(encoded, 0)
            assert decoded == s

    def test_decode_with_offset(self):
        """Should correctly handle offset into data."""
        prefix = b"\xFF\xFF"
        encoded = encode_utf8_string("test")
        data = prefix + encoded
        string, new_offset = decode_utf8_string(data, 2)
        assert string == "test"
        assert new_offset == 8


class TestConnectPacket:
    """Tests for CONNECT packet parsing."""

    def _build_connect_packet(
        self,
        client_id="test_client",
        username=None,
        password=None,
        clean_session=True,
        keep_alive=60,
    ):
        """Helper to build a raw CONNECT variable header + payload."""
        # Protocol name
        data = encode_utf8_string("MQTT")
        # Protocol level (4 = MQTT 3.1.1)
        data += bytes([4])
        # Connect flags
        flags = 0
        if clean_session:
            flags |= 0x02
        if username:
            flags |= 0x80
        if password:
            flags |= 0x40
        data += bytes([flags])
        # Keep alive
        data += struct.pack("!H", keep_alive)
        # Payload: client_id
        data += encode_utf8_string(client_id)
        if username:
            data += encode_utf8_string(username)
        if password:
            data += encode_utf8_string(password)
        return data

    def test_basic_connect(self):
        """Parse a basic CONNECT with no auth."""
        data = self._build_connect_packet(
            client_id="my_device", keep_alive=120
        )
        pkt = parse_connect(data)
        assert pkt.client_id == "my_device"
        assert pkt.username is None
        assert pkt.password is None
        assert pkt.clean_session is True
        assert pkt.keep_alive == 120

    def test_connect_with_credentials(self):
        """Parse a CONNECT with username and password (like Duux fan)."""
        data = self._build_connect_packet(
            client_id="aa:bb:cc:dd:ee:ff",
            username="AA:BB:CC:DD:EE:FF",
            password="a" * 64,
            keep_alive=60,
        )
        pkt = parse_connect(data)
        assert pkt.client_id == "aa:bb:cc:dd:ee:ff"
        assert pkt.username == "AA:BB:CC:DD:EE:FF"
        assert pkt.password == "a" * 64
        assert pkt.keep_alive == 60

    def test_connect_wrong_protocol(self):
        """Should raise ValueError for non-MQTT protocol."""
        data = encode_utf8_string("MQIsdp") + bytes([3, 0x02, 0, 60])
        data += encode_utf8_string("client")
        with pytest.raises(ValueError, match="Unsupported protocol"):
            parse_connect(data)


class TestConnack:
    """Tests for CONNACK building."""

    def test_build_connack_success(self):
        """CONNACK for successful connection."""
        packet = build_connack(session_present=False, return_code=0)
        assert packet[0] == CONNACK << 4
        assert packet[2] == 0  # No session present
        assert packet[3] == 0  # Connection accepted

    def test_build_connack_with_session(self):
        """CONNACK with session present flag."""
        packet = build_connack(session_present=True, return_code=0)
        assert packet[2] == 1  # Session present


class TestSubscribe:
    """Tests for SUBSCRIBE parsing and SUBACK building."""

    def test_parse_single_subscription(self):
        """Parse SUBSCRIBE with one topic."""
        # Packet ID
        data = struct.pack("!H", 1)
        # Topic
        data += encode_utf8_string("sensor/aa:bb:cc/command")
        # QoS
        data += bytes([1])

        packet_id, subs = parse_subscribe(data)
        assert packet_id == 1
        assert len(subs) == 1
        assert subs[0].topic == "sensor/aa:bb:cc/command"
        assert subs[0].qos == 1

    def test_parse_multiple_subscriptions(self):
        """Parse SUBSCRIBE with multiple topics (like Duux fan)."""
        data = struct.pack("!H", 42)
        for topic in ["sensor/mac/command", "sensor/mac/config", "sensor/mac/fw"]:
            data += encode_utf8_string(topic) + bytes([0])

        packet_id, subs = parse_subscribe(data)
        assert packet_id == 42
        assert len(subs) == 3
        assert subs[0].topic == "sensor/mac/command"
        assert subs[1].topic == "sensor/mac/config"
        assert subs[2].topic == "sensor/mac/fw"

    def test_build_suback(self):
        """SUBACK should echo the packet ID and granted QoS levels."""
        packet = build_suback(42, [0, 1, 0])
        assert packet[0] == SUBACK << 4
        # Packet ID
        assert struct.unpack("!H", packet[2:4])[0] == 42
        # Granted QoS
        assert packet[4:7] == bytes([0, 1, 0])


class TestPublish:
    """Tests for PUBLISH parsing and building."""

    def test_parse_qos0_publish(self):
        """Parse a QoS 0 PUBLISH (like state updates from Duux fan)."""
        topic = "sensor/aa:bb:cc/in"
        payload = b'{"sub":{"Tune":[{"power":1}]}}'
        first_byte = PUBLISH << 4  # QoS 0, no retain
        data = encode_utf8_string(topic) + payload

        pkt = parse_publish(first_byte, data)
        assert pkt.topic == topic
        assert pkt.payload == payload
        assert pkt.qos == 0
        assert pkt.retain is False
        assert pkt.packet_id is None

    def test_parse_qos1_publish(self):
        """Parse a QoS 1 PUBLISH."""
        topic = "sensor/aa:bb:cc/in"
        payload = b"test"
        first_byte = (PUBLISH << 4) | 0x02  # QoS 1
        data = encode_utf8_string(topic)
        data += struct.pack("!H", 123)  # Packet ID
        data += payload

        pkt = parse_publish(first_byte, data)
        assert pkt.topic == topic
        assert pkt.payload == payload
        assert pkt.qos == 1
        assert pkt.packet_id == 123

    def test_parse_retained_publish(self):
        """Parse a retained PUBLISH."""
        first_byte = (PUBLISH << 4) | 0x01  # Retain
        data = encode_utf8_string("test/topic") + b"retained data"

        pkt = parse_publish(first_byte, data)
        assert pkt.retain is True

    def test_build_publish_qos0(self):
        """Build a QoS 0 PUBLISH (used for commands to Duux fan)."""
        packet = build_publish("sensor/mac/command", "tune set speed 10")
        # Verify first byte
        assert (packet[0] >> 4) == PUBLISH
        assert (packet[0] & 0x06) >> 1 == 0  # QoS 0

    def test_build_publish_roundtrip(self):
        """Build then parse should return the same data."""
        topic = "sensor/aa:bb:cc:dd:ee:ff/command"
        payload = "tune set speed 15"

        built = build_publish(topic, payload, qos=0, retain=False)
        # Skip fixed header to get to variable header + payload
        first_byte = built[0]
        # Decode remaining length
        remaining_length, length_bytes = decode_remaining_length(built, 1)
        data = built[1 + length_bytes:]

        parsed = parse_publish(first_byte, data)
        assert parsed.topic == topic
        assert parsed.payload == payload.encode("utf-8")
        assert parsed.qos == 0
        assert parsed.retain is False


class TestPingresp:
    """Tests for PINGRESP building."""

    def test_build_pingresp(self):
        """PINGRESP should be exactly 2 bytes."""
        packet = build_pingresp()
        assert len(packet) == 2
        assert packet[0] == PINGRESP << 4
        assert packet[1] == 0  # No remaining payload
