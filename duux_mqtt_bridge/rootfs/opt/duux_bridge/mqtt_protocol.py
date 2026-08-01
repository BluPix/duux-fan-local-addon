"""
MQTT 3.1.1 minimal protocol parser and builder.
Implements the subset of MQTT 3.1.1 needed for the Duux MQTT Bridge.
"""

from dataclasses import dataclass
from typing import Optional, Union
import struct

# Packet type constants
CONNECT = 1
CONNACK = 2
PUBLISH = 3
PUBACK = 4
SUBSCRIBE = 8
SUBACK = 9
PINGREQ = 12
PINGRESP = 13
DISCONNECT = 14

@dataclass
class ConnectPacket:
    """Represents an MQTT CONNECT packet."""
    client_id: str
    username: Optional[str]
    password: Optional[str]
    clean_session: bool
    keep_alive: int

@dataclass
class PublishPacket:
    """Represents an MQTT PUBLISH packet."""
    topic: str
    payload: bytes
    qos: int
    retain: bool
    packet_id: Optional[int]

@dataclass
class SubscribeRequest:
    """Represents a single topic subscription request."""
    topic: str
    qos: int

def decode_remaining_length(data: bytes, offset: int) -> tuple[int, int]:
    """
    Decodes the remaining length field from MQTT packet header.
    Returns (length, bytes_consumed).
    """
    multiplier = 1
    value = 0
    bytes_consumed = 0
    
    while True:
        if offset + bytes_consumed >= len(data):
            raise ValueError("Incomplete remaining length")
        
        encoded_byte = data[offset + bytes_consumed]
        bytes_consumed += 1
        
        value += (encoded_byte & 127) * multiplier
        if (encoded_byte & 128) == 0:
            break
            
        multiplier *= 128
        if multiplier > 128*128*128:
            raise ValueError("Malformed remaining length")
            
    return value, bytes_consumed

def encode_remaining_length(length: int) -> bytes:
    """
    Encodes the remaining length for an MQTT packet header.
    Returns the encoded bytes.
    """
    if length == 0:
        return b'\x00'
        
    encoded = bytearray()
    while length > 0:
        encoded_byte = length % 128
        length = length // 128
        if length > 0:
            encoded_byte = encoded_byte | 128
        encoded.append(encoded_byte)
        
    return bytes(encoded)

def decode_utf8_string(data: bytes, offset: int) -> tuple[str, int]:
    """
    Decodes an MQTT UTF-8 string, which consists of a 2-byte length followed by the string bytes.
    Returns (string, new_offset).
    """
    if offset + 2 > len(data):
        raise ValueError("Incomplete string length")
        
    str_len = struct.unpack("!H", data[offset:offset+2])[0]
    
    if offset + 2 + str_len > len(data):
        raise ValueError("Incomplete string data")
        
    string_data = data[offset+2:offset+2+str_len]
    return string_data.decode('utf-8'), offset + 2 + str_len

def encode_utf8_string(s: str) -> bytes:
    """
    Encodes a string into an MQTT UTF-8 string format (2-byte length prefix).
    """
    encoded_str = s.encode('utf-8')
    return struct.pack("!H", len(encoded_str)) + encoded_str

def parse_connect(data: bytes) -> ConnectPacket:
    """
    Parses the variable header and payload of an MQTT CONNECT packet.
    Expects data to be just the variable header and payload (stripped of fixed header).
    """
    protocol_name, offset = decode_utf8_string(data, 0)
    if protocol_name != "MQTT":
        raise ValueError(f"Unsupported protocol: {protocol_name}")
        
    protocol_level = data[offset]
    if protocol_level != 4:  # MQTT 3.1.1
        raise ValueError(f"Unsupported protocol level: {protocol_level}")
        
    connect_flags = data[offset+1]
    
    clean_session = bool((connect_flags >> 1) & 1)
    has_will = bool((connect_flags >> 2) & 1)
    has_username = bool((connect_flags >> 7) & 1)
    has_password = bool((connect_flags >> 6) & 1)
    
    keep_alive = struct.unpack("!H", data[offset+2:offset+4])[0]
    
    offset += 4
    
    # Payload: Client ID, Will Properties, User Name, Password
    client_id, offset = decode_utf8_string(data, offset)
    
    if has_will:
        _, offset = decode_utf8_string(data, offset)  # Will topic
        will_msg_len = struct.unpack("!H", data[offset:offset+2])[0]
        offset += 2 + will_msg_len
        
    username = None
    if has_username:
        username, offset = decode_utf8_string(data, offset)
        
    password = None
    if has_password:
        # In MQTT 3.1.1, Password is a length-prefixed binary field,
        # but the request expects Optional[str] so we decode it as utf-8.
        password, offset = decode_utf8_string(data, offset)
        
    return ConnectPacket(
        client_id=client_id,
        username=username,
        password=password,
        clean_session=clean_session,
        keep_alive=keep_alive
    )

def build_connack(session_present: bool = False, return_code: int = 0) -> bytes:
    """
    Builds an MQTT CONNACK packet.
    return_code: 0 = Connection Accepted
    """
    header = bytes([(CONNACK << 4)])
    ack_flags = 1 if session_present else 0
    variable_header = bytes([ack_flags, return_code])
    
    remaining_length = encode_remaining_length(len(variable_header))
    
    return header + remaining_length + variable_header

def parse_subscribe(data: bytes) -> tuple[int, list[SubscribeRequest]]:
    """
    Parses the variable header and payload of an MQTT SUBSCRIBE packet.
    Returns (packet_id, list of subscriptions).
    """
    if len(data) < 2:
        raise ValueError("Incomplete SUBSCRIBE packet")
        
    packet_id = struct.unpack("!H", data[0:2])[0]
    offset = 2
    
    subscriptions = []
    while offset < len(data):
        topic, offset = decode_utf8_string(data, offset)
        
        if offset >= len(data):
            raise ValueError("Incomplete SUBSCRIBE packet (missing QoS)")
            
        qos = data[offset]
        offset += 1
        
        subscriptions.append(SubscribeRequest(topic=topic, qos=qos))
        
    return packet_id, subscriptions

def build_suback(packet_id: int, granted_qos: list[int]) -> bytes:
    """
    Builds an MQTT SUBACK packet.
    """
    header = bytes([(SUBACK << 4)])
    variable_header = struct.pack("!H", packet_id)
    payload = bytes(granted_qos)
    
    remaining_length = encode_remaining_length(len(variable_header) + len(payload))
    
    return header + remaining_length + variable_header + payload

def parse_publish(first_byte: int, data: bytes) -> PublishPacket:
    """
    Parses an MQTT PUBLISH packet's variable header and payload.
    Requires the first byte of the fixed header to extract QoS and Retain flags.
    """
    qos = (first_byte >> 1) & 3
    retain = bool(first_byte & 1)
    
    topic, offset = decode_utf8_string(data, 0)
    
    packet_id = None
    if qos > 0:
        if offset + 2 > len(data):
            raise ValueError("Incomplete PUBLISH packet (missing packet identifier)")
        packet_id = struct.unpack("!H", data[offset:offset+2])[0]
        offset += 2
        
    payload = data[offset:]
    
    return PublishPacket(
        topic=topic,
        payload=payload,
        qos=qos,
        retain=retain,
        packet_id=packet_id
    )

def build_publish(topic: str, payload: Union[bytes, str], qos: int = 0, retain: bool = False, packet_id: Optional[int] = None) -> bytes:
    """
    Builds an MQTT PUBLISH packet.
    """
    if isinstance(payload, str):
        payload = payload.encode('utf-8')
        
    if qos > 0 and packet_id is None:
        raise ValueError("packet_id is required when QoS > 0")
        
    first_byte = (PUBLISH << 4) | (qos << 1) | (1 if retain else 0)
    header = bytes([first_byte])
    
    variable_header = encode_utf8_string(topic)
    if qos > 0 and packet_id is not None:
        variable_header += struct.pack("!H", packet_id)
        
    remaining_length = encode_remaining_length(len(variable_header) + len(payload))
    
    return header + remaining_length + variable_header + payload

def build_puback(packet_id: int) -> bytes:
    """
    Builds an MQTT PUBACK packet.
    """
    header = bytes([(PUBACK << 4)])
    variable_header = struct.pack("!H", packet_id)
    remaining_length = encode_remaining_length(len(variable_header))
    return header + remaining_length + variable_header

def parse_puback(data: bytes) -> int:
    """
    Parses an MQTT PUBACK packet's variable header.
    Returns the packet_id.
    """
    return struct.unpack("!H", data[0:2])[0]

def build_pingresp() -> bytes:
    """
    Builds an MQTT PINGRESP packet.
    """
    header = bytes([(PINGRESP << 4)])
    remaining_length = bytes([0])
    return header + remaining_length
