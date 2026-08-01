"""
Embedded MQTT server for Duux smart devices.

Accepts TLS connections on port 443 (configurable), parses MQTT 3.1.1
packets from Duux fans/purifiers, and forwards state updates to
Home Assistant via the HA bridge.

This is NOT a general-purpose MQTT broker — it implements only the
subset of MQTT 3.1.1 required by Duux devices:
- CONNECT / CONNACK
- SUBSCRIBE / SUBACK
- PUBLISH (bidirectional)
- PINGREQ / PINGRESP
- DISCONNECT
"""

from __future__ import annotations

import asyncio
import logging
import ssl
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

try:
    from .mqtt_protocol import (
        CONNECT,
        CONNACK,
        DISCONNECT,
        PINGREQ,
        PUBLISH,
        PUBACK,
        SUBSCRIBE,
        ConnectPacket,
        PublishPacket,
        SubscribeRequest,
        build_connack,
        build_pingresp,
        build_publish,
        build_suback,
        decode_remaining_length,
        parse_connect,
        parse_publish,
        parse_subscribe,
    )
except ImportError:
    from mqtt_protocol import (
        CONNECT,
        CONNACK,
        DISCONNECT,
        PINGREQ,
        PUBLISH,
        PUBACK,
        SUBSCRIBE,
        ConnectPacket,
        PublishPacket,
        SubscribeRequest,
        build_connack,
        build_pingresp,
        build_publish,
        build_suback,
        decode_remaining_length,
        parse_connect,
        parse_publish,
        parse_subscribe,
    )

_LOGGER = logging.getLogger(__name__)

# Callback type for when a device publishes a message
OnDevicePublish = Callable[[str, str, bytes], None]
# Callback type for when a new device connects
OnDeviceConnect = Callable[[str, str, Optional[str]], None]
# Callback type for when a device disconnects
OnDeviceDisconnect = Callable[[str], None]


@dataclass
class ConnectedDevice:
    """Represents a currently connected Duux device."""

    device_id: str
    username: Optional[str]
    password: Optional[str]
    writer: asyncio.StreamWriter
    subscriptions: list[str] = field(default_factory=list)
    keep_alive: int = 60
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def send_packet(self, data: bytes) -> None:
        """Send a raw MQTT packet to the device."""
        async with self._lock:
            try:
                self.writer.write(data)
                await self.writer.drain()
            except (ConnectionError, OSError) as err:
                _LOGGER.warning(
                    "Failed to send packet to device %s: %s",
                    self.device_id,
                    err,
                )


class DuuxMqttServer:
    """Minimal MQTT 3.1.1 server tailored for Duux smart devices.

    Listens on a configurable port with TLS, accepts connections from
    Duux devices, and provides callbacks for device events (connect,
    disconnect, publish).
    """

    def __init__(self) -> None:
        """Initialize the MQTT server."""
        self._server: Optional[asyncio.Server] = None
        self._devices: dict[str, ConnectedDevice] = {}
        self._on_device_publish: Optional[OnDevicePublish] = None
        self._on_device_connect: Optional[OnDeviceConnect] = None
        self._on_device_disconnect: Optional[OnDeviceDisconnect] = None

    @property
    def connected_devices(self) -> dict[str, ConnectedDevice]:
        """Return the currently connected devices."""
        return dict(self._devices)

    def set_on_device_publish(self, callback: OnDevicePublish) -> None:
        """Set callback for when a device publishes a message."""
        self._on_device_publish = callback

    def set_on_device_connect(self, callback: OnDeviceConnect) -> None:
        """Set callback for when a new device connects."""
        self._on_device_connect = callback

    def set_on_device_disconnect(self, callback: OnDeviceDisconnect) -> None:
        """Set callback for when a device disconnects."""
        self._on_device_disconnect = callback

    async def start(
        self, host: str, port: int, certfile: str, keyfile: str
    ) -> None:
        """Start the TLS MQTT server.

        Args:
            host: Interface to bind to (e.g., "0.0.0.0").
            port: Port to listen on (typically 443).
            certfile: Path to the TLS certificate file.
            keyfile: Path to the TLS private key file.
        """
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_ctx.load_cert_chain(certfile, keyfile)
        # Duux fans don't verify the server certificate, but we still
        # want reasonable TLS defaults for the handshake
        ssl_ctx.minimum_version = ssl.TLSVersion.TLSv1_2

        self._server = await asyncio.start_server(
            self._handle_client, host, port, ssl=ssl_ctx
        )

        addr = self._server.sockets[0].getsockname()
        _LOGGER.info("Duux MQTT server listening on %s:%s (TLS)", addr[0], addr[1])

    async def stop(self) -> None:
        """Stop the MQTT server and disconnect all devices."""
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            _LOGGER.info("Duux MQTT server stopped")

        # Close all device connections
        for device_id, device in list(self._devices.items()):
            try:
                device.writer.close()
                await device.writer.wait_closed()
            except (ConnectionError, OSError):
                pass
            self._devices.pop(device_id, None)

    async def publish_to_device(
        self, device_id: str, topic: str, payload: str | bytes
    ) -> bool:
        """Send a PUBLISH packet to a specific connected device.

        Args:
            device_id: The MAC-address based device identifier.
            topic: MQTT topic (e.g., "sensor/{mac}/command").
            payload: Message payload.

        Returns:
            True if the message was sent, False if the device is not connected.
        """
        device = self._devices.get(device_id)
        if not device:
            _LOGGER.warning(
                "Cannot publish to device %s: not connected", device_id
            )
            return False

        if isinstance(payload, str):
            payload = payload.encode("utf-8")

        packet = build_publish(topic, payload, qos=0, retain=False)
        await device.send_packet(packet)
        _LOGGER.debug(
            "[RAW MQTT SEND] Device %s <- Topic: '%s', Payload (%d bytes): %s",
            device_id,
            topic,
            len(payload),
            payload.decode("utf-8", errors="replace"),
        )
        return True

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Handle a single MQTT client connection.

        Implements the full MQTT session lifecycle:
        1. Wait for CONNECT packet, extract credentials
        2. Send CONNACK
        3. Process packets in a loop (SUBSCRIBE, PUBLISH, PINGREQ)
        4. Clean up on DISCONNECT or connection loss
        """
        peer = writer.get_extra_info("peername")
        _LOGGER.info("New TLS connection from %s", peer)

        device: Optional[ConnectedDevice] = None

        try:
            device = await self._handle_connect(reader, writer)
            if not device:
                return

            await self._process_packets(device, reader)

        except asyncio.IncompleteReadError:
            _LOGGER.info(
                "Device %s disconnected (incomplete read)",
                device.device_id if device else "unknown",
            )
        except ConnectionResetError:
            _LOGGER.info(
                "Device %s connection reset",
                device.device_id if device else "unknown",
            )
        except asyncio.TimeoutError:
            _LOGGER.warning(
                "Device %s timed out (keep-alive expired)",
                device.device_id if device else "unknown",
            )
        except Exception:
            _LOGGER.exception(
                "Unexpected error handling device %s",
                device.device_id if device else "unknown",
            )
        finally:
            await self._cleanup_device(device, writer)

    async def _handle_connect(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> Optional[ConnectedDevice]:
        """Read and process the CONNECT packet.

        Returns a ConnectedDevice on success, None on failure.
        """
        # Read the fixed header
        first_byte = await asyncio.wait_for(
            reader.readexactly(1), timeout=30.0
        )
        packet_type = (first_byte[0] >> 4) & 0x0F

        if packet_type != CONNECT:
            _LOGGER.error(
                "Expected CONNECT packet, got type %d", packet_type
            )
            writer.close()
            return None

        # Read remaining length
        remaining_length, length_bytes = await self._read_remaining_length(
            reader
        )

        # Read the rest of the packet
        data = await asyncio.wait_for(
            reader.readexactly(remaining_length), timeout=10.0
        )

        connect_pkt = parse_connect(data)
        _LOGGER.info(
            "CONNECT from client_id=%s, username=%s, keep_alive=%d",
            connect_pkt.client_id,
            connect_pkt.username,
            connect_pkt.keep_alive,
        )

        # Determine device_id from username (MAC address) or client_id
        device_id = (
            connect_pkt.username.lower()
            if connect_pkt.username
            else connect_pkt.client_id.lower()
        )

        # If this device was already connected, close the old connection
        old_device = self._devices.pop(device_id, None)
        if old_device:
            _LOGGER.info(
                "Device %s reconnecting, closing old connection", device_id
            )
            try:
                old_device.writer.close()
            except (ConnectionError, OSError):
                pass

        # Send CONNACK (connection accepted)
        connack = build_connack(session_present=False, return_code=0)
        writer.write(connack)
        await writer.drain()

        device = ConnectedDevice(
            device_id=device_id,
            username=connect_pkt.username,
            password=connect_pkt.password,
            writer=writer,
            keep_alive=connect_pkt.keep_alive,
        )
        self._devices[device_id] = device

        # Notify the bridge about the new connection
        if self._on_device_connect:
            self._on_device_connect(
                device_id, connect_pkt.username, connect_pkt.password
            )

        return device

    async def _process_packets(
        self, device: ConnectedDevice, reader: asyncio.StreamReader
    ) -> None:
        """Process MQTT packets in a loop until disconnect or error.

        Handles SUBSCRIBE, PUBLISH, PINGREQ, and DISCONNECT packets.
        """
        # Keep-alive timeout: 1.5x the client's keep_alive interval
        # per MQTT spec section 3.1.2.10
        timeout = device.keep_alive * 1.5 if device.keep_alive > 0 else 120

        while True:
            first_byte_data = await asyncio.wait_for(
                reader.readexactly(1), timeout=timeout
            )
            first_byte = first_byte_data[0]
            packet_type = (first_byte >> 4) & 0x0F

            if packet_type == DISCONNECT:
                _LOGGER.info("Device %s sent DISCONNECT", device.device_id)
                break

            # Read remaining length
            remaining_length, _ = await self._read_remaining_length(reader)

            # Read payload
            if remaining_length > 0:
                data = await asyncio.wait_for(
                    reader.readexactly(remaining_length), timeout=10.0
                )
            else:
                data = b""

            if packet_type == PINGREQ:
                await self._handle_pingreq(device)

            elif packet_type == SUBSCRIBE:
                await self._handle_subscribe(device, data)

            elif packet_type == PUBLISH:
                await self._handle_publish(device, first_byte, data)

            elif packet_type == PUBACK:
                # We don't track QoS 1 acknowledgements from the device
                _LOGGER.debug(
                    "Received PUBACK from device %s", device.device_id
                )

            else:
                _LOGGER.warning(
                    "Unhandled packet type %d from device %s",
                    packet_type,
                    device.device_id,
                )

    async def _handle_pingreq(self, device: ConnectedDevice) -> None:
        """Respond to a PINGREQ with PINGRESP."""
        pingresp = build_pingresp()
        await device.send_packet(pingresp)
        _LOGGER.debug("PINGREQ/PINGRESP for device %s", device.device_id)

    async def _handle_subscribe(
        self, device: ConnectedDevice, data: bytes
    ) -> None:
        """Process a SUBSCRIBE packet and send SUBACK."""
        packet_id, subscriptions = parse_subscribe(data)

        granted_qos = []
        for sub in subscriptions:
            device.subscriptions.append(sub.topic)
            granted_qos.append(min(sub.qos, 1))  # Grant at most QoS 1
            _LOGGER.info(
                "Device %s subscribed to: %s (QoS %d)",
                device.device_id,
                sub.topic,
                sub.qos,
            )

        suback = build_suback(packet_id, granted_qos)
        await device.send_packet(suback)

    async def _handle_publish(
        self, device: ConnectedDevice, first_byte: int, data: bytes
    ) -> None:
        """Process a PUBLISH packet from the device."""
        pub = parse_publish(first_byte, data)

        _LOGGER.debug(
            "[RAW MQTT RECV] Device %s -> Topic: '%s', Payload (%d bytes): %s (HEX: %s)",
            device.device_id,
            pub.topic,
            len(pub.payload),
            pub.payload.decode("utf-8", errors="replace"),
            pub.payload.hex(),
        )

        # Send PUBACK if QoS 1
        if pub.qos == 1 and pub.packet_id is not None:
            puback = bytes([0x40, 0x02]) + pub.packet_id.to_bytes(2, "big")
            await device.send_packet(puback)
            _LOGGER.debug("[RAW MQTT SEND] Device %s <- PUBACK (packet_id=%d)", device.device_id, pub.packet_id)

        # Notify the bridge
        if self._on_device_publish:
            self._on_device_publish(
                device.device_id, pub.topic, pub.payload
            )

    async def _cleanup_device(
        self,
        device: Optional[ConnectedDevice],
        writer: asyncio.StreamWriter,
    ) -> None:
        """Clean up after a device disconnects."""
        if device:
            self._devices.pop(device.device_id, None)
            _LOGGER.info("Device %s disconnected", device.device_id)
            if self._on_device_disconnect:
                self._on_device_disconnect(device.device_id)

        try:
            writer.close()
            await writer.wait_closed()
        except (ConnectionError, OSError):
            pass

    async def _read_remaining_length(
        self, reader: asyncio.StreamReader
    ) -> tuple[int, int]:
        """Read the MQTT remaining length field from the stream.

        The remaining length is encoded as 1-4 bytes using a variable-length
        encoding scheme per MQTT 3.1.1 spec section 2.2.3.

        Returns:
            Tuple of (remaining_length, number_of_bytes_read).
        """
        multiplier = 1
        value = 0
        bytes_read = 0

        while True:
            encoded_byte = await reader.readexactly(1)
            byte_val = encoded_byte[0]
            value += (byte_val & 0x7F) * multiplier
            bytes_read += 1

            if (byte_val & 0x80) == 0:
                break

            multiplier *= 128
            if bytes_read > 4:
                raise ValueError("Malformed remaining length")

        return value, bytes_read
