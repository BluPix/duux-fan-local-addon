"""
Duux MQTT Bridge — Main entrypoint.

Orchestrates all components:
1. CertManager: ensures TLS certificates exist
2. DuuxMqttServer: accepts connections from Duux devices on port 443
3. HomeAssistantBridge: forwards data to/from HA via MQTT Discovery

Configuration is read from /data/options.json (HA addon standard).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import urllib.request
from typing import Any, Optional

from pathlib import Path

# Add current directory to sys.path for direct script execution
sys.path.insert(0, str(Path(__file__).parent))

try:
    from .cert_manager import CertManager
    from .ha_bridge import (
        DUUX_TOPIC_COMMAND,
        DUUX_TOPIC_STATE,
        DUUX_TOPIC_ONLINE,
        DUUX_TOPIC_UPDATE,
        HomeAssistantBridge,
    )
    from .mqtt_server import DuuxMqttServer
except ImportError:
    from cert_manager import CertManager
    from ha_bridge import (
        DUUX_TOPIC_COMMAND,
        DUUX_TOPIC_STATE,
        DUUX_TOPIC_ONLINE,
        DUUX_TOPIC_UPDATE,
        HomeAssistantBridge,
    )
    from mqtt_server import DuuxMqttServer

_LOGGER = logging.getLogger("duux_mqtt_bridge")

# Path to addon options (HA standard)
OPTIONS_PATH = "/data/options.json"

# Default configuration values
DEFAULT_CONFIG = {
    "mqtt_host": "core-mosquitto",
    "mqtt_port": 1883,
    "mqtt_username": "",
    "mqtt_password": "",
    "broker_port": 443,
    "log_level": "info",
    "language": "auto",
}


def fetch_supervisor_mqtt_service() -> dict[str, Any]:
    """Fetch automatic internal MQTT broker credentials from HA Supervisor Service API (like Zigbee2MQTT)."""
    token = os.environ.get("SUPERVISOR_TOKEN")
    if not token:
        return {}
    url = "http://supervisor/services/mqtt"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            data = json.loads(resp.read().decode())
            if data.get("result") == "ok":
                svc = data.get("data", {})
                _LOGGER.info(
                    "Retrieved internal HA MQTT service credentials from Supervisor (username: %s)",
                    svc.get("username"),
                )
                return svc
    except Exception as err:
        _LOGGER.debug("Could not fetch MQTT service credentials from Supervisor API: %s", err)
    return {}


def load_options() -> dict[str, Any]:
    """Load addon configuration from /data/options.json and HA Supervisor services.

    Falls back to defaults if the file doesn't exist (e.g., during testing).
    Automatically fetches internal system MQTT credentials if not manually specified.
    """
    config = dict(DEFAULT_CONFIG)
    if os.path.exists(OPTIONS_PATH):
        with open(OPTIONS_PATH, "r") as f:
            options = json.load(f)
        _LOGGER.info("Loaded configuration from %s", OPTIONS_PATH)
        config.update({k: v for k, v in options.items() if v != ""})

    # If username is empty, auto-fetch internal MQTT service credentials from Supervisor API (like Zigbee2MQTT)
    if not config.get("mqtt_username"):
        svc = fetch_supervisor_mqtt_service()
        if svc.get("host"):
            config["mqtt_host"] = svc["host"]
        if svc.get("port"):
            config["mqtt_port"] = svc["port"]
        if svc.get("username"):
            config["mqtt_username"] = svc["username"]
        if svc.get("password"):
            config["mqtt_password"] = svc["password"]

    # Environment variables fallback check
    for key, env_var in [
        ("mqtt_host", "HA_MQTT_HOST"),
        ("mqtt_host", "MQTT_HOST"),
        ("mqtt_port", "HA_MQTT_PORT"),
        ("mqtt_port", "MQTT_PORT"),
        ("mqtt_username", "HA_MQTT_USERNAME"),
        ("mqtt_username", "MQTT_USERNAME"),
        ("mqtt_password", "HA_MQTT_PASSWORD"),
        ("mqtt_password", "MQTT_PASSWORD"),
    ]:
        val = os.environ.get(env_var)
        if val and not config.get(key):
            if key == "mqtt_port":
                try:
                    config[key] = int(val)
                except ValueError:
                    pass
            else:
                config[key] = val

    return config


def setup_logging(level_name: str) -> None:
    """Configure logging for the addon."""
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )


class DuuxBridgeApp:
    """Main application orchestrating all components."""

    def __init__(self, config: dict[str, Any]) -> None:
        """Initialize the application with configuration."""
        self._config = config
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._server = DuuxMqttServer()
        self._bridge = HomeAssistantBridge(
            mqtt_host=config["mqtt_host"],
            mqtt_port=config["mqtt_port"],
            mqtt_username=config["mqtt_username"],
            mqtt_password=config["mqtt_password"],
            language=config.get("language", "auto"),
        )
        self._shutdown_event = asyncio.Event()

        # Wire up callbacks
        self._server.set_on_device_connect(self._on_device_connect)
        self._server.set_on_device_disconnect(self._on_device_disconnect)
        self._server.set_on_device_publish(self._on_device_publish)
        self._bridge.set_on_command(self._on_ha_command)

    async def run(self) -> None:
        """Run the application."""
        self._loop = asyncio.get_running_loop()
        # 1. Ensure TLS certificates
        cert_manager = CertManager()
        certfile, keyfile = cert_manager.ensure_certificates()

        # 2. Connect to HA MQTT broker
        _LOGGER.info(
            "Connecting to HA MQTT broker at %s:%s",
            self._config["mqtt_host"],
            self._config["mqtt_port"],
        )
        self._bridge.connect()

        # 3. Start the Duux MQTT server
        port = self._config["broker_port"]
        _LOGGER.info("Starting Duux MQTT server on port %d", port)
        await self._server.start("0.0.0.0", port, certfile, keyfile)

        _LOGGER.info(
            "=== Duux MQTT Bridge is running ===\n"
            "  Duux device port: %d (TLS)\n"
            "  HA MQTT broker:   %s:%d\n"
            "  Waiting for Duux devices to connect...",
            port,
            self._config["mqtt_host"],
            self._config["mqtt_port"],
        )

        # 4. Wait for shutdown signal
        await self._shutdown_event.wait()

        # 5. Cleanup
        _LOGGER.info("Shutting down...")
        await self._server.stop()
        self._bridge.disconnect()

    def shutdown(self) -> None:
        """Signal the application to shut down."""
        self._shutdown_event.set()

    def _on_device_connect(
        self, device_id: str, username: Optional[str], password: Optional[str]
    ) -> None:
        """Called when a Duux device connects to our MQTT server.

        Publishes MQTT Discovery config to HA so entities are auto-created.
        """
        _LOGGER.info(
            "New Duux device connected: %s (username/MAC: %s)",
            device_id,
            username,
        )
        _LOGGER.debug(
            "Device %s connected with token/password: %s",
            device_id,
            password,
        )

        # Try to determine model from the device_id or default to whisper_flex_2
        # The actual model detection happens when the device sends its
        # first /update message with the model name
        model = self._bridge._device_models.get(device_id, "whisper_flex_2")
        self._bridge.set_device_model(device_id, model)
        self._bridge.publish_discovery(device_id, model)
        self._bridge.publish_device_online(device_id)

    def _on_device_disconnect(self, device_id: str) -> None:
        """Called when a Duux device disconnects."""
        _LOGGER.info("Duux device disconnected: %s", device_id)
        self._bridge.publish_device_offline(device_id)

    def _on_device_publish(
        self, device_id: str, topic: str, payload: bytes
    ) -> None:
        """Called when a Duux device publishes a message.

        Parses the Duux MQTT payload and forwards it to HA.
        """
        try:
            payload_str = payload.decode("utf-8", errors="replace")
        except Exception:
            _LOGGER.warning(
                "Could not decode payload from device %s on topic %s",
                device_id,
                topic,
            )
            return

        # Determine the topic type based on suffix
        if topic.endswith("/in"):
            self._handle_state_update(device_id, payload_str)
        elif topic.endswith("/online"):
            self._handle_online_status(device_id, payload_str)
        elif topic.endswith("/update"):
            self._handle_device_update(device_id, payload_str)
        else:
            _LOGGER.info(
                "Unhandled or raw topic '%s' from device %s: %s",
                topic,
                device_id,
                payload_str,
            )

    def _handle_state_update(
        self, device_id: str, payload_str: str
    ) -> None:
        """Parse a state update from the device and forward to HA.

        Payload format:
        {"sub":{"Tune":[{"power":1,"speed":10,"mode":0,...}]}}

        Some models (Bright 2) have double nesting:
        {"sub":{"Tune":[{"uid":"...","sub":{"Tune":[{...}]}}]}}
        """
        try:
            data = json.loads(payload_str)
            fan_data = data.get("sub", {}).get("Tune", [{}])[0]

            # Handle double nesting (Bright 2 and similar)
            if (
                isinstance(fan_data, dict)
                and "sub" in fan_data
                and "Tune" in fan_data["sub"]
            ):
                fan_data = fan_data["sub"]["Tune"][0]

            if isinstance(fan_data, dict) and fan_data:
                self._bridge.publish_device_state(device_id, fan_data)
            else:
                _LOGGER.debug(
                    "Empty fan_data from device %s, skipping", device_id
                )
        except (json.JSONDecodeError, KeyError, IndexError) as err:
            _LOGGER.warning(
                "Could not parse state from device %s: %s (error: %s)",
                device_id,
                payload_str[:200],
                err,
            )

    def _handle_online_status(
        self, device_id: str, payload_str: str
    ) -> None:
        """Handle online status messages from the device."""
        try:
            data = json.loads(payload_str)
            is_online = data.get("online", False)
            if is_online:
                self._bridge.publish_device_online(device_id)
            else:
                self._bridge.publish_device_offline(device_id)
        except json.JSONDecodeError:
            _LOGGER.debug(
                "Could not parse online status from device %s", device_id
            )

    def _handle_device_update(
        self, device_id: str, payload_str: str
    ) -> None:
        """Handle device update/info messages.

        Payload format: {"pid":"xyz","tune":"DUUX Whisper Flex 2"}

        This lets us detect the actual device model.
        """
        try:
            data = json.loads(payload_str)
            model_name = data.get("tune", "")
            _LOGGER.info(
                "Device %s reported model: %s", device_id, model_name
            )

            # Try to match model name to our profiles
            detected_model = self._detect_model(model_name)
            if detected_model:
                current_model = self._bridge._device_models.get(device_id)
                if current_model != detected_model:
                    _LOGGER.info(
                        "Updating device %s model from %s to %s",
                        device_id,
                        current_model,
                        detected_model,
                    )
                    self._bridge.set_device_model(device_id, detected_model)
                    # Re-publish discovery with correct model
                    self._bridge.publish_discovery(device_id, detected_model)

        except json.JSONDecodeError:
            _LOGGER.debug(
                "Could not parse update from device %s", device_id
            )

    def _detect_model(self, model_name: str) -> Optional[str]:
        """Detect the device model key from the reported model name.

        Args:
            model_name: The name reported by the device (e.g., "DUUX Whisper Flex 2").

        Returns:
            The model key (e.g., "whisper_flex_2") or None if not recognized.
        """
        name_lower = model_name.lower()

        if "bright" in name_lower and "2" in name_lower:
            return "bright_2"
        if "ultimate" in name_lower:
            return "whisper_flex_ultimate"
        if "flex 2" in name_lower or "flex2" in name_lower:
            return "whisper_flex_2"
        if "flex" in name_lower:
            return "whisper_flex_1"

        _LOGGER.warning("Could not detect model from name: %s", model_name)
        return None

    def _on_ha_command(self, device_id: str, command: str) -> None:
        """Called when HA sends a command for a device.

        Forwards the command to the device via the Duux MQTT server.
        """
        _LOGGER.info(
            "HA command for device %s: %s", device_id, command
        )

        # Build the Duux command topic
        topic = DUUX_TOPIC_COMMAND.format(device_id=device_id)

        # Schedule the async publish on the main asyncio event loop
        if self._loop and self._loop.is_running():
            asyncio.run_coroutine_threadsafe(
                self._server.publish_to_device(device_id, topic, command),
                self._loop,
            )
        else:
            _LOGGER.error(
                "Event loop not available to execute command for device %s: %s",
                device_id,
                command,
            )


def main() -> None:
    """Main entrypoint for the addon."""
    config = load_options()
    setup_logging(config.get("log_level", "info"))

    _LOGGER.info("Starting Duux MQTT Bridge v0.1.0")

    app = DuuxBridgeApp(config)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Handle shutdown signals gracefully
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, app.shutdown)

    try:
        loop.run_until_complete(app.run())
    except KeyboardInterrupt:
        _LOGGER.info("Interrupted by user")
    finally:
        loop.close()

    _LOGGER.info("Duux MQTT Bridge stopped")


if __name__ == "__main__":
    main()
