"""
Home Assistant MQTT Bridge for Duux devices.

Translates between the Duux-specific MQTT protocol (used by the embedded
server) and the Home Assistant MQTT ecosystem (via MQTT Discovery through
the Mosquitto addon).

Responsibilities:
- Connect to the HA Mosquitto broker (core-mosquitto:1883)
- Publish MQTT Discovery configs for auto-creating HA entities
- Forward device state updates to HA state topics
- Subscribe to HA command topics and relay commands back to devices
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from typing import Any, Callable, Optional

import paho.mqtt.client as mqtt

_LOGGER = logging.getLogger(__name__)

try:
    from .translations import TRANSLATIONS
except ImportError:
    from translations import TRANSLATIONS


def detect_ha_language(configured_lang: str = "auto") -> str:
    """Query Home Assistant Supervisor / Core API to detect system language."""
    if configured_lang and configured_lang not in ("auto", ""):
        _LOGGER.info("Language selection: explicit option set to '%s'", configured_lang)
        return configured_lang

    _LOGGER.info("Language selection: mode 'auto' -> querying Supervisor API for HA system language...")
    token = os.environ.get("SUPERVISOR_TOKEN")
    if token:
        endpoints = [
            "http://supervisor/core/api/config",
            "http://supervisor/config",
            "http://supervisor/info",
            "http://supervisor/core/info",
        ]
        for url in endpoints:
            req = urllib.request.Request(
                url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
            )
            try:
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
                    raw_lang = (
                        data.get("language")
                        or data.get("data", {}).get("language", "")
                        or data.get("language_code", "")
                    )
                    if raw_lang:
                        lang = raw_lang.split("-")[0].lower()
                        _LOGGER.info(
                            "🌐 Auto-detected HA system language via %s: '%s' (raw locale: '%s')",
                            url,
                            lang,
                            raw_lang,
                        )
                        return lang
            except Exception as err:
                _LOGGER.debug("Language query to %s failed: %s", url, err)
    else:
        _LOGGER.debug("SUPERVISOR_TOKEN not set; skipping Supervisor API language auto-detection")

    _LOGGER.info("🌐 Auto-detection fallback: defaulting language to 'en'")
    return "en"

# MQTT Discovery prefix used by Home Assistant
DISCOVERY_PREFIX = "homeassistant"

# Topic prefix for Duux device state/command in HA MQTT
DUUX_TOPIC_PREFIX = "duux"

# Duux device topic patterns (from the device firmware)
DUUX_TOPIC_STATE = "sensor/{device_id}/in"
DUUX_TOPIC_ONLINE = "sensor/{device_id}/online"
DUUX_TOPIC_UPDATE = "sensor/{device_id}/update"
DUUX_TOPIC_COMMAND = "sensor/{device_id}/command"

# Callback for relaying commands from HA to the Duux device
OnCommandReceived = Callable[[str, str], None]


# --- Device profile definitions ---
# Mirrors the device profiles from the custom_component, but formatted
# for MQTT Discovery payloads.

DEVICE_PROFILES: dict[str, dict[str, Any]] = {
    "whisper_flex_1": {
        "name": "Duux Whisper Flex",
        "manufacturer": "Duux",
        "model": "Whisper Flex",
        "fan": {"max_speed": 26},
        "numbers": {
            "timer": {
                "name": "Timer",
                "min": 0, "max": 12, "step": 1,
                "unit": "h", "icon": "mdi:timer-outline",
                "command_key": "timer",
            },
            "speed": {
                "name": "Speed",
                "min": 1, "max": 26, "step": 1,
                "icon": "mdi:speedometer",
                "command_key": "speed",
            },
        },
        "switches": {
            "swing": {
                "name": "Horizontal Oscillation",
                "icon": "mdi:arrow-left-right",
                "command_key": "swing",
            },
            "tilt": {
                "name": "Vertical Oscillation",
                "icon": "mdi:arrow-up-down",
                "command_key": "tilt",
            },
        },
        "selects": {
            "mode": {
                "name": "Fan Mode",
                "icon": "mdi:weather-windy",
                "options": ["Normal", "Natural", "Night"],
                "command_key": "mode",
            },
        },
        "sensors": {},
        "binary_sensors": {},
    },
    "whisper_flex_2": {
        "name": "Duux Whisper Flex 2",
        "manufacturer": "Duux",
        "model": "Whisper Flex 2",
        "fan": {"max_speed": 30},
        "numbers": {
            "timer": {
                "name": "Timer",
                "min": 0, "max": 12, "step": 1,
                "unit": "h", "icon": "mdi:timer-outline",
                "command_key": "timer",
            },
            "speed": {
                "name": "Speed",
                "min": 1, "max": 30, "step": 1,
                "icon": "mdi:speedometer",
                "command_key": "speed",
            },
        },
        "switches": {
            "power": {
                "name": "Power",
                "icon": "mdi:power",
                "command_key": "power",
            },
            "night": {
                "name": "Night Mode",
                "icon": "mdi:weather-night",
                "command_key": "night",
            },
            "lock": {
                "name": "Child Lock",
                "icon": "mdi:account-lock",
                "command_key": "lock",
            },
        },
        "selects": {
            "mode": {
                "name": "Fan Mode",
                "icon": "mdi:weather-windy",
                "options": ["Normal", "Natural"],
                "command_key": "mode",
            },
            "horosc": {
                "name": "Horizontal Oscillation",
                "icon": "mdi:arrow-left-right",
                "options": ["Off", "30°", "60°", "90°"],
                "command_key": "horosc",
            },
            "verosc": {
                "name": "Vertical Oscillation",
                "icon": "mdi:arrow-up-down",
                "options": ["Off", "45°", "100°"],
                "command_key": "verosc",
            },
        },
        "sensors": {
            "batlvl": {
                "name": "Battery Level",
                "device_class": "battery",
                "unit": "%",
                "icon": "mdi:battery",
                "multiplier": 10,
            },
        },
        "binary_sensors": {
            "batcha": {
                "name": "Charging",
                "device_class": "battery_charging",
                "icon": "mdi:battery-charging",
            },
        },
    },
    "whisper_flex_ultimate": {
        "name": "Duux Whisper Flex Ultimate",
        "manufacturer": "Duux",
        "model": "Whisper Flex Ultimate",
        "fan": {"max_speed": 30},
        "numbers": {
            "speed": {
                "name": "Speed",
                "min": 1, "max": 30, "step": 1,
                "icon": "mdi:speedometer",
                "command_key": "speed",
            },
            "timer": {
                "name": "Timer",
                "min": 0, "max": 12, "step": 1,
                "unit": "h", "icon": "mdi:timer-outline",
                "command_key": "timer",
            },
            "sp": {
                "name": "Setpoint",
                "min": 17, "max": 28, "step": 1,
                "unit": "°C", "icon": "mdi:thermometer",
                "command_key": "sp",
            },
        },
        "switches": {},
        "selects": {
            "mode": {
                "name": "Fan Mode",
                "icon": "mdi:weather-windy",
                "options": ["Regular", "Natural", "Night"],
                "command_key": "mode",
            },
            "swing": {
                "name": "Swing",
                "icon": "mdi:arrow-left-right",
                "options": ["Off", "30°", "60°", "90°"],
                "command_key": "swing",
            },
            "tilt": {
                "name": "Tilt",
                "icon": "mdi:arrow-up-down",
                "options": ["Off", "90°", "105°"],
                "command_key": "tilt",
            },
        },
        "sensors": {},
        "binary_sensors": {},
    },
    "bright_2": {
        "name": "Duux Bright 2",
        "manufacturer": "Duux",
        "model": "Bright 2",
        "fan": {"max_speed": 4},
        "numbers": {
            "speed": {
                "name": "Speed",
                "min": 0, "max": 4, "step": 1,
                "icon": "mdi:speedometer",
                "command_key": "speed",
            },
        },
        "switches": {
            "mode": {
                "name": "Night Mode",
                "icon": "mdi:weather-night",
                "command_key": "mode",
            },
            "ion": {
                "name": "ION Setting",
                "icon": "mdi:blur",
                "command_key": "ion",
            },
        },
        "selects": {},
        "sensors": {
            "filter": {
                "name": "Filter Life",
                "unit": "%",
                "icon": "mdi:air-filter",
                "multiplier": 1,
            },
            "ppm": {
                "name": "PM10",
                "device_class": "pm10",
                "unit": "µg/m³",
                "icon": "mdi:molecule",
                "multiplier": 1,
            },
            "AQ": {
                "name": "Air Quality",
                "device_class": "aqi",
                "icon": "mdi:air-filter",
                "multiplier": 1,
            },
            "TVOC": {
                "name": "TVOC",
                "device_class": "volatile_organic_compounds",
                "unit": "µg/m³",
                "icon": "mdi:molecule",
                "multiplier": 1,
            },
        },
        "binary_sensors": {},
    },
}


class HomeAssistantBridge:
    """Bridges Duux device MQTT data to Home Assistant via MQTT Discovery.

    Connects to the HA Mosquitto broker and:
    1. Publishes MQTT Discovery configs to auto-create HA entities
    2. Publishes device state updates to HA state topics
    3. Subscribes to HA command topics and relays them to devices
    """

    def __init__(
        self,
        mqtt_host: str = "core-mosquitto",
        mqtt_port: int = 1883,
        mqtt_username: str = "",
        mqtt_password: str = "",
        language: str = "auto",
    ) -> None:
        """Initialize the bridge.

        Args:
            mqtt_host: Hostname of the HA MQTT broker.
            mqtt_port: Port of the HA MQTT broker.
            mqtt_username: MQTT username (empty for no auth).
            mqtt_password: MQTT password.
            language: Target language code ("auto", "cs", "en", "nl", "de").
        """
        self._mqtt_host = mqtt_host
        self._mqtt_port = mqtt_port
        self._mqtt_username = mqtt_username
        self._mqtt_password = mqtt_password
        self._language = language
        self._client = mqtt.Client(
            client_id="duux_mqtt_bridge",
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        )
        self._on_command: Optional[OnCommandReceived] = None
        self._discovered_devices: set[str] = set()
        self._device_models: dict[str, str] = {}

        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message

    def set_on_command(self, callback: OnCommandReceived) -> None:
        """Set callback for when HA sends a command for a device."""
        self._on_command = callback

    def connect(self) -> None:
        """Connect to the HA MQTT broker."""
        if self._mqtt_username:
            self._client.username_pw_set(
                self._mqtt_username, self._mqtt_password
            )

        try:
            self._client.connect(self._mqtt_host, self._mqtt_port, 60)
            self._client.loop_start()
            _LOGGER.info(
                "Connected to HA MQTT broker at %s:%s",
                self._mqtt_host,
                self._mqtt_port,
            )
        except (OSError, ConnectionRefusedError) as err:
            _LOGGER.error("Failed to connect to HA MQTT broker: %s", err)
            raise

    def disconnect(self) -> None:
        """Disconnect from the HA MQTT broker."""
        self._client.loop_stop()
        self._client.disconnect()
        _LOGGER.info("Disconnected from HA MQTT broker")

    def set_device_model(self, device_id: str, model: str) -> None:
        """Set the model for a device (used for discovery)."""
        self._device_models[device_id] = model

    def publish_discovery(self, device_id: str, model: str = "") -> None:
        """Publish MQTT Discovery configurations for a Duux device.

        Creates HA entities (fan, switches, sensors, etc.) automatically with
        auto-detected system language translations (CS, NL, DE, EN).

        Args:
            device_id: The device's MAC address (lowercase).
            model: The device model key (e.g., "whisper_flex_2").
        """
        if not model:
            model = self._device_models.get(device_id, "whisper_flex_2")

        profile = DEVICE_PROFILES.get(model)
        if not profile:
            _LOGGER.warning("Unknown device model: %s", model)
            return

        # Auto-detect system language via Supervisor API
        lang = detect_ha_language(self._language)
        lang_dict = TRANSLATIONS.get(lang, {})

        def t(text: str) -> str:
            """Translate text based on HA system language."""
            return lang_dict.get(text, text)

        safe_id = device_id.replace(":", "_")
        device_info = {
            "identifiers": [f"duux_{safe_id}"],
            "name": f"Duux {profile['model']}",
            "manufacturer": profile["manufacturer"],
            "model": profile["model"],
            "connections": [["mac", device_id]],
        }

        state_topic = f"{DUUX_TOPIC_PREFIX}/{safe_id}/state"
        avail_topic = f"{DUUX_TOPIC_PREFIX}/{safe_id}/availability"

        # --- Fan entity ---
        fan_config = {
            "name": None,  # Use device name
            "unique_id": f"duux_{safe_id}_fan",
            "object_id": f"duux_{safe_id}",
            "device": device_info,
            "state_topic": state_topic,
            "state_value_template": "{{ 'ON' if value_json.power | int == 1 else 'OFF' }}",
            "command_topic": f"{DUUX_TOPIC_PREFIX}/{safe_id}/set/power",
            "payload_on": "ON",
            "payload_off": "OFF",
            "percentage_state_topic": state_topic,
            "percentage_value_template": (
                "{{ ((value_json.speed | int) / "
                f"{profile['fan']['max_speed']} * 100) | round(0) }}"
            ),
            "percentage_command_topic": f"{DUUX_TOPIC_PREFIX}/{safe_id}/set/speed_pct",
            "speed_range_min": 1,
            "speed_range_max": profile["fan"]["max_speed"],
            "availability_topic": avail_topic,
            "payload_available": "online",
            "payload_not_available": "offline",
        }
        self._publish_discovery_config("fan", safe_id, "fan", fan_config)

        # --- Switch entities ---
        for key, sw_def in profile.get("switches", {}).items():
            sw_config = {
                "name": t(sw_def["name"]),
                "unique_id": f"duux_{safe_id}_{key}",
                "object_id": f"duux_{safe_id}_{key}",
                "device": device_info,
                "state_topic": state_topic,
                "value_template": (
                    f"{{{{ 'ON' if value_json.{key} is defined and "
                    f"value_json.{key} | int > 0 else 'OFF' }}}}"
                ),
                "command_topic": f"{DUUX_TOPIC_PREFIX}/{safe_id}/set/{key}",
                "payload_on": "ON",
                "payload_off": "OFF",
                "icon": sw_def.get("icon", ""),
                "availability_topic": avail_topic,
            }
            self._publish_discovery_config("switch", safe_id, key, sw_config)

        # --- Number entities ---
        for key, num_def in profile.get("numbers", {}).items():
            num_config = {
                "name": t(num_def["name"]),
                "unique_id": f"duux_{safe_id}_{key}",
                "object_id": f"duux_{safe_id}_{key}",
                "device": device_info,
                "state_topic": state_topic,
                "value_template": f"{{{{ value_json.{num_def['command_key']} | default(0) }}}}",
                "command_topic": f"{DUUX_TOPIC_PREFIX}/{safe_id}/set/{num_def['command_key']}",
                "min": num_def["min"],
                "max": num_def["max"],
                "step": num_def["step"],
                "icon": num_def.get("icon", ""),
                "availability_topic": avail_topic,
            }
            if num_def.get("unit"):
                num_config["unit_of_measurement"] = num_def["unit"]
            self._publish_discovery_config("number", safe_id, key, num_config)

        # --- Select entities ---
        for key, sel_def in profile.get("selects", {}).items():
            raw_options = sel_def["options"]
            translated_options = [t(opt) for opt in raw_options]
            cmd_key = sel_def["command_key"]
            template_parts = []
            for i, opt_name in enumerate(translated_options):
                if i == 0:
                    template_parts.append(f"{{% if value_json.{cmd_key} | default(0) | int == {i} %}}{opt_name}")
                else:
                    template_parts.append(f"{{% elif value_json.{cmd_key} | default(0) | int == {i} %}}{opt_name}")
            template_parts.append(f"{{% else %}}{translated_options[0]}{{% endif %}}")
            template = "".join(template_parts)

            sel_config = {
                "name": t(sel_def["name"]),
                "unique_id": f"duux_{safe_id}_{key}",
                "object_id": f"duux_{safe_id}_{key}",
                "device": device_info,
                "state_topic": state_topic,
                "value_template": template,
                "command_topic": f"{DUUX_TOPIC_PREFIX}/{safe_id}/set/{cmd_key}",
                "options": translated_options,
                "icon": sel_def.get("icon", ""),
                "availability_topic": avail_topic,
            }
            self._publish_discovery_config("select", safe_id, key, sel_config)

        # --- Sensor entities ---
        for key, sen_def in profile.get("sensors", {}).items():
            multiplier = sen_def.get("multiplier", 1)
            if multiplier != 1:
                val_template = (
                    f"{{{{ (value_json.{key} | default(0)) * {multiplier} }}}}"
                )
            else:
                val_template = f"{{{{ value_json.{key} | default(0) }}}}"

            sen_config = {
                "name": t(sen_def["name"]),
                "unique_id": f"duux_{safe_id}_{key}",
                "object_id": f"duux_{safe_id}_{key}",
                "device": device_info,
                "state_topic": state_topic,
                "value_template": val_template,
                "icon": sen_def.get("icon", ""),
                "availability_topic": avail_topic,
            }
            if sen_def.get("device_class"):
                sen_config["device_class"] = sen_def["device_class"]
            if sen_def.get("unit"):
                sen_config["unit_of_measurement"] = sen_def["unit"]
            self._publish_discovery_config("sensor", safe_id, key, sen_config)

        # --- Binary sensor entities ---
        for key, bs_def in profile.get("binary_sensors", {}).items():
            bs_config = {
                "name": t(bs_def["name"]),
                "unique_id": f"duux_{safe_id}_{key}",
                "object_id": f"duux_{safe_id}_{key}",
                "device": device_info,
                "state_topic": state_topic,
                "value_template": (
                    f"{{{{ 'ON' if value_json.{key} is defined and "
                    f"value_json.{key} | int > 0 else 'OFF' }}}}"
                ),
                "icon": bs_def.get("icon", ""),
                "availability_topic": avail_topic,
            }
            if bs_def.get("device_class"):
                bs_config["device_class"] = bs_def["device_class"]
            self._publish_discovery_config(
                "binary_sensor", safe_id, key, bs_config
            )

        # Subscribe to command topics for this device
        self._subscribe_device_commands(safe_id)

        # Publish availability
        self._client.publish(avail_topic, "online", retain=True)

        self._discovered_devices.add(device_id)
        _LOGGER.info(
            "Published MQTT Discovery for device %s (model: %s)",
            device_id,
            model,
        )

    def publish_device_state(
        self, device_id: str, fan_data: dict[str, Any]
    ) -> None:
        """Publish device state update to HA.

        Args:
            device_id: The device's MAC address (lowercase).
            fan_data: Dictionary of state values (power, speed, mode, etc.).
        """
        safe_id = device_id.replace(":", "_")
        state_topic = f"{DUUX_TOPIC_PREFIX}/{safe_id}/state"
        payload = json.dumps(fan_data)
        self._client.publish(state_topic, payload, retain=True)
        _LOGGER.debug(
            "Published state for device %s: %s", device_id, payload
        )

    def publish_device_offline(self, device_id: str) -> None:
        """Mark a device as offline in HA."""
        safe_id = device_id.replace(":", "_")
        avail_topic = f"{DUUX_TOPIC_PREFIX}/{safe_id}/availability"
        self._client.publish(avail_topic, "offline", retain=True)
        _LOGGER.info("Device %s marked as offline", device_id)

    def publish_device_online(self, device_id: str) -> None:
        """Mark a device as online in HA."""
        safe_id = device_id.replace(":", "_")
        avail_topic = f"{DUUX_TOPIC_PREFIX}/{safe_id}/availability"
        self._client.publish(avail_topic, "online", retain=True)
        _LOGGER.info("Device %s marked as online", device_id)

    def _publish_discovery_config(
        self,
        component: str,
        safe_id: str,
        entity_key: str,
        config: dict[str, Any],
    ) -> None:
        """Publish a single MQTT Discovery config message.

        Args:
            component: HA component type (fan, switch, sensor, etc.).
            safe_id: Safe device ID (colons replaced with underscores).
            entity_key: Unique key for this entity within the device.
            config: Discovery configuration payload.
        """
        topic = (
            f"{DISCOVERY_PREFIX}/{component}/duux_{safe_id}/"
            f"{entity_key}/config"
        )
        payload = json.dumps(config)
        self._client.publish(topic, payload, retain=True)
        _LOGGER.debug("Discovery config published: %s", topic)

    def _subscribe_device_commands(self, safe_id: str) -> None:
        """Subscribe to all command topics for a device."""
        command_topic = f"{DUUX_TOPIC_PREFIX}/{safe_id}/set/+"
        self._client.subscribe(command_topic)
        _LOGGER.info("Subscribed to command topic: %s", command_topic)

        # Also subscribe to the percentage speed command
        pct_topic = f"{DUUX_TOPIC_PREFIX}/{safe_id}/set/speed_pct"
        self._client.subscribe(pct_topic)

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        """Handle connection to the HA MQTT broker."""
        is_success = (rc == 0) or (getattr(rc, "value", None) == 0)
        if is_success:
            _LOGGER.info("Connected to HA MQTT broker")
            # Re-subscribe to all device command topics on reconnect
            for device_id in self._discovered_devices:
                safe_id = device_id.replace(":", "_")
                self._subscribe_device_commands(safe_id)
        else:
            _LOGGER.error(
                "Failed to connect to HA MQTT broker, return code: %s", rc
            )

    def _on_message(self, client, userdata, msg):
        """Handle incoming messages from HA (commands for devices).

        Messages arrive on topics like:
            duux/{safe_id}/set/power  → payload "1" or "0"
            duux/{safe_id}/set/speed  → payload "10"
            duux/{safe_id}/set/mode   → payload "Natural" (select name)
        """
        topic = msg.topic
        payload = msg.payload.decode("utf-8", errors="replace")

        _LOGGER.debug("HA command received: %s = %s", topic, payload)

        # Parse the topic: duux/{safe_id}/set/{command_key}
        parts = topic.split("/")
        if len(parts) != 4 or parts[0] != DUUX_TOPIC_PREFIX or parts[2] != "set":
            return

        safe_id = parts[1]
        command_key = parts[3]
        device_id = safe_id.replace("_", ":")

        # Handle percentage speed conversion
        if command_key == "speed_pct":
            model = self._device_models.get(device_id, "whisper_flex_2")
            profile = DEVICE_PROFILES.get(model, {})
            max_speed = profile.get("fan", {}).get("max_speed", 30)
            try:
                pct = float(payload)
                speed = max(1, round(pct / 100 * max_speed))
                command_key = "speed"
                payload = str(speed)
            except ValueError:
                _LOGGER.warning("Invalid speed percentage: %s", payload)
                return

        # Handle select entities: convert option name to numeric value
        model = self._device_models.get(device_id, "whisper_flex_2")
        profile = DEVICE_PROFILES.get(model, {})
        for sel_key, sel_def in profile.get("selects", {}).items():
            if sel_def["command_key"] == command_key:
                options = sel_def["options"]
                if payload in options:
                    payload = str(options.index(payload))
                break

        # Translate ON/OFF payloads to 1/0 for Duux commands
        if payload.upper() == "ON":
            payload = "1"
        elif payload.upper() == "OFF":
            payload = "0"

        # Build the Duux MQTT command: "tune set {key} {value}"
        duux_command = f"tune set {command_key} {payload}"

        if self._on_command:
            self._on_command(device_id, duux_command)
