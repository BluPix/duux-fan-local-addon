# Home Assistant Add-on: Duux MQTT Bridge

Local control bridge for **Duux** smart devices (Whisper Flex, Whisper Flex 2, Whisper Flex Ultimate, Bright 2) via embedded TLS MQTT server on port 443.

## How it works

1. The Duux fan connects via TLS MQTT on port 443 to `collector3.cloudgarden.nl`.
2. A local DNS rewrite (AdGuard Home, Pi-hole, UniFi, or Router) points `collector3.cloudgarden.nl` to your Home Assistant IP address.
3. This add-on binds to port 443 with self-signed TLS certificates, accepts the fan's MQTT connection, and auto-detects its credentials (MAC address & token).
4. Data and state updates from the fan are translated into standard **Home Assistant MQTT Discovery** entities (fan speed, oscillation modes, night mode, battery sensor, PM10, etc.).

## Prerequisites

- **Home Assistant OS** or **Supervised** installation (required for add-ons)
- **Mosquitto broker** add-on installed and configured in Home Assistant
- **DNS Spoofing / Redirect**: A DNS rule pointing `collector3.cloudgarden.nl` to your Home Assistant IP address

## Installation

1. Go to **Settings** → **Apps** (or **Add-ons**) → **App Store** (or **Add-on Store**).
2. Click the three dots menu (top right) → **Repositories**.
3. Add `https://github.com/BluPix/duux-fan-local-addon` as a repository.
4. Search for **Duux MQTT Bridge** and click **Install**.
5. Click **Start**.

## Configuration

Default options work out of the box for standard Home Assistant OS setups:

```yaml
mqtt_host: "core-mosquitto"
mqtt_port: 1883
mqtt_username: ""
mqtt_password: ""
broker_port: 443
log_level: "info"
```

- `broker_port`: Port to accept Duux device connections on (default: `443`).
- `mqtt_host`: Hostname of Mosquitto add-on (default: `core-mosquitto`).

## Troubleshooting

- **Check logs**: Look at the add-on logs to see if your fan is attempting to connect.
- **Port 443 conflict**: Ensure no other add-on or process on your Home Assistant host is using port 443. (Home Assistant's web interface runs on port 8123 by default, so port 443 is usually free).
- **Restart fan**: Unplug the fan for 10 seconds and plug it back in to force a fresh DNS lookup and MQTT connection.
