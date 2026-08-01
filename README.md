# 🌀 Duux MQTT Bridge — Home Assistant Add-on

> ⚠️ **DISCLAIMER & MAINTENANCE NOTE:**
> **This project is 100% AI Slop / Vibe Coded** (built using Google Antigravity AI agent).
> It is based on and inspired by the original work in [LouisR-git/duux-fan-local](https://github.com/LouisR-git/duux-fan-local).
> It was created as a standalone Home Assistant Add-on to solve local control for my own **Duux smart device** without relying on proprietary cloud services.
> 
> **I only own a single Duux device and do not have the time, hardware, or resources to further expand or maintain this project.**
> Now that my own device works, I am stepping away from active development. If it works for you, awesome! If you want to add support for other devices or fix bugs, **please feel free to FORK THIS REPOSITORY and take over development!** 🚀

---

## 💡 Why this project exists (The Problem & The Solution)

Duux smart devices (like the **Duux Whisper Flex 2**, **Whisper Flex Ultimate**, **Bright 2**, etc.) communicate with the Duux Cloud (`collector3.cloudgarden.nl`) via **MQTT over TLS on port 443**.

Normally, taking back local control of these devices required:
1. Running a complex external MQTT broker (like EMQX or standalone Mosquitto) on port 443.
2. Generating custom TLS/SSL certificates and configuring reverse proxies.
3. Writing manual YAML configurations or custom integrations with hardcoded MAC addresses.

### 🌟 The Solution: Duux MQTT Bridge Add-on

This project is a standalone **Home Assistant OS Add-on** that:
- Runs an embedded, lightweight Python MQTT server directly on **port 443 (TLS)**.
- Automatically generates self-signed TLS certificates for `collector3.cloudgarden.nl`.
- Intercepts incoming connections from Duux devices on your local network.
- Decodes Duux's nested MQTT JSON payloads (`{"sub":{"Tune":[{...}]}}`).
- Automatically creates and updates entities in Home Assistant via **standard HA MQTT Discovery** (`homeassistant/fan/...`, `homeassistant/number/...`, `homeassistant/select/...`).

No manual MAC address entry. No complex certificate setups. Zero-config local control!

---

## ⚡ Quick Start / Installation

### 1. Install the Add-on in Home Assistant OS

1. Open **Home Assistant** → **Settings** → **Apps** (or **Add-ons**) → **App Store** (or **Add-on Store**).
2. Click the **three dots (⋮)** in the top-right corner → **Repositories**.
3. Add this repository URL:
   `https://github.com/BluPix/duux-fan-local-addon`
4. Search for **Duux MQTT Bridge** and click **Install**.
5. Click **Start**.

*(Note: The Add-on automatically connects to your local Home Assistant Mosquitto broker using Supervisor auto-discovery. You don't need to configure any passwords!)*

### 2. Local DNS Rewrite (The Only Required Step)

In your local DNS resolver (AdGuard Home, Pi-hole, UniFi Gateway, or Router DNS), add a DNS rewrite rule:

```text
collector3.cloudgarden.nl  ➔  <YOUR_HOME_ASSISTANT_IP> (e.g. 10.0.0.9)
```

### 3. Restart your Duux Device

Unplug your Duux fan/purifier from power for 5 seconds and plug it back in. 

The device will connect to your Home Assistant IP on port 443, perform the TLS handshake with the add-on, and **all entities (Fan speed, modes, night mode, child lock, oscillation, battery level) will automatically appear in Home Assistant!** 🎉

---

## 📱 Supported Devices & Features

### Duux Whisper Flex 2 (Smart)
- 🌀 **Fan Control**: Power, Speed (1-30), Fan Mode (Regular / Natural Wind)
- ↔️ **Horizontal Oscillation**: Off, 30°, 60°, 90°
- ↕️ **Vertical Oscillation**: Off, 45°, 100°
- 🌙 **Night Mode**: On / Off
- 🔒 **Child Lock**: On / Off
- ⏱️ **Timer**: 0 to 12 hours
- 🔋 **Battery Sensors**: Battery Level (%) & Charging Status

### Duux Bright 2 (Air Purifier) & Whisper Flex Ultimate
- Supported via protocol parser mapping.

---

## 🧪 Technical Details

- **Port 443 Listener**: `asyncio` TLS server running inside Docker with `host_network: true`.
- **MQTT 3.1.1 Parser**: Lightweight custom stdlib parser (`duux_mqtt_bridge/rootfs/opt/duux_bridge/mqtt_protocol.py`).
- **HA Bridge**: Translates device state (`sensor/{mac}/in`) to HA state topics and forwards HA commands (`duux/{mac}/set/...`) back to device command topics (`sensor/{mac}/command`).
- **Cert Manager**: Auto-generates 10-year RSA 2048 self-signed certs in `/ssl/duux_mqtt_bridge`.

---

## 🤝 Credits & Acknowledgements

- Based on and inspired by [LouisR-git/duux-fan-local](https://github.com/LouisR-git/duux-fan-local).
- Community forks are warmly welcomed to add support for more Duux devices or new features.

*Crafted with 🤖 AI Slop & 🌀 Vibe Coding.*
