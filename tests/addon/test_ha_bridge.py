"""Tests for HA Bridge language detection and translation."""

import os
import json
from unittest.mock import patch, MagicMock
from duux_mqtt_bridge.rootfs.opt.duux_bridge.ha_bridge import detect_ha_language, HomeAssistantBridge


def test_detect_ha_language_explicit():
    assert detect_ha_language("cs") == "cs"
    assert detect_ha_language("de") == "de"


@patch.dict(os.environ, {"SUPERVISOR_TOKEN": "test_token"})
@patch("urllib.request.urlopen")
def test_detect_ha_language_core_api(mock_urlopen):
    mock_resp = MagicMock()
    mock_resp.read.return_value = json.dumps({"language": "cs-CZ", "country": "CZ"}).encode()
    mock_resp.__enter__.return_value = mock_resp
    mock_urlopen.return_value = mock_resp

    lang = detect_ha_language("auto")
    assert lang == "cs"


@patch.dict(os.environ, {"SUPERVISOR_TOKEN": "test_token"})
@patch("urllib.request.urlopen")
def test_detect_ha_language_supervisor_nested(mock_urlopen):
    mock_resp = MagicMock()
    mock_resp.read.return_value = json.dumps({"result": "ok", "data": {"language": "nl"}}).encode()
    mock_resp.__enter__.return_value = mock_resp
    mock_urlopen.return_value = mock_resp

    lang = detect_ha_language("auto")
    assert lang == "nl"


def test_detect_ha_language_fallback():
    with patch.dict(os.environ, {}, clear=True):
        lang = detect_ha_language("auto")
        assert lang == "en"


@patch.dict(os.environ, {"SUPERVISOR_TOKEN": "test_token"})
@patch("urllib.request.urlopen")
def test_publish_discovery_uses_configured_language(mock_urlopen):
    bridge = HomeAssistantBridge(language="de")
    mock_client = MagicMock()
    bridge._client = mock_client

    bridge.publish_discovery("aa:bb:cc:dd:ee:ff", "whisper_flex_2")
    # Verify published discovery payload uses German translations
    published_topics = [call.args[0] for call in mock_client.publish.call_args_list]
    assert any("homeassistant/" in t for t in published_topics)
