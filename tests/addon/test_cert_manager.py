"""Tests for CertManager module."""

import os
from pathlib import Path
from unittest.mock import patch, MagicMock

from duux_mqtt_bridge.rootfs.opt.duux_bridge.cert_manager import CertManager


def test_cert_manager_existing(tmp_path: Path):
    """Test using existing certificates."""
    cert_dir = tmp_path / "ssl"
    cert_dir.mkdir()
    cert_file = cert_dir / "cert.pem"
    key_file = cert_dir / "key.pem"
    cert_file.write_text("DUMMY CERT")
    key_file.write_text("DUMMY KEY")

    cm = CertManager()
    cm.cert_dir = cert_dir
    cm.cert_file = cert_file
    cm.key_file = key_file

    cert_path, key_path = cm.ensure_certificates()
    assert cert_path == str(cert_file)
    assert key_path == str(key_file)


@patch("subprocess.run")
def test_cert_manager_generate(mock_run: MagicMock, tmp_path: Path):
    """Test generating new self-signed certificates."""
    cert_dir = tmp_path / "ssl"
    cert_file = cert_dir / "cert.pem"
    key_file = cert_dir / "key.pem"

    def fake_subprocess_run(cmd, **kwargs):
        cert_dir.mkdir(parents=True, exist_ok=True)
        cert_file.write_text("NEW CERT")
        key_file.write_text("NEW KEY")
        return MagicMock(returncode=0)

    mock_run.side_effect = fake_subprocess_run

    cm = CertManager()
    cm.cert_dir = cert_dir
    cm.cert_file = cert_file
    cm.key_file = key_file

    cert_path, key_path = cm.ensure_certificates()

    assert mock_run.called
    assert cert_path == str(cert_file)
    assert key_path == str(key_file)


@patch("subprocess.run")
def test_cert_manager_checkend_30_days(mock_run: MagicMock, tmp_path: Path):
    """Test that certificate validity checks for 30 days (2592000 seconds)."""
    mock_run.return_value = MagicMock(returncode=0)

    cm = CertManager()
    cm.cert_file = tmp_path / "cert.pem"

    valid = cm._is_certificate_valid()
    assert valid is True

    # Check subprocess.run call args for -checkend 2592000
    cmd = mock_run.call_args[0][0]
    assert "-checkend" in cmd
    checkend_idx = cmd.index("-checkend")
    assert cmd[checkend_idx + 1] == str(30 * 86400)  # "2592000"
