"""Certificate manager for the Duux MQTT Bridge.

This module is responsible for ensuring that the necessary TLS certificates
are available for the MQTT broker, generating self-signed certificates
if they do not exist.
"""

import logging
import subprocess
from pathlib import Path

# Constants for certificate generation
CERT_DIR = "/ssl/duux_mqtt_bridge"
HOSTNAME = "collector3.cloudgarden.nl"

_LOGGER = logging.getLogger(__name__)


class CertManager:
    """Manages SSL/TLS certificates for the MQTT broker."""

    def __init__(self) -> None:
        """Initialize the CertManager."""
        self.cert_dir = Path(CERT_DIR)
        self.cert_file = self.cert_dir / "cert.pem"
        self.key_file = self.cert_dir / "key.pem"

    def ensure_certificates(self) -> tuple[str, str]:
        """Ensure that certificates exist and are valid, generating them if necessary.

        Returns:
            tuple[str, str]: A tuple containing the paths to the (certificate, key) files.
        """
        if self.cert_file.exists() and self.key_file.exists():
            if self._is_certificate_valid():
                _LOGGER.info(
                    "Using existing valid certificates in %s", self.cert_dir
                )
                return str(self.cert_file), str(self.key_file)
            _LOGGER.warning(
                "Existing certificate in %s will expire within 30 days or is invalid. Regenerating...",
                self.cert_dir,
            )

        _LOGGER.info(
            "Generating new self-signed certificates for %s...",
            HOSTNAME,
        )

        # Create directory if it doesn't exist
        self.cert_dir.mkdir(parents=True, exist_ok=True)

        # Generate self-signed certificate using openssl
        try:
            subprocess.run(
                [
                    "openssl",
                    "req",
                    "-x509",
                    "-newkey",
                    "rsa:2048",
                    "-keyout",
                    str(self.key_file),
                    "-out",
                    str(self.cert_file),
                    "-days",
                    "3650",
                    "-nodes",
                    "-subj",
                    f"/CN={HOSTNAME}",
                    "-addext",
                    f"subjectAltName=DNS:{HOSTNAME}",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as err:
            _LOGGER.error(
                "Failed to generate certificates: %s\n%s", err, err.stderr
            )
            raise

        # Set appropriate file permissions
        self.cert_file.chmod(0o644)
        self.key_file.chmod(0o600)

        _LOGGER.info(
            "Successfully generated self-signed certificates in %s",
            self.cert_dir,
        )

        return str(self.cert_file), str(self.key_file)

    def _is_certificate_valid(self, min_remaining_seconds: int = 30 * 86400) -> bool:
        """Check if the certificate is valid for at least min_remaining_seconds (default: 30 days / 1 month)."""
        try:
            res = subprocess.run(
                [
                    "openssl",
                    "x509",
                    "-checkend",
                    str(min_remaining_seconds),
                    "-noout",
                    "-in",
                    str(self.cert_file),
                ],
                capture_output=True,
                text=True,
            )
            return res.returncode == 0
        except Exception as err:
            _LOGGER.warning("Could not check certificate validity: %s", err)
            return False
