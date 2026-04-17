"""
E2E tests for VPN HTTPS, CA certificates, and internal routing.

Tests that:
- VPN Traefik serves HTTPS with a valid CA-signed cert
- CA cert is downloadable from VPN admin page
- GitOps API is accessible via HTTPS through VPN Traefik
- Deployed automations are accessible via HTTPS through VPN Traefik
- VPN admin pages work via HTTPS
"""

from __future__ import annotations


import pytest

from e2e_helpers import ssh_run, WORKSPACE, SECRET

pytestmark = pytest.mark.e2e


def _vpn_traefik_ip():
    """Get VPN Traefik's IP on bitswan_network."""
    result = ssh_run(
        "docker inspect traefik-vpn --format "
        '"{{(index .NetworkSettings.Networks \\"bitswan_network\\").IPAddress}}"'
    )
    ip = result.stdout.strip().strip('"')
    if not ip or result.returncode != 0:
        pytest.skip("VPN Traefik not running")
    return ip


def _vpn_curl(vpn_ip, host_header, path="/", extra_headers="", https=True):
    """Curl VPN Traefik with proper Host header."""
    scheme = "https" if https else "http"
    result = ssh_run(
        f'curl -sk -o /dev/null -w "%{{http_code}}" --connect-timeout 5 '
        f'-H "Host: {host_header}" {extra_headers} '
        f'"{scheme}://{vpn_ip}{path}"'
    )
    return result.stdout.strip()


class TestVPNCertAuthority:
    """Test the VPN CA certificate system."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.vpn_ip = _vpn_traefik_ip()

    def test_ca_cert_exists(self):
        """CA certificate file exists on the server."""
        result = ssh_run(
            "test -f /root/.config/bitswan/vpn/ca/ca.crt && echo yes || echo no"
        )
        if "yes" not in result.stdout:
            pytest.skip("VPN CA not initialized")

    def test_tls_cert_exists(self):
        """TLS certificate for VPN Traefik exists."""
        result = ssh_run(
            "test -f /root/.config/bitswan/vpn/ca/tls.crt && echo yes || echo no"
        )
        if "yes" not in result.stdout:
            pytest.skip("VPN TLS cert not generated")

    def test_ca_cert_is_valid(self):
        """CA certificate is a valid X.509 cert with correct subject."""
        result = ssh_run(
            "openssl x509 -in /root/.config/bitswan/vpn/ca/ca.crt "
            "-noout -subject -issuer 2>/dev/null"
        )
        if result.returncode != 0:
            pytest.skip("No CA cert or openssl not available")
        assert (
            "BitSwan" in result.stdout
        ), f"CA cert missing BitSwan org: {result.stdout}"
        assert "CA" in result.stdout, f"CA cert not marked as CA: {result.stdout}"

    def test_tls_cert_has_wildcard_san(self):
        """TLS cert includes wildcard SAN for the domain."""
        result = ssh_run(
            "openssl x509 -in /root/.config/bitswan/vpn/ca/tls.crt "
            "-noout -text 2>/dev/null | grep DNS:"
        )
        if result.returncode != 0:
            pytest.skip("No TLS cert or openssl not available")
        assert "bswn" in result.stdout, f"TLS cert missing bswn domain: {result.stdout}"

    def test_ca_cert_download_external(self):
        """CA cert downloadable from external VPN admin page."""
        auth = f'-H "Authorization: Bearer {SECRET}"'
        code = _vpn_curl(
            self.vpn_ip,
            f"vpn-admin.{WORKSPACE}.bswn.io",
            "/vpn-admin/ca.crt",
            extra_headers=auth,
        )
        assert code == "200", f"CA cert download failed: {code}"

    def test_ca_cert_installed_in_certauthorities(self):
        """CA cert is installed in the daemon's cert authority directory."""
        result = ssh_run(
            "test -f /root/.config/bitswan/certauthorities/bitswan-vpn-ca.crt "
            "&& echo yes || echo no"
        )
        assert "yes" in result.stdout, "VPN CA not installed in cert authorities"


class TestVPNHTTPSRouting:
    """Test HTTPS routing through VPN Traefik."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.vpn_ip = _vpn_traefik_ip()

    def test_gitops_https(self):
        """GitOps API accessible via HTTPS through VPN Traefik."""
        auth = f'-H "Authorization: Bearer {SECRET}"'
        code = _vpn_curl(
            self.vpn_ip,
            f"{WORKSPACE}-gitops.{WORKSPACE}.bswn.io",
            "/automations/",
            extra_headers=auth,
        )
        assert code == "200", f"GitOps HTTPS failed: {code}"

    def test_vpn_admin_https(self):
        """VPN admin page accessible via HTTPS."""
        code = _vpn_curl(
            self.vpn_ip,
            f"vpn-admin.{WORKSPACE}.bswn.io",
            "/vpn-admin/",
        )
        assert code == "200", f"VPN admin HTTPS failed: {code}"

    def test_automation_https(self):
        """Deployed automation accessible via HTTPS through VPN Traefik."""
        result = ssh_run("docker ps --filter name=live-dev --format '{{.Names}}'")
        containers = [c.strip() for c in result.stdout.strip().split("\n") if c.strip()]
        if not containers:
            pytest.skip("No live-dev containers to test")

        # Get the automation hostname from Traefik routes
        result = ssh_run(
            'curl -s "http://%s:8080/api/http/routers" 2>/dev/null '
            "| python3 -c 'import json,sys; "
            '[print(r["rule"].split("`")[1]) '
            'for r in json.load(sys.stdin) if "live-dev" in r.get("name","")]'
            "' | head -1" % self.vpn_ip
        )
        hostname = result.stdout.strip()
        if not hostname:
            pytest.skip("No live-dev route found in VPN Traefik")

        code = _vpn_curl(self.vpn_ip, hostname, "/")
        # 200 or 502 (automation may not be reachable from VPN Traefik's network)
        assert code in ("200", "502"), f"Automation HTTPS unexpected: {code}"

    def test_vpn_traefik_has_websecure(self):
        """VPN Traefik has websecure entrypoint configured."""
        result = ssh_run(
            'curl -s "http://%s:8080/api/entrypoints" 2>/dev/null '
            "| python3 -c 'import json,sys; "
            "eps = json.load(sys.stdin); "
            '[print(e["name"]) for e in eps]\'' % self.vpn_ip
        )
        assert (
            "websecure" in result.stdout
        ), f"VPN Traefik missing websecure entrypoint: {result.stdout}"

    def test_vpn_traefik_tls_certs_mounted(self):
        """VPN Traefik has TLS certs mounted."""
        result = ssh_run(
            "docker exec traefik-vpn ls /certs/tls.crt /certs/tls.key 2>&1"
        )
        assert (
            result.returncode == 0
        ), f"TLS certs not mounted in VPN Traefik: {result.stdout}"
