import pytest
from kubernetes.client import Configuration
from onyx import OnyxConfig

from cherami.utils import init_kubernetes, init_onyx


def test_init_onyx(monkeypatch):
    monkeypatch.setenv("ONYX_DOMAIN", "example.com")
    monkeypatch.setenv("ONYX_TOKEN", "secret-token")
    config = init_onyx()

    assert isinstance(config, OnyxConfig)
    assert config.domain == "example.com"
    assert config.token == "secret-token"


def test_init_onyx_missing_env(monkeypatch):
    monkeypatch.delenv("ONYX_DOMAIN", raising=False)
    monkeypatch.delenv("ONYX_TOKEN", raising=False)
    with pytest.raises(ValueError, match="Missing environment variable"):
        init_onyx()


def test_init_kubernetes(monkeypatch, mocker):
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    token_content = "fake-jwt-token"
    mock_open = mocker.mock_open(read_data=token_content)
    mocker.patch("pathlib.Path.open", mock_open)
    mock_set_default = mocker.patch(
        "kubernetes.client.Configuration.set_default"
    )
    init_kubernetes()

    mock_set_default.assert_called_once()
    config = mock_set_default.call_args[0][0]
    assert isinstance(config, Configuration)
    assert config.host == "https://10.0.0.1"
    assert config.api_key["authorization"] == token_content
    assert config.api_key_prefix["authorization"] == "Bearer"
    assert (
        config.ssl_ca_cert
        == "/run/secrets/kubernetes.io/serviceaccount/ca.crt"
    )
