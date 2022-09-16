import pytest

def pytest_addoption(parser):
    parser.addoption("--namespace")
    parser.addoption("--artifact-bucket")

@pytest.fixture
def namespace(request):
    return request.config.getoption("namespace")

@pytest.fixture
def artifact_bucket(request):
    return request.config.getoption("artifact_bucket")

@pytest.fixture
def syn():
    pass

def safe_load_config(artifact_bucket, namespace):
    config_uri = (
            f"s3://{artifact_bucket}/BridgeDownstream/{namespace}/"
            "config/config.yaml"
    )
    reque
