import pytest

# requires pytest-datadir to be installed


def pytest_addoption(parser):
    parser.addoption("--namespace")
    parser.addoption("--artifact-bucket")


@pytest.fixture(scope="session")
def namespace(request):
    return request.config.getoption("namespace")


@pytest.fixture(scope="session")
def artifact_bucket(request):
    return request.config.getoption("artifact_bucket")
