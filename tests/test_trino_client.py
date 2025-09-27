from pathlib import Path
import sys

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.services.trino_client import TrinoClient


def test_parse_standard_jdbc_url():
    params = TrinoClient._parse_jdbc_url(
        "jdbc://example.com:8080/sample_catalog/sample_schema?user=alice"
    )

    assert params.host == "example.com"
    assert params.port == 8080
    assert params.catalog == "sample_catalog"
    assert params.schema == "sample_schema"
    assert params.user == "alice"
    assert params.http_scheme == "http"


@pytest.mark.parametrize(
    "jdbc_url",
    [
        "jdbc:trino://example.com:8443/sample_catalog/sample_schema?user=alice&https=true",
        "jdbc:presto://example.com:8443/sample_catalog/sample_schema?user=alice&ssl=1",
    ],
)
def test_parse_trino_prefixed_jdbc_url(jdbc_url):
    params = TrinoClient._parse_jdbc_url(jdbc_url)

    assert params.host == "example.com"
    assert params.port == 8443
    assert params.catalog == "sample_catalog"
    assert params.schema == "sample_schema"
    assert params.user == "alice"
    assert params.http_scheme == "https"
