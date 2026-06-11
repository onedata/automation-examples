"""
Shared fixtures for the REST checksum tests: run pytest-httpserver over TLS (the handler
talks to the provider via `https://`), with an ad-hoc CA from `trustme`. Tests set
`VERIFY_SSL_CERTIFICATES=false` so `requests` accepts the self-signed cert.
"""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2022-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import ssl

import pytest
import trustme


@pytest.fixture(scope="session")
def ca() -> trustme.CA:
    return trustme.CA()


@pytest.fixture(scope="session")
def httpserver_ssl_context(ca: trustme.CA) -> ssl.SSLContext:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ca.issue_cert("localhost").configure_cert(context)
    return context
