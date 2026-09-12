# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import pytest
from triplestore_service import start_triplestore, stop_triplestore


@pytest.fixture(scope="session", autouse=True)
def triplestore(request: pytest.FixtureRequest) -> None:
    request.addfinalizer(stop_triplestore)  # noqa: PT021
    start_triplestore()
