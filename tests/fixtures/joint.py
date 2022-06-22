# Copyright 2022 Universität Tübingen, DKFZ and EMBL
# for the German Human Genome-Phenome Archive (GHGA)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Join the functionality of all fixtures for API-level integration testing."""

__all__ = ["psql_fixture", "joint_fixture", "JointFixture"]

from dataclasses import dataclass

import pytest

from tests.fixtures.config import get_config
from tests.fixtures.psql import PsqlFixture, psql_fixture
from tests.fixtures.rest import RestTestClient
from ucs.config import Config
from ucs.container import Container
from ucs.main import setup_container


@dataclass
class JointFixture:
    """Returned by the `joint_fixture`."""

    config: Config
    container: Container
    psql: PsqlFixture
    rest_client: RestTestClient


@pytest.fixture
def joint_fixture(psql_fixture: PsqlFixture) -> JointFixture:
    """A fixture that embeds all other fixtures for API-level integration testing"""

    # merge configs from different sources with the default one:
    config = get_config(sources=[psql_fixture.config])

    # create a DI container instance:
    container = setup_container(config=config)

    # setup an API test client:
    rest_client = RestTestClient(config=config)

    return JointFixture(
        config=config,
        container=container,
        psql=psql_fixture,
        rest_client=rest_client,
    )
