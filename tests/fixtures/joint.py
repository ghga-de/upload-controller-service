# Copyright 2021 - 2022 Universität Tübingen, DKFZ and EMBL
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

__all__ = [
    "joint_fixture",
    "JointFixture",
    "psql_fixture",
    "amqp_fixture",
    "s3_fixture",
]

from dataclasses import dataclass

import fastapi.testclient
import pytest_asyncio

from tests.fixtures.amqp import AmqpFixture, amqp_fixture
from tests.fixtures.config import get_config
from tests.fixtures.psql import PsqlFixture, psql_fixture
from tests.fixtures.s3 import S3Fixture, s3_fixture
from ucs.config import Config
from ucs.container import Container
from ucs.main import get_configured_container, get_rest_api


@dataclass
class JointFixture:
    """Returned by the `joint_fixture`."""

    config: Config
    container: Container
    psql: PsqlFixture
    amqp: AmqpFixture
    rest_client: fastapi.testclient.TestClient
    s3: S3Fixture


@pytest_asyncio.fixture
async def joint_fixture(
    psql_fixture: PsqlFixture, amqp_fixture: AmqpFixture, s3_fixture: S3Fixture
) -> JointFixture:
    """A fixture that embeds all other fixtures for API-level integration testing"""

    # merge configs from different sources with the default one:
    config = get_config(
        sources=[psql_fixture.config, amqp_fixture.config, s3_fixture.config]
    )

    # create a DI container instance:
    async with get_configured_container(config=config) as container:
        container.wire(modules=["ucs.translators.inbound.fastapi_.routes"])

        # setup an API test client:
        api = get_rest_api(config=config)
        rest_client = fastapi.testclient.TestClient(api)

        return JointFixture(
            config=config,
            container=container,
            psql=psql_fixture,
            amqp=amqp_fixture,
            rest_client=rest_client,
            s3=s3_fixture,
        )
