# Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

"""Define fixtures with module scope"""

import asyncio

import pytest
import pytest_asyncio
from hexkit.providers.testing.fixtures import (
    KafkaFixture,
    MongoDbFixture,
    S3Fixture,
    get_fixture,
)

from tests.fixtures.joint import JointFixture, get_joint_fixture


@pytest.fixture(scope="module")
def event_loop():
    """event loop fixture"""
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture()
async def reset_state(joint_fixture: JointFixture):
    """Reset joint_fixture state before and after the test function"""
    await joint_fixture.clear_state()
    yield
    await joint_fixture.clear_state()


kafka_fixture = get_fixture(KafkaFixture, "module")
mongodb_fixture = get_fixture(MongoDbFixture, "module")
s3_fixture = get_fixture(S3Fixture, "module")
joint_fixture = get_joint_fixture("module")
