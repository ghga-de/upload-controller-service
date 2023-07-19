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

"""Contains module-scoped fixtures"""

import pytest_asyncio
from hexkit.providers.akafka.testutils import get_kafka_fixture
from hexkit.providers.mongodb.testutils import get_mongodb_fixture
from hexkit.providers.s3.testutils import get_s3_fixture
from hexkit.providers.testing.utils import get_event_loop

from tests.fixtures.joint import JointFixture, get_joint_fixture


@pytest_asyncio.fixture
async def reset_state(joint_fixture: JointFixture):  # noqa: F811
    """Clear joint_fixture state before and after tests that use this fixture.

    This is a function-level fixture because it needs to run in each test.
    """
    await joint_fixture.reset_state()
    yield
    await joint_fixture.reset_state()


event_loop = get_event_loop("module")
mongodb_fixture = get_mongodb_fixture("module")
kafka_fixture = get_kafka_fixture("module")
s3_fixture = get_s3_fixture("module")
joint_fixture = get_joint_fixture("module")
