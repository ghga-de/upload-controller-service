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

from hexkit.providers.akafka.testutils import get_kafka_fixture
from hexkit.providers.mongodb.testutils import get_mongodb_fixture
from hexkit.providers.s3.testutils import get_s3_fixture
from hexkit.providers.testing.utils import get_event_loop

from tests.fixtures.joint import get_joint_fixture

SCOPE = "module"

event_loop = get_event_loop(SCOPE)

mongodb_fixture = get_mongodb_fixture(SCOPE)
kafka_fixture = get_kafka_fixture(SCOPE)
s3_fixture = get_s3_fixture(SCOPE)
joint_fixture = get_joint_fixture(SCOPE)
