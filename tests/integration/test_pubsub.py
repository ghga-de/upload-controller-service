# Copyright 2021 Universität Tübingen, DKFZ and EMBL
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

"""Test the messaging API (pubsub)"""

from typing import Any, Callable, Dict

from ghga_service_chassis_lib.utils import exec_with_timeout

from upload_controller_service.pubsub import schemas, subscribe_new_study_created

from ..fixtures import (  # noqa: F401
    DEFAULT_CONFIG,
    amqp_fixture,
    get_config,
    psql_fixture,
    s3_fixture,
    state,
)

# def test_subscribe_new_study_created(
#     psql_fixture, s3_fixture, amqp_fixture
# ):  # noqa: F811
#     """Test `subscribe_new_study_created` function"""

#     config = get_config(
#         sources=[psql_fixture.config, s3_fixture.config, amqp_fixture.config]
#     )

#     # initialize upstream and downstream test services that will publish or receive
#     # messages to or from this service:
#     upstream_publisher = amqp_fixture.get_test_publisher(
#         topic_name=DEFAULT_CONFIG.topic_name_new_study,
#         message_schema=schemas.NEW_STUDY,
#     )

#     # publish a stage request:
#     upstream_publisher.publish(upstream_message)

#     # process the stage request:
#     exec_with_timeout(
#         func=lambda: subscribe_new_study_created(config=config, run_forever=False),
#         timeout_after=2,
#     )
