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

"""Test the event consumption"""

from ghga_message_schemas import schemas

from ..fixtures import amqp_fixture, get_cont_and_conf, state  # noqa: F401


def test_publish_upload_received(
    amqp_fixture,  # noqa: F811
):  # noqa: F811
    """Test `subscribe_new_study` method"""
    container, config = get_cont_and_conf(sources=[amqp_fixture.config])
    event_publisher = container.event_publisher()

    file_info = state.FILES["in_inbox"].file_info

    # initialize downstream test service that will receive the message from this service:
    downstream_subscriber = amqp_fixture.get_test_subscriber(
        topic_name=config.topic_upload_received,
        message_schema=schemas.SCHEMAS["file_upload_received"],
    )

    event_publisher.publish_upload_received(file_info)

    # receive the published message:
    downstream_message = downstream_subscriber.subscribe(timeout_after=2)
    assert downstream_message["file_id"] == file_info.file_id
