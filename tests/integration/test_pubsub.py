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

from datetime import datetime

from ghga_service_chassis_lib.utils import exec_with_timeout

from upload_controller_service.core.main import check_uploaded_file
from upload_controller_service.pubsub import (
    publish_upload_received,
    schemas,
    subscribe_new_study,
)

from ..fixtures import (  # noqa: F401
    amqp_fixture,
    get_config,
    psql_fixture,
    s3_fixture,
    state,
)


def test_subscribe_new_study(
    psql_fixture,  # noqa: F811
    s3_fixture,  # noqa: F811
    amqp_fixture,  # noqa: F811
):  # noqa: F811
    """Test `subscribe_new_study` function"""

    config = get_config(
        sources=[psql_fixture.config, s3_fixture.config, amqp_fixture.config]
    )
    file_info = state.FILES["unknown"].file_info

    # build the upstream message:
    now_isostring = datetime.utcnow().isoformat()
    upstream_message = {
        "study": {"id": file_info.grouping_label},
        "associated_files": [
            {
                "file_id": file_info.file_id,
                "md5_checksum": file_info.md5_checksum,
                "size": str(file_info.size),
                "file_name": file_info.file_name,
                "creation_date": now_isostring,
                "update_date": now_isostring,
                "format": "yaml",
            }
        ],
        "timestamp": now_isostring,
    }

    # initialize upstream test service that will publish to this service:
    upstream_publisher = amqp_fixture.get_test_publisher(
        topic_name=config.topic_name_new_study,
        message_schema=schemas.NEW_STUDY,
    )

    # publish a stage request:
    upstream_publisher.publish(upstream_message)

    # process the stage request:
    exec_with_timeout(
        func=lambda: subscribe_new_study(config=config, run_forever=False),
        timeout_after=1000,
    )

    # check if file exists in db:
    psql_fixture.database.get_file(file_info.file_id)


def test_publish_upload_received(
    psql_fixture,  # noqa: F811
    s3_fixture,  # noqa: F811
    amqp_fixture,  # noqa: F811
):  # noqa: F811
    """Test `subscribe_new_study` function"""
    config = get_config(
        sources=[psql_fixture.config, s3_fixture.config, amqp_fixture.config]
    )

    file_id = state.FILES["in_inbox"].file_info.file_id

    # initialize downstream test service that will receive the message from this service:
    downstream_subscriber = amqp_fixture.get_test_subscriber(
        topic_name=config.topic_name_upload_received,
        message_schema=schemas.UPLOAD_RECEIVED,
    )

    check_uploaded_file(
        file_id,
        publish_upload_received=publish_upload_received,
        config=config,
    )

    # receive the published message:
    downstream_message = downstream_subscriber.subscribe(timeout_after=2)
    assert downstream_message["file_id"] == file_id
