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


from datetime import datetime, timezone

from ghga_message_schemas import schemas
from ghga_service_chassis_lib.utils import exec_with_timeout

from ..fixtures import (  # noqa: F401
    amqp_fixture,
    get_cont_and_conf,
    psql_fixture,
    s3_fixture,
    state,
)


def test_subscribe_new_study(
    psql_fixture,  # noqa: F811
    s3_fixture,  # noqa: F811
    amqp_fixture,  # noqa: F811
):  # noqa: F811
    """Test `subscribe_new_study` method"""

    container, config = get_cont_and_conf(
        sources=[psql_fixture.config, s3_fixture.config, amqp_fixture.config]
    )
    event_subscriber = container.event_subscriber()
    file_info = state.FILES["unknown"].file_info

    # build the upstream message:
    now_isostring = datetime.now(timezone.utc).isoformat()
    upstream_message = {
        "study": {"id": file_info.grouping_label},
        "associated_files": [
            {
                "file_id": file_info.file_id,
                "md5_checksum": file_info.md5_checksum,
                "size": file_info.size,
                "file_name": file_info.file_name,
                "creation_date": file_info.creation_date.isoformat(),
                "update_date": file_info.update_date.isoformat(),
                "format": file_info.format,
            }
        ],
        "timestamp": now_isostring,
    }

    # initialize upstream test service that will publish to this service:
    upstream_publisher = amqp_fixture.get_test_publisher(
        topic_name=config.topic_new_study,
        message_schema=schemas.SCHEMAS["new_study_created"],
    )

    # publish a stage request:
    upstream_publisher.publish(upstream_message)

    # process the stage request:
    exec_with_timeout(
        func=lambda: event_subscriber.subscribe_new_study(run_forever=False),
        timeout_after=2,
    )

    # check if file exists in db:
    psql_fixture.file_info_dao.get(file_info.file_id)


def test_file_registered(
    psql_fixture,  # noqa: F811
    s3_fixture,  # noqa: F811
    amqp_fixture,  # noqa: F811
):  # noqa: F811
    """Test `subscribe_file_registered` method"""

    container, config = get_cont_and_conf(
        sources=[psql_fixture.config, s3_fixture.config, amqp_fixture.config]
    )
    event_subscriber = container.event_subscriber()
    file_info = state.FILES["in_inbox"].file_info

    # build the upstream message:
    now_isostring = datetime.now(timezone.utc).isoformat()
    upstream_message = {
        "file_id": file_info.file_id,
        "md5_checksum": file_info.md5_checksum,
        "size": file_info.size,
        "creation_date": file_info.creation_date.isoformat(),
        "update_date": file_info.update_date.isoformat(),
        "format": file_info.format,
        "timestamp": now_isostring,
        "grouping_label": file_info.grouping_label,
    }

    # initialize upstream test service that will publish to this service:
    upstream_publisher = amqp_fixture.get_test_publisher(
        topic_name=config.topic_file_registered,
        message_schema=schemas.SCHEMAS["file_internally_registered"],
    )

    # publish a stage request:
    upstream_publisher.publish(upstream_message)

    # process the stage request:
    exec_with_timeout(
        func=lambda: event_subscriber.subscribe_file_registered(run_forever=False),
        timeout_after=1000000,
    )

    # now the file should be deleted:
    # with pytest.raises(ObjectNotFoundError):
    assert not s3_fixture.storage.does_object_exist(
        bucket_id=config.s3_inbox_bucket_id, object_id=file_info.file_id
    )
