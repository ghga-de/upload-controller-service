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

from tests.fixtures.joint import *  # noqa: F403
from ucs.domain import models

EXAMPLE_FILES = (
    models.FileMetadata(
        file_id="testFile001",
        file_name="Test File 001",
        md5_checksum="fake-checksum",
        size=12345678,
        grouping_label="test",
        creation_date=datetime.now(),
        update_date=datetime.now(),
        format="txt",
    ),
    models.FileMetadata(
        file_id="testFile002",
        file_name="Test File 002",
        md5_checksum="fake-checksum",
        size=12345678,
        grouping_label="test",
        creation_date=datetime.now(),
        update_date=datetime.now(),
        format="txt",
    ),
)


def test_subscribe_new_study(joint_fixture: JointFixture):  # noqa: F405
    """Test `subscribe_new_study` method"""

    # build the upstream event:
    associated_files = [
        {
            "file_id": file.file_id,
            "md5_checksum": file.md5_checksum,
            "size": file.size,
            "file_name": file.file_name,
            "creation_date": file.creation_date.isoformat(),
            "update_date": file.update_date.isoformat(),
            "format": file.format,
        }
        for file in EXAMPLE_FILES
    ]
    now_isostring = datetime.now(timezone.utc).isoformat()
    event = {
        "study": {"id": EXAMPLE_FILES[0].grouping_label},
        "associated_files": associated_files,
        "timestamp": now_isostring,
    }

    # publish test event:
    upstream_publisher = joint_fixture.amqp.get_test_publisher(
        topic_name=joint_fixture.config.topic_new_study,
        message_schema=schemas.SCHEMAS["new_study_created"],
    )
    upstream_publisher.publish(event)

    # use the event subscriber to receive and process the event:
    event_subscriber = joint_fixture.container.event_subscriber()
    exec_with_timeout(
        func=lambda: event_subscriber.subscribe_new_study(run_forever=False),
        timeout_after=2,
    )

    # check if file metadata has been persisted successfully:
    file_metadata_dao = joint_fixture.container.file_metadata_dao()
    with file_metadata_dao as fm_dao:
        for expected_file in EXAMPLE_FILES:
            obtained_file = fm_dao.get(expected_file.file_id)
            assert obtained_file == expected_file
