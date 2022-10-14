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

"""Test the FileMetadataService"""

from datetime import datetime

from tests.fixtures.psql import PsqlFixture, psql_fixture  # noqa: F401
from ucs.core import models
from ucs.core.file_service import FileMetadataServive
from ucs.translators.outbound.psql.adapters import (
    PsqlFileMetadataDAO,
    PsqlUploadAttemptDAO,
)

# Examples:
# - there are two files
# - two upload attempts that can be registered to the first file
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

EXAMPLE_UPLOADS = (
    models.UploadAttempt(
        upload_id="testUpload001",
        file_id="testFile001",
        status=models.UploadStatus.CANCELLED,
        part_size=1234,
    ),
    models.UploadAttempt(
        upload_id="testUpload002",
        file_id="testFile001",
        status=models.UploadStatus.PENDING,
        part_size=1234,
    ),
)


def test_happy(psql_fixture: PsqlFixture):  # noqa: F811
    """Tests the basic happy path of using the FileMetadataService"""

    # construct service and dependencies:
    upload_attempt_dao = PsqlUploadAttemptDAO(config=psql_fixture.config)
    file_metadata_dao = PsqlFileMetadataDAO(config=psql_fixture.config)
    file_metadata_service = FileMetadataServive(
        file_metadata_dao=file_metadata_dao,
        upload_attempt_dao=upload_attempt_dao,
    )

    # create file metadata:
    file_metadata_service.upsert_multiple(EXAMPLE_FILES)

    # check that the newly created file metadata is available:
    for expected_file in EXAMPLE_FILES:
        obtained_file = file_metadata_service.get(file_id=expected_file.file_id)

        # compare the parameters that are part of the expected_file:
        expected_file_data = expected_file.dict()
        assert expected_file_data == obtained_file.dict(
            include=expected_file_data.keys()
        )

        # no upload is expected to exist, yet:
        assert obtained_file.latest_upload_id is None

    # side-load upload attempts to the first file example:
    psql_fixture.populate_upload_attempts(EXAMPLE_UPLOADS)
    expected_upload_id = EXAMPLE_UPLOADS[-1].upload_id
    corresponding_file_id = EXAMPLE_FILES[0].file_id

    # check the latest upload attempt using the service again:
    obtained_file = file_metadata_service.get(file_id=corresponding_file_id)
    assert obtained_file.latest_upload_id == expected_upload_id
