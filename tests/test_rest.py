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

"""Test the api module"""

import json
from datetime import datetime

import pytest
from fastapi import status

from tests.fixtures.joint import *  # noqa: 403
from ucs.domain import models

# Examples:
# - there are two files
# - two upload attempts that can be registered to the first file

EXAMPLE_FILE = models.FileMetadata(
    file_id="testFile001",
    file_name="Test File 001",
    md5_checksum="fake-checksum",
    size=12345678,
    grouping_label="test",
    creation_date=datetime.now(),
    update_date=datetime.now(),
    format="txt",
)

EXAMPLE_UPLOADS = (
    models.UploadAttempt(
        upload_id="testUpload001",
        file_id="testFile001",
        status=models.UploadStatus.PENDING,
    ),
    models.UploadAttempt(
        upload_id="testUpload002",
        file_id="testFile001",
        status=models.UploadStatus.PENDING,
    ),
)


def test_get_health(joint_fixture: JointFixture):  # noqa: F405
    """Test the GET /health endpoint"""

    response = joint_fixture.rest_client.get("/health")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {"status": "OK"}


@pytest.mark.parametrize("populate_uploads", (True, False))
def test_get_file_metadata_happy(
    populate_uploads: bool, joint_fixture: JointFixture  # noqa: F405
):  # noqa: F811
    """Test the happy path of using the get_file_metadata endpoint"""

    expected_content = json.loads(EXAMPLE_FILE.json())
    expected_content["latest_upload_id"] = (
        EXAMPLE_UPLOADS[-1].upload_id if populate_uploads else None
    )

    # populate the database:
    joint_fixture.psql.populate_file_metadata([EXAMPLE_FILE])
    if populate_uploads:
        joint_fixture.psql.populate_upload_attempts(EXAMPLE_UPLOADS)

    file_id = expected_content["file_id"]
    response = joint_fixture.rest_client.get(f"/files/{file_id}")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == expected_content


def test_get_file_metadata_not_found(joint_fixture: JointFixture):  # noqa: F405
    """Test the get_file_metadata endpoint with an non-existing file id."""

    file_id = "myNonExistingFile001"
    response = joint_fixture.rest_client.get(f"/files/{file_id}")

    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.json()["exceptionId"] == "fileNotRegistered"
