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

"""Test edge cases of the fastapi_ adapter not covered by `test.test_api_journey`."""

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


def test_get_health(joint_fixture: JointFixture):  # noqa: F405
    """Test the GET /health endpoint"""

    response = joint_fixture.rest_client.get("/health")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {"status": "OK"}


def test_get_file_metadata_not_found(joint_fixture: JointFixture):  # noqa: F405
    """Test the get_file_metadata endpoint with an non-existing file id."""

    file_id = "myNonExistingFile001"
    response = joint_fixture.rest_client.get(f"/files/{file_id}")

    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.json()["exceptionId"] == "fileNotRegistered"


def test_create_upload_not_found(joint_fixture: JointFixture):  # noqa: F405
    """Test the create_upload endpoint with an non-existing file id."""

    file_id = "myNonExistingFile001"
    response = joint_fixture.rest_client.post("/uploads", json={"file_id": file_id})

    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert response.json()["exceptionId"] == "fileNotRegistered"


@pytest.mark.parametrize(
    "existing_status",
    [
        status_
        for status_ in [
            models.UploadStatus.PENDING,
            models.UploadStatus.UPLOADED,
            models.UploadStatus.ACCEPTED,
        ]
    ],
)
def test_create_upload_other_active(
    existing_status: models.UploadStatus, joint_fixture: JointFixture  # noqa: F405
):
    """Test the create_upload endpoint when there is another active update already
    existing."""

    existing_upload = EXAMPLE_UPLOADS[0].copy(update={"status": existing_status})

    # insert a pending upload into the database:
    joint_fixture.psql.populate_file_metadata([EXAMPLE_FILE])
    joint_fixture.psql.populate_upload_attempts([existing_upload])

    response = joint_fixture.rest_client.post(
        "/uploads", json={"file_id": EXAMPLE_FILE.file_id}
    )

    assert response.status_code == status.HTTP_400_BAD_REQUEST
    response_body = response.json()
    assert response_body["exceptionId"] == "existingActiveUpload"
    assert response_body["data"]["active_upload"] == json.loads(existing_upload.json())


def test_get_upload_not_found(joint_fixture: JointFixture):  # noqa: F405
    """Test the get_upload endpoint with non-existing upload ID."""

    upload_id = "myNonExistingUpload001"
    response = joint_fixture.rest_client.get(f"/uploads/{upload_id}")

    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.json()["exceptionId"] == "noSuchUpload"


def test_update_upload_status_not_found(joint_fixture: JointFixture):  # noqa: F405
    """Test the update_upload_status endpoint with non existing upload ID."""

    upload_id = "myNonExistingUpload001"

    response = joint_fixture.rest_client.patch(
        f"/uploads/{upload_id}", json={"status": models.UploadStatus.CANCELLED.value}
    )

    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.json()["exceptionId"] == "noSuchUpload"


@pytest.mark.parametrize(
    "new_status",
    [
        status_
        for status_ in models.UploadStatus
        if status_ not in [models.UploadStatus.CANCELLED, models.UploadStatus.UPLOADED]
    ],
)
def test_update_upload_status_invalid_new_status(
    new_status: models.UploadStatus, joint_fixture: JointFixture  # noqa: F405
):
    """Test the update_upload_status endpoint with invalid new status values."""

    upload_id = "myNonExistingUpload001"
    # (Input data validation should happen before checking the existence of the
    # specified resource, thus a non existing upload ID can be used here.)

    response = joint_fixture.rest_client.patch(
        f"/uploads/{upload_id}", json={"status": new_status.value}
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.parametrize(
    "old_status",
    [
        status_
        for status_ in models.UploadStatus
        if status_ != models.UploadStatus.PENDING
    ],
)
def test_update_upload_status_non_pending(
    old_status: models.UploadStatus, joint_fixture: JointFixture  # noqa: F405
):
    """Test the update_upload_status endpoint on non pending upload."""

    target_upload = EXAMPLE_UPLOADS[0].copy(update={"status": old_status})

    # insert a pending and non_pending upload into the database:
    joint_fixture.psql.populate_file_metadata([EXAMPLE_FILE])
    joint_fixture.psql.populate_upload_attempts([target_upload])

    for new_status in [models.UploadStatus.CANCELLED, models.UploadStatus.UPLOADED]:
        response = joint_fixture.rest_client.patch(
            f"/uploads/{target_upload.upload_id}", json={"status": new_status.value}
        )

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        response_body = response.json()
        assert response_body["exceptionId"] == "uploadNotPending"
        assert response_body["data"]["current_upload_status"] == old_status.value


def test_create_presigned_url_not_found(joint_fixture: JointFixture):  # noqa: F405
    """Test the create_presigned_url endpoint with non existing upload ID."""

    upload_id = "myNonExistingUpload001"

    response = joint_fixture.rest_client.post(
        f"/uploads/{upload_id}/parts/{1}/signed_urls"
    )

    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.json()["exceptionId"] == "noSuchUpload"
