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

"""Test edge cases of interacting with the services API.

Note: This test module uses the module-scoped fixtures.
"""

import json

import pytest
from fastapi import status

from tests.fixtures.example_data import EXAMPLE_FILE, EXAMPLE_UPLOADS
from tests.fixtures.module_scope_fixtures import (  # noqa: F401
    JointFixture,
    event_loop,
    joint_fixture,
    kafka_fixture,
    mongodb_fixture,
    reset_state,
    s3_fixture,
)
from ucs.core import models


@pytest.mark.asyncio
async def test_get_health(joint_fixture: JointFixture):  # noqa: F811
    """Test the GET /health endpoint.

    reset_state fixture isn't needed because the test is unaffected by state.
    """

    response = await joint_fixture.rest_client.get("/health")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {"status": "OK"}


@pytest.mark.asyncio
async def test_get_file_metadata_not_found(joint_fixture: JointFixture):  # noqa: F811
    """Test the get_file_metadata endpoint with an non-existing file id."""

    file_id = "myNonExistingFile001"
    response = await joint_fixture.rest_client.get(f"/files/{file_id}")

    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.json()["exception_id"] == "fileNotRegistered"


@pytest.mark.asyncio
async def test_create_upload_not_found(joint_fixture: JointFixture):  # noqa: F811
    """Test the create_upload endpoint with an non-existing file id."""

    file_id = "myNonExistingFile001"
    response = await joint_fixture.rest_client.post(
        "/uploads", json={"file_id": file_id, "submitter_public_key": "test-key"}
    )

    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert response.json()["exception_id"] == "fileNotRegistered"


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
@pytest.mark.asyncio
async def test_create_upload_other_active(
    existing_status: models.UploadStatus,
    joint_fixture: JointFixture,  # noqa: F811
):
    """Test the create_upload endpoint when there is another active update already
    existing."""

    existing_upload = EXAMPLE_UPLOADS[0].copy(update={"status": existing_status})

    # insert a pending upload into the database:
    daos = await joint_fixture.container.dao_collection()
    await daos.file_metadata.insert(EXAMPLE_FILE)
    await daos.upload_attempts.insert(existing_upload)

    response = await joint_fixture.rest_client.post(
        "/uploads",
        json={"file_id": EXAMPLE_FILE.file_id, "submitter_public_key": "test-key"},
    )

    assert response.status_code == status.HTTP_400_BAD_REQUEST
    response_body = response.json()
    assert response_body["exception_id"] == "existingActiveUpload"
    assert response_body["data"]["active_upload"] == json.loads(existing_upload.json())


@pytest.mark.parametrize(
    "existing_status",
    [
        models.UploadStatus.UPLOADED,
        models.UploadStatus.ACCEPTED,
    ],
)
@pytest.mark.asyncio
async def test_create_upload_accepted(
    existing_status: models.UploadStatus,
    joint_fixture: JointFixture,  # noqa: F811
):
    """Test the create_upload endpoint when another update has already been accepted
    or is currently being evaluated."""

    existing_upload = EXAMPLE_UPLOADS[0].copy(update={"status": existing_status})

    # insert the existing upload into the database:
    daos = await joint_fixture.container.dao_collection()
    await daos.file_metadata.insert(EXAMPLE_FILE)
    await daos.upload_attempts.insert(existing_upload)

    # try to create a new upload:
    response = await joint_fixture.rest_client.post(
        "/uploads",
        json={"file_id": EXAMPLE_FILE.file_id, "submitter_public_key": "test-key"},
    )

    assert response.status_code == status.HTTP_400_BAD_REQUEST
    response_body = response.json()
    assert response_body["exception_id"] == "existingActiveUpload"
    assert response_body["data"]["active_upload"] == json.loads(existing_upload.json())


@pytest.mark.asyncio
async def test_get_upload_not_found(joint_fixture: JointFixture):  # noqa: F811
    """Test the get_upload endpoint with non-existing upload ID."""

    upload_id = "myNonExistingUpload001"
    response = await joint_fixture.rest_client.get(f"/uploads/{upload_id}")

    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.json()["exception_id"] == "noSuchUpload"


@pytest.mark.asyncio
async def test_update_upload_status_not_found(
    joint_fixture: JointFixture,  # noqa: F811
):
    """Test the update_upload_status endpoint with non existing upload ID."""

    upload_id = "myNonExistingUpload001"

    response = await joint_fixture.rest_client.patch(
        f"/uploads/{upload_id}", json={"status": models.UploadStatus.CANCELLED.value}
    )

    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.json()["exception_id"] == "noSuchUpload"


@pytest.mark.parametrize(
    "new_status",
    [
        status_
        for status_ in models.UploadStatus
        if status_ not in [models.UploadStatus.CANCELLED, models.UploadStatus.UPLOADED]
    ],
)
@pytest.mark.asyncio
async def test_update_upload_status_invalid_new_status(
    new_status: models.UploadStatus,
    joint_fixture: JointFixture,  # noqa: F811
):
    """Test the update_upload_status endpoint with invalid new status values."""

    upload_id = "myNonExistingUpload001"
    # (Input data validation should happen before checking the existence of the
    # specified resource, thus a non existing upload ID can be used here.)

    response = await joint_fixture.rest_client.patch(
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
@pytest.mark.asyncio
async def test_update_upload_status_non_pending(
    old_status: models.UploadStatus,
    joint_fixture: JointFixture,  # noqa: F811
):
    """Test the update_upload_status endpoint on non pending upload."""

    target_upload = EXAMPLE_UPLOADS[0].copy(update={"status": old_status})

    # insert a pending and non_pending upload into the database:
    daos = await joint_fixture.container.dao_collection()
    await daos.file_metadata.insert(EXAMPLE_FILE)
    await daos.upload_attempts.insert(target_upload)

    for new_status in [models.UploadStatus.CANCELLED, models.UploadStatus.UPLOADED]:
        response = await joint_fixture.rest_client.patch(
            f"/uploads/{target_upload.upload_id}", json={"status": new_status.value}
        )

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        response_body = response.json()
        assert response_body["exception_id"] == "uploadNotPending"
        assert response_body["data"]["current_upload_status"] == old_status.value


@pytest.mark.asyncio
async def test_create_presigned_url_not_found(
    joint_fixture: JointFixture,  # noqa: F811
):
    """Test the create_presigned_url endpoint with non existing upload ID."""

    upload_id = "myNonExistingUpload001"

    response = await joint_fixture.rest_client.post(
        f"/uploads/{upload_id}/parts/{1}/signed_urls"
    )

    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.json()["exception_id"] == "noSuchUpload"
