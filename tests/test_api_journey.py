# Copyright 2022 Universität Tübingen, DKFZ and EMBL
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

"""Simulate client behavior and test a typical journey through the APIs exposed by this
service (incl. REST and event-driven APIs)."""

import json
from datetime import datetime
from typing import Literal

from fastapi import status

from tests.fixtures.joint import *  # noqa: 403
from tests.fixtures.s3 import upload_part_via_url

from ucs.domain import models
from ucs.domain.part_calc import DEFAULT_PART_SIZE

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


def perform_upload(
    joint_fixture: JointFixture,  # noqa: F405
    *,
    file_id: str,
    final_status: Literal["cancelled", "uploaded"],
) -> str:
    """Initialize a new upload for the file with the given ID. Upload some parts.
    Finally either confirm the upload (final_status="uploaded") or cancel it
    (final_status="cancelled").

    Returns: The ID of the created upload.
    """

    # initiate new upload:
    response = joint_fixture.rest_client.post("/uploads", json={"file_id": file_id})
    assert response.status_code == status.HTTP_200_OK
    upload_details = response.json()
    assert upload_details["status"] == "pending"
    assert upload_details["file_id"] == file_id
    assert "upload_id" in upload_details
    assert "part_size" in upload_details

    # check that the latest_upload_id points to the newly created upload:
    response = joint_fixture.rest_client.get(f"/files/{file_id}")
    assert response.status_code == status.HTTP_200_OK
    assert response.json()["latest_upload_id"] == upload_details["upload_id"]

    # get upload metadata via an ID:
    response = joint_fixture.rest_client.get(f"/uploads/{upload_details['upload_id']}")
    assert response.status_code == status.HTTP_200_OK
    assert response.json() == upload_details

    # upload a couple of file parts:
    for part_no in range(1, 4):
        # request an upload URL for a part:
        response = joint_fixture.rest_client.post(
            f"/uploads/{upload_details['upload_id']}/parts/{part_no}/signed_urls"
        )
        assert response.status_code == status.HTTP_200_OK
        part_upload_details = response.json()
        assert "url" in part_upload_details

        # upload a file part with arbitrary content
        upload_part_via_url(
            url=part_upload_details["url"], size=upload_details["part_size"]
        )

    # set the final status:
    response = joint_fixture.rest_client.patch(
        f"/uploads/{upload_details['upload_id']}", json={"status": final_status}
    )
    assert response.status_code == status.HTTP_204_NO_CONTENT

    # confirm the final status:
    response = joint_fixture.rest_client.get(f"/uploads/{upload_details['upload_id']}")
    assert response.status_code == status.HTTP_200_OK
    assert response.json()["status"] == final_status

    return upload_details["upload_id"]


def test_happy_journey(joint_fixture: JointFixture):  # noqa: F405
    """Test the typical anticipated/successful journey through the service's APIs."""

    # register new files to the service:
    file_metadata_service = (
        joint_fixture.container.file_metadata_service()
    )  # to be replaced with event-based API
    file_metadata_service.upsert_multiple(EXAMPLE_FILES)

    for file in EXAMPLE_FILES:
        # get file metadata:
        response = joint_fixture.rest_client.get(f"/files/{file.file_id}")
        assert response.status_code == status.HTTP_200_OK
        expected_metadata = json.loads(file.json())
        obtained_metadata = response.json()
        for field in expected_metadata:
            assert obtained_metadata[field] == expected_metadata[field]
        assert obtained_metadata["latest_upload_id"] is None

        # perform an upload and cancel it:
        perform_upload(joint_fixture, file_id=file.file_id, final_status="cancelled")

        # perform another upload and confirm it:
        perform_upload(joint_fixture, file_id=file.file_id, final_status="uploaded")
