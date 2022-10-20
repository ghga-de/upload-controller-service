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

"""Simulate client behavior and test a typical journey through the APIs exposed by this
service (incl. REST and event-driven APIs)."""

import asyncio
import json
from datetime import datetime
from typing import Literal

import pytest
from fastapi import status
from ghga_message_schemas import schemas
from ghga_service_chassis_lib.utils import exec_with_timeout

from tests.fixtures.example_data import EXAMPLE_FILES
from tests.fixtures.joint import *  # noqa: 403
from tests.fixtures.s3 import upload_part_via_url


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

    # get upload metadata via the upload ID:
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

    # initialize upstream event publisher and downstream event subscriber:
    study_publisher = joint_fixture.amqp.get_test_publisher(
        topic_name=joint_fixture.config.topic_new_study,
        message_schema=schemas.SCHEMAS["new_study_created"],
    )
    acceptance_publisher = joint_fixture.amqp.get_test_publisher(
        topic_name=joint_fixture.config.topic_file_accepted,
        message_schema=schemas.SCHEMAS["file_internally_registered"],
    )
    downstream_subscriber = joint_fixture.amqp.get_test_subscriber(
        topic_name=joint_fixture.config.topic_upload_received,
        message_schema=schemas.SCHEMAS["file_upload_received"],
    )

    # publish an event to register new files:
    study_id = EXAMPLE_FILES[0].grouping_label
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
    new_study_event = {
        "study": {"id": study_id},
        "associated_files": associated_files,
        "timestamp": datetime.now().isoformat(),
    }
    study_publisher.publish(new_study_event)

    # use the event subscriber to receive and process the event:
    async def get_subscriber():
        return await joint_fixture.container.event_subscriber()

    event_subscriber = asyncio.run(get_subscriber())
    exec_with_timeout(
        func=lambda: event_subscriber.subscribe_new_study(run_forever=False),
        timeout_after=2,
    )

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

        # receive the event that a new file was uploaded:
        downstream_message = downstream_subscriber.subscribe(timeout_after=2)
        assert downstream_message["file_id"] == file.file_id

        # publish an event to mark the file as accepted:
        file_accepted_event = {
            "file_id": file.file_id,
            "md5_checksum": file.md5_checksum,
            "size": file.size,
            "creation_date": file.creation_date.isoformat(),
            "update_date": file.update_date.isoformat(),
            "format": file.format,
            "grouping_label": study_id,
            "timestamp": datetime.now().isoformat(),
        }
        acceptance_publisher.publish(file_accepted_event)

        # receive the acceptance event:
        exec_with_timeout(
            func=lambda: event_subscriber.subscribe_file_accepted(run_forever=False),
            timeout_after=2,
        )

        # make sure that the latest upload of the corresponding file was marked as
        # accepted:
        # (first get the ID of the latest upload for that file:)
        response = joint_fixture.rest_client.get(f"/files/{file.file_id}")
        assert response.status_code == status.HTTP_200_OK
        latest_upload_id = response.json()["latest_upload_id"]

        # (Then get details on that upload:)
        response = joint_fixture.rest_client.get(f"/uploads/{latest_upload_id}")
        assert response.status_code == status.HTTP_200_OK
        assert response.json()["status"] == "accepted"
