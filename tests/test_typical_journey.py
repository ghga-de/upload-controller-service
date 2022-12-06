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

import json
from datetime import datetime
from typing import Literal

import nest_asyncio
import pytest
from fastapi import status
from ghga_event_schemas import pydantic_ as event_schemas
from hexkit.providers.s3.testutils import upload_part_via_url

from tests.fixtures.example_data import EXAMPLE_FILE
from tests.fixtures.joint import *  # noqa: 403

# this is a temporary solution to run an event loop within another event loop
# will be solved once transitioning to kafka:
nest_asyncio.apply()


async def perform_upload(
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
    response = await joint_fixture.rest_client.post(
        "/uploads", json={"file_id": file_id, "submitter_public_key": "test-key"}
    )
    assert response.status_code == status.HTTP_200_OK
    upload_details = response.json()
    assert upload_details["status"] == "pending"
    assert upload_details["file_id"] == file_id
    assert "upload_id" in upload_details
    assert "part_size" in upload_details

    # check that the latest_upload_id points to the newly created upload:
    response = await joint_fixture.rest_client.get(f"/files/{file_id}")
    assert response.status_code == status.HTTP_200_OK
    assert response.json()["latest_upload_id"] == upload_details["upload_id"]

    # get upload metadata via the upload ID:
    response = await joint_fixture.rest_client.get(
        f"/uploads/{upload_details['upload_id']}"
    )
    assert response.status_code == status.HTTP_200_OK
    assert response.json() == upload_details

    # upload a couple of file parts:
    for part_no in range(1, 4):
        # request an upload URL for a part:
        response = await joint_fixture.rest_client.post(
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
    response = await joint_fixture.rest_client.patch(
        f"/uploads/{upload_details['upload_id']}", json={"status": final_status}
    )
    assert response.status_code == status.HTTP_204_NO_CONTENT

    # confirm the final status:
    response = await joint_fixture.rest_client.get(
        f"/uploads/{upload_details['upload_id']}"
    )
    assert response.status_code == status.HTTP_200_OK
    assert response.json()["status"] == final_status

    return upload_details["upload_id"]


@pytest.mark.asyncio
async def test_happy_journey(joint_fixture: JointFixture):  # noqa: F405
    """Test the typical anticipated/successful journey through the service's APIs."""

    # populate s3 storage:
    await joint_fixture.s3.populate_buckets([joint_fixture.config.inbox_bucket])

    # publish event to register a new file for uplaod:
    file_to_register = event_schemas.MetadataSubmissionFiles(
        file_id=EXAMPLE_FILE.file_id,
        file_name=EXAMPLE_FILE.file_name,
        decrypted_size=EXAMPLE_FILE.decrypted_size,
        decrypted_sha256=EXAMPLE_FILE.decrypted_sha256,
    )
    file_metadata_event = event_schemas.MetadataSubmissionUpserted(
        associated_files=[file_to_register]
    )
    await joint_fixture.kafka.publish_event(
        payload=file_metadata_event.dict(),
        type_=joint_fixture.config.file_metadata_event_type,
        topic=joint_fixture.config.file_metadata_event_topic,
    )

    # consume the event:
    event_subscriber = await joint_fixture.container.kafka_event_subscriber()
    await event_subscriber.run(forever=False)

    # check that the new file has been registered:
    response = await joint_fixture.rest_client.get(f"/files/{file_to_register.file_id}")
    assert response.status_code == status.HTTP_200_OK
    registered_file = response.json()
    assert registered_file["file_name"] == file_to_register.file_name
    assert registered_file["decrypted_sha256"] == file_to_register.decrypted_sha256
    assert registered_file["decrypted_size"] == file_to_register.decrypted_size
    assert registered_file["latest_upload_id"] is None

    # perform an upload and cancel it:
    _ = await perform_upload(
        joint_fixture, file_id=file_to_register.file_id, final_status="cancelled"
    )

    # perform another upload and confirm it:
    async with joint_fixture.kafka.record_events(
        in_topic=joint_fixture.config.upload_received_event_topic
    ) as recorder:
        await perform_upload(
            joint_fixture, file_id=file_to_register.file_id, final_status="uploaded"
        )

    # check for the  events:
    assert len(recorder.recorded_events) == 1
    assert (
        recorder.recorded_events[0].type_
        == joint_fixture.config.upload_received_event_type
    )
    payload = event_schemas.FileUploadReceived(**recorder.recorded_events[0].payload)
    assert payload.file_id == file_to_register.file_id
    assert payload.expected_decrypted_sha256 == file_to_register.decrypted_sha256

    # publish an event to mark the upload as accepted:
    acceptance_event = event_schemas.FileInternallyRegistered(
        file_id=file_to_register.file_id,
        upload_date=datetime.utcnow().isoformat(),
        decrypted_sha256=file_to_register.decrypted_sha256,
        decrypted_size=file_to_register.decrypted_size,
        decryption_secret_id="some-secret",
        content_offset=123456,
        encrypted_part_size=123456,
        encrypted_parts_md5=["somechecksum", "anotherchecksum"],
        encrypted_parts_sha256=["somechecksum", "anotherchecksum"],
    )
    await joint_fixture.kafka.publish_event(
        payload=json.loads(acceptance_event.json()),
        type_=joint_fixture.config.upload_accepted_event_type,
        topic=joint_fixture.config.upload_accepted_event_topic,
    )

    # consume the acceptance event:
    await event_subscriber.run(forever=False)

    # make sure that the latest upload of the corresponding file was marked as
    # accepted:
    # (first get the ID of the latest upload for that file:)
    response = await joint_fixture.rest_client.get(f"/files/{file_to_register.file_id}")
    assert response.status_code == status.HTTP_200_OK
    latest_upload_id = response.json()["latest_upload_id"]

    # (Then get details on that upload:)
    response = await joint_fixture.rest_client.get(f"/uploads/{latest_upload_id}")
    assert response.status_code == status.HTTP_200_OK
    assert response.json()["status"] == "accepted"
