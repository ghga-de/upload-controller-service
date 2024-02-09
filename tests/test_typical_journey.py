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

"""Test happy/unhappy journey.

Simulate client behavior and test a typical journey through the APIs exposed by this
service (incl. REST and event-driven APIs).
"""

import json
import logging
from typing import Literal

import pytest
from fastapi import status
from ghga_event_schemas import pydantic_ as event_schemas
from ghga_service_commons.utils.utc_dates import now_as_utc
from hexkit.providers.s3.testutils import upload_part_via_url

from tests.fixtures.example_data import UPLOAD_DETAILS_1, UPLOAD_DETAILS_2
from tests.fixtures.joint import (  # noqa: F401
    JointFixture,
    joint_fixture,
    kafka_fixture,
    mongodb_fixture,
    s3_fixture,
    second_s3_fixture,
)
from ucs.core.models import UploadStatus

TARGET_BUCKET_ID = "test-staging"


async def run_until_uploaded(
    joint_fixture: JointFixture,  # noqa: F811
    file_to_register: event_schemas.MetadataSubmissionFiles,
    storage_alias: str,
):
    """Utility function to process kafka events related to the upload.

    Run steps until uploaded data has been received and the upload attempt has been
    marked as uploaded
    """
    # populate s3 storage:
    # await joint_fixture.s3.populate_buckets([joint_fixture.config])

    # publish event to register a new file for uplaod:

    file_metadata_event = event_schemas.MetadataSubmissionUpserted(
        associated_files=[file_to_register]
    )
    await joint_fixture.kafka.publish_event(
        payload=file_metadata_event.model_dump(),
        type_=joint_fixture.config.file_metadata_event_type,
        topic=joint_fixture.config.file_metadata_event_topic,
    )

    # consume the event:
    await joint_fixture.event_subscriber.run(forever=False)

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
        joint_fixture,
        file_id=file_to_register.file_id,
        final_status="cancelled",
        storage_alias=storage_alias,
    )

    # perform another upload and confirm it:
    async with joint_fixture.kafka.record_events(
        in_topic=joint_fixture.config.upload_received_event_topic
    ) as recorder:
        await perform_upload(
            joint_fixture,
            file_id=file_to_register.file_id,
            final_status="uploaded",
            storage_alias=storage_alias,
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


async def perform_upload(
    joint_fixture: JointFixture,  # noqa: F811
    *,
    file_id: str,
    final_status: Literal["cancelled", "uploaded"],
    storage_alias: str,
) -> str:
    """Process an upload with a specific status.

    Initialize a new upload for the file with the given ID. Upload some parts.
    Finally either confirm the upload (final_status="uploaded") or cancel it
    (final_status="cancelled").

    Returns: The ID of the created upload.
    """
    # initiate new upload:
    response = await joint_fixture.rest_client.post(
        "/uploads",
        json={
            "file_id": file_id,
            "submitter_public_key": "test-key",
            "storage_alias": storage_alias,
        },
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
async def test_happy_journey(joint_fixture: JointFixture):  # noqa: F811
    """Test the typical anticipated/successful journey through the service's APIs."""
    for upload_details in (UPLOAD_DETAILS_1, UPLOAD_DETAILS_2):
        storage_alias = upload_details.storage_alias
        file_to_register = upload_details.submission_metadata

        await run_until_uploaded(
            joint_fixture=joint_fixture,
            file_to_register=file_to_register,
            storage_alias=storage_alias,
        )

        # publish an event to mark the upload as accepted:
        acceptance_event = event_schemas.FileInternallyRegistered(
            s3_endpoint_alias=storage_alias,
            file_id=file_to_register.file_id,
            object_id=upload_details.upload_attempt.object_id,
            bucket_id=TARGET_BUCKET_ID,
            upload_date=now_as_utc().isoformat(),
            decrypted_sha256=file_to_register.decrypted_sha256,
            decrypted_size=file_to_register.decrypted_size,
            decryption_secret_id="some-secret",
            content_offset=123456,
            encrypted_part_size=123456,
            encrypted_parts_md5=["somechecksum", "anotherchecksum"],
            encrypted_parts_sha256=["somechecksum", "anotherchecksum"],
        )
        await joint_fixture.kafka.publish_event(
            payload=json.loads(acceptance_event.model_dump_json()),
            type_=joint_fixture.config.upload_accepted_event_type,
            topic=joint_fixture.config.upload_accepted_event_topic,
        )

        # consume the acceptance event:
        await joint_fixture.event_subscriber.run(forever=False)

        # make sure that the latest upload of the corresponding file was marked as
        # accepted:
        # (first get the ID of the latest upload for that file:)
        response = await joint_fixture.rest_client.get(
            f"/files/{file_to_register.file_id}"
        )
        assert response.status_code == status.HTTP_200_OK
        latest_upload_id = response.json()["latest_upload_id"]

        # (Then get details on that upload:)
        response = await joint_fixture.rest_client.get(f"/uploads/{latest_upload_id}")
        assert response.status_code == status.HTTP_200_OK
        assert response.json()["status"] == "accepted"


@pytest.mark.asyncio
async def test_unhappy_journey(joint_fixture: JointFixture):  # noqa: F811
    """Test the typical journey.

    Work through the service's APIs, but reject the upload attempt due to a file
    validation error.
    """
    for upload_details in (UPLOAD_DETAILS_1, UPLOAD_DETAILS_2):
        storage_alias = upload_details.storage_alias
        file_to_register = upload_details.submission_metadata

        await run_until_uploaded(
            joint_fixture=joint_fixture,
            file_to_register=file_to_register,
            storage_alias=storage_alias,
        )

        # publish an event to mark the upload as rejected due to validation failure
        failure_event = event_schemas.FileUploadValidationFailure(
            s3_endpoint_alias=storage_alias,
            file_id=file_to_register.file_id,
            object_id=upload_details.upload_attempt.object_id,
            bucket_id=TARGET_BUCKET_ID,
            upload_date=now_as_utc().isoformat(),
            reason="Sorry, but this has to fail.",
        )

        await joint_fixture.kafka.publish_event(
            payload=json.loads(failure_event.model_dump_json()),
            type_=joint_fixture.config.upload_rejected_event_type,
            topic=joint_fixture.config.upload_rejected_event_topic,
        )

        # consume the validation failure event:
        await joint_fixture.event_subscriber.run(forever=False)

        # make sure that the latest upload of the corresponding file was marked as rejected:
        # (first get the ID of the latest upload for that file:)
        response = await joint_fixture.rest_client.get(
            f"/files/{file_to_register.file_id}"
        )
        assert response.status_code == status.HTTP_200_OK
        latest_upload_id = response.json()["latest_upload_id"]

        # (Then get details on that upload:)
        response = await joint_fixture.rest_client.get(f"/uploads/{latest_upload_id}")
        assert response.status_code == status.HTTP_200_OK
        assert response.json()["status"] == "rejected"


@pytest.mark.asyncio
async def test_inbox_inspector(
    caplog,
    joint_fixture: JointFixture,  # noqa: F811
):
    """Sanity check for inbox inspection functionality."""
    # get configured bucket_id. To simplify things, it's the same for both storage aliases
    bucket_id = next(iter(joint_fixture.config.object_storages.values())).bucket

    for upload_details in (UPLOAD_DETAILS_1, UPLOAD_DETAILS_2):
        storage_alias = upload_details.storage_alias
        file_to_register = upload_details.submission_metadata

        await run_until_uploaded(
            joint_fixture=joint_fixture,
            file_to_register=file_to_register,
            storage_alias=storage_alias,
        )

    # this should remove the associated file
    failure_event = event_schemas.FileUploadValidationFailure(
        s3_endpoint_alias=UPLOAD_DETAILS_1.storage_alias,
        file_id=UPLOAD_DETAILS_1.submission_metadata.file_id,
        object_id=UPLOAD_DETAILS_1.upload_attempt.object_id,
        bucket_id=bucket_id,
        upload_date=now_as_utc().isoformat(),
        reason="Sorry, but this has to fail.",
    )

    await joint_fixture.kafka.publish_event(
        payload=json.loads(failure_event.model_dump_json()),
        type_=joint_fixture.config.upload_rejected_event_type,
        topic=joint_fixture.config.upload_rejected_event_topic,
    )

    # Manipulate upload attempt to simulate stale file in need of removal
    metadata = await joint_fixture.daos.file_metadata.get_by_id(
        id_=UPLOAD_DETAILS_2.file_metadata.file_id
    )

    current_upload_id = metadata.latest_upload_id
    assert current_upload_id

    attempt = await joint_fixture.daos.upload_attempts.get_by_id(id_=current_upload_id)
    attempt.status = UploadStatus.REJECTED
    await joint_fixture.daos.upload_attempts.upsert(attempt)

    caplog.clear()
    caplog.set_level(level=logging.INFO, logger="ucs.core.storage_inspector")

    await joint_fixture.inbox_inspector.check_buckets()
    expected_message = (
        f"Stale object '{attempt.object_id}' found for file '{attempt.file_id}' in bucket"
        + f" '{bucket_id}' of storage '{attempt.storage_alias}'."
    )

    assert len(caplog.messages) == 1
    assert expected_message in caplog.messages
