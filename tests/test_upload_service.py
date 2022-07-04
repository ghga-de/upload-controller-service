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

"""Test the FileMetadataService"""

from datetime import datetime

import pytest
from ghga_message_schemas import schemas
from ghga_service_chassis_lib.utils import TEST_FILE_PATHS

from tests.fixtures.config import DEFAULT_CONFIG
from tests.fixtures.joint import *  # noqa: 403
from tests.fixtures.s3 import FileObject, upload_part_via_url
from ucs.domain import models
from ucs.domain.interfaces.inbound.upload_service import (
    ExistingActiveUploadError,
    FileAlreadyInInboxError,
    FileUnkownError,
    UploadStatusMissmatchError,
    UploadUnkownError,
)
from ucs.domain.part_calc import DEFAULT_PART_SIZE

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

EXAMPLE_OBJECT = FileObject(
    file_path=TEST_FILE_PATHS[0],
    bucket_id=DEFAULT_CONFIG.s3_inbox_bucket_id,
    object_id=EXAMPLE_FILE.file_id,
)

EXAMPLE_UPLOAD = models.UploadAttempt(
    upload_id="testUpload001",
    file_id="testFile001",
    status=models.UploadStatus.PENDING,
    part_size=1234,
)


def test_happy(joint_fixture: JointFixture):  # noqa: F405
    """Tests the basic happy path of using the UploadService"""

    # populate database with file metadata:
    joint_fixture.psql.populate_file_metadata([EXAMPLE_FILE])

    # initialize a subscriber that will consume downstream events:
    downstream_subscriber = joint_fixture.amqp.get_test_subscriber(
        topic_name=joint_fixture.config.topic_upload_received,
        message_schema=schemas.SCHEMAS["file_upload_received"],
    )

    # construct service:
    upload_service = joint_fixture.container.upload_service()

    # initate upload
    upload_init = upload_service.initiate_new(file_id=EXAMPLE_FILE.file_id)
    assert upload_init.file_id == EXAMPLE_FILE.file_id
    assert upload_init.status == models.UploadStatus.PENDING
    upload_id = upload_init.upload_id

    # check the created upload
    upload_post_init = upload_service.get_details(upload_id=upload_id)
    assert upload_post_init == upload_init

    # upload two parts:
    for part_no in [1, 2]:
        part_url = upload_service.create_part_url(upload_id=upload_id, part_no=part_no)
        upload_part_via_url(url=part_url, size=DEFAULT_PART_SIZE)

    # confirm the upload completion:
    upload_service.complete(upload_id=upload_id)

    # check status again
    upload_post_complete = upload_service.get_details(upload_id=upload_id)
    assert upload_post_complete.status == models.UploadStatus.UPLOADED

    # receive the event that a new file was uploaded:
    downstream_message = downstream_subscriber.subscribe(timeout_after=2)
    assert downstream_message["file_id"] == EXAMPLE_FILE.file_id


def test_cancel(joint_fixture: JointFixture):  # noqa: F405
    """Tests canceling an upload"""

    # populate database with file metadata:
    joint_fixture.psql.populate_file_metadata([EXAMPLE_FILE])

    # construct service:
    upload_service = joint_fixture.container.upload_service()

    # initate upload
    upload_init = upload_service.initiate_new(file_id=EXAMPLE_FILE.file_id)
    upload_id = upload_init.upload_id

    # upload two parts:
    for part_no in [1, 2]:
        part_url = upload_service.create_part_url(upload_id=upload_id, part_no=part_no)
        upload_part_via_url(url=part_url, size=DEFAULT_PART_SIZE)

    # confirm the upload completion:
    upload_service.cancel(upload_id=upload_id)

    # check status again
    upload = upload_service.get_details(upload_id=upload_id)
    assert upload.status == models.UploadStatus.CANCELLED

    # ensure that no new upload urls can be requested:
    with pytest.raises(UploadStatusMissmatchError):
        part_url = upload_service.create_part_url(upload_id=upload_id, part_no=part_no)


def test_init_upload_unkown_file(joint_fixture: JointFixture):  # noqa: F405
    """Test initializing a new upload for a non existing file."""

    # construct service:
    upload_service = joint_fixture.container.upload_service()

    # use service:
    with pytest.raises(FileUnkownError):
        _ = upload_service.initiate_new(file_id=EXAMPLE_FILE.file_id)


def test_init_other_active_upload(joint_fixture: JointFixture):  # noqa: F405
    """Test initializing a new upload when there is another active update already
    existing."""

    # insert a pending upload into the database:
    joint_fixture.psql.populate_file_metadata([EXAMPLE_FILE])
    joint_fixture.psql.populate_upload_attempts([EXAMPLE_UPLOAD])

    # construct service:
    upload_service = joint_fixture.container.upload_service()

    # try to init new upload for the same file:
    with pytest.raises(ExistingActiveUploadError):
        _ = upload_service.initiate_new(file_id=EXAMPLE_FILE.file_id)


def test_init_already_in_inbox(joint_fixture: JointFixture):  # noqa: F405
    """Test initializing a new upload when the corresponding file is already in the
    inbox."""

    # insert a file into the database and the storage:
    joint_fixture.psql.populate_file_metadata([EXAMPLE_FILE])
    joint_fixture.s3.populate_file_objects([EXAMPLE_OBJECT])

    # construct service:
    upload_service = joint_fixture.container.upload_service()

    # try to init new upload for the same file:
    with pytest.raises(FileAlreadyInInboxError):
        _ = upload_service.initiate_new(file_id=EXAMPLE_FILE.file_id)


@pytest.mark.parametrize("accept", (True, False))
def test_accept_and_reject(accept: bool, joint_fixture: JointFixture):  # noqa: F405
    """Test accepting or rejecting an upload."""

    upload = EXAMPLE_UPLOAD.copy(update={"status": models.UploadStatus.UPLOADED})

    # populate databases and storages:
    joint_fixture.psql.populate_file_metadata([EXAMPLE_FILE])
    joint_fixture.psql.populate_upload_attempts([upload])
    joint_fixture.s3.populate_file_objects([EXAMPLE_OBJECT])

    # construct service:
    upload_service = joint_fixture.container.upload_service()

    # accept or reject the upload:
    if accept:
        _ = upload_service.accept_latest(file_id=upload.file_id)
    else:
        _ = upload_service.reject_latest(file_id=upload.file_id)

    # make sure that the status in the database was updated:
    with joint_fixture.container.upload_attempt_dao() as ua_dao:
        obtained_update = ua_dao.get(upload.upload_id)
    assert (
        obtained_update.status == models.UploadStatus.ACCEPTED
        if accept
        else models.UploadStatus.REJECTED
    )

    # make sure that the object in the storage is gone:
    with joint_fixture.container.object_storage() as storage:
        assert not storage.does_object_exist(
            bucket_id=EXAMPLE_OBJECT.bucket_id, object_id=EXAMPLE_OBJECT.object_id
        )


def test_unkown_upload(joint_fixture: JointFixture):  # noqa: F405
    """Test working on an upload that does not exist."""

    upload_id = "myNonExistingUpload"

    # construct service:
    upload_service = joint_fixture.container.upload_service()

    # try to work with non existing upload:
    with pytest.raises(UploadUnkownError):
        _ = upload_service.get_details(upload_id=upload_id)

    with pytest.raises(UploadUnkownError):
        _ = upload_service.create_part_url(upload_id=upload_id, part_no=1)

    with pytest.raises(UploadUnkownError):
        _ = upload_service.complete(upload_id=upload_id)

    with pytest.raises(UploadUnkownError):
        _ = upload_service.cancel(upload_id=upload_id)

    with pytest.raises(UploadUnkownError):
        _ = upload_service.accept(upload_id=upload_id)

    with pytest.raises(UploadUnkownError):
        _ = upload_service.reject(upload_id=upload_id)


@pytest.mark.parametrize(
    "status",
    (status for status in models.UploadStatus if status != models.UploadStatus.PENDING),
)
def test_non_pending_upload(
    status: models.UploadStatus, joint_fixture: JointFixture  # noqa: F405
):
    """Test working on a non-pending upload."""

    non_pending_upload = EXAMPLE_UPLOAD.copy(update={"status": status})
    upload_id = non_pending_upload.upload_id

    # insert non pending upload into the database:
    joint_fixture.psql.populate_file_metadata([EXAMPLE_FILE])
    joint_fixture.psql.populate_upload_attempts([non_pending_upload])

    # construct service:
    upload_service = joint_fixture.container.upload_service()

    # try to work with non existing upload:
    with pytest.raises(UploadStatusMissmatchError):
        _ = upload_service.create_part_url(upload_id=upload_id, part_no=1)

    with pytest.raises(UploadStatusMissmatchError):
        _ = upload_service.complete(upload_id=upload_id)

    with pytest.raises(UploadStatusMissmatchError):
        _ = upload_service.cancel(upload_id=upload_id)


@pytest.mark.parametrize(
    "status",
    (
        status
        for status in models.UploadStatus
        if status != models.UploadStatus.UPLOADED
    ),
)
def test_non_uploaded_upload(
    status: models.UploadStatus, joint_fixture: JointFixture  # noqa: F405
):
    """Test working on a non-uploaded upload."""

    non_uploaded_upload = EXAMPLE_UPLOAD.copy(update={"status": status})
    file_id = non_uploaded_upload.file_id

    # insert non uploaded upload into the database:
    joint_fixture.psql.populate_file_metadata([EXAMPLE_FILE])
    joint_fixture.psql.populate_upload_attempts([non_uploaded_upload])

    # construct service:
    upload_service = joint_fixture.container.upload_service()

    # try to work with non existing upload:
    with pytest.raises(UploadStatusMissmatchError):
        _ = upload_service.accept_latest(file_id=file_id)

    with pytest.raises(UploadStatusMissmatchError):
        _ = upload_service.reject_latest(file_id=file_id)
