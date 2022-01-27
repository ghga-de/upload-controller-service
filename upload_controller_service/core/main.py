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


"""Main business-logic of this service"""

from typing import Callable, List

from upload_controller_service.dao.db import FileInfoNotFoundError

from ..config import CONFIG, Config
from ..dao import (
    Database,
    FileInfoAlreadyExistsError,
    ObjectAlreadyExistsError,
    ObjectNotFoundError,
    ObjectStorage,
)
from ..models import FileInfoExternal, FileInfoInternal, UploadState


class FileAlreadyInInboxError(RuntimeError):
    """Thrown when a file is unexpectedly already in the inbox."""

    def __init__(self, file_id: str):
        message = f"The file with external id {file_id} is already in the inbox."
        super().__init__(message)


class FileNotInInboxError(RuntimeError):
    """Thrown when a file is unexpectedly not found in the inbox."""

    def __init__(self, file_id: str):
        message = f"The file with external id {file_id} not in the inbox."
        super().__init__(message)


class FileAlreadyRegisteredError(RuntimeError):
    """Thrown when a file is unexpectedly already registered."""

    def __init__(self, file_id: str):
        message = (
            f"The file with external id {file_id} has already been"
            + " registered for upload."
        )
        super().__init__(message)


class FileNotRegisteredError(RuntimeError):
    """Thrown when a file is unexpectedly not registered."""

    def __init__(self, file_id: str):
        message = f"The file with external id {file_id} has not been registered yet."
        super().__init__(message)


class FileNotReadyForConfirmUpload(RuntimeError):
    """Thrown when a file is not set to 'pending' when trying to confirm."""

    def __init__(self, file_id: str):
        message = f"The file with external id {file_id} is not set to 'pending'."
        super().__init__(message)


def handle_new_study(study_files: List[FileInfoInternal], config: Config = CONFIG):
    """
    Put the information for files into the database
    """

    for file in study_files:
        with Database(config=config) as database:
            try:
                database.register_file(file)
            except FileInfoAlreadyExistsError as error:
                raise FileAlreadyRegisteredError(file_id=file.file_id) from error


def handle_file_registered(file_id: str, config: Config = CONFIG):
    """
    Delete the file from inbox, flag it as registered in the database
    """

    # Flagging will be done in GDEV-478

    with ObjectStorage(config=config) as storage:
        try:
            storage.delete_object(
                bucket_id=config.s3_inbox_bucket_id, object_id=file_id
            )
        except ObjectNotFoundError as error:
            raise FileNotInInboxError(file_id=file_id) from error

    with Database(config=config) as database:
        try:
            database.update_file_state(file_id=file_id, state=UploadState.COMPLETED)
        except FileInfoNotFoundError as error:
            raise FileNotRegisteredError(file_id=file_id) from error


def get_upload_url(file_id: str, config: Config = CONFIG):
    """
    Checks if the file_id is in the database, the proceeds to create a presigned
    post url for an s3 staging bucket
    """

    # Check if file is in db
    with Database(config=config) as database:
        try:
            database.get_file(file_id=file_id)
            database.update_file_state(file_id=file_id, state=UploadState.PENDING)

        except FileInfoNotFoundError as error:
            raise FileNotRegisteredError(file_id=file_id) from error

    # Create presigned post for file_id
    with ObjectStorage(config=config) as storage:
        if not storage.does_bucket_exist(bucket_id=config.s3_inbox_bucket_id):
            storage.create_bucket(config.s3_inbox_bucket_id)

        try:
            presigned_post = storage.get_object_upload_url(
                bucket_id=config.s3_inbox_bucket_id, object_id=file_id, expires_after=10
            )
        except ObjectAlreadyExistsError as error:
            raise FileAlreadyInInboxError(file_id=file_id) from error

    return presigned_post


def check_uploaded_file(
    file_id: str,
    publish_upload_received: Callable[[FileInfoExternal, Config], None],
    config: Config = CONFIG,
):
    """
    Checks if the file with the specified file_id was uploaded. Throws an
    FileNotInInboxError if this is not the case.
    """

    with Database(config=config) as database:
        try:
            file = database.get_file(file_id=file_id)
            if file.state is not UploadState.PENDING:
                raise FileNotReadyForConfirmUpload(file_id=file_id)
            database.update_file_state(file_id=file_id, state=UploadState.UPLOADED)
        except FileInfoNotFoundError as error:
            raise FileNotRegisteredError(file_id=file_id) from error

    with ObjectStorage(config=config) as storage:
        if not storage.does_object_exist(
            object_id=file_id,
            bucket_id=config.s3_inbox_bucket_id,
        ):
            raise FileNotInInboxError(file_id=file_id)

    publish_upload_received(file, config)
