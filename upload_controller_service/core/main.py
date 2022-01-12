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

from ghga_service_chassis_lib.object_storage_dao import (
    BucketNotFoundError,
    ObjectNotFoundError,
)

from ..config import CONFIG, Config
from ..dao import Database, FileInfoAlreadyExistsError, ObjectStorage
from ..models import FileInfoInternal


class FileAlreadyInInboxError(RuntimeError):
    """Thrown when a file is unexpectedly already in the inbox."""

    def __init__(self, external_file_id: str):
        message = (
            f"The file with external id {external_file_id} is already in the inbox."
        )
        super().__init__(message)


class FileAlreadyRegisteredError(RuntimeError):
    """Thrown when a file is unexpectedly already registered."""

    def __init__(self, external_file_id: str):
        message = (
            f"The file with external id {external_file_id} has already been"
            + " registered for upload."
        )
        super().__init__(message)


class FileNotRegisteredError(RuntimeError):
    """Thrown when a file is unexpectedly not registered."""

    def __init__(self, external_file_id: str):
        message = (
            f"The file with external id {external_file_id} has not been registered yet."
        )
        super().__init__(message)


class FileNotInInboxError(RuntimeError):
    """Thrown when a file is unexpectedly not in the inbox."""

    def __init__(self, external_file_id: str):
        message = (
            f"The file with external id {external_file_id} does not exist in the inbox."
        )
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
                raise FileAlreadyRegisteredError(
                    external_file_id=file.file_id
                ) from error


def get_upload_url(file_id: str, config: Config = CONFIG):
    """
    Checks if the file_id is in the database, the proceeds to create a presigned
    post url for an s3 staging bucket
    """

    # Check if file is in db, will throw an exception if not
    with Database(config=config) as database:
        database.get_file(file_id=file_id)

    # Create presigned post for file_id
    with ObjectStorage(config=config) as storage:
        if not storage.does_bucket_exist(bucket_id=config.s3_inbox_bucket_id):
            storage.create_bucket(config.s3_inbox_bucket_id)

        presigned_post = storage.get_object_upload_url(
            bucket_id=config.s3_inbox_bucket_id, object_id=file_id
        )

    return presigned_post


def check_uploaded_file(
    file_id: str,
    publish_upload_received: Callable[[FileInfoInternal, Config], None],
    config: Config = CONFIG,
):
    """
    Checks if the file with the specified file_id was uploaded
    """

    with Database(config=config) as database:
        file = database.get_file(file_id=file_id)

    with ObjectStorage(config=config) as storage:
        if not storage.does_bucket_exist(bucket_id=config.inbox_bucket_name):
            raise BucketNotFoundError

        if not storage.does_object_exist(
            object_id=file_id,
            bucket_id=config.inbox_bucket_name,
        ):
            raise ObjectNotFoundError

    publish_upload_received(file, config)
