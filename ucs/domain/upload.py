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


"""The main upload handling logic."""

from typing import List

from ucs.domain.interfaces.inbound.upload import (
    FileAlreadyInInboxError,
    FileAlreadyRegisteredError,
    FileNotInInboxError,
    FileNotReadyForConfirmUpload,
    FileNotRegisteredError,
    IUploadService,
)
from ucs.domain.interfaces.outbound.event_pub import IEventPublisher
from ucs.domain.interfaces.outbound.file_metadata import (
    FileMetadataAlreadyExistsError,
    FileMetadataNotFoundError,
    IFileMetadataDAO,
)
from ucs.domain.interfaces.outbound.storage import (
    IObjectStorage,
    ObjectAlreadyExistsError,
    ObjectNotFoundError,
)
from ucs.domain.models import FileMetadataInternal, UploadState


class UploadService(IUploadService):
    """Main service class for handling uploads to the Inbox."""

    # pylint: disable=super-init-not-called
    def __init__(
        self,
        *,
        s3_inbox_bucket_id: str,
        file_metadata_dao: IFileMetadataDAO,
        object_storage_dao: IObjectStorage,
        event_publisher: IEventPublisher,
    ):
        """Ininitalize class instance with configs and outbound adapter objects."""
        self._file_metadata_dao = file_metadata_dao
        self._object_storage_dao = object_storage_dao
        self._event_publisher = event_publisher
        self._s3_inbox_bucket_id = s3_inbox_bucket_id

    def handle_new_study(self, study_files: List[FileMetadataInternal]):
        """
        Put the information for files into the database
        """

        for file_metadata in study_files:
            with self._file_metadata_dao as fi_dao:
                try:
                    fi_dao.register(file_metadata)
                except FileMetadataAlreadyExistsError as error:
                    raise FileAlreadyRegisteredError(
                        file_id=file_metadata.file_id
                    ) from error

    def handle_file_registered(
        self,
        file_id: str,
    ):
        """
        Delete the file from inbox, flag it as registered in the database
        """

        # Flagging will be done in GDEV-478

        with self._object_storage_dao as storage:
            try:
                storage.delete_object(
                    bucket_id=self._s3_inbox_bucket_id, object_id=file_id
                )
            except ObjectNotFoundError as error:
                raise FileNotInInboxError(file_id=file_id) from error

        with self._file_metadata_dao as fi_dao:
            try:
                fi_dao.update_file_state(file_id=file_id, state=UploadState.COMPLETED)
            except FileMetadataNotFoundError as error:
                raise FileNotRegisteredError(file_id=file_id) from error

    def get_upload_url(
        self,
        file_id: str,
    ):
        """
        Checks if the file_id is in the database, the proceeds to create a presigned
        post url for an s3 staging bucket
        """

        # Check if file is in db
        with self._file_metadata_dao as fi_dao:
            try:
                fi_dao.get(file_id=file_id)

            except FileMetadataNotFoundError as error:
                raise FileNotRegisteredError(file_id=file_id) from error

            # Create presigned post for file_id
            with self._object_storage_dao as storage:
                if not storage.does_bucket_exist(bucket_id=self._s3_inbox_bucket_id):
                    storage.create_bucket(self._s3_inbox_bucket_id)

                try:
                    presigned_post = storage.get_object_upload_url(
                        bucket_id=self._s3_inbox_bucket_id,
                        object_id=file_id,
                        expires_after=10,
                    )
                except ObjectAlreadyExistsError as error:
                    raise FileAlreadyInInboxError(file_id=file_id) from error

            fi_dao.update_file_state(file_id=file_id, state=UploadState.PENDING)
        return presigned_post

    def confirm_file_upload(
        self,
        file_id: str,
    ):
        """
        Checks if the file with the specified file_id was uploaded. Throws an
        FileNotInInboxError if this is not the case.
        """

        with self._file_metadata_dao as fi_dao:
            try:
                file_metadata = fi_dao.get(file_id=file_id)
                if file_metadata.state is not UploadState.PENDING:
                    raise FileNotReadyForConfirmUpload(file_id=file_id)
            except FileMetadataNotFoundError as error:
                raise FileNotRegisteredError(file_id=file_id) from error

            with self._object_storage_dao as storage:
                if not storage.does_object_exist(
                    object_id=file_id,
                    bucket_id=self._s3_inbox_bucket_id,
                ):
                    raise FileNotInInboxError(file_id=file_id)

            fi_dao.update_file_state(file_id=file_id, state=UploadState.UPLOADED)

        self._event_publisher.publish_upload_received(file_metadata)
