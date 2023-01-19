# Copyright 2021 - 2023 Universität Tübingen, DKFZ and EMBL
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

from datetime import datetime
from typing import Callable

from hexkit.utils import calc_part_size
from pydantic import BaseSettings

from ucs.core import models
from ucs.ports.inbound.upload_service import UploadServicePort
from ucs.ports.outbound.dao import DaoCollectionPort, ResourceNotFoundError
from ucs.ports.outbound.event_pub import EventPublisherPort
from ucs.ports.outbound.storage import ObjectStoragePort


class UploadServiceConfig(BaseSettings):
    """Config parameters and their defaults."""

    inbox_bucket: str = "inbox"


class UploadService(UploadServicePort):
    """Service for handling multi-part uploads to the Inbox storage."""

    def __init__(
        self,
        *,
        config: UploadServiceConfig,
        daos: DaoCollectionPort,
        object_storage: ObjectStoragePort,
        event_publisher: EventPublisherPort,
        # domain internal dependencies are immediately injected:
        part_size_calculator: Callable[[int], int] = lambda file_size: calc_part_size(
            file_size=file_size
        ),
    ):
        """Initialize class instance with configs and outbound adapter objects."""

        self._inbox_bucket = config.inbox_bucket
        self._daos = daos
        self._object_storage = object_storage
        self._event_publisher = event_publisher
        self._part_size_calculator = part_size_calculator

    async def _get_upload_if_status(
        self, upload_id: str, status: models.UploadStatus
    ) -> models.UploadAttempt:
        """Makes sure that the upload with the given ID exists and that its current
        status matches the specified status. If that is the case, the upload is return. Otherwise
        an UploadStatusMissmatchError is raised."""

        upload = await self.get_details(upload_id=upload_id)

        if upload.status != status:
            raise self.UploadStatusMissmatchError(
                upload_id=upload.upload_id,
                expected_status=status,
                current_status=upload.status,
            )

        return upload

    async def _cancel_with_final_status(
        self, *, upload_id: str, final_status: models.UploadStatus
    ) -> None:
        """
        Cancel the multi-part upload with the given ID and set the specified final
        status.
        """

        upload = await self._get_upload_if_status(
            upload_id, status=models.UploadStatus.PENDING
        )

        # mark the upload as aborted in the object storage:
        try:
            await self._object_storage.abort_multipart_upload(
                upload_id=upload_id,
                bucket_id=self._inbox_bucket,
                object_id=upload.file_id,
            )
        except ObjectStoragePort.MultiPartUploadAbortError as error:
            raise self.UploadCancelError(upload_id=upload_id) from error
        except ObjectStoragePort.MultiPartUploadNotFoundError:
            # This correspond to an inconsistency between the database and
            # the storage, however, since this cancel method might be used to
            # resolve this inconsistency, this exception will be ignored.
            pass

        # change the final status of the upload in the database:
        updated_upload = upload.copy(update={"status": final_status})
        await self._daos.upload_attempts.update(updated_upload)

    async def _clear_latest_with_final_status(
        self, *, file_id: str, final_status: models.UploadStatus
    ):
        """
        Clear a multi-part upload after receiving a final accept/reject decision.
        """

        try:
            file = await self._daos.file_metadata.get_by_id(file_id)
        except ResourceNotFoundError as error:
            raise self.FileUnkownError(file_id=file_id) from error

        if file.latest_upload_id is None:
            raise self.NoLatestUploadError(file_id=file_id)

        latest_upload = await self.get_details(upload_id=file.latest_upload_id)

        if latest_upload.status != models.UploadStatus.UPLOADED:
            raise self.UploadStatusMissmatchError(
                upload_id=latest_upload.upload_id,
                expected_status=models.UploadStatus.UPLOADED,
                current_status=latest_upload.status,
            )

        # remove the object from the object storage:
        try:
            await self._object_storage.delete_object(
                bucket_id=self._inbox_bucket,
                object_id=latest_upload.file_id,
            )
        except ObjectStoragePort.ObjectNotFoundError as error:
            # This is unexpected. Thus setting the status of the upload attempt to
            # failed and raise.
            await self._cancel_with_final_status(
                upload_id=latest_upload.upload_id,
                final_status=models.UploadStatus.FAILED,
            )
            raise self.StorageAndDatabaseOutOfSyncError(
                problem=(
                    f"Trying to clear the upload with ID {latest_upload.upload_id}"
                    + f" (final status: {final_status}), however, the corresponding"
                    + f" file with id {file_id} could not be found in the database."
                )
            ) from error

        # mark the upload as complete (uploaded) in the database:
        updated_upload = latest_upload.copy(update={"status": final_status})
        await self._daos.upload_attempts.update(updated_upload)

    async def _assert_no_active_upload(self, *, file_id: str) -> None:
        """Asserts that there is no upload currently active for the file with the given
        ID. Otherwise, raises an ExistingActiveUploadError."""

        existing_attempts = self._daos.upload_attempts.find_all(
            mapping={"file_id": file_id}
        )
        async for attempt in existing_attempts:
            if attempt.status in (
                models.UploadStatus.ACCEPTED,
                models.UploadStatus.PENDING,
                models.UploadStatus.UPLOADED,
            ):
                raise self.ExistingActiveUploadError(active_upload=attempt)

    async def _init_multipart_upload(self, *, file_id: str) -> str:
        """Initialize a new multipart upload and returns the upload ID.
        This will only interact with the object storage but not update the database."""

        try:
            return await self._object_storage.init_multipart_upload(
                bucket_id=self._inbox_bucket, object_id=file_id
            )
        except ObjectStoragePort.MultiPartUploadAlreadyExistsError as error:
            raise self.FileAlreadyInInboxError(file_id=file_id) from error

    async def _insert_upload(self, *, upload: models.UploadAttempt) -> None:
        """Insert a new upload attempt to the database assuming a corresponding
        multipart upload has already been initiated at the object storage.
        If that operation fails unexpectedly, the initiation of the upload at
        the object storage is roled back.
        """

        try:
            await self._daos.upload_attempts.insert(upload)
        except:
            # One source of error might be that an upload with the given ID
            # already exists. In that case the assumption that the object
            # storage assigns unique IDs is violated. However, at this stage
            # there is nothing we can do to handel this exception.
            await self._object_storage.abort_multipart_upload(
                upload_id=upload.upload_id,
                bucket_id=self._inbox_bucket,
                object_id=upload.file_id,
            )
            raise

    async def _set_latest_upload_for_file(
        self, *, file: models.FileMetadata, new_upload_id: str
    ) -> None:
        """Sets the `latest_upload_id` metadata field of the specified file in the
        database to the provided new_upload_id.
        It is assumed that a multipart upload has already been initiated at the object
        storage and that a new upload entry was persisted to the database.
        If this operation fails unexpectedly, both the database and the object storage
        are roled back by eliminating any traces of this new upload.
        """

        updated_file = file.copy(update={"latest_upload_id": new_upload_id})
        try:
            await self._daos.file_metadata.update(updated_file)
        except:
            # this shouldn't happen, but if it does, we need to cleanup:
            await self._object_storage.abort_multipart_upload(
                upload_id=new_upload_id,
                bucket_id=self._inbox_bucket,
                object_id=file.file_id,
            )
            await self._daos.upload_attempts.delete(id_=new_upload_id)
            raise

    async def initiate_new(
        self, *, file_id: str, submitter_public_key: str
    ) -> models.UploadAttempt:
        """
        Initiates a new multi-part upload for the file with the given ID.
        """

        try:
            file = await self._daos.file_metadata.get_by_id(file_id)
        except ResourceNotFoundError as error:
            raise self.FileUnkownError(file_id=file_id) from error

        await self._assert_no_active_upload(file_id=file_id)

        upload_id = await self._init_multipart_upload(file_id=file_id)

        # get the recommended part size:
        part_size = self._part_size_calculator(file.decrypted_size)

        # assemble the upload attempts details:
        upload = models.UploadAttempt(
            upload_id=upload_id,
            file_id=file_id,
            status=models.UploadStatus.PENDING,
            part_size=part_size,
            creation_date=datetime.utcnow(),
            submitter_public_key=submitter_public_key,
        )

        await self._insert_upload(upload=upload)
        await self._set_latest_upload_for_file(file=file, new_upload_id=upload_id)

        return upload

    async def get_details(self, *, upload_id: str) -> models.UploadAttempt:
        """
        Get details on an existing multipart upload by specifing its ID.
        """

        try:
            return await self._daos.upload_attempts.get_by_id(upload_id)
        except ResourceNotFoundError as error:
            raise self.UploadUnkownError(upload_id=upload_id) from error

    async def create_part_url(self, *, upload_id: str, part_no: int) -> str:
        """
        Create and return a pre-signed URL to upload the bytes for the file part with
        the given number of the upload with the given ID.
        """

        upload = await self._get_upload_if_status(
            upload_id, status=models.UploadStatus.PENDING
        )

        try:
            return await self._object_storage.get_part_upload_url(
                upload_id=upload_id,
                bucket_id=self._inbox_bucket,
                object_id=upload.file_id,
                part_number=part_no,
            )
        except ObjectStoragePort.MultiPartUploadNotFoundError as error:
            raise self.StorageAndDatabaseOutOfSyncError(
                problem=(
                    f"The upload attempt with ID {upload_id} was marked as 'pending' in"
                    + "the database, but no corresponding upload exists in the object"
                    + "storage."
                )
            ) from error

    async def complete(self, *, upload_id: str) -> None:
        """
        Confirm the completion of the multi-part upload with the given ID.
        """

        upload = await self._get_upload_if_status(
            upload_id, status=models.UploadStatus.PENDING
        )

        # mark the upload as complete in the object storage:
        try:
            await self._object_storage.complete_multipart_upload(
                upload_id=upload_id,
                bucket_id=self._inbox_bucket,
                object_id=upload.file_id,
            )
        except ObjectStoragePort.MultiPartUploadConfirmError as error:
            # This can typically not be repaired, so aborting the upload attempt
            # and marking it as failed in the database:
            await self._cancel_with_final_status(
                upload_id=upload_id, final_status=models.UploadStatus.FAILED
            )

            raise self.UploadCompletionError(
                upload_id=upload_id, reason=str(error)
            ) from error

        # mark the upload as complete (uploaded) in the database:
        completion_date = datetime.utcnow()
        updated_upload = upload.copy(
            update={
                "status": models.UploadStatus.UPLOADED,
                "completion_date": completion_date,
            }
        )
        await self._daos.upload_attempts.update(updated_upload)

        # publish an event, informing other services that a new upload was received:
        file = await self._daos.file_metadata.get_by_id(upload.file_id)
        await self._event_publisher.publish_upload_received(
            file_metadata=file,
            upload_date=completion_date,
            submitter_public_key=updated_upload.submitter_public_key,
        )

    async def cancel(self, *, upload_id: str) -> None:
        """
        Cancel the multi-part upload with the given ID.
        """

        await self._cancel_with_final_status(
            upload_id=upload_id, final_status=models.UploadStatus.CANCELLED
        )

    async def accept_latest(self, *, file_id: str) -> None:
        """
        Accept the latest multi-part upload for the given file.

        Here the file ID is used, as this method is triggered by downstream services
        that only know the file ID not the upload attempt.
        """

        await self._clear_latest_with_final_status(
            file_id=file_id, final_status=models.UploadStatus.ACCEPTED
        )

    async def reject_latest(self, *, file_id: str) -> None:
        """
        Reject the latest multi-part upload for the given file.

        Here the file ID is used, as this method is triggered by downstream services
        that only know the file ID not the upload attempt.
        """
        await self._clear_latest_with_final_status(
            file_id=file_id, final_status=models.UploadStatus.REJECTED
        )
