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

from pydantic import BaseSettings

from ucs.core import models
from ucs.core.interfaces.part_calc import IPartSizeCalculator
from ucs.core.part_calc import calculate_part_size
from ucs.ports.inbound.upload_service import (
    ExistingActiveUploadError,
    FileAlreadyInInboxError,
    FileUnkownError,
    IUploadService,
    NoLatestUploadError,
    StorageAndDatabaseOutOfSyncError,
    UploadCancelError,
    UploadCompletionError,
    UploadStatusMissmatchError,
    UploadUnkownError,
)
from ucs.ports.outbound.event_pub import IEventPublisher
from ucs.ports.outbound.file_dao import FileMetadataNotFoundError, IFileMetadataDAO
from ucs.ports.outbound.storage import (
    IObjectStorage,
    MultiPartUploadAbortError,
    MultiPartUploadConfirmError,
    MultiPartUploadNotFoundError,
    ObjectNotFoundError,
)
from ucs.ports.outbound.upload_dao import IUploadAttemptDAO, UploadAttemptNotFoundError


class UploadServiceConfig(BaseSettings):
    """Config parameters and their defaults."""

    inbox_bucket: str = "inbox"


class UploadService(IUploadService):
    """Service for handling multi-part uploads to the Inbox storage.

    Raises:
        - FileUnkownError
        - FileAlreadyInInboxError
        - FileNotInInboxError
        - UploadNotPendingError
        - ExistingActiveUploadError
        - StorageAndDatabaseOutOfSyncError
    """

    def __init__(
        self,
        *,
        config: UploadServiceConfig,
        file_metadata_dao: IFileMetadataDAO,
        upload_attempt_dao: IUploadAttemptDAO,
        object_storage: IObjectStorage,
        event_publisher: IEventPublisher,
        # domain internal dependencies are immediately injected:
        part_size_calculator: IPartSizeCalculator = calculate_part_size,
    ):
        """Ininitalize class instance with configs and outbound adapter objects."""

        self._inbox_bucket = config.inbox_bucket
        self._file_metadata_dao = file_metadata_dao
        self._upload_attempt_dao = upload_attempt_dao
        self._object_storage = object_storage
        self._event_publisher = event_publisher
        self._part_size_calculator = part_size_calculator

        # Create inbox bucket if it doesn't exist:
        with self._object_storage as storage:
            if not storage.does_bucket_exist(bucket_id=self._inbox_bucket):
                storage.create_bucket(self._inbox_bucket)

    def _get_upload_if_status(
        self, upload_id: str, status: models.UploadStatus
    ) -> models.UploadAttempt:
        """Makes sure that the upload with the given ID exists and that its current
        status matches the specified status. If that is the case, the upload is return. Otherwise
        an UploadStatusMissmatchError is raised."""

        upload = self.get_details(upload_id=upload_id)

        if upload.status != status:
            raise UploadStatusMissmatchError(
                upload_id=upload.upload_id,
                expected_status=status,
                current_status=upload.status,
            )

        return upload

    def _cancel_with_final_status(
        self, *, upload_id: str, final_status: models.UploadStatus
    ) -> None:
        """
        Cancel the multi-part upload with the given ID and set the specified final
        status.
        """

        upload = self._get_upload_if_status(
            upload_id, status=models.UploadStatus.PENDING
        )

        # mark the upload as aborted in the object storage:
        with self._object_storage as storage:
            try:
                storage.abort_multipart_upload(
                    upload_id=upload_id,
                    bucket_id=self._inbox_bucket,
                    object_id=upload.file_id,
                )
            except MultiPartUploadAbortError as error:
                raise UploadCancelError(upload_id=upload_id) from error
            except MultiPartUploadNotFoundError:
                # This correspond to an inconsistency between the database and
                # the storage, however, since this cancel method might be used to
                # resolve this inconsistency, this exception will be ignored.
                pass

        # change the final status of the upload in the database:
        with self._upload_attempt_dao as ua_dao:
            updated_upload = upload.copy(update={"status": final_status})
            ua_dao.update(updated_upload)

    def _clear_latest_with_final_status(
        self, *, file_id: str, final_status: models.UploadStatus
    ):
        """
        Clear a multi-part upload after receiving a final accept/reject decision.
        """

        with self._upload_attempt_dao as ua_dao:
            try:
                latest_upload = ua_dao.get_latest_by_file(file_id)
            except FileMetadataNotFoundError as error:
                raise FileUnkownError(file_id=file_id) from error

        if latest_upload is None:
            raise NoLatestUploadError(file_id=file_id)

        if latest_upload.status != models.UploadStatus.UPLOADED:
            raise UploadStatusMissmatchError(
                upload_id=latest_upload.upload_id,
                expected_status=models.UploadStatus.UPLOADED,
                current_status=latest_upload.status,
            )

        # remove the object from the object storage:
        with self._object_storage as storage:
            try:
                storage.delete_object(
                    bucket_id=self._inbox_bucket,
                    object_id=latest_upload.file_id,
                )
            except ObjectNotFoundError as error:
                # This is unexpected. Thus setting the status of the upload attempt to
                # failed and raise.
                self._cancel_with_final_status(
                    upload_id=latest_upload.upload_id,
                    final_status=models.UploadStatus.FAILED,
                )
                raise StorageAndDatabaseOutOfSyncError(
                    problem=(
                        f"Trying to clear the upload with ID {latest_upload.upload_id}"
                        + f" (final status: {final_status}), however, the corresponding"
                        + f" file with id {file_id} could not be found in the database."
                    )
                ) from error

        # mark the upload as complete (uploaded) in the database:
        with self._upload_attempt_dao as ua_dao:
            updated_upload = latest_upload.copy(update={"status": final_status})
            ua_dao.update(updated_upload)

    def initiate_new(self, *, file_id: str) -> models.UploadAttempt:
        """
        Initiates a new multi-part upload for the file with the given ID.
        """

        # check if the file exists:
        with self._file_metadata_dao as fm_dao:
            try:
                file = fm_dao.get(file_id)
            except FileMetadataNotFoundError as error:
                raise FileUnkownError(file_id=file_id) from error

        # check if another upload is currently active or accepted:
        with self._upload_attempt_dao as ua_dao:
            existing_attempts = ua_dao.get_all_by_file(file_id)
            for attempt in existing_attempts:
                if attempt.status in (
                    models.UploadStatus.ACCEPTED,
                    models.UploadStatus.PENDING,
                    models.UploadStatus.UPLOADED,
                ):
                    raise ExistingActiveUploadError(active_upload=attempt)

        with self._object_storage as storage:
            # check if the file already exists in the inbox:
            if storage.does_object_exist(
                bucket_id=self._inbox_bucket, object_id=file_id
            ):
                raise FileAlreadyInInboxError(file_id=file_id)

            # otherwise initiate the multipart upload:
            upload_id = storage.init_multipart_upload(
                bucket_id=self._inbox_bucket, object_id=file_id
            )

            # get the recommended part size:
            part_size = self._part_size_calculator(file.size)

            # assemble the upload attempts details:
            upload = models.UploadAttempt(
                upload_id=upload_id,
                file_id=file_id,
                status=models.UploadStatus.PENDING,
                part_size=part_size,
            )

            # persist the upload to the database:
            # (If that fails unexpectedly, role back the initiation of the upload at
            # the storage)
            with self._upload_attempt_dao as ua_dao:
                try:
                    ua_dao.create(upload)
                except:
                    # One source of error might be that an upload with the given ID
                    # already exists. In that case the assumption that the object
                    # storage assigns unique IDs is violated. However, at this stage
                    # there is nothing we can do to handel this exception.
                    storage.abort_multipart_upload(
                        upload_id=upload_id,
                        bucket_id=self._inbox_bucket,
                        object_id=file_id,
                    )
                    raise

            return upload

    def get_details(self, *, upload_id: str) -> models.UploadAttempt:
        """
        Get details on an existing multipart upload by specifing its ID.
        """

        with self._upload_attempt_dao as ua_dao:
            try:
                return ua_dao.get(upload_id)
            except UploadAttemptNotFoundError as error:
                raise UploadUnkownError(upload_id=upload_id) from error

    def create_part_url(self, *, upload_id: str, part_no: int) -> str:
        """
        Create and return a pre-signed URL to upload the bytes for the file part with
        the given number of the upload with the given ID.
        """

        upload = self._get_upload_if_status(
            upload_id, status=models.UploadStatus.PENDING
        )

        with self._object_storage as storage:
            try:
                return storage.get_part_upload_url(
                    upload_id=upload_id,
                    bucket_id=self._inbox_bucket,
                    object_id=upload.file_id,
                    part_number=part_no,
                )
            except MultiPartUploadNotFoundError as error:
                raise StorageAndDatabaseOutOfSyncError(
                    problem=(
                        f"The upload attempt with ID {upload_id} was marked as 'pending' in"
                        + "the database, but no corresponding upload exists in the object"
                        + "storage."
                    )
                ) from error

    def complete(self, *, upload_id: str) -> None:
        """
        Confirm the completion of the multi-part upload with the given ID.
        """

        upload = self._get_upload_if_status(
            upload_id, status=models.UploadStatus.PENDING
        )

        # mark the upload as complete in the object storage:
        with self._object_storage as storage:
            try:
                storage.complete_multipart_upload(
                    upload_id=upload_id,
                    bucket_id=self._inbox_bucket,
                    object_id=upload.file_id,
                )
            except MultiPartUploadConfirmError as error:
                # This can typically not be repaired, so aborting the upload attempt
                # and marking it as failed in the database:
                self._cancel_with_final_status(
                    upload_id=upload_id, final_status=models.UploadStatus.FAILED
                )

                raise UploadCompletionError(
                    upload_id=upload_id, reason=str(error)
                ) from error

        # mark the upload as complete (uploaded) in the database:
        with self._upload_attempt_dao as ua_dao:
            updated_upload = upload.copy(
                update={"status": models.UploadStatus.UPLOADED}
            )
            ua_dao.update(updated_upload)

        # publish an event, informing other services that a new upload was received:
        with self._file_metadata_dao as fm_dao:
            file = fm_dao.get(upload.file_id)
        self._event_publisher.publish_upload_received(file_metadata=file)

    def cancel(self, *, upload_id: str) -> None:
        """
        Cancel the multi-part upload with the given ID.
        """

        self._cancel_with_final_status(
            upload_id=upload_id, final_status=models.UploadStatus.CANCELLED
        )

    def accept_latest(self, *, file_id: str) -> None:
        """
        Accept the latest multi-part upload for the given file.

        Here the file ID is used, as this method is triggered by downstream services
        that only know the file ID not the upload attempt.
        """

        self._clear_latest_with_final_status(
            file_id=file_id, final_status=models.UploadStatus.ACCEPTED
        )

    def reject_latest(self, *, file_id: str) -> None:
        """
        Accept the latest multi-part upload for the given file.

        Here the file ID is used, as this method is triggered by downstream services
        that only know the file ID not the upload attempt.
        """
        self._clear_latest_with_final_status(
            file_id=file_id, final_status=models.UploadStatus.REJECTED
        )
