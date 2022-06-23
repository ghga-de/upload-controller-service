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

"""Interfaces for the main upload handling logic of this service."""


from typing import Optional, Protocol

from ucs.domain import models

# shortcuts:
# pylint: disable=unused-import
from ucs.domain.interfaces.inbound.file_service import FileUnkownError  # noqa: F401


class FileAlreadyInInboxError(RuntimeError):
    """Thrown when a file is unexpectedly already in the inbox."""

    def __init__(self, file_id: str):
        message = f"The file with ID {file_id} is already in the inbox."
        super().__init__(message)


class FileNotInInboxError(RuntimeError):
    """Thrown when a file is declared to be uploaded but was not found in the inbox."""

    def __init__(self, file_id: str):
        message = f"The file with ID {file_id} not in the inbox."
        super().__init__(message)


class UploadUnkownError(RuntimeError):
    """Thrown when an upload attempt with the given ID is not known."""

    def __init__(self, upload_id: str):
        message = f"The upload attempt with ID {upload_id} is unkown."
        super().__init__(message)


class UploadNotPendingError(RuntimeError):
    """Thrown when an upload was expected in "pending" state for performing an action."""

    def __init__(self, upload_id: str, *, current_status: models.UploadStatus):
        self.current_status = current_status
        message = (
            f"The upload with ID {upload_id} must be in 'pending' state to perform"
            + f" the requested action, however, its current state is: {current_status}"
        )
        super().__init__(message)


class ExistingActiveUploadError(RuntimeError):
    """Thrown when trying to create a new upload while there is another upload active."""

    def __init__(self, *, active_upload: models.UploadAttempt):
        self.active_upload = active_upload
        message = (
            "Failed to create a new multi-part upload for the file with ID"
            + f" {self.active_upload.file_id} because another upload is"
            + " currently active or has been accepted."
            + f" ID, status of the existing upload: {self.active_upload.upload_id},"
            + f" {self.active_upload.status}"
        )
        super().__init__(message)


class UploadCompletionError(RuntimeError):
    """Thrown when the confirmation of an upload attempt failed."""

    def __init__(self, upload_id: str, reason: Optional[str]):
        message = (
            f"The confirmation of the upload attempt with ID {upload_id} failed."
            + " The upload attempt was aborted and cannot be resumed."
            + ("" if reason is None else f" The reason was: {reason}")
        )
        super().__init__(message)


class UploadCancelError(RuntimeError):
    """Thrown when the cancelling of an upload attempt failed."""

    def __init__(self, upload_id: str):
        message = (
            f"Failed to cancel the multi-part upload {upload_id}. An ongoing part upload"
            + " might be a reason. Please complete all part upload and try to cancel"
            + " again."
        )
        super().__init__(message)


class StorageAndDatabaseOutOfSyncError(RuntimeError):
    """Thrown when the state of the storage and the state of the database are out of
    sync."""

    def __init__(self, *, problem: str):
        message = f"The object storage and the database are out of sync: {problem}"
        super().__init__(message)


class IUploadService(Protocol):
    """Interface of a service handling uploads to the Inbox storage.

    Raises:
        - FileUnkownError
        - FileAlreadyInInboxError
        - FileNotInInboxError
        - UploadNotPendingError
        - ExistingActiveUploadError
        - StorageAndDatabaseOutOfSyncError
    """

    def initiate_new(self, *, file_id: str) -> models.UploadAttempt:
        """
        Initiates a new multi-part upload for the file with the given ID.
        """
        ...

    def get_details(self, *, upload_id: str) -> models.UploadAttempt:
        """
        Get details on an existing multipart upload by specifing its ID.
        """
        ...

    def create_part_url(self, *, upload_id: str, part_no: int) -> str:
        """
        Create and return a pre-signed URL to upload the bytes for the file part with
        the given number of the upload with the given ID.
        """
        ...

    def complete(self, *, upload_id: str) -> None:
        """
        Confirm the completion of the multi-part upload with the given ID.
        """
        ...

    def cancel(self, *, upload_id: str) -> None:
        """
        Cancel the multi-part upload with the given ID.
        """
        ...
