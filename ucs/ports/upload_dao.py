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

"""Interface for Upload Attempts DAO adapters and the exception they may throw."""

from typing import Optional, Protocol

from ucs.core import models

# pylint: disable=unused-import
from ucs.ports.outbound.file_dao import FileMetadataNotFoundError  # noqa: F401


class UploadAttemptNotFoundError(RuntimeError):
    """Thrown when trying to access an upload attempt with an ID that doesn't exist in
    the database."""

    def __init__(self, *, upload_id: Optional[str]):
        message = (
            f"The upload attempt with ID '{upload_id}' does not exist in the database."
        )
        super().__init__(message)


class UploadAttemptAlreadExistsError(RuntimeError):
    """Thrown when trying create a new upload attempt with an ID that already exist in
    the database."""

    def __init__(self, *, upload_id: Optional[str]):
        message = (
            f"An upload attempt with ID '{upload_id}' does already exist in the"
            + " database."
        )
        super().__init__(message)


# Since this is just a DAO stub without implementation, following pylint error are
# expected:
# pylint: disable=unused-argument,no-self-use
class IUploadAttemptDAO(Protocol):
    """
    A DAO interface for managing upload attempts in the database.

    Raises:
        - FileMetadataNotFoundError
        - UploadAttemptNotFoundError
        - UploadAttemptAlreadExistsError
    """

    def __enter__(self) -> "IUploadAttemptDAO":
        """Setup logic. (Maybe create transaction manager.)"""
        ...

    def __exit__(self, err_type, err_value, err_traceback):
        """Teardown logic. (Maybe close transaction manager.)"""
        ...

    def get(self, upload_id: str) -> models.UploadAttempt:
        """Get upload attempt from the database"""
        ...

    def get_all_by_file(self, file_id: str) -> list[models.UploadAttempt]:
        """Get all upload attempts for a specific file from the database"""
        ...

    def get_latest_by_file(self, file_id: str) -> Optional[models.UploadAttempt]:
        """Get the latest upload attempts for a specific file from the database"""
        ...

    def create(self, upload: models.UploadAttempt) -> None:
        """Create a new upload attempt."""
        ...

    def update(self, upload: models.UploadAttempt) -> None:
        """Update an existing upload attempt."""
        ...
