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

"""Interfaces for File Info DAO adapters and the exception they may throw."""

from typing import Optional, Protocol

from ucs.domain import models


# Since this is just a DAO stub without implementation, following pylint error are
# expected:
# pylint: disable=unused-argument,no-self-use
class IFileInfoDAO(Protocol):
    """
    A DAO interface for managing file info in the database.
    It might throw following exception to communicate selected error events:
        - FileInfoNotFoundError
        - FileInfoAlreadyExistsError
    """

    def __enter__(self) -> "IFileInfoDAO":
        """Setup logic. (Maybe create transaction manager.)"""
        ...

    def __exit__(self, err_type, err_value, err_traceback):
        """Teardown logic. (Maybe close transaction manager.)"""
        ...

    def get(self, file_id: str) -> models.FileInfoExternal:
        """Get file from the database"""
        ...

    def register(self, file: models.FileInfoInternal) -> None:
        """Register a new file to the database."""
        ...

    def update_file_state(self, file_id: str, state: models.UploadState) -> None:
        """Update the file state of a file in the database."""
        ...

    def unregister(self, file_id: str) -> None:
        """
        Unregister a new file with the specified file ID from the database.
        """
        ...


class FileInfoDaoError(RuntimeError):
    "Base class for all error thrown by the an implementation of the IFileInfoDAO."

    pass  # pylint: disable=unnecessary-pass


class FileInfoNotFoundError(FileInfoDaoError):
    """Thrown when trying to access a file with a file ID that doesn't
    exist in the database."""

    def __init__(self, file_id: Optional[str]):
        message = (
            "The file"
            + (f" with file ID '{file_id}' " if file_id else "")
            + " does not exist in the database."
        )
        super().__init__(message)


class FileInfoAlreadyExistsError(FileInfoDaoError):
    """Thrown when trying to add a file with an file ID that already
    exist in the database."""

    def __init__(self, file_id: Optional[str]):
        message = (
            "The file"
            + (f" with file ID '{file_id}' " if file_id else "")
            + " already exist in the database."
        )
        super().__init__(message)
