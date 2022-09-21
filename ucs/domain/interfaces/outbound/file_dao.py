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
class IFileMetadataDAO(Protocol):
    """
    A DAO interface for managing file info in the database.

    Raises:
        - FileMetadataNotFoundError
    """

    def __enter__(self) -> "IFileMetadataDAO":
        """Setup logic. (Maybe create transaction manager.)"""
        ...

    def __exit__(self, err_type, err_value, err_traceback):
        """Teardown logic. (Maybe close transaction manager.)"""
        ...

    def get(self, file_id: str) -> models.FileMetadata:
        """Get file from the database"""
        ...

    def upsert(self, file: models.FileMetadata) -> None:
        """Register or update a file."""
        ...


class FileMetadataNotFoundError(RuntimeError):
    """Thrown when trying to access a file with a file ID that doesn't
    exist in the database."""

    def __init__(self, *, file_id: Optional[str]):
        message = f"The file with file ID '{file_id}' does not exist in the database."
        super().__init__(message)
