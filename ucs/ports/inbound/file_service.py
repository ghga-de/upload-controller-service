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


from typing import Iterable, Protocol, Sequence

from ucs.core import models

UPDATABLE_METADATA_FIELDS = {"status"}


class FileUnkownError(RuntimeError):
    """Thrown when a file with the given ID is not known."""

    def __init__(self, *, file_id: str):
        self.file_id = file_id
        message = f"The file with ID {file_id} is unkown."
        super().__init__(message)


class InvalidFileMetadatUpdateError(RuntimeError):
    """Thrown when trying to update a metadata field of a file that is not allowed to
    change (i.e. not in the UPDATABLE_METADATA_FIELDS set)."""

    def __init__(self, *, file_id: str, invalid_fields: Iterable[str]):
        self.file_id = file_id
        message = (
            f"Following fields for the with ID {file_id} cannot be updated: "
            + ", ".join(invalid_fields)
        )
        super().__init__(message)


class FileMetadataPort(Protocol):
    """Interface of a service handling file metata."""

    async def upsert_multiple(self, files: Sequence[models.FileMetadataUpsert]) -> None:
        """Registeres new files or updates existing ones.

        Raises:
            InvalidFileMetadatUpdateError:
                When trying to update a metadata field, that can only be set on
                creation.
        """
        ...

    async def get_by_id(
        self,
        file_id: str,
    ) -> models.FileMetadata:
        """Get metadata on the filed with the provided ID.

        Raises:
            FileUnkownError:
                When a file with the corresponding ID does not exist.
        """
        ...
