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


from typing import Protocol, Sequence

from ucs.domain import models


class FileUnkownError(RuntimeError):
    """Thrown when a file with the given ID is not known."""

    def __init__(self, *, file_id: str):
        self.file_id = file_id
        message = f"The file with ID {file_id} is unkown."
        super().__init__(message)


class IFileMetadataService(Protocol):
    """Interface of a service handling file metata.

    Raises:
        - FileUnkownError
    """

    def upsert_multiple(self, files: Sequence[models.FileMetadata]) -> None:
        """
        Registeres new files or updates existing ones.
        """
        ...

    def get(
        self,
        file_id: str,
    ) -> models.FileMetadataWithUpload:
        """
        Get metadata on the filed with the provided id.
        """
        ...
