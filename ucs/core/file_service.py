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

from typing import Sequence

from ucs.core import models
from ucs.ports.inbound.file_service import FileUnkownError, FileMetadataPort
from ucs.ports.outbound.dao import DaoCollection, ResourceNotFoundError
from ucs.ports.outbound.upload_dao import IUploadAttemptDAO


class FileMetadataServive(FileMetadataPort):
    """Implementation of a service handling file metata.

    Raises:
        - FileUnkownError
    """

    def __init__(self, *, daos: DaoCollection):
        """Ininitalize class instance with configs and outbound adapter objects."""
        self._daos = daos

    async def _complete_upsert_metadata(
        self, upsert_metadata: models.FileMetadataUpsert
    ) -> models.FileMetadata:
        """The given upsert metadata is supplemented with data from the database (if
        existent).
        """

        try:
            existing_metadata = await self._daos.file_metadata.get_by_id(
                upsert_metadata.file_id
            )
        except ResourceNotFoundError:
            # there is no entry for that file in the database, yet
            return models.FileMetadata(**upsert_metadata.dict(), latest_upload_id=None)

        return models.FileMetadata(
            **upsert_metadata.dict(),
            latest_upload_id=existing_metadata.latest_upload_id
        )

    async def upsert_multiple(self, files: Sequence[models.FileMetadataUpsert]) -> None:
        """Registeres new files or updates existing ones."""

        for file in files:
            full_file_metadata = await self._complete_upsert_metadata(file)

            await self._daos.file_metadata.upsert(full_file_metadata)

    async def get_by_id(
        self,
        file_id: str,
    ) -> models.FileMetadata:
        """Get metadata on the filed with the provided id."""

        try:
            return await self._daos.file_metadata.get_by_id(file_id)
        except ResourceNotFoundError as error:
            raise FileUnkownError(file_id=file_id) from error
