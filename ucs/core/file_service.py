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
from ucs.ports.inbound.file_service import FileUnkownError, IFileMetadataService
from ucs.ports.outbound.file_dao import FileMetadataNotFoundError, IFileMetadataDAO
from ucs.ports.outbound.upload_dao import IUploadAttemptDAO


class FileMetadataServive(IFileMetadataService):
    """Implementation of a service handling file metata.

    Raises:
        - FileUnkownError
    """

    def __init__(
        self,
        *,
        file_metadata_dao: IFileMetadataDAO,
        upload_attempt_dao: IUploadAttemptDAO,
    ):
        """Ininitalize class instance with configs and outbound adapter objects."""
        self._file_metadata_dao = file_metadata_dao
        self._upload_attempt_dao = upload_attempt_dao

    def upsert_multiple(self, files: Sequence[models.FileMetadata]) -> None:
        """
        Registeres new files or updates existing ones.
        """
        with self._file_metadata_dao as fm_dao:
            for file in files:
                fm_dao.upsert(file)

    def get(
        self,
        file_id: str,
    ) -> models.FileMetadataWithUpload:
        """
        Get metadata on the filed with the provided id.
        """

        # get basic file metadata:
        with self._file_metadata_dao as fm_dao:
            try:
                file_metadata = fm_dao.get(file_id)
            except FileMetadataNotFoundError as error:
                raise FileUnkownError(file_id=file_id) from error

        # get the latest upload attempt
        with self._upload_attempt_dao as ua_dao:
            latest_upload = ua_dao.get_latest_by_file(file_id)
        latest_upload_id = None if latest_upload is None else latest_upload.upload_id

        # assemble information:
        return models.FileMetadataWithUpload(
            latest_upload_id=latest_upload_id, **file_metadata.dict()
        )
