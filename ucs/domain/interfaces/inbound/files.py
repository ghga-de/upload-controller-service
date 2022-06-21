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


from typing import List, Protocol

from ucs.domain.models import FileMetadataInternal


class IUploadService(Protocol):
    """Interface for the main service class for handling uploads to the Inbox."""

    def handle_new_study(self, study_files: List[FileMetadataInternal]):
        """
        Put the information for files into the database
        """
        ...

    def handle_file_registered(
        self,
        file_id: str,
    ):
        """
        Delete the file from inbox, flag it as registered in the database
        """
        ...

    def get_upload_url(
        self,
        file_id: str,
    ):
        """
        Checks if the file_id is in the database, the proceeds to create a presigned
        post url for an s3 staging bucket
        """
        ...

    def confirm_file_upload(
        self,
        file_id: str,
    ):
        """
        Checks if the file with the specified file_id was uploaded. Throws an
        FileNotInInboxError if this is not the case.
        """
        ...
