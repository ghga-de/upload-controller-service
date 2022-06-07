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

"""Defines dataclasses for holding business-logic data"""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import UUID4, BaseModel


# fmt: off
class UploadStatus(Enum):

    """
    The current upload state. Can be one of:
        - PENDING (the user has requested an upload url)
        - CANCELLED (the user has canceled the upload)
        - UPLOADED (the user has confirmed the upload)
        - FAILED (the upload has failed for a technical reason)
        - ACCEPTED (the upload was accepted by a downstream service)
        - REJECTED (the upload was rejected by a downstream service)
    """

    PENDING = "pending"
    CANCELLED = "cancelled"
    UPLOADED = "uploaded"
    FAILED = "failed"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
# fmt: on


class CurrentUploadAttempt(BaseModel):
    """
    A model containing details on the most recent upload attempt for a specific File.
    Please note this should only be used within the `FileMetadata` model.
    """

    external_id: str
    status: UploadStatus


class FileMetadataInternal(BaseModel):
    """
    A model containing all the metadata needed to pass it on to other microservices
    """

    id: UUID4
    external_id: str
    grouping_label: str
    md5_checksum: str
    size: int
    creation_date: datetime
    update_date: datetime
    format: str
    file_name: str
    current_upload_attempt: Optional[CurrentUploadAttempt]


class FileMetadataExternal(BaseModel):
    """
    A model containing all the metadata needed to pass it on to other microservices
    """

    grouping_label: str
    file_id: str
    md5_checksum: str
    size: int
    creation_date: datetime
    update_date: datetime
    format: str


class FileMetadataCreation(FileMetadataExternal):
    """
    A model containing all the metadata needed to register a new file to the database.
    """

    file_name: str
