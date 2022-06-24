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

from pydantic import BaseModel, Field


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


class BaseModelORM(BaseModel):
    """Pydantic base model with orm mode enabled."""

    class Config:
        """Additional pydantic configs."""

        orm_mode = True


class UploadAttempt(BaseModel):
    """
    A model containing details on an upload attempt for a specific File.
    """

    upload_id: str
    file_id: str = Field(
        ..., description="The ID of the file corresponding to this upload."
    )
    status: UploadStatus
    part_size: int = Field(
        ..., description="Part size to be used for upload. Specified in bytes."
    )

    class Config:
        """Additional Model Config."""

        orm_mode = True
        title = "Multi-Part Upload Details"


class FileMetadata(BaseModel):
    """
    A model containing basic metadata on a file.
    """

    file_id: str
    file_name: str
    md5_checksum: str
    size: int
    grouping_label: str
    creation_date: datetime
    update_date: datetime
    format: str

    class Config:
        """Additional Model Config."""

        orm_mode = True
        title = "Basic File Metadata"


class FileMetadataWithUpload(FileMetadata):
    """
    A model containing basic metadata on a file plus information on the current
    upload.
    """

    latest_upload_id: Optional[str] = Field(
        None,
        description="ID of the current upload. `Null` if no update has been initiated, yet.",
    )

    class Config:
        """Additional Model Config."""

        title = "File Metadata"
