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
    creation_date: datetime = Field(
        ..., description="Datetime when the upload attempt was created."
    )
    completion_date: Optional[datetime] = Field(
        None,
        description=(
            "Datetime when the upload attempt was declared as completed by the client."
            + " `None` if the upload is ongoing."
        ),
    )
    submitter_public_key: str = Field(
        ..., description="The public key used by the submittter to encrypt the file."
    )

    class Config:
        """Additional Model Config."""

        orm_mode = True
        title = "Multi-Part Upload Details"


class FileMetadataUpsert(BaseModel):
    """
    A model for creating new or updating existing file metadata entries.
    """

    file_id: str
    file_name: str
    decrypted_sha256: str
    decrypted_size: int

    class Config:
        """Additional Model Config."""

        orm_mode = True
        title = "File Metadata Creation"


class FileMetadata(FileMetadataUpsert):
    """
    A model containing the full metadata on a file.
    """

    latest_upload_id: Optional[str] = Field(
        None,
        description=(
            "ID of the latest upload (attempt). `Null/None`"
            + " if no update has been initiated, yet."
        ),
    )

    class Config:
        """Additional Model Config."""

        orm_mode = True
        title = "File Metadata"
