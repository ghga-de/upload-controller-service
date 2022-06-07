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

"""REST API-specific data models (not used by core package)"""

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field


class FileMetadata(BaseModel):
    """
    A model containing all the metadata needed to pass it on to other microservices
    """

    file_id: str
    file_name: str
    md5_checksum: str
    size: int
    grouping_label: str
    creation_date: datetime
    update_date: datetime
    format: str
    current_upload_id: Optional[str] = Field(
        None,
        description="ID of the current upload. `Null` if no update has been initiated, yet.",
    )


class UploadCreation(BaseModel):
    """Properties required to create a new upload."""

    file_id: int = Field(
        ..., description="The ID of the file corresponding to this upload."
    )

    class Config:
        """Additional Model Config."""

        title = "Properties required to create a new upload"


class UploadDetails(UploadCreation):
    """Details returned upon creation of a new multipart upload."""

    upload_id: str
    part_size: int = Field(
        ..., description="Part size to be used for upload. Specified in bytes."
    )

    class Config:
        """Additional Model Config."""

        title = "Multi-Part Upload Details"


class UploadUpdate(BaseModel):
    """Request body to update an existing mutli-part upload."""

    status: Literal["uploaded", "cancelled"]

    class Config:
        """Additional Model Config."""

        title = "Multi-Part Upload Update"
