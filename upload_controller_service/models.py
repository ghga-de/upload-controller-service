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

from pydantic import BaseModel


class FileInfoExternal(BaseModel):
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

    class Config:
        """Additional pydantic configs."""

        orm_mode = True


class FileInfoInternal(FileInfoExternal):
    """
    A model containing all the metadata submitted for one file from the metadata service
    with the new_study_created topic.
    """

    file_name: str
