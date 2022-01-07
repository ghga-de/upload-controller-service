# Copyright 2021 Universität Tübingen, DKFZ and EMBL
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

"""Defines all database specific ORM models"""

import uuid

from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.decl_api import DeclarativeMeta

Base: DeclarativeMeta = declarative_base()


class FileInfo(Base):
    """
    GHGA Files announced by an uploader.
    """

    __tablename__ = "files"
    id = Column(
        UUID(
            as_uuid=True,
        ),
        default=uuid.uuid4,
        primary_key=True,
        doc="Service-internal file ID.",
    )
    file_id = Column(
        String,
        nullable=False,
        unique=True,
        doc=(
            "ID used to refer to this file across services."
            + " May be presented to users."
            + " This string is also used to derive the DRS ID."
        ),
    )
    file_name = Column(
        String,
        nullable=False,
        default=None,
        doc=("Name of the uploaded file"),
    )
    md5_checksum = Column(
        String,
        nullable=False,
        default=None,
        doc=("MD5 checksum of the file content."),
    )
    size = Column(
        Integer,
        nullable=True,
        default=None,
        doc=("Size of the file content in bytes."),
    )
    study_id = Column(
        String,
        nullable=False,
        doc=("ID used to refer to the study this file belongs to"),
    )
    registration_date = Column(
        DateTime,
        nullable=True,
        doc=("Date/time when the file was registered."),
    )
