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

"""DAO implementation to manage File Info in a database."""

import uuid

from sqlalchemy import Column, DateTime, Enum, Integer, String, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.decl_api import DeclarativeMeta
from sqlalchemy.orm import relationship

from ucs.domain.models import UploadStatus

Base: DeclarativeMeta = declarative_base()


class FileMetadata(Base):
    """
    GHGA Files announced by an uploader.
    """

    __tablename__ = "file_metadata"
    id = Column(
        UUID(
            as_uuid=True,
        ),
        default=uuid.uuid4,
        primary_key=True,
        doc="database-internal file ID.",
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
        nullable=False,
        default=None,
        doc=("Size of the file content in bytes."),
    )
    grouping_label = Column(
        String,
        nullable=False,
        doc=("ID used to refer to the study this file belongs to"),
    )
    creation_date = Column(
        DateTime,
        nullable=False,
        unique=False,
        doc="Timestamp (in ISO 8601 format) when the entity was created.",
    )
    update_date = Column(
        DateTime,
        nullable=False,
        unique=False,
        doc="Timestamp (in ISO 8601 format) when the entity was updated.",
    )
    format = Column(
        String,
        nullable=False,
        unique=False,
        doc="The format of the file: BAM, SAM, CRAM, BAI, etc.",
    )

    upload_attempts = relationship("UploadAttempt", back_populates="file_metadata")


class UploadAttempt(Base):
    """
    ORM base class containing information on multi-part upload attemps.
    Each upload attempt is linked to a specific `FileMetadata` entry.
    """

    __tablename__ = "upload_attempts"

    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        doc=(
            "Auto-incrementing ID used for sorting records by insertion order."
            + " This is not share with the user."
        ),
    )
    upload_id = Column(
        String,
        nullable=False,
        unique=True,
        doc=(
            "ID for the upload assign by the object storage implementation."
            + " This ID shared with the user."
        ),
    )
    file_id = Column(
        String,
        ForeignKey("file_metadata.file_id"),
        nullable=False,
        unique=False,
        doc="ID of the file metadata record coresponding to this upload attempt.",
    )
    status = Column(
        Enum(UploadStatus),
        default=UploadStatus.PENDING,
        nullable=False,
        unique=False,
        doc=(
            "The status of the upload state."
            + " Please note more that one upload per file_id may have a state that is"
            + " set to `pending` , `uploaded`, or `accepted`. Moreover, within the list"
            + " of states from uploads corresponding to one file, these `pending` ,"
            + " `uploaded`, and `accepted` are mutually exclusive."
        ),
    )
    file_metadata = relationship("FileMetadata", back_populates="upload_attempts")
