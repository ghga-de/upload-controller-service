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

"""Defines all database specific ORM models"""

import uuid

from sqlalchemy import Column, DateTime, Enum, Integer, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.decl_api import DeclarativeMeta

from ..models import UploadState

Base: DeclarativeMeta = declarative_base()


# class UploadState(Enum):

#     """
#     The current upload state. Can be registered (no information),
#     pending (the user has requested an upload url),
#     uploaded (the user has confirmed the upload),
#     or registered (the file has been registered with the internal-file-registry).
#     """

#     REGISTERED = ("registered",)
#     PENDING = ("pending",)
#     UPLOADED = ("uploaded",)
#     COMPLETED = ("completed",)


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
    state = Column(
        Enum(UploadState),
        default=UploadState.REGISTERED,
        nullable=False,
        unique=False,
        doc=(
            "The current upload state. Can be pending (no information), "
            + "confirmed (the user has confirmed the upload) "
            + "or registered (the file has been registered with the upload-controller)."
        ),
    )
