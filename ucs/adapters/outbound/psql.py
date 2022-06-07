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
from typing import Any

from ghga_service_chassis_lib.postgresql import (
    PostgresqlConfigBase,
    SyncPostgresqlConnector,
)
from sqlalchemy import Column, DateTime, Enum, Integer, String, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.future import select
from sqlalchemy.orm import relationship
from sqlalchemy.orm.decl_api import DeclarativeMeta

from ucs.domain import models
from ucs.domain.interfaces.outbound.file_metadata import (
    FileMetadataAlreadyExistsError,
    FileMetadataNotFoundError,
    IFileMetadataDAO,
)
from ucs.domain.models import UploadStatus

Base: DeclarativeMeta = declarative_base()


class FileMetadata(Base):
    """
    ORM base class for containing general metadata on files that were registed for
    upload.
    """

    __tablename__ = "file_metadata"
    id = Column(
        UUID(
            as_uuid=True,
        ),
        default=uuid.uuid4,
        primary_key=True,
        doc="Service-internal file ID.",
    )
    external_id = Column(
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
    external_id = Column(
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
        ForeignKey("file_metadata.id"),
        nullable=False,
        unique=True,
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
    file_metadata = relationship("FileMetadata", back_populates="uploads")


class PsqlFileMetadataDAO(IFileMetadataDAO):
    """
    An implementation of the IFileMetadataDAO interface using a PostgreSQL backend.
    """

    # pylint: disable=super-init-not-called
    def __init__(self, *, db_url: str, db_print_logs: bool = False):
        """initialze DAO implementation"""

        self._config = PostgresqlConfigBase(db_url=db_url, db_print_logs=db_print_logs)
        self._postgresql_connector = SyncPostgresqlConnector(self._config)

        # will be defined on __enter__:
        self._session_cm: Any = None
        self._session: Any = None

    def __enter__(self):
        """Setup database connection"""

        self._session_cm = self._postgresql_connector.transactional_session()
        self._session = self._session_cm.__enter__()  # pylint: disable=no-member
        return self

    def __exit__(self, error_type, error_value, error_traceback):
        """Teardown database connection"""
        # pylint: disable=no-member
        self._session_cm.__exit__(error_type, error_value, error_traceback)

    def _get_orm_file(self, file_id: str) -> FileMetadata:
        """Internal method to get the ORM representation of a file by specifying
        its file ID"""

        statement = select(FileMetadata).filter_by(file_id=file_id)
        orm_file = self._session.execute(statement).scalars().one_or_none()

        if orm_file is None:
            raise FileMetadataNotFoundError(file_id=file_id)

        return orm_file

    def get(self, file_id: str) -> models.FileMetadataExternal:
        """Get Metadata on a single file from the database"""

        orm_file = self._get_orm_file(file_id=file_id)
        return models.FileMetadataExternal.from_orm(orm_file)

    def add(self, file: models.FileMetadataInternal) -> None:
        """Register a new file to the database."""

        # check for collisions in the database:
        try:
            self._get_orm_file(file_id=file.file_id)
        except FileMetadataNotFoundError:
            # this is expected
            pass
        else:
            # this is a problem
            raise FileMetadataAlreadyExistsError(file_id=file.file_id)

        file_dict = {
            **file.dict(),
        }
        orm_file = FileMetadata(**file_dict)
        self._session.add(orm_file)

    def update_file_state(self, file_id: str, state: UploadStatus) -> None:
        """Update the file state of a file in the database."""

        orm_file = self._get_orm_file(file_id=file_id)
        orm_file.state = state  # type: ignore

    def unregister(self, file_id: str) -> None:
        """
        Unregister a file with the specified file ID from the database.
        """

        orm_file = self._get_orm_file(file_id=file_id)
        self._session.delete(orm_file)
