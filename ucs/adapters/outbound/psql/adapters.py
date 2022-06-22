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

from typing import Any, Optional

from ghga_service_chassis_lib.postgresql import (
    PostgresqlConfigBase,
    SyncPostgresqlConnector,
)
from sqlalchemy import desc
from sqlalchemy.future import select
from sqlalchemy.orm.decl_api import DeclarativeMeta

from ucs.adapters.outbound.psql import orm_models
from ucs.domain import models
from ucs.domain.interfaces.outbound.file_metadata import (
    FileMetadataNotFoundError,
    IFileMetadataDAO,
)
from ucs.domain.interfaces.outbound.upload_attempts import (
    IUploadAttemptDAO,
    UploadAttemptNotFoundError,
)


class PsqlDaoBase:
    """
    A base for DAOs with PostgreSQL backend.
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

    def _get_orm_file(self, file_id: str) -> orm_models.FileMetadata:
        """Internal method to get the ORM representation of a file by specifying
        its file ID"""

        statement = select(orm_models.FileMetadata).filter_by(file_id=file_id)
        orm_file = self._session.execute(statement).scalars().one_or_none()

        if orm_file is None:
            raise FileMetadataNotFoundError(file_id=file_id)

        return orm_file

    def _create(
        self, *, obj: models.BaseModelORM, orm_model: type[DeclarativeMeta]
    ) -> None:
        """Register a new file."""

        orm_obj = orm_model(**obj.dict())
        self._session.add(orm_obj)

    def _update(self, *, obj: models.BaseModelORM, orm_obj: DeclarativeMeta) -> None:
        """Update an existing file."""

        for key, value in obj.dict().items():
            if "_id" in key:
                # omit the id
                continue
            setattr(orm_obj, key, value)

        self._session.commit()


class PsqlFileMetadataDAO(PsqlDaoBase, IFileMetadataDAO):
    """
    An implementation of the IFileMetadataDAO interface using a PostgreSQL backend.

    Raises:
        - FileMetadataNotFoundError
    """

    def get(self, file_id: str) -> models.FileMetadata:
        """Get file from the database"""

        orm_file = self._get_orm_file(file_id)
        return models.FileMetadata.from_orm(orm_file)

    def upsert(self, file: models.FileMetadata) -> None:
        """Register or update a file."""

        try:
            orm_file = self._get_orm_file(file.file_id)
        except FileMetadataNotFoundError:
            # file does not exist yet, will be created:
            self._create(obj=file, orm_model=orm_models.FileMetadata)
        else:
            # file already exists, will be updated:
            self._update(obj=file, orm_obj=orm_file)


class PsqlUploadAttemptDAO(PsqlDaoBase, IUploadAttemptDAO):
    """
    An implementation of the IUploadAttemptDAO interface using a PostgreSQL backend.

    Raises:
        - FileMetadataNotFoundError
        - UploadAttemptNotFoundError
    """

    def _get_orm_upload(self, upload_id: str) -> orm_models.FileMetadata:
        """Internal method to get the ORM representation of an upload attempt by
        specifying its upload ID"""

        statement = select(orm_models.UploadAttempt).filter_by(upload_id=upload_id)
        orm_upload = self._session.execute(statement).scalars().one_or_none()

        if orm_upload is None:
            raise UploadAttemptNotFoundError(upload_id=upload_id)

        return orm_upload

    def get(self, upload_id: str) -> models.UploadAttempt:
        """Get upload attempt from the database"""

        orm_upload = self._get_orm_upload(upload_id)
        return models.UploadAttempt.from_orm(orm_upload)

    def get_all_by_file(self, file_id: str) -> list[models.UploadAttempt]:
        """Get all upload attempts for a specific file from the database"""

        orm_file = self._get_orm_file(file_id)
        return [
            models.UploadAttempt.from_orm(orm_upload)
            for orm_upload in orm_file.upload_attempts
        ]

    def get_latest_by_file(self, file_id: str) -> Optional[models.UploadAttempt]:
        """Get the latest upload attempts for a specific file from the database"""

        # check if corresponding file exists, will raise a FileMetadataNotFoundError
        # otherwise:
        _ = self._get_orm_file(file_id)

        statement = (
            select(orm_models.UploadAttempt)
            .filter_by(file_id=file_id)
            .order_by(desc(orm_models.UploadAttempt.id))
        )
        orm_upload = self._session.execute(statement).scalars().first()

        if orm_upload is None:
            return None

        return models.UploadAttempt.from_orm(orm_upload)

    def upsert(self, upload: models.UploadAttempt) -> None:
        """Create or update an upload attempt."""

        # check if corresponding file exists, will raise a FileMetadataNotFoundError
        # otherwise:
        _ = self._get_orm_file(upload.file_id)

        try:
            orm_upload = self._get_orm_upload(upload.upload_id)
        except UploadAttemptNotFoundError:
            # upload does not exist yet, will be created:
            self._create(obj=upload, orm_model=orm_models.UploadAttempt)
        else:
            # upload already exists, will be updated:
            self._update(obj=upload, orm_obj=orm_upload)
