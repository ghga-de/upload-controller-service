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

from typing import Any

from sqlalchemy.future import select
from ghga_service_chassis_lib.postgresql import (
    PostgresqlConfigBase,
    SyncPostgresqlConnector,
)
from ucs.domain import models
from ucs.domain.interfaces.outbound.file_metadata import (
    FileMetadataNotFoundError,
    IFileMetadataDAO,
)
from ucs.adapters.outbound.psql import orm_models


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

    def _get_orm_file(self, file_id: str) -> orm_models.FileMetadata:
        """Internal method to get the ORM representation of a file by specifying
        its file ID"""

        statement = select(orm_models.FileMetadata).filter_by(file_id=file_id)
        orm_file = self._session.execute(statement).scalars().one_or_none()

        if orm_file is None:
            raise FileMetadataNotFoundError(file_id=file_id)

        return orm_file

    def _register(self, file: models.FileMetadata) -> None:
        """Register a new file."""

        file_dict = {
            **file.dict(),
        }
        orm_file = orm_models.FileMetadata(**file_dict)
        self._session.add(orm_file)

    def _update(
        self, file: models.FileMetadata, orm_file: orm_models.FileMetadata
    ) -> None:
        """Update an existing file."""

        for key, value in file.dict().items():
            if key == "file_id":
                continue
            setattr(orm_file, key, value)

        self._session.commit()

    def get(self, file_id: str) -> models.FileMetadata:
        """Get file from the database"""

        orm_file = self._get_orm_file(file_id=file_id)
        return models.FileMetadata.from_orm(orm_file)

    def upsert(self, file: models.FileMetadata) -> None:
        """Register or update a file."""

        # check for collisions in the database:
        try:
            orm_file = self._get_orm_file(file_id=file.file_id)
        except FileMetadataNotFoundError:
            # file does not exist yet, will be created:
            self._register(file)
        else:
            # file already exists, will be updated:
            self._update(file=file, orm_file=orm_file)
