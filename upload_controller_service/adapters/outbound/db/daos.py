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

"""DAO interface implementation to connect to the database."""

from typing import Any

from ghga_service_chassis_lib.postgresql import (
    PostgresqlConfigBase,
    SyncPostgresqlConnector,
)
from sqlalchemy.future import select

from upload_controller_service.domain import models
from upload_controller_service.domain.outbound_interfaces.file_info import (
    IFileInfoDAO,
    FileInfoNotFoundError,
    FileInfoAlreadyExistsError,
)
from upload_controller_service.config import CONFIG
from upload_controller_service.adapters.outbound.db import orm_models


class PostgresDatabase(IFileInfoDAO):
    """
    An implementation of the DatabaseDao interface using a PostgreSQL backend.
    """

    def __init__(self, config: PostgresqlConfigBase = CONFIG):
        """initialze DAO implementation"""

        super().__init__(config)
        self._postgresql_connector = SyncPostgresqlConnector(config)

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

    def _get_orm_file(self, file_id: str) -> orm_models.FileInfo:
        """Internal method to get the ORM representation of a file by specifying
        its file ID"""

        statement = select(orm_models.FileInfo).filter_by(file_id=file_id)
        orm_file = self._session.execute(statement).scalars().one_or_none()

        if orm_file is None:
            raise FileInfoNotFoundError(file_id=file_id)

        return orm_file

    def get_file(self, file_id: str) -> models.FileInfoExternal:
        """Get file from the database"""

        orm_file = self._get_orm_file(file_id=file_id)
        return models.FileInfoExternal.from_orm(orm_file)

    def register_file(self, file: models.FileInfoInternal) -> None:
        """Register a new file to the database."""

        # check for collisions in the database:
        try:
            self._get_orm_file(file_id=file.file_id)
        except FileInfoNotFoundError:
            # this is expected
            pass
        else:
            # this is a problem
            raise FileInfoAlreadyExistsError(file_id=file.file_id)

        file_dict = {
            **file.dict(),
        }
        orm_file = orm_models.FileInfo(**file_dict)
        self._session.add(orm_file)

    def update_file_state(self, file_id: str, state: orm_models.UploadState) -> None:
        """Update the file state of a file in the database."""

        orm_file = self._get_orm_file(file_id=file_id)
        orm_file.state = state  # type: ignore

    def unregister_file(self, file_id: str) -> None:
        """
        Unregister a file with the specified file ID from the database.
        """

        orm_file = self._get_orm_file(file_id=file_id)
        self._session.delete(orm_file)
