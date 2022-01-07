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

"""DAO interface implementation to connect to the database."""

from typing import Any, Optional

from ghga_service_chassis_lib.postgresql import (
    PostgresqlConfigBase,
    SyncPostgresqlConnector,
)
from ghga_service_chassis_lib.utils import DaoGenericBase
from sqlalchemy.future import select

from .. import models
from ..config import CONFIG
from . import db_models


class FileInfoNotFoundError(RuntimeError):
    """Thrown when trying to access a file with a file ID that doesn't
    exist in the database."""

    def __init__(self, file_id: Optional[str]):
        message = (
            "The file"
            + (f" with file ID '{file_id}' " if file_id else "")
            + " does not exist in the database."
        )
        super().__init__(message)


class FileInfoAlreadyExistsError(RuntimeError):
    """Thrown when trying to add a file with an file ID that already
    exist in the database."""

    def __init__(self, file_id: Optional[str]):
        message = (
            "The file"
            + (f" with file ID '{file_id}' " if file_id else "")
            + " already exist in the database."
        )
        super().__init__(message)


# Since this is just a DAO stub without implementation, following pylint error are
# expected:
# pylint: disable=unused-argument,no-self-use
class DatabaseDao(DaoGenericBase):
    """
    A DAO base class for interacting with the database.
    It might throw following exception to communicate selected error events:
        - FileNotFoundError
        - FileAlreadyExistsError
    """

    def get_file(self, file_id: str) -> models.FileInfoInternal:
        """Get file from the database"""
        ...

    def register_file(self, file: models.FileInfoInternal) -> None:
        """Register a new file to the database."""
        ...

    def unregister_file(self, file_id: str) -> None:
        """
        Unregister a new file with the specified file ID from the database.
        """
        ...


class PostgresDatabase(DatabaseDao):
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

    def _get_orm_file(self, file_id: str) -> db_models.FileInfo:
        """Internal method to get the ORM representation of a file by specifying
        its file ID"""

        statement = select(db_models.FileInfo).filter_by(file_id=file_id)
        orm_file = self._session.execute(statement).scalars().one_or_none()

        if orm_file is None:
            raise FileInfoNotFoundError(file_id=file_id)

        return orm_file

    def get_file(self, file_id: str) -> models.FileInfoInternal:
        """Get file from the database"""

        orm_file = self._get_orm_file(file_id=file_id)
        return models.FileInfoInternal.from_orm(orm_file)

    def register_file(self, file: models.FileInfoInternal) -> None:
        """Register a new file to the database."""

        # check for collisions in the database:
        try:
            self._get_orm_file(file_id=file.file_id)
        except FileNotFoundError:
            # this is expected
            pass
        else:
            # this is a problem
            raise FileInfoAlreadyExistsError(file_id=file.file_id)

        file_dict = {
            **file.dict(),
        }
        orm_file = db_models.FileInfo(**file_dict)
        self._session.add(orm_file)

    def unregister_file(self, file_id: str) -> None:
        """
        Unregister a file with the specified file ID from the database.
        """

        orm_file = self._get_orm_file(file_id=file_id)
        self._session.delete(orm_file)
