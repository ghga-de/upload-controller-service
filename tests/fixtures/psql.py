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

"""Fixtures for testing the PostgreSQL functionalities"""

from typing import Generator

import pytest
from ghga_service_chassis_lib.postgresql import PostgresqlConfigBase
from ghga_service_chassis_lib.postgresql_testing import config_from_psql_container
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.decl_api import DeclarativeMeta
from testcontainers.postgres import PostgresContainer

from ucs.adapters.outbound.psql import orm_models
from ucs.domain import models

from .config import get_cont_and_conf


class PsqlFixture:
    """Returned by the psql fixture."""

    def __init__(
        self,
        *,
        config: PostgresqlConfigBase,
        session_factory: sessionmaker,
    ):
        """Initialize with config."""

        self.session_factory = session_factory
        self.config = config

    def populate(self, orm_objects: list[object]):
        """Add a file entry to the database."""

        with self.session_factory() as session:
            for orm_object in orm_objects:
                session.add(orm_object)
            session.commit()

    def populate_file_metadata(self, files: list[models.FileMetadata]):
        """Add a file entry to the database."""

        orm_files = [orm_models.FileMetadata(**file.dict()) for file in files]
        self.populate(orm_files)


@pytest.fixture
def psql_fixture(
    base: DeclarativeMeta = orm_models.Base,
) -> Generator[PsqlFixture, None, None]:
    """Pytest fixture for tests of the Prostgres DAO implementation."""

    with PostgresContainer() as postgres:
        psq_config = config_from_psql_container(postgres)
        _, config = get_cont_and_conf(sources=[psq_config])

        # setup database and tables:
        engine = create_engine(config.db_url)
        base.metadata.create_all(engine)
        session_factory = sessionmaker(engine)

        yield PsqlFixture(config=config, session_factory=session_factory)
