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

from dataclasses import dataclass
from typing import Generator, List

import pytest
from ghga_service_chassis_lib.postgresql import PostgresqlConfigBase
from ghga_service_chassis_lib.postgresql_testing import config_from_psql_container
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from testcontainers.postgres import PostgresContainer

from ulc.adapters.outbound.psql import Base, FileInfo, PsqlFileInfoDAO
from ulc.domain import models

from . import get_cont_and_conf, state

existing_file_infos: List[models.FileInfoInternal] = []
non_existing_file_infos: List[models.FileInfoInternal] = []

for file in state.FILES.values():
    if file.in_db:
        existing_file_infos.append(file.file_info)
    else:
        non_existing_file_infos.append(file.file_info)


def populate_db(db_url: str, file_infos: List[models.FileInfoInternal]):
    """Create and populates the DB"""

    # setup database and tables:
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)

    # populate with test data:
    session_factor = sessionmaker(engine)
    with session_factor() as session:
        for existing_file_info in file_infos:
            param_dict = {
                **existing_file_info.dict(),
            }
            orm_entry = FileInfo(**param_dict)
            session.add(orm_entry)
        session.commit()


@dataclass
class PsqlState:
    """Info yielded by the `psql_fixture` function"""

    config: PostgresqlConfigBase
    file_info_dao: PsqlFileInfoDAO
    existing_file_infos: List[models.FileInfoInternal]
    non_existing_file_infos: List[models.FileInfoInternal]


@pytest.fixture
def psql_fixture() -> Generator[PsqlState, None, None]:
    """Pytest fixture for tests of the Prostgres DAO implementation."""

    with PostgresContainer() as postgres:
        psq_config = config_from_psql_container(postgres)
        container, config = get_cont_and_conf(sources=[psq_config])
        file_info_dao = container.file_info_dao()

        populate_db(config.db_url, file_infos=existing_file_infos)

        with file_info_dao as fi_dao:
            yield PsqlState(
                config=config,
                file_info_dao=fi_dao,
                existing_file_infos=existing_file_infos,
                non_existing_file_infos=non_existing_file_infos,
            )
