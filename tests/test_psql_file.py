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

"""Tests FileMetadataDAO implementations base on PostgreSQL"""

import pytest

from tests.fixtures.example_data import EXAMPLE_FILE
from tests.fixtures.psql import PsqlFixture, psql_fixture  # noqa: F401
from ucs.ports.outbound.file_dao import FileMetadataNotFoundError
from ucs.translators.outbound.psql.adapters import PsqlFileMetadataDAO


def test_get_existing_file_metadata(psql_fixture: PsqlFixture):  # noqa: F811
    """Test getting exiting file metadata."""
    expected_file = EXAMPLE_FILE

    psql_fixture.populate_file_metadata([expected_file])

    with PsqlFileMetadataDAO(db_url=psql_fixture.config.db_url) as file_metadata_dao:
        obtained_file = file_metadata_dao.get(expected_file.file_id)

    assert expected_file == obtained_file


def test_get_non_existing_file_metadata(psql_fixture: PsqlFixture):  # noqa: F811
    """Test getting not existing file metadata and expect corresponding error."""
    expected_file = EXAMPLE_FILE

    with PsqlFileMetadataDAO(db_url=psql_fixture.config.db_url) as file_metadata_dao:
        with pytest.raises(FileMetadataNotFoundError):
            _ = file_metadata_dao.get(expected_file.file_id)


def test_create_file_metadata(psql_fixture: PsqlFixture):  # noqa: F811
    """Test registering not existing file metadata."""
    expected_file = EXAMPLE_FILE

    with PsqlFileMetadataDAO(db_url=psql_fixture.config.db_url) as file_metadata_dao:
        file_metadata_dao.upsert(expected_file)
        obtained_file = file_metadata_dao.get(expected_file.file_id)

    assert expected_file == obtained_file


def test_update_file_metadata(psql_fixture: PsqlFixture):  # noqa: F811
    """Test updating exiting file metadata."""
    original_file = EXAMPLE_FILE
    expected_file = original_file.copy(update={"file_name": "another name"})

    psql_fixture.populate_file_metadata([original_file])

    with PsqlFileMetadataDAO(db_url=psql_fixture.config.db_url) as file_metadata_dao:
        file_metadata_dao.upsert(expected_file)
        obtained_file = file_metadata_dao.get(original_file.file_id)

    assert expected_file == obtained_file
