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

"""Tests the database DAO implementation base on PostgreSQL"""

import pytest

from upload_controller_service.dao.db import (
    FileInfoAlreadyExistsError,
    FileInfoNotFoundError,
)

from ..fixtures import psql_fixture  # noqa: F401


def test_get_existing_file_info(psql_fixture):  # noqa: F811
    """Test getting exiting file info."""
    existing_file_info = psql_fixture.existing_file_infos[0]

    returned_file_info = psql_fixture.database.get_file(
        existing_file_info.file_id,
    )
    assert existing_file_info.md5_checksum == returned_file_info.md5_checksum


def test_get_non_existing_file_info(psql_fixture):  # noqa: F811
    """Test getting not existing file info and expect corresponding error."""
    non_existing_file_info = psql_fixture.non_existing_file_infos[0]

    with pytest.raises(FileInfoNotFoundError):
        psql_fixture.database.get_file(non_existing_file_info.file_id)


def test_register_non_existing_file_info(psql_fixture):  # noqa: F811
    """Test registering not existing file info."""
    non_existing_file_info = psql_fixture.non_existing_file_infos[0]

    psql_fixture.database.register_file(non_existing_file_info)
    returned_file_info = psql_fixture.database.get_file(non_existing_file_info.file_id)

    assert non_existing_file_info.md5_checksum == returned_file_info.md5_checksum


def test_register_existing_file_info(psql_fixture):  # noqa: F811
    """Test registering an already existing file info and expect corresponding
    error."""
    existing_file_info = psql_fixture.existing_file_infos[0]

    with pytest.raises(FileInfoAlreadyExistsError):
        psql_fixture.database.register_file(existing_file_info)


def test_unregister_existing_file_info(psql_fixture):  # noqa: F811
    """Test unregistering an existing file info."""
    existing_file_info = psql_fixture.existing_file_infos[0]

    psql_fixture.database.unregister_file(existing_file_info.file_id)

    # check if file info can no longer be found:
    with pytest.raises(FileInfoNotFoundError):
        psql_fixture.database.get_file(existing_file_info.file_id)


def test_unregister_non_existing_file_info(psql_fixture):  # noqa: F811
    """Test unregistering not existing file info and expect corresponding error."""
    non_existing_file_info = psql_fixture.non_existing_file_infos[0]

    with pytest.raises(FileInfoNotFoundError):
        psql_fixture.database.unregister_file(non_existing_file_info.file_id)
