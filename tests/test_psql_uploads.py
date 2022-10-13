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

"""Tests UploadAttemptDAO implementations base on PostgreSQL"""

import pytest

from tests.fixtures.example_data import EXAMPLE_FILE, EXAMPLE_UPLOAD
from tests.fixtures.psql import PsqlFixture, psql_fixture  # noqa: F401
from ucs.core import models
from ucs.ports.outbound.upload_dao import (
    FileMetadataNotFoundError,
    UploadAttemptAlreadExistsError,
    UploadAttemptNotFoundError,
)
from ucs.translators.outbound.psql.adapters import PsqlUploadAttemptDAO


def test_get_existing(psql_fixture: PsqlFixture):  # noqa: F811
    """Test getting exiting upload attempt."""
    expected_upload = EXAMPLE_UPLOAD
    corresponding_file = EXAMPLE_FILE

    psql_fixture.populate_file_metadata([corresponding_file])
    psql_fixture.populate_upload_attempts([expected_upload])

    with PsqlUploadAttemptDAO(db_url=psql_fixture.config.db_url) as upload_attempt_dao:
        obtained_upload = upload_attempt_dao.get(expected_upload.upload_id)

    assert expected_upload == obtained_upload


def test_get_non_existing(psql_fixture: PsqlFixture):  # noqa: F811
    """Test getting not existing upload attempt and expect corresponding error."""
    expected_upload = EXAMPLE_UPLOAD

    with PsqlUploadAttemptDAO(db_url=psql_fixture.config.db_url) as upload_attempt_dao:
        with pytest.raises(UploadAttemptNotFoundError):
            _ = upload_attempt_dao.get(expected_upload.upload_id)


def test_create(psql_fixture: PsqlFixture):  # noqa: F811
    """Test creating not existing upload attempt."""
    expected_upload = EXAMPLE_UPLOAD
    corresponding_file = EXAMPLE_FILE

    psql_fixture.populate_file_metadata([corresponding_file])

    with PsqlUploadAttemptDAO(db_url=psql_fixture.config.db_url) as upload_attempt_dao:
        upload_attempt_dao.create(expected_upload)
        obtained_upload = upload_attempt_dao.get(expected_upload.upload_id)

    assert expected_upload == obtained_upload


def test_create_already_exist(psql_fixture: PsqlFixture):  # noqa: F811
    """Test creating already existing upload attempt."""
    existing_upload = EXAMPLE_UPLOAD
    corresponding_file = EXAMPLE_FILE

    psql_fixture.populate_file_metadata([corresponding_file])
    psql_fixture.populate_upload_attempts([existing_upload])

    with PsqlUploadAttemptDAO(db_url=psql_fixture.config.db_url) as upload_attempt_dao:
        with pytest.raises(UploadAttemptAlreadExistsError):
            upload_attempt_dao.create(existing_upload)


def test_update(psql_fixture: PsqlFixture):  # noqa: F811
    """Test updating exiting upload attempt."""
    original_upload = EXAMPLE_UPLOAD
    expected_upload = original_upload.copy(
        update={"status": models.UploadStatus.ACCEPTED}
    )
    corresponding_file = EXAMPLE_FILE

    psql_fixture.populate_file_metadata([corresponding_file])
    psql_fixture.populate_upload_attempts([original_upload])

    with PsqlUploadAttemptDAO(db_url=psql_fixture.config.db_url) as upload_attempt_dao:
        upload_attempt_dao.update(expected_upload)
        obtained_upload = upload_attempt_dao.get(original_upload.upload_id)

    assert expected_upload == obtained_upload


def test_update_not_exist(psql_fixture: PsqlFixture):  # noqa: F811
    """Test updating a non exiting upload attempt."""
    non_existing_upload = EXAMPLE_UPLOAD
    corresponding_file = EXAMPLE_FILE

    psql_fixture.populate_file_metadata([corresponding_file])

    with PsqlUploadAttemptDAO(db_url=psql_fixture.config.db_url) as upload_attempt_dao:
        with pytest.raises(UploadAttemptNotFoundError):
            upload_attempt_dao.update(non_existing_upload)


def test_get_all(psql_fixture: PsqlFixture):  # noqa: F811
    """Test getting all upload attempts for a file."""
    corresponding_file = EXAMPLE_FILE
    expected_uploads = [
        EXAMPLE_UPLOAD,
        EXAMPLE_UPLOAD.copy(update={"upload_id": "testUpload002"}),
    ]

    psql_fixture.populate_file_metadata([corresponding_file])
    psql_fixture.populate_upload_attempts(expected_uploads)

    with PsqlUploadAttemptDAO(db_url=psql_fixture.config.db_url) as upload_attempt_dao:
        obtained_uploads = upload_attempt_dao.get_all_by_file(
            corresponding_file.file_id
        )

    assert expected_uploads == obtained_uploads


def test_get_latest(psql_fixture: PsqlFixture):  # noqa: F811
    """Test getting latest upload attempts for a file."""
    corresponding_file = EXAMPLE_FILE
    uploads = [
        EXAMPLE_UPLOAD,
        EXAMPLE_UPLOAD.copy(update={"upload_id": "testUpload002"}),
    ]
    expected_upload = uploads[-1]

    psql_fixture.populate_file_metadata([corresponding_file])
    psql_fixture.populate_upload_attempts(uploads)

    with PsqlUploadAttemptDAO(db_url=psql_fixture.config.db_url) as upload_attempt_dao:
        obtained_upload = upload_attempt_dao.get_latest_by_file(
            corresponding_file.file_id
        )

    assert expected_upload == obtained_upload


def test_get_latest_none(psql_fixture: PsqlFixture):  # noqa: F811
    """Test getting latest upload attempts for a file where there is no upload attempt,
    yet."""
    corresponding_file = EXAMPLE_FILE

    psql_fixture.populate_file_metadata([corresponding_file])

    with PsqlUploadAttemptDAO(db_url=psql_fixture.config.db_url) as upload_attempt_dao:
        obtained_upload = upload_attempt_dao.get_latest_by_file(
            corresponding_file.file_id
        )

    assert obtained_upload is None


def test_file_missing(psql_fixture: PsqlFixture):  # noqa: F811
    """Test creating or getting all upload attempt for which the corresponding file
    is missing."""
    expected_upload = EXAMPLE_UPLOAD

    with PsqlUploadAttemptDAO(db_url=psql_fixture.config.db_url) as upload_attempt_dao:
        # Test all methods that should raise FileMetadataNotFoundError:

        with pytest.raises(FileMetadataNotFoundError):
            upload_attempt_dao.create(expected_upload)

        with pytest.raises(FileMetadataNotFoundError):
            upload_attempt_dao.get_all_by_file(expected_upload.file_id)

        with pytest.raises(FileMetadataNotFoundError):
            upload_attempt_dao.get_latest_by_file(expected_upload.file_id)
