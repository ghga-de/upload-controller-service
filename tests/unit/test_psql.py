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

"""Tests the File metadata DAO implementation base on PostgreSQL"""

from datetime import datetime

import pytest

from ucs.adapters.outbound.psql.adapters import (
    PsqlFileMetadataDAO,
    PsqlUploadAttemptDAO,
)
from ucs.domain import models
from ucs.domain.interfaces.outbound.file_metadata import FileMetadataNotFoundError
from ucs.domain.interfaces.outbound.upload_attempts import UploadAttemptNotFoundError

from ..fixtures.psql import PsqlFixture, psql_fixture  # noqa: F401

EXAMPLE_FILE = models.FileMetadata(
    file_id="testFile001",
    file_name="Test File 001",
    md5_checksum="fake-checksum",
    size=12345678,
    grouping_label="test",
    creation_date=datetime.now(),
    update_date=datetime.now(),
    format="txt",
)

EXAMPLE_UPLOAD = models.UploadAttempt(
    upload_id="testUpload001", file_id="testFile001", status=models.UploadStatus.PENDING
)


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


def test_get_existing_upload_attempt(psql_fixture: PsqlFixture):  # noqa: F811
    """Test getting exiting upload attempt."""
    expected_upload = EXAMPLE_UPLOAD
    corresponding_file = EXAMPLE_FILE

    psql_fixture.populate_file_metadata([corresponding_file])
    psql_fixture.populate_upload_attempts([expected_upload])

    with PsqlUploadAttemptDAO(db_url=psql_fixture.config.db_url) as upload_attempt_dao:
        obtained_upload = upload_attempt_dao.get(expected_upload.upload_id)

    assert expected_upload == obtained_upload


def test_get_non_existing_upload_attempt(psql_fixture: PsqlFixture):  # noqa: F811
    """Test getting not existing upload attempt and expect corresponding error."""
    expected_upload = EXAMPLE_UPLOAD

    with PsqlUploadAttemptDAO(db_url=psql_fixture.config.db_url) as upload_attempt_dao:
        with pytest.raises(UploadAttemptNotFoundError):
            _ = upload_attempt_dao.get(expected_upload.upload_id)


def test_create_upload_attempt(psql_fixture: PsqlFixture):  # noqa: F811
    """Test creating not existing upload attempt."""
    expected_upload = EXAMPLE_UPLOAD
    corresponding_file = EXAMPLE_FILE

    psql_fixture.populate_file_metadata([corresponding_file])

    with PsqlUploadAttemptDAO(db_url=psql_fixture.config.db_url) as upload_attempt_dao:
        upload_attempt_dao.upsert(expected_upload)
        obtained_upload = upload_attempt_dao.get(expected_upload.upload_id)

    assert expected_upload == obtained_upload


def test_update_upload_attempt(psql_fixture: PsqlFixture):  # noqa: F811
    """Test updating exiting upload attempt."""
    original_upload = EXAMPLE_UPLOAD
    expected_upload = original_upload.copy(
        update={"status": models.UploadStatus.ACCEPTED}
    )
    corresponding_file = EXAMPLE_FILE

    psql_fixture.populate_file_metadata([corresponding_file])
    psql_fixture.populate_upload_attempts([original_upload])

    with PsqlUploadAttemptDAO(db_url=psql_fixture.config.db_url) as upload_attempt_dao:
        upload_attempt_dao.upsert(expected_upload)
        obtained_upload = upload_attempt_dao.get(original_upload.upload_id)

    assert expected_upload == obtained_upload


def test_get_all_upload_attempts(psql_fixture: PsqlFixture):  # noqa: F811
    """Test getting all upload attempts for a file."""
    corresponding_file = EXAMPLE_FILE
    expected_uploads = [
        EXAMPLE_UPLOAD,
        EXAMPLE_UPLOAD.copy(update={"upload_id": "testUpload002"}),
    ]

    psql_fixture.populate_file_metadata([corresponding_file])
    psql_fixture.populate_upload_attempts(list(expected_uploads))

    with PsqlUploadAttemptDAO(db_url=psql_fixture.config.db_url) as upload_attempt_dao:
        obtained_uploads = upload_attempt_dao.get_all_by_file(
            corresponding_file.file_id
        )

    assert expected_uploads == obtained_uploads


def test_upload_attempt_file_missing(psql_fixture: PsqlFixture):  # noqa: F811
    """Test creating or getting all upload attempt for which the corresponding file
    is missing."""
    expected_upload = EXAMPLE_UPLOAD

    with PsqlUploadAttemptDAO(db_url=psql_fixture.config.db_url) as upload_attempt_dao:
        with pytest.raises(FileMetadataNotFoundError):
            upload_attempt_dao.upsert(expected_upload)
