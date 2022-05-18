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

"""Test the UploadService class"""

from typing import Optional, Type

import pytest

from ucs.domain.interfaces.inbound.upload import (
    FileAlreadyInInboxError,
    FileAlreadyRegisteredError,
    FileNotInInboxError,
    FileNotReadyForConfirmUpload,
    FileNotRegisteredError,
)

from ..fixtures import (  # noqa: F401
    amqp_fixture,
    get_cont_and_conf,
    psql_fixture,
    s3_fixture,
    state,
)


@pytest.mark.parametrize(
    "file_state_name,expected_exception",
    [("unknown", None), ("in_db_only", FileAlreadyRegisteredError)],
)
def test_handle_new_study(
    file_state_name: str,
    expected_exception: Optional[Type[BaseException]],
    psql_fixture,  # noqa: F811
    s3_fixture,  # noqa: F811
):
    """Test the `handle_new_study` method."""
    container, _ = get_cont_and_conf(sources=[psql_fixture.config, s3_fixture.config])
    upload_service = container.upload_service()
    file_state = state.FILES[file_state_name]

    run = lambda: upload_service.handle_new_study(
        study_files=[file_state.file_metadata]
    )
    if expected_exception is None:
        run()
        # check if file exists in db:
        psql_fixture.file_metadata_dao.get(file_state.file_metadata.file_id)
    else:
        with pytest.raises(expected_exception):
            run()


@pytest.mark.parametrize(
    "file_state_name,expected_exception",
    [
        ("in_db_only", FileNotInInboxError),
        ("unknown", FileNotInInboxError),
        ("in_inbox", None),
    ],
)
def test_handle_file_registered(
    file_state_name: str,
    expected_exception: Optional[Type[BaseException]],
    psql_fixture,  # noqa: F811
    s3_fixture,  # noqa: F811
):
    """Test the `handle_file_registered` method."""
    container, config = get_cont_and_conf(
        sources=[psql_fixture.config, s3_fixture.config]
    )
    upload_service = container.upload_service()
    file_state = state.FILES[file_state_name]

    run = lambda: upload_service.handle_file_registered(
        file_id=file_state.file_metadata.file_id
    )
    if expected_exception is None:
        run()
        # check if file exists in db:
        assert not s3_fixture.storage.does_object_exist(
            bucket_id=config.s3_inbox_bucket_id,
            object_id=file_state.file_metadata.file_id,
        )
    else:
        with pytest.raises(expected_exception):
            run()


@pytest.mark.parametrize(
    "file_state_name,expected_exception",
    [
        ("in_db_only", None),
        ("unknown", FileNotRegisteredError),
        ("in_inbox", FileAlreadyInInboxError),
    ],
)
def test_get_upload_url(
    file_state_name: str,
    expected_exception: Optional[Type[BaseException]],
    psql_fixture,  # noqa: F811
    s3_fixture,  # noqa: F811
):
    """Test the `get_upload_url` method."""
    container, _ = get_cont_and_conf(sources=[psql_fixture.config, s3_fixture.config])
    upload_service = container.upload_service()
    file_state = state.FILES[file_state_name]

    run = lambda: upload_service.get_upload_url(file_state.file_metadata.file_id)
    if expected_exception is None:
        run()
    else:
        with pytest.raises(expected_exception):
            run()


@pytest.mark.parametrize(
    "file_state_name,expected_exception",
    [
        ("in_inbox", None),
        ("unknown", FileNotRegisteredError),
        ("in_db_only", FileNotReadyForConfirmUpload),
        ("in_inbox_confirmed", FileNotReadyForConfirmUpload),
    ],
)
def test_confirm_file_upload(
    file_state_name: str,
    expected_exception: Optional[Type[BaseException]],
    psql_fixture,  # noqa: F811
    s3_fixture,  # noqa: F811
    amqp_fixture,  # noqa: F811
):
    """Test the `confirm_file_upload` method."""
    container, _ = get_cont_and_conf(
        sources=[psql_fixture.config, s3_fixture.config, amqp_fixture.config]
    )
    upload_service = container.upload_service()
    file_state = state.FILES[file_state_name]

    run = lambda: upload_service.confirm_file_upload(
        file_state.file_metadata.file_id,
    )
    if expected_exception is None:
        run()
    else:
        with pytest.raises(expected_exception):
            run()
