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

"""Test the core functionality"""

from typing import Optional, Type

import pytest

from upload_controller_service.core.main import (
    FileAlreadyRegisteredError,
    handle_new_study,
)

from ..fixtures import get_config, psql_fixture, s3_fixture, state  # noqa: F401


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
    """Test the `handle_new_study` function."""
    config = get_config(sources=[psql_fixture.config, s3_fixture.config])
    file_state = state.FILES[file_state_name]

    run = lambda: handle_new_study(study_files=[file_state.file_info], config=config)
    if expected_exception is None:
        run()
        # check if file exists in db:
        psql_fixture.database.get_file(file_state.file_info.file_id)
    else:
        with pytest.raises(expected_exception):
            run()
