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

"""Test the api module"""

import pytest
from fastapi import status

from ..fixtures import (  # noqa: F401
    ApiTestClient,
    get_config,
    psql_fixture,
    s3_fixture,
    state,
)


def test_get_health():
    """Test the GET /health endpoint"""

    client = ApiTestClient()
    response = client.get("/health")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {"status": "OK"}


@pytest.mark.parametrize(
    "file_state_name,expected_status_code",
    [
        ("in_db_only", status.HTTP_200_OK),
        ("unknown", status.HTTP_404_NOT_FOUND),
    ],
)
def test_get_presigned_post(
    file_state_name: str,
    expected_status_code: int,
    s3_fixture,  # noqa: F811
    psql_fixture,  # noqa: F811
):
    """Test the GET /presigned_post/{file_id} endpoint"""
    config = get_config(sources=[psql_fixture.config, s3_fixture.config])
    file_state = state.FILES[file_state_name]

    client = ApiTestClient(config=config)
    response = client.get(f"/presigned_post/{file_state.file_info.file_id}")

    assert response.status_code == expected_status_code

    if expected_status_code == status.HTTP_200_OK:
        response_body = response.json()
        assert "presigned_post" in response_body
        assert (
            "url" in response_body["presigned_post"]
            and "fields" in response_body["presigned_post"]
        )
