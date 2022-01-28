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
from ghga_message_schemas import schemas

from ..fixtures import (  # noqa: F401
    ApiTestClient,
    amqp_fixture,
    get_config,
    psql_fixture,
    s3_fixture,
    state,
)
from ..fixtures.utils import is_success_http_code


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
    file_id = state.FILES[file_state_name].file_info.file_id

    client = ApiTestClient(config=config)
    response = client.get(f"/presigned_post/{file_id}")

    assert response.status_code == expected_status_code

    if is_success_http_code(expected_status_code):
        response_body = response.json()
        assert "presigned_post" in response_body
        assert (
            "url" in response_body["presigned_post"]
            and "fields" in response_body["presigned_post"]
        )


@pytest.mark.parametrize(
    "file_state_name,expected_status_code,json_body",
    [
        (
            "in_inbox",
            status.HTTP_204_NO_CONTENT,
            {
                "state": "uploaded",
            },
        ),
        (
            "in_inbox_confirmed",
            status.HTTP_400_BAD_REQUEST,
            {
                "state": "uploaded",
            },
        ),
        (
            "unknown",
            status.HTTP_404_NOT_FOUND,
            {
                "state": "uploaded",
            },
        ),
        (
            "in_db_only",
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            {
                "state": "uploaded",
            },
        ),
        (
            "in_inbox",
            status.HTTP_400_BAD_REQUEST,
            {
                "state": "completed",
            },
        ),
        (
            "in_inbox",
            status.HTTP_400_BAD_REQUEST,
            {"state": "uploaded", "file_id": "test123"},
        ),
    ],
)
def test_confirm_upload(
    file_state_name: str,
    expected_status_code: int,
    json_body: dict,
    s3_fixture,  # noqa: F811
    psql_fixture,  # noqa: F811
    amqp_fixture,  # noqa: F811
):
    """Test the GET /confirm_upload/{file_id} endpoint"""
    config = get_config(
        sources=[psql_fixture.config, s3_fixture.config, amqp_fixture.config]
    )
    file_id = state.FILES[file_state_name].file_info.file_id

    # initialize downstream test service that will receive the message from this service:
    downstream_subscriber = amqp_fixture.get_test_subscriber(
        topic_name=config.topic_name_upload_received,
        message_schema=schemas.SCHEMAS["file_upload_received"],
    )

    # make request:
    client = ApiTestClient(config=config)
    response = client.patch(url=f"/confirm_upload/{file_id}", json=json_body)

    assert response.status_code == expected_status_code

    if is_success_http_code(expected_status_code):
        # receive the published message:
        downstream_message = downstream_subscriber.subscribe(timeout_after=2)
        assert downstream_message["file_id"] == file_id
