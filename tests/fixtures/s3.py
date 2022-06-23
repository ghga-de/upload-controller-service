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

"""Fixtures for testing the storage DAO"""

from typing import Generator

import pytest
import requests
from ghga_service_chassis_lib.object_storage_dao import ObjectStorageDao
from ghga_service_chassis_lib.object_storage_dao_testing import (
    ObjectFixture as FileObject,
)
from ghga_service_chassis_lib.object_storage_dao_testing import populate_storage
from ghga_service_chassis_lib.s3 import ObjectStorageS3, S3ConfigBase
from ghga_service_chassis_lib.s3_testing import config_from_localstack_container
from testcontainers.localstack import LocalStackContainer


class S3Fixture:
    """Yielded by the `s3_fixture` function"""

    def __init__(self, config: S3ConfigBase, storage: ObjectStorageDao):
        """Initialize with config."""
        self.config = config
        self.storage = storage

    def populate_buckets(self, buckets: list[str]):
        """Populate the storage with buckets."""

        populate_storage(self.storage, bucket_fixtures=buckets, object_fixtures=[])

    def populate_file_objects(self, file_objects: list[FileObject]):
        """Populate the storage with file objects."""

        populate_storage(self.storage, bucket_fixtures=[], object_fixtures=file_objects)


@pytest.fixture
def s3_fixture() -> Generator[S3Fixture, None, None]:
    """Pytest fixture for tests depending on the ObjectStorageS3 DAO."""

    with LocalStackContainer(image="localstack/localstack:0.14.2").with_services(
        "s3"
    ) as localstack:
        config = config_from_localstack_container(localstack)

        with ObjectStorageS3(config=config) as storage:
            yield S3Fixture(config=config, storage=storage)


def upload_part_via_url(*, url: str, size: int):
    """Upload a file part of given size using the given URL."""

    content = b"\0" * size
    response = requests.put(url, data=content)
    response.raise_for_status()
