# Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

"""Join the functionality of all fixtures for API-level integration testing."""

__all__ = [
    "joint_fixture",
    "JointFixture",
    "mongodb_fixture",
    "kafka_fixture",
    "s3_fixture",
    "second_s3_fixture",
]

from collections.abc import AsyncGenerator
from dataclasses import dataclass

import httpx
import pytest_asyncio
from ghga_service_commons.api.testing import AsyncTestClient
from ghga_service_commons.utils.multinode_storage import (
    S3ObjectStorageNodeConfig,
    S3ObjectStoragesConfig,
)
from hexkit.providers.akafka import KafkaEventSubscriber
from hexkit.providers.akafka.testutils import KafkaFixture, get_kafka_fixture
from hexkit.providers.mongodb.testutils import MongoDbFixture, get_mongodb_fixture
from hexkit.providers.s3.testutils import S3Fixture, get_s3_fixture
from pytest_asyncio.plugin import _ScopeName

from tests.fixtures.config import get_config
from tests.fixtures.example_data import STORAGE_ALIASES
from ucs.adapters.outbound.dao import DaoCollectionTranslator
from ucs.config import Config
from ucs.inject import prepare_core, prepare_event_subscriber, prepare_rest_app
from ucs.ports.inbound.file_service import FileMetadataServicePort
from ucs.ports.inbound.upload_service import UploadServicePort
from ucs.ports.outbound.dao import DaoCollectionPort


@dataclass
class JointFixture:
    """Returned by the `joint_fixture`."""

    config: Config
    daos: DaoCollectionPort
    upload_service: UploadServicePort
    file_metadata_service: FileMetadataServicePort
    rest_client: httpx.AsyncClient
    event_subscriber: KafkaEventSubscriber
    mongodb: MongoDbFixture
    kafka: KafkaFixture
    s3: S3Fixture
    second_s3: S3Fixture

    async def reset_state(self):
        """Completely reset fixture states"""
        await self.s3.empty_buckets()
        await self.second_s3.empty_buckets()
        self.mongodb.empty_collections()
        self.kafka.clear_topics()


async def joint_fixture_function(
    mongodb_fixture: MongoDbFixture,
    kafka_fixture: KafkaFixture,
    s3_fixture: S3Fixture,
    second_s3_fixture: S3Fixture,
) -> AsyncGenerator[JointFixture, None]:
    """A fixture that embeds all other fixtures for API-level integration testing.

    **Do not call directly** Instead, use get_joint_fixture().
    """
    bucket_id = "test-inbox"

    node_config = S3ObjectStorageNodeConfig(
        bucket=bucket_id, credentials=s3_fixture.config
    )
    second_node_config = S3ObjectStorageNodeConfig(
        bucket=bucket_id, credentials=second_s3_fixture.config
    )
    object_storages_config = S3ObjectStoragesConfig(
        object_storages={
            STORAGE_ALIASES[0]: node_config,
            STORAGE_ALIASES[1]: second_node_config,
        }
    )

    # merge configs from different sources with the default one:
    config = get_config(
        sources=[mongodb_fixture.config, kafka_fixture.config, object_storages_config]
    )

    daos = await DaoCollectionTranslator.construct(provider=mongodb_fixture.dao_factory)
    await s3_fixture.populate_buckets([bucket_id])
    await second_s3_fixture.populate_buckets([bucket_id])

    # create a DI container instance:translators
    async with prepare_core(config=config) as (upload_service, file_metadata_service):
        async with (
            prepare_rest_app(
                config=config, core_override=(upload_service, file_metadata_service)
            ) as app,
            prepare_event_subscriber(
                config=config, core_override=(upload_service, file_metadata_service)
            ) as event_subscriber,
        ):
            async with AsyncTestClient(app=app) as rest_client:
                yield JointFixture(
                    config=config,
                    daos=daos,
                    upload_service=upload_service,
                    file_metadata_service=file_metadata_service,
                    rest_client=rest_client,
                    event_subscriber=event_subscriber,
                    mongodb=mongodb_fixture,
                    kafka=kafka_fixture,
                    s3=s3_fixture,
                    second_s3=second_s3_fixture,
                )


def get_joint_fixture(scope: _ScopeName = "function"):
    """Produce a joint fixture with desired scope"""
    return pytest_asyncio.fixture(joint_fixture_function, scope=scope)


joint_fixture = get_joint_fixture()
mongodb_fixture = get_mongodb_fixture()
kafka_fixture = get_kafka_fixture()
s3_fixture = get_s3_fixture()
second_s3_fixture = get_s3_fixture()
