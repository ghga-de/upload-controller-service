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

"""Module hosting the dependency injection container."""

from hexkit.inject import ContainerBase, get_configurator, get_constructor
from hexkit.providers.akafka import KafkaEventPublisher, KafkaEventSubscriber
from hexkit.providers.mongodb import MongoDbDaoFactory

from ucs.adapters.inbound.akafka import EventSubTranslator
from ucs.adapters.outbound.akafka import EventPubTranslator
from ucs.adapters.outbound.dao import DaoCollectionTranslator
from ucs.adapters.outbound.s3 import S3ObjectStorage
from ucs.config import Config
from ucs.core.file_service import FileMetadataServive
from ucs.core.upload_service import UploadService


class Container(ContainerBase):
    """DI Container"""

    config = get_configurator(Config)

    # outbound providers:
    dao_factory = get_constructor(MongoDbDaoFactory, config=config)
    kafka_event_publisher = get_constructor(KafkaEventPublisher, config=config)

    # outbound translators:
    dao_collection = get_constructor(DaoCollectionTranslator, provider=dao_factory)
    event_pub_translator = get_constructor(
        EventPubTranslator, config=config, provider=kafka_event_publisher
    )

    # outbound adapters (not following the triple hexagonal translator/provider
    # pattern):
    object_storage = get_constructor(S3ObjectStorage, config=config)

    # domain:
    file_metadata_service = get_constructor(FileMetadataServive, daos=dao_collection)
    upload_service = get_constructor(
        UploadService,
        daos=dao_collection,
        object_storage=object_storage,
        event_publisher=event_pub_translator,
        config=config,
    )

    # inbound translators:
    event_sub_translator = get_constructor(
        EventSubTranslator,
        file_metadata_service=file_metadata_service,
        upload_service=upload_service,
        config=config,
    )

    # inbound providers:
    kafka_event_subscriber = get_constructor(
        KafkaEventSubscriber, config=config, translator=event_sub_translator
    )
