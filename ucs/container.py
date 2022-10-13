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

"""Module hosting the dependency injection container."""

from dependency_injector import containers, providers
from hexkit.inject import ContainerBase, get_configurator, get_constructor
from ucs.config import Config

from ucs.core.file_service import FileMetadataServive
from ucs.core.upload_service import UploadService
from ucs.translators.inbound.rabbitmq_consume import RabbitMQEventConsumer
from ucs.translators.outbound.psql.adapters import (
    PsqlFileMetadataDAO,
    PsqlUploadAttemptDAO,
)
from ucs.translators.outbound.rabbitmq_produce import RabbitMQEventPublisher
from ucs.translators.outbound.s3 import S3ObjectStorage


class Container(ContainerBase):
    """DI Container"""

    config = get_configurator(Config)

    # outbound adapters:

    file_metadata_dao = get_constructor(PsqlFileMetadataDAO, config=config)

    upload_attempt_dao = get_constructor(PsqlUploadAttemptDAO, config=config)

    object_storage = get_constructor(S3ObjectStorage, config=config)

    event_publisher = get_constructor(RabbitMQEventPublisher, config=config)

    # domain:

    file_metadata_service = get_constructor(
        FileMetadataServive,
        file_metadata_dao=file_metadata_dao,
        upload_attempt_dao=upload_attempt_dao,
    )

    upload_service = get_constructor(
        UploadService,
        file_metadata_dao=file_metadata_dao,
        upload_attempt_dao=upload_attempt_dao,
        object_storage=object_storage,
        event_publisher=event_publisher,
        config=config,
    )

    # inbound adapters:

    event_subscriber = get_constructor(
        RabbitMQEventConsumer,
        file_metadata_service=file_metadata_service,
        upload_service=upload_service,
        config=config,
    )
