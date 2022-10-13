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

from ucs.core.file_service import FileMetadataServive
from ucs.core.upload_service import UploadService
from ucs.ports.inbound.file_service import IFileMetadataService
from ucs.ports.inbound.upload_service import IUploadService
from ucs.ports.outbound.event_pub import IEventPublisher
from ucs.ports.outbound.file_dao import IFileMetadataDAO
from ucs.ports.outbound.storage import IObjectStorage
from ucs.ports.outbound.upload_dao import IUploadAttemptDAO
from ucs.translators.inbound.rabbitmq_consume import RabbitMQEventConsumer
from ucs.translators.outbound.psql.adapters import (
    PsqlFileMetadataDAO,
    PsqlUploadAttemptDAO,
)
from ucs.translators.outbound.rabbitmq_produce import RabbitMQEventPublisher
from ucs.translators.outbound.s3 import S3ObjectStorage


class Container(containers.DeclarativeContainer):
    """DI Container"""

    config = providers.Configuration()

    # outbound adapters:

    file_metadata_dao = providers.Factory[IFileMetadataDAO](
        PsqlFileMetadataDAO, db_url=config.db_url, db_print_logs=config.db_print_logs
    )

    upload_attempt_dao = providers.Factory[IUploadAttemptDAO](
        PsqlUploadAttemptDAO, db_url=config.db_url, db_print_logs=config.db_print_logs
    )

    object_storage = providers.Factory[IObjectStorage](
        S3ObjectStorage,
        s3_endpoint_url=config.s3_endpoint_url,
        s3_access_key_id=config.s3_access_key_id,
        s3_secret_access_key=config.s3_secret_access_key,
        s3_session_token=config.s3_session_token,
        aws_config_ini=config.aws_config_ini,
    )

    event_publisher = providers.Factory[IEventPublisher](
        RabbitMQEventPublisher,
        service_name=config.service_name,
        rabbitmq_host=config.rabbitmq_host,
        rabbitmq_port=config.rabbitmq_port,
        topic_upload_received=config.topic_upload_received,
    )

    # domain:

    file_metadata_service = providers.Factory[IFileMetadataService](
        FileMetadataServive,
        file_metadata_dao=file_metadata_dao,
        upload_attempt_dao=upload_attempt_dao,
    )

    upload_service = providers.Factory[IUploadService](
        UploadService,
        s3_inbox_bucket_id=config.s3_inbox_bucket_id,
        file_metadata_dao=file_metadata_dao,
        upload_attempt_dao=upload_attempt_dao,
        object_storage=object_storage,
        event_publisher=event_publisher,
    )

    # inbound adapters:

    event_subscriber = providers.Factory[RabbitMQEventConsumer](
        RabbitMQEventConsumer,
        service_name=config.service_name,
        rabbitmq_host=config.rabbitmq_host,
        rabbitmq_port=config.rabbitmq_port,
        topic_new_study=config.topic_new_study,
        topic_file_accepted=config.topic_file_accepted,
        file_metadata_service=file_metadata_service,
        upload_service=upload_service,
    )
