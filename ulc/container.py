# Copyright 2022 Universität Tübingen, DKFZ and EMBL
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

from ulc.adapters.outbound.psql import PsqlFileInfoDAO
from ulc.adapters.outbound.s3 import S3ObjectStorage
from ulc.adapters.outbound.rabbitmq_produce import RabbitMQEventPublisher

from ulc.adapters.inbound.rabbitmq_consume import RabbitMQEventConsumer

from ulc.domain.upload import UploadService


class Container(containers.DeclarativeContainer):
    """DI Container"""

    config = providers.Configuration()

    # outbound adapters:

    file_info_dao = providers.Factory(
        PsqlFileInfoDAO, db_url=config.db_url, db_print_logs=config.db_print_logs
    )

    object_storage_dao = providers.Factory(
        S3ObjectStorage,
        s3_endpoint_url=config.s3_endpoint_url,
        s3_access_key_id=config.s3_access_key_id,
        s3_secret_access_key=config.s3_secret_access_key,
        s3_session_token=config.s3_session_token,
        aws_config_ini=config.aws_config_ini,
    )

    event_publisher = providers.Factory(
        RabbitMQEventPublisher,
        service_name=config.service_name,
        rabbitmq_host=config.rabbitmq_host,
        rabbitmq_port=config.rabbitmq_port,
        topic_upload_received=config.topic_upload_received,
    )

    # domain:

    upload_service = providers.Factory(
        UploadService,
        s3_inbox_bucket_id=config.s3_inbox_bucket_id,
        file_info_dao=file_info_dao,
        object_storage_dao=object_storage_dao,
        event_publisher=event_publisher,
    )

    # inbound adapters:

    event_subscriber = providers.Factory(
        RabbitMQEventConsumer,
        service_name=config.service_name,
        rabbitmq_host=config.rabbitmq_host,
        rabbitmq_port=config.rabbitmq_port,
        topic_new_study=config.topic_new_study,
        topic_file_registered=config.topic_file_registered,
        upload_service=upload_service,
    )
