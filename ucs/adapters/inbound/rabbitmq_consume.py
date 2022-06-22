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

"""
Subscriptions to async topics
"""

from pathlib import Path

from ghga_message_schemas import schemas
from ghga_service_chassis_lib.pubsub import AmqpTopic, PubSubConfigBase

from ucs.domain import models
from ucs.domain.interfaces.inbound.file_service import IFileMetadataService

HERE = Path(__file__).parent.resolve()


class RabbitMQEventConsumer:
    """Adapter that consumes events received from an Apache RabbitMQ broker."""

    # pylint: disable=super-init-not-called
    def __init__(
        self,
        *,
        service_name: str,
        rabbitmq_host: str,
        rabbitmq_port: str,
        topic_new_study: str,
        topic_file_registered: str,
        file_metadata_service: IFileMetadataService,
    ):
        """Ininitalize class instance with config and inbound adapter objects."""
        self._file_metadata_service = file_metadata_service
        self._config = PubSubConfigBase(
            service_name=service_name,
            rabbitmq_host=rabbitmq_host,
            rabbitmq_port=rabbitmq_port,
        )
        self._topic_new_study = topic_new_study
        self._topic_file_registered = topic_file_registered

    def _process_new_study_message(self, message: dict):
        """
        Processes the message by checking if the file really is in the outbox,
        otherwise throwing an error
        """
        study_files = message["associated_files"]
        grouping_label = message["study"]["id"]

        files = [
            models.FileMetadata(
                file_id=file["file_id"],
                grouping_label=grouping_label,
                md5_checksum=file["md5_checksum"],
                size=file["size"],
                file_name=file["file_name"],
                creation_date=file["creation_date"],
                update_date=file["update_date"],
                format=file["format"],
            )
            for file in study_files
        ]

        self._file_metadata_service.upsert_multiple(files)

    def subscribe_new_study(self, run_forever: bool = True) -> None:
        """
        Subscribes to the "new_study_created" topic
        """

        # create a topic object:
        topic = AmqpTopic(
            config=self._config,
            topic_name=self._topic_new_study,
            json_schema=schemas.SCHEMAS["new_study_created"],
        )

        # subscribe:
        topic.subscribe(
            exec_on_message=self._process_new_study_message,
            run_forever=run_forever,
        )
