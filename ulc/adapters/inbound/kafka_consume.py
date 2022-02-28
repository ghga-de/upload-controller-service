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
from ghga_service_chassis_lib.pubsub import AmqpTopic

from ulc.config import CONFIG, Config
from ulc.domain.interfaces.inbound.upload import IUploadHandler
from ulc.domain.models import FileInfoInternal

HERE = Path(__file__).parent.resolve()


class KafkaEventConsumer:
    """Adapter that consumes events received from an Apache Kafka broker."""

    # pylint: disable=super-init-not-called
    def __init__(self, *, upload_handler: IUploadHandler, config: Config = CONFIG):
        """Ininitalize class instance with config and inbound adapter objects."""
        self._config = config
        self._upload_handler = upload_handler

    def _process_file_registered_message(self, message: dict):
        """
        Processes the message by checking if the file really is in the outbox,
        otherwise throwing an error
        """

        file_id = message["file_id"]

        self._upload_handler.handle_file_registered(file_id)

    def _process_new_study_message(self, message: dict):
        """
        Processes the message by checking if the file really is in the outbox,
        otherwise throwing an error
        """

        files = message["associated_files"]
        grouping_label = message["study"]["id"]

        study_files = [
            FileInfoInternal(
                file_id=file["file_id"],
                grouping_label=grouping_label,
                md5_checksum=file["md5_checksum"],
                size=file["size"],
                file_name=file["file_name"],
                creation_date=file["creation_date"],
                update_date=file["update_date"],
                format=file["format"],
            )
            for file in files
        ]

        self._upload_handler.handle_new_study(study_files)

    def subscribe_new_study(self, run_forever: bool = True) -> None:
        """
        Runs a subscribing process for the "new_study_created" topic
        """

        # create a topic object:
        topic = AmqpTopic(
            config=self._config,
            topic_name=self._config.topic_name_new_study,
            json_schema=schemas.SCHEMAS["new_study_created"],
        )

        # subscribe:
        topic.subscribe(
            exec_on_message=self._process_new_study_message,
            run_forever=run_forever,
        )

    def subscribe_file_registered(self, run_forever: bool = True) -> None:
        """
        Runs a subscribing process for the "new_study_created" topic
        """

        # create a topic object:
        topic = AmqpTopic(
            config=self._config,
            topic_name=self._config.topic_name_new_study,
            json_schema=schemas.SCHEMAS["file_internally_registered"],
        )

        # subscribe:
        topic.subscribe(
            exec_on_message=self._process_file_registered_message,
            run_forever=run_forever,
        )
