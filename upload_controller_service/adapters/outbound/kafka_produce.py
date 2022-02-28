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
Publish asynchronous topics
"""

from ghga_message_schemas import schemas
from ghga_service_chassis_lib.pubsub import AmqpTopic

from upload_controller_service.config import CONFIG, Config

from upload_controller_service.domain import models
from upload_controller_service.domain.interfaces.outbound.event_pub import (
    IEventPublisher,
)


class KafkaEventPublisher(IEventPublisher):
    """A Kafka-based implementation of the IEventPublisher interface."""

    # pylint: disable=super-init-not-called
    def __init__(self, *, config: Config = CONFIG):
        """Ininitalize class instance with config object."""
        self._config = config

    def publish_upload_received(
        self,
        file: models.FileInfoExternal,
    ) -> None:
        """
        Publishes a message to a specified topic
        """

        message = {
            "file_id": file.file_id,
            "grouping_label": file.grouping_label,
            "md5_checksum": file.md5_checksum,
            "format": file.format,
            "creation_date": file.creation_date.isoformat(),
            "update_date": file.update_date.isoformat(),
            "size": file.size,
        }

        # create a topic object:
        topic = AmqpTopic(
            config=self._config,
            topic_name=self._config.topic_name_upload_received,
            json_schema=schemas.SCHEMAS["file_upload_received"],
        )

        topic.publish(message)
