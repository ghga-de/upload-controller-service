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
from ghga_service_chassis_lib.pubsub import AmqpTopic, PubSubConfigBase

from ucs.core import models
from ucs.ports.outbound.event_pub import EventPublisherPort


class RMQPublisherConfig(PubSubConfigBase):
    """Config parameters and their defaults."""

    topic_upload_received: str = "file_upload_received"


class RabbitMQEventPublisher(EventPublisherPort):
    """A RabbitMQ-based implementation of the EventPublisher interface."""

    # pylint: disable=super-init-not-called
    def __init__(self, *, config: RMQPublisherConfig):
        """Ininitalize class instance with config object."""

        self._config = config
        self._topic_upload_received = config.topic_upload_received

    def publish_upload_received(
        self,
        *,
        file_metadata: models.FileMetadata,
    ) -> None:
        """
        Publishes a message to a specified topic
        """

        message = {
            "file_id": file_metadata.file_id,
            "grouping_label": file_metadata.grouping_label,
            "md5_checksum": file_metadata.md5_checksum,
            "format": file_metadata.format,
            "creation_date": file_metadata.creation_date.isoformat(),
            "update_date": file_metadata.update_date.isoformat(),
            "size": file_metadata.size,
        }

        # create a topic object:
        topic = AmqpTopic(
            config=self._config,
            topic_name=self._topic_upload_received,
            json_schema=schemas.SCHEMAS["file_upload_received"],
        )

        topic.publish(message)
