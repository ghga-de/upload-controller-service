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

"""Kafka-based event publishing adapters and the exception they may throw."""

import json
from datetime import datetime

from ghga_event_schemas import pydantic_ as event_schemas
from hexkit.protocols.eventpub import EventPublisherProtocol
from pydantic import Field
from pydantic_settings import BaseSettings

from ucs.core import models
from ucs.ports.outbound.event_pub import EventPublisherPort


class EventPubTanslatorConfig(BaseSettings):
    """Config for publishing file upload-related events."""

    upload_received_event_topic: str = Field(
        ...,
        description="Name of the topic to publish events that inform about new file "
        + "uploads.",
        examples=["file_uploads"],
    )
    upload_received_event_type: str = Field(
        ...,
        description="The type to use for event that inform about new file uploads.",
        examples=["file_upload_received"],
    )


class EventPubTranslator(EventPublisherPort):
    """A translator (according to the triple hexagonal architecture) for publishing
    events using the EventPublisherProtocol.
    """

    def __init__(
        self, *, config: EventPubTanslatorConfig, provider: EventPublisherProtocol
    ):
        """Initialize with a suitable protocol provider."""
        self._config = config
        self._provider = provider

    async def publish_upload_received(  # noqa: PLR0913
        self,
        *,
        file_metadata: models.FileMetadata,
        upload_date: datetime,
        submitter_public_key: str,
        object_id: str,
        bucket_id: str,
    ) -> None:
        """Publish event informing that a new file upload was received."""
        event_payload = event_schemas.FileUploadReceived(
            s3_endpoint_alias="test",
            file_id=file_metadata.file_id,
            object_id=object_id,
            bucket_id=bucket_id,
            upload_date=upload_date.isoformat(),
            submitter_public_key=submitter_public_key,
            decrypted_size=file_metadata.decrypted_size,
            expected_decrypted_sha256=file_metadata.decrypted_sha256,
        )

        await self._provider.publish(
            payload=json.loads(event_payload.json()),
            type_=self._config.upload_received_event_type,
            topic=self._config.upload_received_event_topic,
            key=file_metadata.file_id,
        )
