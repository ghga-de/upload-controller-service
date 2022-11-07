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

"""Kafka-based event publishing adapters and the exception they may throw."""

from hexkit.protocols.eventpub import EventPublisherProtocol

from ucs.ports.outbound.event_pub import EventPublisherPort
from ucs.core import models


class EventPublisher(EventPublisherPort):
    """A translator (according to the triple hexagonal architecture) for publishing
    events using the EventPublisherProtocol."""

    def __init__(self, *, provider: EventPublisherProtocol):
        """Initialize with a suitable protocol provider."""

        self._provider = provider

    def publish_upload_received(
        self,
        *,
        file_metadata: models.FileMetadata,
    ) -> None:
        """Publish event informing that a new upload was received."""
        ...
