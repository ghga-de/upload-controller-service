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

from datetime import datetime

from ghga_service_chassis_lib.pubsub import AmqpTopic

from .. import models
from ..config import CONFIG, Config
from . import schemas


def publish_upload_received(file: models.FileInfoInternal, config: Config = CONFIG):
    """
    Publishes a message to a specified topic
    """

    message = {
        "request_id": "",
        "file_id": file.file_id,
        "grouping_label": file.grouping_label,
        "md5_checksum": file.md5_checksum,
        "timestamp": datetime.utcnow().isoformat(),
    }

    # create a topic object:
    topic = AmqpTopic(
        config=config,
        topic_name=config.topic_name_upload_received,
        json_schema=schemas.UPLOAD_RECEIVED,
    )

    topic.publish(message)
