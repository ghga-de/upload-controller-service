# Copyright 2022 Universität Tübingen, DKFZ and EMBL
# for the German Human Genome-Phenome Archive (GHGA)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by apilicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""In this module object construction and dependency injection is carried out."""

from fastapi import FastAPI
from ghga_service_chassis_lib.api import configure_app

from ulc.config import Config
from ulc.container import Container

from ulc.adapters.inbound.fastapi_ import router
from ulc.adapters.inbound.kafka_consume import KafkaEventConsumer


def setup_container(*, config: Config) -> Container:
    """Create and configure a DI container."""

    container = Container()
    container.config.from_pydantic(config)
    container.init_resources()

    return container


def get_rest_api(*, config: Config) -> FastAPI:
    """Creates a FastAPI app."""

    api = FastAPI()
    api.container = setup_container(config=config)
    api.container.wire(modules=["ulc.adapters.inbound.fastapi_"])
    api.include_router(router)
    configure_app(api, config=config)

    return api


def get_event_consumer(*, config: Config) -> KafkaEventConsumer:
    """Create an instance of KafkaEventConsumer"""
    container = setup_container(config=config)
    return container.kafka_consumer()
