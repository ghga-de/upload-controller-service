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

"""Entrypoint of the package"""

from enum import Enum

from typer import Typer
from ghga_service_chassis_lib.api import run_server

from ulc.config import CONFIG
from ulc.main import (
    get_event_consumer,
    get_rest_app,
)


class Topics(str, Enum):
    """Supported topics"""

    NEW_STUDY = "new_study"
    FILE_REGISTERED = "file_registered"


rest_app = get_rest_app(config=CONFIG)

cli = Typer()


@cli.command()
def run_rest():
    """Run the REST API."""

    run_server(app="upload_controller_service.__main__:rest_app", config=CONFIG)


@cli.command()
def consume_events(topic: Topics, run_forever: bool = True):
    """Run an event consumer listening to the specified topic."""

    event_consumer = get_event_consumer(config=CONFIG)

    if topic == topic.NEW_STUDY:
        event_consumer.subscribe_new_study(run_forever=run_forever)
    else:
        event_consumer.subscribe_file_registered(run_forever=run_forever)
