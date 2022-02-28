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

"""Fixtures and utilities for dealing with the API."""

from typing import Optional

import fastapi.testclient

from ulc.adapters.inbound.deps import get_config
from ulc.adapters.inbound.rest import app
from ulc.config import Config


class ApiTestClient(fastapi.testclient.TestClient):
    """A test client to which you can inject a custom Config object."""

    def __init__(self, config: Optional[Config] = None):
        """Create the test client with a custom Config object."""
        # Overwrite the get_config dep if config is specified, otherwise restore the
        # default:
        overwrite_func = get_config if config is None else lambda: config
        app.dependency_overrides[get_config] = overwrite_func

        super().__init__(app)
