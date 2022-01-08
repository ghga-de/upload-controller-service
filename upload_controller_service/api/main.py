# Copyright 2021 Universität Tübingen, DKFZ and EMBL
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
Module containing the main FastAPI router and (optionally) top-level API enpoints.
Additional endpoints might be structured in dedicated modules
(each of them having a sub-router).
"""

from fastapi import Depends, FastAPI, HTTPException, status
from ghga_service_chassis_lib.api import configure_app

from ..config import CONFIG, Config
from ..core import get_upload_url
from ..dao.db import FileInfoNotFoundError
from .deps import get_config

app = FastAPI(openapi_url="/openapi.yaml")
configure_app(app, config=CONFIG)


@app.get("/", summary="index")
async def index():
    """Index"""
    return "Hello World."


@app.get("/health", summary="health", status_code=status.HTTP_200_OK)
async def health():
    """Used to test if this service is alive"""
    return {"status": "OK"}


@app.get("/presigned_post/{file_id}", summary="health")
async def get_presigned_post(
    file_id: str,
    config: Config = Depends(get_config),
):
    """
    Requesting a pre-signed post URL for a new file in the inbox
    using the file_id representing the file.
    """

    # call core functionality
    try:
        url = get_upload_url(file_id=file_id, config=config)
    except FileInfoNotFoundError as file_info_not_found_error:
        raise HTTPException(
            status_code=404, detail="The submitted file_id does not exist."
        ) from file_info_not_found_error

    return {"presigned_post": url}
