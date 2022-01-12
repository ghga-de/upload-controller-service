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
Module containing the main FastAPI router and (optionally) top-level API enpoints.
Additional endpoints might be structured in dedicated modules
(each of them having a sub-router).
"""

from fastapi import Depends, FastAPI, HTTPException, status
from ghga_service_chassis_lib.api import configure_app
from ghga_service_chassis_lib.object_storage_dao import ObjectNotFoundError

from ..config import CONFIG, Config
from ..core import check_uploaded_file, get_upload_url
from ..dao.db import FileInfoNotFoundError
from .deps import get_config

app = FastAPI()
configure_app(app, config=CONFIG)


@app.get("/health", summary="health", status_code=status.HTTP_200_OK)
async def health():
    """Used to test if this service is alive"""
    return {"status": "OK"}


@app.get("/presigned_post/{file_id}", summary="presigned_post")
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


@app.get("/confirm_upload/{file_id}", summary="confirm_upload")
async def confirm_upload(
    file_id: str,
    config: Config = Depends(get_config),
):
    """
    Requesting a confirmation of the upload of a specific file using the file id.
    Returns 200, if the file exists in the inbox, 404 if not
    """

    # call core functionality
    try:
        check_uploaded_file(file_id=file_id, config=config)
    except FileInfoNotFoundError as file_info_not_found_error:
        raise HTTPException(
            status_code=400,
            detail=f"The submitted file_id {file_id} does not exist.",
        ) from file_info_not_found_error
    except ObjectNotFoundError as object_not_found_error:
        raise HTTPException(
            status_code=404,
            detail=f"The file with the file_id {file_id} does not exist.",
        ) from object_not_found_error

    return True
