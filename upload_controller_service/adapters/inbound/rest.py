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

from fastapi import Depends, FastAPI, HTTPException, Response, status
from ghga_service_chassis_lib.api import configure_app

from upload_controller_service.config import CONFIG
from upload_controller_service.domain.models import FileInfoPatchState, UploadState
from upload_controller_service.domain.interfaces.inbound.upload import (
    IUploadHandler,
    FileNotInInboxError,
    FileNotReadyForConfirmUpload,
    FileNotRegisteredError,
)

app = FastAPI()
configure_app(app, config=CONFIG)


def get_config():
    """Get runtime configuration. For use as FastAPI dependency."""
    return CONFIG


class HttpFileNotFoundException(HTTPException):
    """Thrown when a file with given ID could not be found."""

    def __init__(self, file_id: str):
        """Construct message and init the exception."""
        super().__init__(
            status_code=404,
            detail=f'The file with the file_id "{file_id}" does not exist.',
        )


@app.get("/health", summary="health", status_code=status.HTTP_200_OK)
async def health():
    """Used to test if this service is alive"""
    return {"status": "OK"}


@app.get("/presigned_post/{file_id}", summary="presigned_post")
async def get_presigned_post(
    file_id: str,
    upload_handler: IUploadHandler = Depends(lambda: ...),
):
    """
    Requesting a pre-signed post URL for a new file in the inbox
    using the file_id representing the file.
    """

    # call core functionality
    try:
        url = upload_handler.get_upload_url(file_id=file_id)
    except FileNotRegisteredError as error:
        raise HttpFileNotFoundException(file_id) from error

    return {"presigned_post": url}


@app.patch(
    "/confirm_upload/{file_id}",
    summary="confirm_upload",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def patch_confirm_upload(
    file_id: str,
    file_info_patch: FileInfoPatchState,
    upload_handler: IUploadHandler = Depends(lambda: ...),
):
    """
    Requesting a confirmation of the upload of a specific file using the file id.
    Returns:
        204 - if the file is registered and its content is in the inbox
        400 - if there is a bad request
        404 - if the file is unkown
    """

    if file_info_patch.state is not UploadState.UPLOADED:
        raise HTTPException(
            status_code=400,
            detail=(
                'The file with id "{file_id}" can`t be set to "{file_info_patch.state}"'
            ),
        )

    # call core functionality
    try:
        upload_handler.confirm_file_upload(file_id)
    except FileNotReadyForConfirmUpload as error:
        raise HTTPException(
            status_code=400,
            detail=(
                'The file with id "{file_id}" is not ready to be set to "CONFIRMED"'
            ),
        ) from error
    except FileNotRegisteredError as error:
        raise HttpFileNotFoundException(file_id) from error
    except FileNotInInboxError as error:
        raise HTTPException(
            status_code=400,
            detail=(
                'The file with id "{file_id}" is registered for upload'
                + " but its content was not found in the inbox."
            ),
        ) from error

    return Response(status_code=status.HTTP_204_NO_CONTENT)
