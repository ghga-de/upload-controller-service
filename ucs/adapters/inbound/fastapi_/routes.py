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
Module containing the main FastAPI router and all route functions.
"""

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, Path, status
from httpyexpect.server import HTTPException

from ucs.adapters.inbound.fastapi_ import rest_models
from ucs.container import Container
from ucs.domain.interfaces.inbound.file_service import (
    FileUnkownError,
    IFileMetadataService,
)

router = APIRouter()


ERROR_DESCRIPTIONS = {
    403: (
        "Exceptions by ID:"
        + "\n- noFileAccess: The user is not registered as a Data Submitter for the"
        + " corresponding file."
        ""
    ),
    404: (
        "Exceptions by ID:"
        + "\n- noSuchUpload: The multi-part upload with the given ID does not exist."
    ),
}


class HttpFileNotFoundException(HTTPException):
    """Thrown when a file with given ID could not be found."""

    def __init__(self, file_id: str):
        """Construct message and init the exception."""
        super().__init__(
            status_code=404,
            exception_id="fileNotRegistered",
            description=(
                f"The file with ID {file_id} has not (yet) been registered for upload."
            ),
            data={"file_id": file_id},
        )


@router.get("/health", summary="health", status_code=status.HTTP_200_OK)
def health():
    """Used to test if this service is alive"""
    return {"status": "OK"}


@router.get(
    "/files/{file_id}",
    summary="Get file metadata including the current upload attempt.",
    operation_id="getFileMetadata",
    status_code=status.HTTP_200_OK,
    response_model=rest_models.FileMetadataWithUpload,
    response_description="File metadata including the current upload attempt",
    responses={
        status.HTTP_403_FORBIDDEN: {"description": ERROR_DESCRIPTIONS[403]},
        status.HTTP_404_NOT_FOUND: {
            "description": (
                "Exceptions by ID:"
                + "\n- fileNotRegistered: The file with the given ID has not (yet) been"
                + " registered for upload."
            )
        },
    },
)
@inject
def get_file_metadata(
    file_id: str,
    file_metadata_service: IFileMetadataService = Depends(
        Provide[Container.file_metadata_service]
    ),
):
    """Get file metadata including the current upload attempt."""
    try:
        return file_metadata_service.get(file_id)
    except FileUnkownError as error:
        raise HttpFileNotFoundException(file_id=file_id) from error


@router.post(
    "/uploads",
    summary="Initiate a new multi-part upload.",
    operation_id="createUpload",
    response_model=rest_models.UploadAttempt,
    status_code=status.HTTP_200_OK,
    response_description="Details on the newly created upload.",
    responses={
        status.HTTP_400_BAD_REQUEST: {
            "description": (
                "Exceptions by ID:"
                + "\n- uploadAttemptPresentPending: Imposible to create a new upload"
                + " for the file with the specific ID. Another upload in pending status"
                + " already exists for this file."
                + "\n- uploadAttemptPresentUploaded: Imposible to create a new upload"
                + " for the file with the specific ID. Another upload in uploaded"
                + " status already exists for this file."
                + "\n- uploadAttemptPresentAccepted: Imposible to create a new upload"
                + " for the file with the specific ID. Another upload in accepted"
                + " status already exists for this file."
            )
        },
        status.HTTP_403_FORBIDDEN: {"description": ERROR_DESCRIPTIONS[403]},
    },
)
def create_upload(upload_creation: rest_models.UploadAttemptCreation):
    """Initiate a new mutli-part upload for the given file."""

    print(upload_creation)
    ...

    return ...


@router.get(
    "/uploads/{upload_id}",
    summary="Get details on a specific upload.",
    operation_id="getUploadDetails",
    status_code=status.HTTP_200_OK,
    response_model=rest_models.UploadAttempt,
    response_description="Details on a specific upload.",
    responses={
        status.HTTP_403_FORBIDDEN: {"description": ERROR_DESCRIPTIONS[403]},
        status.HTTP_404_NOT_FOUND: {"description": ERROR_DESCRIPTIONS[404]},
    },
)
def get_upload_details(upload_id: str):
    """
    Get details on a specific upload.
    """
    print(upload_id)
    ...


@router.patch(
    "/uploads/{upload_id}",
    summary="Update the status of an existing multi-part upload.",
    operation_id="updateUploadStatus",
    status_code=status.HTTP_204_NO_CONTENT,
    response_description="Multi-part upload successfully updated.",
    responses={
        status.HTTP_400_BAD_REQUEST: {
            "description": (
                "Exceptions by ID:"
                + "\n- pendingInvalidChange: Cannot change status from pending to"
                + " accepted or rejected"
                + "\n- uploadedInvalidChange: Cannot change status from uploaded to"
                + " cancelled or pending"
                + "\n- invalidChangeFromCancelled: Cannot change status from cancelled"
                + " for this upload attempt. If you want to restart, initiate a new"
                + " multi-part upload instead."
                + "\n- invalidChangeFromFailed: Cannot change status from failed for"
                + " this upload attempt. If you want to restart, initiate a new"
                + " multi-part upload instead."
                + "\n- invalidChangeFromAccepted: Upload has already been accepted, a "
                + " status change or new upload attempt is no longer possible."
                + "\n- invalidChangeFromRejected: Cannot change status from rejected"
                + " for this upload attempt. If you want to restart, initiate a new"
                + " multi-part upload instead."
            )
        },
        status.HTTP_403_FORBIDDEN: {"description": ERROR_DESCRIPTIONS[403]},
        status.HTTP_404_NOT_FOUND: {"description": ERROR_DESCRIPTIONS[404]},
    },
)
def update_upload_status(upload_id: str, update: rest_models.UploadAttemptUpdate):
    """
    Declare a multi-part upload as complete by setting its status to "uploaded".
    Or cancel a multi-part upload by setting its status to "cancelled".
    """
    print(upload_id, update)
    ...


@router.post(
    "/uploads/{upload_id}/parts/{part_no}/signed_urls",
    summary="Create new pre-signed URL for a specific part.",
    operation_id="createPreSignedURL",
    status_code=status.HTTP_200_OK,
    response_model=rest_models.AccessURL,
    response_description="The newly created pre-signed URL.",
    responses={
        status.HTTP_403_FORBIDDEN: {"description": ERROR_DESCRIPTIONS[403]},
        status.HTTP_404_NOT_FOUND: {"description": ERROR_DESCRIPTIONS[404]},
    },
)
def create_presigned_url(upload_id: str, part_no: int = Path(..., ge=1, le=10000)):
    """
    Create a pre-signed URL for the specified part number of the specified multi-part
    upload.
    """

    print(upload_id, part_no)
    ...
