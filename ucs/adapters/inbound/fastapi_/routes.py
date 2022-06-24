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

from ucs.adapters.inbound.fastapi_ import http_exceptions, rest_models
from ucs.container import Container
from ucs.domain.interfaces.inbound.file_service import (
    FileUnkownError,
    IFileMetadataService,
)
from ucs.domain.interfaces.inbound.upload_service import (
    ExistingActiveUploadError,
    FileAlreadyInInboxError,
    FileNotInInboxError,
    FileUnkownError,
    IUploadService,
    StorageAndDatabaseOutOfSyncError,
    UploadCancelError,
    UploadCompletionError,
    UploadNotPendingError,
    UploadUnkownError,
)

router = APIRouter()


ERROR_DESCRIPTIONS = {
    "noFileAccess": (
        "Exceptions by ID:"
        + "\n- noFileAccess: The user is not registered as a Data Submitter for the"
        + " corresponding file."
        ""
    ),
    "noSuchUpload": (
        "Exceptions by ID:"
        + "\n- noSuchUpload: The multi-part upload with the given ID does not exist."
    ),
    "fileNotRegistered": (
        "Exceptions by ID:"
        + "\n- fileNotRegistered: The file with the given ID has not (yet) been"
        + " registered for upload."
    ),
}


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
        status.HTTP_403_FORBIDDEN: {"description": ERROR_DESCRIPTIONS["noFileAccess"]},
        status.HTTP_404_NOT_FOUND: {
            "description": ERROR_DESCRIPTIONS["fileNotRegistered"]
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
        raise http_exceptions.HttpFileNotFoundError(file_id=file_id) from error


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
        status.HTTP_404_NOT_FOUND: {
            "description": ERROR_DESCRIPTIONS["fileNotRegistered"]
        },
        status.HTTP_403_FORBIDDEN: {"description": ERROR_DESCRIPTIONS["noFileAccess"]},
    },
)
@inject
def create_upload(
    upload_creation: rest_models.UploadAttemptCreation,
    upload_service: IUploadService = Depends(Provide[Container.upload_service]),
):
    """Initiate a new mutli-part upload for the given file."""

    try:
        return upload_service.initiate_new(file_id=upload_creation.file_id)
    except ExistingActiveUploadError as error:
        raise http_exceptions.HttpUploadPresentError(
            file_id=upload_creation.file_id,
            status=error.active_upload.status,
        ) from error
    except FileUnkownError as error:
        raise http_exceptions.HttpFileNotFoundError(
            file_id=upload_creation.file_id
        ) from error


@router.get(
    "/uploads/{upload_id}",
    summary="Get details on a specific upload.",
    operation_id="getUploadDetails",
    status_code=status.HTTP_200_OK,
    response_model=rest_models.UploadAttempt,
    response_description="Details on a specific upload.",
    responses={
        status.HTTP_403_FORBIDDEN: {"description": ERROR_DESCRIPTIONS["noFileAccess"]},
        status.HTTP_404_NOT_FOUND: {"description": ERROR_DESCRIPTIONS["noSuchUpload"]},
    },
)
@inject
def get_upload_details(
    upload_id: str,
    upload_service: IUploadService = Depends(Provide[Container.upload_service]),
):
    """
    Get details on a specific upload.
    """

    try:
        return upload_service.get_details(upload_id=upload_id)
    except UploadUnkownError as error:
        raise http_exceptions.HttpUploadNotFoundError(upload_id=upload_id) from error


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
                + "\n- invalidChangeFromCancelled: Cannot update an upload with status"
                + " cancelled."
                + " If you want to restart, initiate a new multi-part upload instead."
                + "\n- invalidChangeFromFailed: Cannot change status from failed for"
                + " this upload attempt."
                + " If you want to restart, initiate a new multi-part upload instead."
                + "\n- invalidChangeFromUploaded: Cannot update an upload with status"
                + " uploaded."
                "\n- invalidChangeFromAccepted: Cannot update an upload with status"
                + " accepted."
                "\n- invalidChangeFromRejected: Cannot update an upload with status"
                + " cancelled."
                + " If you want to restart, initiate a new multi-part upload instead."
            )
        },
        status.HTTP_403_FORBIDDEN: {"description": ERROR_DESCRIPTIONS["noFileAccess"]},
        status.HTTP_404_NOT_FOUND: {"description": ERROR_DESCRIPTIONS["noSuchUpload"]},
    },
)
@inject
def update_upload_status(
    upload_id: str,
    update: rest_models.UploadAttemptUpdate,
    upload_service: IUploadService = Depends(Provide[Container.upload_service]),
):
    """
    Declare a multi-part upload as complete by setting its status to "uploaded".
    Or cancel a multi-part upload by setting its status to "cancelled".
    """

    try:
        if update.status == "uploaded":
            upload_service.complete(upload_id=upload_id)
        else:
            upload_service.cancel(upload_id=upload_id)
    except UploadNotPendingError as error:
        raise http_exceptions.HttpInvalidUploadChange(
            upload_id=upload_id, current_status=error.current_status
        ) from error
    except UploadUnkownError as error:
        raise http_exceptions.HttpUploadNotFoundError(upload_id=upload_id) from error


@router.post(
    "/uploads/{upload_id}/parts/{part_no}/signed_urls",
    summary="Create new pre-signed URL for a specific part.",
    operation_id="createPreSignedURL",
    status_code=status.HTTP_200_OK,
    response_model=rest_models.PartUploadDetails,
    response_description="The newly created pre-signed URL.",
    responses={
        status.HTTP_403_FORBIDDEN: {"description": ERROR_DESCRIPTIONS["noFileAccess"]},
        status.HTTP_404_NOT_FOUND: {"description": ERROR_DESCRIPTIONS["noSuchUpload"]},
    },
)
@inject
def create_presigned_url(
    upload_id: str,
    part_no: int = Path(..., ge=1, le=10000),
    upload_service: IUploadService = Depends(Provide[Container.upload_service]),
):
    """
    Create a pre-signed URL for the specified part number of the specified multi-part
    upload.
    """

    try:
        presigned_url = upload_service.create_part_url(
            upload_id=upload_id, part_no=part_no
        )
    except UploadUnkownError as error:
        raise http_exceptions.HttpUploadNotFoundError(upload_id=upload_id) from error

    return rest_models.PartUploadDetails(url=presigned_url)
