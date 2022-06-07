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

from fastapi import APIRouter, HTTPException, Path, status

from ucs.adapters.inbound.fastapi_.models import (
    FileMetadata,
    UploadDetails,
    UploadUpdate,
)

router = APIRouter()


class HttpFileNotFoundException(HTTPException):
    """Thrown when a file with given ID could not be found."""

    def __init__(self, file_id: str):
        """Construct message and init the exception."""
        super().__init__(
            status_code=404,
            detail=f'The file with the file_id "{file_id}" does not exist.',
        )


@router.get("/health", summary="health", status_code=status.HTTP_200_OK)
def health():
    """Used to test if this service is alive"""
    return {"status": "OK"}


@router.post(
    "/files/{file_id}/uploads",
    summary="Initiate a new mutli-part upload.",
    operation_id="createUpload",
    response_model=UploadDetails,
    status_code=status.HTTP_200_OK,
    response_description="Details on the newly created upload.",
    responses={
        status.HTTP_403_FORBIDDEN: {
            "description": """
Either the user is not registered as a Data Submitter for the corresponding file.
Or it is currently not possible to create a new upload for the file with the specified
ID because:
- a previous upload for that file was already accepted
- another upload for that file is currently in progress or is waiting for acceptance
"""
        },
        status.HTTP_404_NOT_FOUND: {
            "description": (
                "The file with the given ID has not (yet) been registered for upload."
            )
        },
    },
)
def create_upload(file_id: str):
    """Initiate a new mutli-part upload for the given file."""

    print(file_id)
    ...

    return ...


@router.get(
    "/files/{file_id}",
    summary="Get file metadata including the current upload attempt.",
    operation_id="getFileMetadata",
    response_model=FileMetadata,
    status_code=status.HTTP_200_OK,
    response_description="File metadata including the current upload attempt",
    responses={
        status.HTTP_403_FORBIDDEN: {
            "description": (
                "The user is not registered as a Data Submitter for the corresponding file."
            )
        },
        status.HTTP_404_NOT_FOUND: {
            "description": (
                "The file with the given ID has not (yet) been registered for upload."
            )
        },
    },
)
def get_file_metadata(
    file_id: str,
) -> FileMetadata:
    """Get file metadata including the current upload attempt."""

    print(file_id)
    ...

    return ...


@router.patch(
    "/uploads/{upload_id}",
    summary="Update the status of an existing multi-part upload.",
    operation_id="updateUploadStatus",
    status_code=status.HTTP_204_NO_CONTENT,
    response_description="Multi-part upload successfully updated.",
    responses={
        status.HTTP_403_FORBIDDEN: {
            "description": (
                "The user is not registered as a Data Submitter for the file"
                + " corresponding to this multi-part upload."
            )
        },
        status.HTTP_404_NOT_FOUND: {
            "description": "The multi-part upload with the given ID does not exist."
        },
    },
)
def update_upload_status(upload_id: str, update: UploadUpdate):
    """
    Declare a multi-part upload as complete by setting its status to "uploaded".
    Or cancel a multi-part upload by setting its status to "cancelled".
    """
    print(upload_id, update)
    ...


@router.post(
    "/uploads/{upload_id}/parts/{part_no}/signed_posts",
    summary="Update the status of an existing multi-part upload.",
    operation_id="createPreSignedPost",
    status_code=status.HTTP_200_OK,
    response_model=dict,
    response_description="The newly create pre-signed POST.",
    responses={
        status.HTTP_200_OK: {
            "content": {
                "application/json": {
                    "schema": {
                        "title": None,
                        "description": "To be defined in further detail.",
                    }
                }
            }
        },
        status.HTTP_403_FORBIDDEN: {
            "description": (
                "The user is not registered as a Data Submitter for the file"
                + " corresponding to this multi-part upload."
            )
        },
        status.HTTP_404_NOT_FOUND: {
            "description": "The multi-part upload with the given ID does not exist."
        },
    },
)
def create_presigned_post(upload_id: str, part_no: int = Path(..., ge=1, le=10000)):
    """
    Create a pre-signed post for the specified part number of the specified multi-part
    upload.
    """

    print(upload_id, part_no)
    ...
