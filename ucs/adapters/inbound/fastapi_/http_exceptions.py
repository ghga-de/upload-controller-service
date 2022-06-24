# Copyright 2022 Universität Tübingen, DKFZ and EMBL
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

"""A collextion of http exceptions."""

from typing import cast

from httpyexpect.server import HTTPException

from ucs.domain import models


def _format_upload_status(status: models.UploadStatus):
    """Format upload status to be using in exception IDs"""

    status_value = cast(str, status.value)
    capitalized_first_letter = status_value[0].upper()
    rest = status_value[1 : len(status_value)]

    return f"{capitalized_first_letter}{rest}"


class HttpFileNotFoundError(HTTPException):
    """Thrown when a file with given ID could not be found."""

    def __init__(self, *, file_id: str):
        """Construct message and init the exception."""
        super().__init__(
            status_code=404,
            exception_id="fileNotRegistered",
            description=(
                f"The file with ID {file_id} has not (yet) been registered for upload."
            ),
            data={"file_id": file_id},
        )


class HttpUploadNotFoundError(HTTPException):
    """Thrown when an upload with given ID could not be found."""

    def __init__(self, *, upload_id: str):
        """Construct message and init the exception."""
        super().__init__(
            status_code=404,
            exception_id="noSuchUpload",
            description=(f"The upload with ID {upload_id} does not exist."),
            data={"upload_id": upload_id},
        )


class HttpUploadPresentError(HTTPException):
    """Thrown when trying to create a new upload while there is another upload active."""

    def __init__(self, *, file_id: str, status: models.UploadStatus):
        """Construct message and init the exception."""
        super().__init__(
            status_code=404,
            exception_id=f"uploadAttemptPresent{_format_upload_status(status)}",
            description=(
                f"An upload attempt with status {status.value} is already"
                + "present for the file with ID {file_id}. Cannot create a new one."
            ),
            data={"file_id": file_id, "upload_status": status.value},
        )


class HttpInvalidUploadChange(HTTPException):
    """Thrown when updating an upload that cannot be uploaded anymore."""

    def __init__(self, *, upload_id: str, current_status: models.UploadStatus):
        """Construct message and init the exception."""
        super().__init__(
            status_code=404,
            exception_id=f"invalidChangeFrom{_format_upload_status(current_status)}",
            description=(
                f"The upload with ID {upload_id} has the status {current_status}"
                + " and cannot be updated anymore."
            ),
            data={
                "upload_id": upload_id,
                "current_upload_status": current_status.value,
            },
        )
