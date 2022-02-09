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

"""Exceptions thrown by the domain package."""


class FileAlreadyInInboxError(RuntimeError):
    """Thrown when a file is unexpectedly already in the inbox."""

    def __init__(self, file_id: str):
        message = f"The file with external id {file_id} is already in the inbox."
        super().__init__(message)


class FileNotInInboxError(RuntimeError):
    """Thrown when a file is unexpectedly not found in the inbox."""

    def __init__(self, file_id: str):
        message = f"The file with external id {file_id} not in the inbox."
        super().__init__(message)


class FileAlreadyRegisteredError(RuntimeError):
    """Thrown when a file is unexpectedly already registered."""

    def __init__(self, file_id: str):
        message = (
            f"The file with external id {file_id} has already been"
            + " registered for upload."
        )
        super().__init__(message)


class FileNotRegisteredError(RuntimeError):
    """Thrown when a file is unexpectedly not registered."""

    def __init__(self, file_id: str):
        message = f"The file with external id {file_id} has not been registered yet."
        super().__init__(message)


class FileNotReadyForConfirmUpload(RuntimeError):
    """Thrown when a file is not set to 'pending' when trying to confirm."""

    def __init__(self, file_id: str):
        message = f"The file with external id {file_id} is not set to 'pending'."
        super().__init__(message)
