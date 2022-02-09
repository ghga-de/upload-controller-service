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

"""Interfaces for especially for outbound adapters"""

from ghga_service_chassis_lib.utils import DaoGenericBase


class DatabaseDao(DaoGenericBase):
    """
    A DAO base class for interacting with the database.
    It might throw following exception to communicate selected error events:
        - FileInfoNotFoundError
        - FileInfoAlreadyExistsError
    """

    def get_file(self, file_id: str) -> models.FileInfoExternal:
        """Get file from the database"""
        ...

    def register_file(self, file: models.FileInfoInternal) -> None:
        """Register a new file to the database."""
        ...

    def update_file_state(self, file_id: str, state: models.UploadState) -> None:
        """Update the file state of a file in the database."""
        ...

    def unregister_file(self, file_id: str) -> None:
        """
        Unregister a new file with the specified file ID from the database.
        """
        ...
