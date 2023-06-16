# Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

"""Example data used for testing."""

from datetime import datetime

from hexkit.providers.s3.testutils import TEST_FILE_PATHS, FileObject

from tests.fixtures.config import DEFAULT_CONFIG
from ucs.core import models

# Example metadata on a single file:
EXAMPLE_FILE = models.FileMetadata(
    file_id="testFile001",
    file_name="Test File 001",
    decrypted_sha256="fake-checksum",
    decrypted_size=12345678,
    latest_upload_id="testUpload001",
)

# A list of metadata in case multiple file entries are needed:
# (The example storage objects and example uploads below only correspond to the first
# file in the list.)
EXAMPLE_FILES = [
    EXAMPLE_FILE,
    EXAMPLE_FILE.copy(update={"file_id": "testFile002", "file_name": "Test File 002"}),
]

# An example of a storage file object corresponding to the EXAMPLE_FILE:
EXAMPLE_STORAGE_OBJECT = FileObject(
    file_path=TEST_FILE_PATHS[0],
    bucket_id=DEFAULT_CONFIG.inbox_bucket,
    object_id="object001",
)

# An details on an example upload corresponding to the EXAMPLE_FILE:
EXAMPLE_UPLOAD = models.UploadAttempt(
    upload_id="testUpload001",
    file_id="testFile001",
    object_id="object001",
    status=models.UploadStatus.PENDING,
    part_size=1234,
    creation_date=datetime.now(),
    submitter_public_key="test-key",
    completion_date=None,
)

# Multiple example uploads corresponding to EXAMPLE_FILE:
EXAMPLE_UPLOADS = (
    EXAMPLE_UPLOAD.copy(update={"status": models.UploadStatus.CANCELLED}),
    EXAMPLE_UPLOAD.copy(update={"upload_id": "testUpload002"}),
)
