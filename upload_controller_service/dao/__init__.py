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

"""This sub-package contains implementations of DAO interfaces."""

# forward imports for usage outside of the `dao` subpackage:
# from ghga_service_chassis_lib.object_storage_dao import (  # noqa: F401
#     ObjectAlreadyExistsError,
#     ObjectNotFoundError,
#     ObjectStorageDao,
# )
from ghga_service_chassis_lib.s3 import ObjectStorageS3 as ObjectStorage  # noqa: F401

from .db import PostgresDatabase as Database  # noqa: F401
