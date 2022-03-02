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

"""Implementation of object storage adapters."""

# pylint: disable=unused-import
from ghga_service_chassis_lib.s3 import ObjectStorageS3 as _S3ObjectStorage
from ghga_service_chassis_lib.s3 import S3ConfigBase


class S3ObjectStorage(_S3ObjectStorage):
    """
    An implementation of the IObjectStorage interface for interacting specifically
    with S3 object storages.
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        s3_endpoint_url: str,
        s3_access_key_id: str,
        s3_secret_access_key: str,
        s3_session_token: str,
        aws_config_ini: str,
    ):
        """Init S3 object storage with config params."""

        config = S3ConfigBase(
            s3_endpoint_url=s3_endpoint_url,
            s3_access_key_id=s3_access_key_id,
            s3_secret_access_key=s3_secret_access_key,
            s3_session_token=s3_session_token,
            aws_config_ini=aws_config_ini,
        )

        super().__init__(config=config)
