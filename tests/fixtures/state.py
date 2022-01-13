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

"""Test data"""

import os
from pathlib import Path
from typing import Dict, List

from ghga_service_chassis_lib.object_storage_dao_testing import ObjectFixture, calc_md5
from ghga_service_chassis_lib.utils import TEST_FILE_PATHS

from upload_controller_service import models

from .config import DEFAULT_CONFIG


def get_grouping_label_example(index: int) -> str:
    "Generate an example study ID."
    return f"mystudy-{index}"


def get_file_id_example(index: int) -> str:
    "Generate an example file ID."
    return f"myfile-{index}"


class FileState:
    def __init__(
        self,
        id: str,
        grouping_label: str,
        file_path: Path,
        already_uploaded: bool,
        in_inbox: bool,
        in_db: bool = True,
    ):
        """
        Initialize file state and create imputed attributes.
        Please specify whether the file has been already uploaded (`already_uploaded`)
        and whether the file is available in the inbox (`in_inbox`).
        These options are separate because, after an uploaded file has been processed by
        the downstream service, it will be removed from the inbox but stays marked as
        `already_uploaded`.
        """
        self.id = id
        self.grouping_label = grouping_label
        self.file_path = file_path
        self.in_inbox = in_inbox
        self.already_uploaded = already_uploaded
        self.in_db = in_db

        # computed attributes:
        with open(self.file_path, "rb") as file:
            self.content = file.read()

        self.md5 = calc_md5(self.content)
        self.file_info = models.FileInfoInternal(
            file_id=self.id,
            grouping_label=self.grouping_label,
            md5_checksum=self.md5,
            size=len(self.content),
            file_name=os.path.basename(self.file_path),
        )

        self.storage_objects: List[ObjectFixture] = []
        if self.in_inbox:
            self.storage_objects.append(
                ObjectFixture(
                    file_path=self.file_path,
                    bucket_id=DEFAULT_CONFIG.s3_inbox_bucket_id,
                    object_id=self.id,
                )
            )


FILES: Dict[str, FileState] = {
    "in_db_only": FileState(
        id=get_file_id_example(0),
        grouping_label=get_grouping_label_example(0),
        file_path=TEST_FILE_PATHS[0],
        already_uploaded=False,
        in_inbox=False,
    ),
    "in_inbox": FileState(
        id=get_file_id_example(1),
        grouping_label=get_grouping_label_example(1),
        file_path=TEST_FILE_PATHS[1],
        already_uploaded=True,
        in_inbox=True,
    ),
    "uploaded_but_not_in_inbox": FileState(
        id=get_file_id_example(2),
        grouping_label=get_grouping_label_example(2),
        file_path=TEST_FILE_PATHS[2],
        already_uploaded=True,
        in_inbox=False,
    ),
    "db_inconsistency": FileState(
        id=get_file_id_example(3),
        grouping_label=get_grouping_label_example(3),
        file_path=TEST_FILE_PATHS[3],
        already_uploaded=True,
        in_inbox=True,
        in_db=False,
    ),
    "unknown": FileState(
        id=get_file_id_example(4),
        grouping_label=get_grouping_label_example(4),
        file_path=TEST_FILE_PATHS[4],
        already_uploaded=False,
        in_inbox=False,
        in_db=False,
    ),
}
