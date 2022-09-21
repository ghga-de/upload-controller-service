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

"""Logic to calculate the part size."""

import math
from typing import Generator

from ghga_service_chassis_lib.object_storage_dao import (
    DEFAULT_PART_SIZE,
    MAX_FILE_PART_NUMBER,
)


def _part_size_options() -> Generator[int, None, None]:
    """
    Yields the possible part size options in increasing order.
    Will never exhaust.
    """

    # start with the default part size:
    part_size = DEFAULT_PART_SIZE

    while True:
        yield part_size

        # double the part size after every iteration:
        part_size = part_size * 2


# pylint: disable=inconsistent-return-statements
def calculate_part_size(file_size: int) -> int:  # type: ignore
    # MyPy and pyling complain that the function does not return, however, they do not
    # recognize that the `_part_size_options` generator will never exhaust.
    """Calculates the recommended part size for up-/downloading a given file.

    Args:
        file_size: The total size of the file in bytes.

    Returns:
        The recommended part size in bytes.
    """

    # calculate the minimal possible part size:
    min_part_size = math.ceil(file_size / MAX_FILE_PART_NUMBER)

    for part_size in _part_size_options():
        if part_size >= min_part_size:
            return part_size
