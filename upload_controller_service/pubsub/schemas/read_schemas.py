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

"""Read in schemas from json files"""

import json
from pathlib import Path
from typing import Dict

HERE = Path(__file__).parent.resolve()


def read_schema(topic_name: str) -> Dict[str, object]:
    """Read schemas from file"""
    with open(HERE / f"{topic_name}.json", "r", encoding="utf8") as schema_file:
        return json.load(schema_file)


NEW_STUDY = read_schema("new_study_created.json")
