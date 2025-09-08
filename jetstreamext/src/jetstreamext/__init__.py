# Copyright 2025 Oliver Lambson
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""JetStream Extensions - utilities providing additional features to JetStream."""

from jetstreamext import getbatch
from jetstreamext._version import __version__
from jetstreamext.getbatch import (
    BatchUnsupportedError,
    InvalidOptionError,
    InvalidResponseError,
    NoMessagesError,
    SubjectRequiredError,
    get_batch,
    get_batch_max_bytes,
    get_batch_seq,
    get_batch_start_time,
    get_batch_subject,
    get_last_msgs_batch_size,
    get_last_msgs_for,
    get_last_msgs_up_to_seq,
    get_last_msgs_up_to_time,
)

__all__ = [
    "BatchUnsupportedError",
    "InvalidOptionError",
    "InvalidResponseError",
    "NoMessagesError",
    "SubjectRequiredError",
    "__version__",
    "get_batch",
    "get_batch_max_bytes",
    "get_batch_seq",
    "get_batch_start_time",
    "get_batch_subject",
    "get_last_msgs_batch_size",
    "get_last_msgs_for",
    "get_last_msgs_up_to_seq",
    "get_last_msgs_up_to_time",
    "getbatch",
]
