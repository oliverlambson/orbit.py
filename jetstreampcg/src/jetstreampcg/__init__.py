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

"""JetStream partitioned consumer groups is an implementation of a client-side partitioned consumer group feature for NATS streams."""

from jetstreampcg._version import __version__
from jetstreampcg.common import (
    ConsumerGroupConsumeContext,
    ConsumerGroupMsg,
    MemberMapping,
    compose_key,
    generate_partition_filters,
)
from jetstreampcg.static import (
    StaticConsumerGroupConfig,
    StaticConsumerGroupConsumerInstance,
    create_static,
    delete_static,
    get_static_consumer_group_config,
    list_static_active_members,
    list_static_consumer_groups,
    static_consume,
    static_member_step_down,
    validate_static_config,
)

__all__ = [
    # Common
    "ConsumerGroupConsumeContext",
    "ConsumerGroupMsg",
    "MemberMapping",
    # Static
    "StaticConsumerGroupConfig",
    "StaticConsumerGroupConsumerInstance",
    "__version__",
    "compose_key",
    "create_static",
    "delete_static",
    "generate_partition_filters",
    "get_static_consumer_group_config",
    "list_static_active_members",
    "list_static_consumer_groups",
    "static_consume",
    "static_member_step_down",
    "validate_static_config",
]
