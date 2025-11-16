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
from jetstreampcg.elastic import (
    ElasticConsumerGroupConfig,
    ElasticConsumerGroupConsumerInstance,
    add_members,
    create_elastic,
    delete_elastic,
    delete_member_mappings,
    delete_members,
    elastic_consume,
    elastic_get_partition_filters,
    elastic_is_in_membership_and_active,
    elastic_member_step_down,
    get_elastic_consumer_group_config,
    list_elastic_active_members,
    list_elastic_consumer_groups,
    set_member_mappings,
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
    # Elastic
    "ElasticConsumerGroupConfig",
    "ElasticConsumerGroupConsumerInstance",
    "MemberMapping",
    # Static
    "StaticConsumerGroupConfig",
    "StaticConsumerGroupConsumerInstance",
    "__version__",
    "add_members",
    "compose_key",
    "create_elastic",
    "create_static",
    "delete_elastic",
    "delete_member_mappings",
    "delete_members",
    "delete_static",
    "elastic_consume",
    "elastic_get_partition_filters",
    "elastic_is_in_membership_and_active",
    "elastic_member_step_down",
    "generate_partition_filters",
    "get_elastic_consumer_group_config",
    "get_static_consumer_group_config",
    "list_elastic_active_members",
    "list_elastic_consumer_groups",
    "list_static_active_members",
    "list_static_consumer_groups",
    "set_member_mappings",
    "static_consume",
    "static_member_step_down",
    "validate_static_config",
]
