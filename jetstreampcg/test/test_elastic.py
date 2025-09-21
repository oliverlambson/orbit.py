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

"""Tests for elastic.py module."""

import pytest

from jetstreampcg.common import MemberMapping
from jetstreampcg.elastic import (
    ElasticConsumerGroupConfig,
    _get_partitioning_transform_dest,
    _validate_config,
    elastic_get_partition_filters,
)


class TestElasticConsumerGroupConfig:
    """Test cases for ElasticConsumerGroupConfig class.

    These tests are ported from orbit.go/pcgroups/partitioned_consumer_groups_test.go
    """

    def test_get_partitioning_transform_dest(self):
        """Test the partitioning transform destination string generation."""
        config = ElasticConsumerGroupConfig(
            max_members=4,
            filter="foo.*.*.>",
            partitioning_wildcards=[1, 2],
        )
        dest = _get_partitioning_transform_dest(config)
        expected = "{{Partition(4,1,2)}}.foo.{{Wildcard(1)}}.{{Wildcard(2)}}.>"
        assert dest == expected

    def test_elastic_get_partition_filters_6_members(self):
        """Test partition filter generation with 6 members and 3 consumers."""
        config = ElasticConsumerGroupConfig(
            max_members=6,
            filter="foo.*.*.>",
            partitioning_wildcards=[1, 2],
            members=["m1", "m2", "m3"],
        )

        assert elastic_get_partition_filters(config, "m1") == ["0.>", "1.>"]
        assert elastic_get_partition_filters(config, "m2") == ["2.>", "3.>"]
        assert elastic_get_partition_filters(config, "m3") == ["4.>", "5.>"]

    def test_elastic_get_partition_filters_7_members(self):
        """Test partition filter generation with 7 members and 3 consumers."""
        config = ElasticConsumerGroupConfig(
            max_members=7,
            filter="foo.*.*.>",
            partitioning_wildcards=[1, 2],
            members=["m1", "m2", "m3"],
        )

        assert elastic_get_partition_filters(config, "m1") == ["0.>", "1.>", "6.>"]
        assert elastic_get_partition_filters(config, "m2") == ["2.>", "3.>"]
        assert elastic_get_partition_filters(config, "m3") == ["4.>", "5.>"]

    def test_elastic_get_partition_filters_8_members(self):
        """Test partition filter generation with 8 members and 3 consumers."""
        config = ElasticConsumerGroupConfig(
            max_members=8,
            filter="foo.*.*.>",
            partitioning_wildcards=[1, 2],
            members=["m1", "m2", "m3"],
        )

        assert elastic_get_partition_filters(config, "m1") == ["0.>", "1.>", "6.>"]
        assert elastic_get_partition_filters(config, "m2") == ["2.>", "3.>", "7.>"]
        assert elastic_get_partition_filters(config, "m3") == ["4.>", "5.>"]

    def test_is_in_membership_with_members(self):
        """Test is_in_membership with members list."""
        config = ElasticConsumerGroupConfig(
            max_members=3,
            filter="test.*",
            partitioning_wildcards=[1],
            members=["m1", "m2", "m3"],
        )
        assert config.is_in_membership("m1") is True
        assert config.is_in_membership("m2") is True
        assert config.is_in_membership("m3") is True
        assert config.is_in_membership("m4") is False

    def test_is_in_membership_with_member_mappings(self):
        """Test is_in_membership with member mappings."""
        config = ElasticConsumerGroupConfig(
            max_members=3,
            filter="test.*",
            partitioning_wildcards=[1],
            member_mappings=[
                MemberMapping(member="m1", partitions=[0]),
                MemberMapping(member="m2", partitions=[1, 2]),
            ],
        )
        assert config.is_in_membership("m1") is True
        assert config.is_in_membership("m2") is True
        assert config.is_in_membership("m3") is False

    def test_to_dict(self):
        """Test conversion to dictionary."""
        config = ElasticConsumerGroupConfig(
            max_members=3,
            filter="test.*",
            partitioning_wildcards=[1],
            max_buffered_msgs=100,
            max_buffered_bytes=1024,
            members=["m1", "m2"],
            member_mappings=[MemberMapping(member="m3", partitions=[0, 1])],
        )
        result = config.to_dict()
        assert result["max_members"] == 3
        assert result["filter"] == "test.*"
        assert result["partitioning_wildcards"] == [1]
        assert result["max_buffered_msgs"] == 100
        assert result["max_buffered_bytes"] == 1024
        assert result["members"] == ["m1", "m2"]
        assert result["member_mappings"] == [{"member": "m3", "partitions": [0, 1]}]

    def test_to_dict_minimal(self):
        """Test conversion to dictionary with minimal fields."""
        config = ElasticConsumerGroupConfig(
            max_members=3, filter="test.*", partitioning_wildcards=[1]
        )
        result = config.to_dict()
        assert result["max_members"] == 3
        assert result["filter"] == "test.*"
        assert result["partitioning_wildcards"] == [1]
        assert "max_buffered_msgs" not in result
        assert "max_buffered_bytes" not in result
        assert "members" not in result
        assert "member_mappings" not in result

    def test_from_dict(self):
        """Test creation from dictionary."""
        data = {
            "max_members": 3,
            "filter": "test.*",
            "partitioning_wildcards": [1],
            "max_buffered_msgs": 100,
            "max_buffered_bytes": 1024,
            "members": ["m1", "m2"],
            "member_mappings": [{"member": "m3", "partitions": [0, 1]}],
        }
        config = ElasticConsumerGroupConfig.from_dict(data)
        assert config.max_members == 3
        assert config.filter == "test.*"
        assert config.partitioning_wildcards == [1]
        assert config.max_buffered_msgs == 100
        assert config.max_buffered_bytes == 1024
        assert config.members == ["m1", "m2"]
        assert len(config.member_mappings) == 1
        assert config.member_mappings[0].member == "m3"
        assert config.member_mappings[0].partitions == [0, 1]

    def test_from_dict_minimal(self):
        """Test creation from dictionary with minimal fields."""
        data = {
            "max_members": 3,
            "filter": "test.*",
            "partitioning_wildcards": [1],
        }
        config = ElasticConsumerGroupConfig.from_dict(data)
        assert config.max_members == 3
        assert config.filter == "test.*"
        assert config.partitioning_wildcards == [1]
        assert config.max_buffered_msgs is None
        assert config.max_buffered_bytes is None
        assert config.members == []
        assert config.member_mappings == []


class TestValidateElasticConfig:
    """Test cases for _validate_config function for elastic consumer groups.

    These tests are ported from orbit.go/pcgroups/partitioned_consumer_groups_test.go:75-111
    """

    def test_valid_config_with_members(self):
        """Test validation with valid members list."""
        config = ElasticConsumerGroupConfig(
            max_members=2,
            filter="foo.*",
            partitioning_wildcards=[1],
            members=["m1", "m2"],
        )
        _validate_config(config)  # Should not raise

    def test_valid_config_with_member_mappings(self):
        """Test validation with valid member mappings."""
        config = ElasticConsumerGroupConfig(
            max_members=2,
            filter="foo.*",
            partitioning_wildcards=[1],
            member_mappings=[MemberMapping(member="m1", partitions=[0, 1])],
        )
        _validate_config(config)  # Should not raise

    def test_invalid_both_members_and_mappings(self):
        """Test validation fails when both members and member_mappings are provided."""
        config = ElasticConsumerGroupConfig(
            max_members=2,
            filter="foo.*",
            partitioning_wildcards=[1],
            members=["m1", "m2"],
            member_mappings=[MemberMapping(member="m1", partitions=[0, 1])],
        )
        with pytest.raises(
            ValueError, match="either members or member mappings must be provided"
        ):
            _validate_config(config)

    def test_invalid_duplicate_partitions(self):
        """Test validation fails with duplicate partition numbers."""
        config = ElasticConsumerGroupConfig(
            max_members=2,
            filter="foo.*",
            partitioning_wildcards=[1],
            member_mappings=[MemberMapping(member="m1", partitions=[1, 1])],
        )
        with pytest.raises(
            ValueError, match="partition numbers must be used only once"
        ):
            _validate_config(config)

    def test_invalid_insufficient_partitions(self):
        """Test validation fails when not all partitions are covered."""
        config = ElasticConsumerGroupConfig(
            max_members=2,
            filter="foo.*",
            partitioning_wildcards=[1],
            member_mappings=[MemberMapping(member="m1", partitions=[1])],
        )
        with pytest.raises(
            ValueError,
            match="number of unique partition numbers must be equal to the max",
        ):
            _validate_config(config)

    def test_invalid_too_many_partitions(self):
        """Test validation fails with too many partitions."""
        config = ElasticConsumerGroupConfig(
            max_members=2,
            filter="foo.*",
            partitioning_wildcards=[1],
            member_mappings=[MemberMapping(member="m1", partitions=[0, 1, 2])],
        )
        with pytest.raises(
            ValueError, match="partition numbers must be between 0 and one less"
        ):
            _validate_config(config)

    def test_invalid_partition_out_of_range(self):
        """Test validation fails with partition number out of range."""
        config = ElasticConsumerGroupConfig(
            max_members=2,
            filter="foo.*",
            partitioning_wildcards=[1],
            member_mappings=[MemberMapping(member="m1", partitions=[0, 2])],
        )
        with pytest.raises(
            ValueError, match="partition numbers must be between 0 and one less"
        ):
            _validate_config(config)

    def test_invalid_duplicate_members(self):
        """Test validation fails with duplicate member names."""
        config = ElasticConsumerGroupConfig(
            max_members=2,
            filter="foo.*",
            partitioning_wildcards=[1],
            member_mappings=[
                MemberMapping(member="m1", partitions=[0, 1]),
                MemberMapping(member="m1", partitions=[0, 1]),
            ],
        )
        with pytest.raises(ValueError, match="member names must be unique"):
            _validate_config(config)

    def test_invalid_partition_overlap(self):
        """Test validation fails when partitions overlap between members."""
        config = ElasticConsumerGroupConfig(
            max_members=2,
            filter="foo.*",
            partitioning_wildcards=[1],
            member_mappings=[
                MemberMapping(member="m1", partitions=[0, 1]),
                MemberMapping(member="m2", partitions=[0, 1]),
            ],
        )
        with pytest.raises(
            ValueError, match="partition numbers must be used only once"
        ):
            _validate_config(config)

    def test_invalid_member_mappings_out_of_range_high(self):
        """Test validation fails when member_mappings partitions are out of range (too high)."""
        config = ElasticConsumerGroupConfig(
            max_members=2,
            filter="foo.*",
            partitioning_wildcards=[1],
            member_mappings=[
                MemberMapping(member="m1", partitions=[0]),
                MemberMapping(member="m2", partitions=[1]),
            ],
        )
        _validate_config(config)  # Should not raise

        # Now make max_members 3 but keep the same mappings (missing partition 2)
        config.max_members = 3
        with pytest.raises(
            ValueError,
            match="number of unique partition numbers must be equal to the max",
        ):
            _validate_config(config)

    def test_invalid_partial_overlap(self):
        """Test validation fails with partial partition overlap."""
        config = ElasticConsumerGroupConfig(
            max_members=3,
            filter="foo.*",
            partitioning_wildcards=[1],
            member_mappings=[
                MemberMapping(member="m1", partitions=[0, 2]),
                MemberMapping(member="m2", partitions=[1, 2]),
            ],
        )
        with pytest.raises(
            ValueError, match="partition numbers must be used only once"
        ):
            _validate_config(config)

    def test_valid_complex_member_mappings(self):
        """Test validation with complex but valid member mappings."""
        config = ElasticConsumerGroupConfig(
            max_members=3,
            filter="foo.*",
            partitioning_wildcards=[1],
            member_mappings=[
                MemberMapping(member="m1", partitions=[0, 2]),
                MemberMapping(member="m2", partitions=[1]),
            ],
        )
        _validate_config(config)  # Should not raise

    def test_invalid_max_members_zero(self):
        """Test validation fails with max_members = 0."""
        config = ElasticConsumerGroupConfig(
            max_members=0, filter="foo.*", partitioning_wildcards=[1]
        )
        with pytest.raises(ValueError, match="max number of members must be >= 1"):
            _validate_config(config)

    def test_invalid_max_members_negative(self):
        """Test validation fails with negative max_members."""
        config = ElasticConsumerGroupConfig(
            max_members=-1, filter="foo.*", partitioning_wildcards=[1]
        )
        with pytest.raises(ValueError, match="max number of members must be >= 1"):
            _validate_config(config)

    def test_invalid_filter_no_wildcards(self):
        """Test validation fails when filter has no wildcards."""
        config = ElasticConsumerGroupConfig(
            max_members=2, filter="foo.bar", partitioning_wildcards=[1]
        )
        with pytest.raises(ValueError, match="filter must contain at least one"):
            _validate_config(config)

    def test_invalid_partitioning_wildcards_too_many(self):
        """Test validation fails when partitioning_wildcards exceeds available wildcards."""
        config = ElasticConsumerGroupConfig(
            max_members=2,
            filter="foo.*",
            partitioning_wildcards=[1, 2],  # Only 1 wildcard in filter
        )
        with pytest.raises(
            ValueError, match="number of partitioning wildcards must be between"
        ):
            _validate_config(config)

    def test_invalid_partitioning_wildcards_zero(self):
        """Test validation fails when partitioning_wildcards is empty."""
        config = ElasticConsumerGroupConfig(
            max_members=2, filter="foo.*", partitioning_wildcards=[]
        )
        with pytest.raises(
            ValueError, match="number of partitioning wildcards must be between"
        ):
            _validate_config(config)

    def test_invalid_partitioning_wildcards_index_out_of_range(self):
        """Test validation fails when partitioning wildcard index is out of range."""
        config = ElasticConsumerGroupConfig(
            max_members=2,
            filter="foo.*",
            partitioning_wildcards=[2],  # Only 1 wildcard, so max index is 1
        )
        with pytest.raises(
            ValueError,
            match="partitioning wildcard indexes must be between 1 and the number",
        ):
            _validate_config(config)

    def test_invalid_partitioning_wildcards_duplicate(self):
        """Test validation fails when partitioning wildcards has duplicates."""
        config = ElasticConsumerGroupConfig(
            max_members=2,
            filter="foo.*.bar.*",
            partitioning_wildcards=[1, 1],
        )
        with pytest.raises(
            ValueError, match="partitioning wildcard indexes must be unique"
        ):
            _validate_config(config)

    def test_invalid_partitioning_wildcards_zero_index(self):
        """Test validation fails when partitioning wildcard index is 0."""
        config = ElasticConsumerGroupConfig(
            max_members=2, filter="foo.*", partitioning_wildcards=[0]
        )
        with pytest.raises(
            ValueError,
            match="partitioning wildcard indexes must be between 1 and the number",
        ):
            _validate_config(config)

    def test_valid_multiple_wildcards(self):
        """Test validation with multiple wildcards."""
        config = ElasticConsumerGroupConfig(
            max_members=4,
            filter="foo.*.bar.*.baz.*",
            partitioning_wildcards=[1, 2, 3],
        )
        _validate_config(config)  # Should not raise

    def test_valid_member_mappings_subset(self):
        """Test validation with member_mappings covering all partitions but fewer members than max."""
        config = ElasticConsumerGroupConfig(
            max_members=4,
            filter="foo.*",
            partitioning_wildcards=[1],
            member_mappings=[
                MemberMapping(member="m1", partitions=[0, 1]),
                MemberMapping(member="m2", partitions=[2, 3]),
            ],
        )
        _validate_config(config)  # Should not raise
