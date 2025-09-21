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

"""Tests for common.py module."""

from jetstreampcg.common import (
    MemberMapping,
    compose_key,
    generate_partition_filters,
)


class TestGeneratePartitionFilters:
    """Test cases for generate_partition_filters function.

    These tests are ported from orbit.go/pcgroups/partitioned_consumer_groups_test.go
    """

    def test_partition_distribution_6_members_3_consumers(self):
        """Test partition distribution with 6 partitions and 3 members."""
        members = ["m1", "m2", "m3"]
        max_members = 6

        # Each member should get 2 partitions
        assert generate_partition_filters(members, max_members, [], "m1") == [
            "0.>",
            "1.>",
        ]
        assert generate_partition_filters(members, max_members, [], "m2") == [
            "2.>",
            "3.>",
        ]
        assert generate_partition_filters(members, max_members, [], "m3") == [
            "4.>",
            "5.>",
        ]

    def test_partition_distribution_7_members_3_consumers(self):
        """Test partition distribution with 7 partitions and 3 members.

        With 7 partitions and 3 members, the distribution should be:
        - m1: 0, 1, 6 (3 partitions)
        - m2: 2, 3 (2 partitions)
        - m3: 4, 5 (2 partitions)
        """
        members = ["m1", "m2", "m3"]
        max_members = 7

        assert generate_partition_filters(members, max_members, [], "m1") == [
            "0.>",
            "1.>",
            "6.>",
        ]
        assert generate_partition_filters(members, max_members, [], "m2") == [
            "2.>",
            "3.>",
        ]
        assert generate_partition_filters(members, max_members, [], "m3") == [
            "4.>",
            "5.>",
        ]

    def test_partition_distribution_8_members_3_consumers(self):
        """Test partition distribution with 8 partitions and 3 members.

        With 8 partitions and 3 members, the distribution should be:
        - m1: 0, 1, 6 (3 partitions)
        - m2: 2, 3, 7 (3 partitions)
        - m3: 4, 5 (2 partitions)
        """
        members = ["m1", "m2", "m3"]
        max_members = 8

        assert generate_partition_filters(members, max_members, [], "m1") == [
            "0.>",
            "1.>",
            "6.>",
        ]
        assert generate_partition_filters(members, max_members, [], "m2") == [
            "2.>",
            "3.>",
            "7.>",
        ]
        assert generate_partition_filters(members, max_members, [], "m3") == [
            "4.>",
            "5.>",
        ]

    def test_member_not_in_list(self):
        """Test behavior when requested member is not in the list."""
        members = ["m1", "m2"]
        max_members = 4

        # Member not in list should get empty filters
        assert generate_partition_filters(members, max_members, [], "m3") == []

    def test_empty_members_list(self):
        """Test behavior with empty members list."""
        assert generate_partition_filters([], 4, [], "m1") == []

    def test_deduplicate_members(self):
        """Test that duplicate members are removed."""
        members = ["m1", "m2", "m1", "m2", "m3"]
        max_members = 6

        # Should deduplicate to ["m1", "m2", "m3"] and then sort
        assert generate_partition_filters(members, max_members, [], "m1") == [
            "0.>",
            "1.>",
        ]
        assert generate_partition_filters(members, max_members, [], "m2") == [
            "2.>",
            "3.>",
        ]
        assert generate_partition_filters(members, max_members, [], "m3") == [
            "4.>",
            "5.>",
        ]

    def test_max_members_limit(self):
        """Test that member list is truncated to max_members."""
        members = ["m1", "m2", "m3", "m4", "m5"]
        max_members = 3

        # Should only use first 3 members after sorting
        assert generate_partition_filters(members, max_members, [], "m1") == ["0.>"]
        assert generate_partition_filters(members, max_members, [], "m2") == ["1.>"]
        assert generate_partition_filters(members, max_members, [], "m3") == ["2.>"]
        # m4 and m5 are ignored
        assert generate_partition_filters(members, max_members, [], "m4") == []
        assert generate_partition_filters(members, max_members, [], "m5") == []

    def test_member_mappings_when_members_empty(self):
        """Test that member_mappings is used when members list is empty."""
        member_mappings = [
            MemberMapping(member="m1", partitions=[0, 2]),
            MemberMapping(member="m2", partitions=[1, 3]),
        ]
        max_members = 4

        # When members is empty, member_mappings is used
        assert generate_partition_filters([], max_members, member_mappings, "m1") == [
            "0.>",
            "2.>",
        ]
        assert generate_partition_filters([], max_members, member_mappings, "m2") == [
            "1.>",
            "3.>",
        ]

    def test_members_takes_precedence_over_member_mappings(self):
        """Test that members list takes precedence over member_mappings when both are provided."""
        members = ["m1", "m2"]
        member_mappings = [
            MemberMapping(member="m1", partitions=[0, 2]),
            MemberMapping(member="m2", partitions=[1, 3]),
        ]
        max_members = 4

        # When both are provided, members list is used and member_mappings is ignored
        assert generate_partition_filters(
            members, max_members, member_mappings, "m1"
        ) == ["0.>", "1.>"]
        assert generate_partition_filters(
            members, max_members, member_mappings, "m2"
        ) == ["2.>", "3.>"]

    def test_member_mappings_not_found(self):
        """Test member_mappings when member is not found."""
        member_mappings = [
            MemberMapping(member="m1", partitions=[0, 1]),
        ]
        max_members = 4

        assert generate_partition_filters([], max_members, member_mappings, "m2") == []

    def test_single_partition(self):
        """Test with single partition and single member."""
        members = ["m1"]
        max_members = 1

        assert generate_partition_filters(members, max_members, [], "m1") == ["0.>"]

    def test_members_sorted(self):
        """Test that members are sorted before distribution."""
        members = ["m3", "m1", "m2"]
        max_members = 3

        # After sorting: ["m1", "m2", "m3"]
        assert generate_partition_filters(members, max_members, [], "m1") == ["0.>"]
        assert generate_partition_filters(members, max_members, [], "m2") == ["1.>"]
        assert generate_partition_filters(members, max_members, [], "m3") == ["2.>"]


class TestComposeKey:
    """Test cases for compose_key function."""

    def test_compose_key(self):
        """Test key composition."""
        assert compose_key("stream1", "group1") == "stream1.group1"
        assert compose_key("my.stream", "my.group") == "my.stream.my.group"
        assert compose_key("", "") == "."


class TestMemberMapping:
    """Test cases for MemberMapping dataclass."""

    def test_member_mapping_creation(self):
        """Test creating MemberMapping instances."""
        mapping = MemberMapping(member="m1", partitions=[0, 1, 2])
        assert mapping.member == "m1"
        assert mapping.partitions == [0, 1, 2]

    def test_member_mapping_empty_partitions(self):
        """Test MemberMapping with empty partitions."""
        mapping = MemberMapping(member="m1", partitions=[])
        assert mapping.member == "m1"
        assert mapping.partitions == []
