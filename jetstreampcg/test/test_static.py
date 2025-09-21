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

"""Tests for static.py module."""

import pytest

from jetstreampcg.common import MemberMapping
from jetstreampcg.static import StaticConsumerGroupConfig, validate_static_config


class TestStaticConsumerGroupConfig:
    """Test cases for StaticConsumerGroupConfig class."""

    def test_is_in_membership_with_members(self):
        """Test is_in_membership with members list."""
        config = StaticConsumerGroupConfig(
            max_members=3, filter="test.*", members=["m1", "m2", "m3"]
        )
        assert config.is_in_membership("m1") is True
        assert config.is_in_membership("m2") is True
        assert config.is_in_membership("m3") is True
        assert config.is_in_membership("m4") is False

    def test_is_in_membership_with_member_mappings(self):
        """Test is_in_membership with member mappings."""
        config = StaticConsumerGroupConfig(
            max_members=3,
            filter="test.*",
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
        config = StaticConsumerGroupConfig(
            max_members=3,
            filter="test.*",
            members=["m1", "m2"],
            member_mappings=[MemberMapping(member="m3", partitions=[0, 1])],
        )
        result = config.to_dict()
        assert result["max_members"] == 3
        assert result["filter"] == "test.*"
        assert result["members"] == ["m1", "m2"]
        assert result["member_mappings"] == [{"member": "m3", "partitions": [0, 1]}]

    def test_to_dict_minimal(self):
        """Test conversion to dictionary with minimal fields."""
        config = StaticConsumerGroupConfig(max_members=3)
        result = config.to_dict()
        assert result == {"max_members": 3, "filter": ""}
        assert "members" not in result
        assert "member_mappings" not in result

    def test_from_dict(self):
        """Test creation from dictionary."""
        data = {
            "max_members": 3,
            "filter": "test.*",
            "members": ["m1", "m2"],
            "member_mappings": [{"member": "m3", "partitions": [0, 1]}],
        }
        config = StaticConsumerGroupConfig.from_dict(data)
        assert config.max_members == 3
        assert config.filter == "test.*"
        assert config.members == ["m1", "m2"]
        assert len(config.member_mappings) == 1
        assert config.member_mappings[0].member == "m3"
        assert config.member_mappings[0].partitions == [0, 1]

    def test_from_dict_minimal(self):
        """Test creation from dictionary with minimal fields."""
        data = {"max_members": 3}
        config = StaticConsumerGroupConfig.from_dict(data)
        assert config.max_members == 3
        assert config.filter == ""
        assert config.members == []
        assert config.member_mappings == []


class TestValidateStaticConfig:
    """Test cases for validate_static_config function."""

    def test_valid_config_with_members(self):
        """Test validation with valid members list."""
        config = StaticConsumerGroupConfig(
            max_members=3, filter="test.*", members=["m1", "m2", "m3"]
        )
        validate_static_config(config)  # Should not raise

    def test_valid_config_with_member_mappings(self):
        """Test validation with valid member mappings."""
        config = StaticConsumerGroupConfig(
            max_members=3,
            member_mappings=[
                MemberMapping(member="m1", partitions=[0]),
                MemberMapping(member="m2", partitions=[1]),
                MemberMapping(member="m3", partitions=[2]),
            ],
        )
        validate_static_config(config)  # Should not raise

    def test_invalid_max_members(self):
        """Test validation fails with invalid max_members."""
        config = StaticConsumerGroupConfig(max_members=0)
        with pytest.raises(ValueError, match="max number of members must be >= 1"):
            validate_static_config(config)

    def test_both_members_and_mappings(self):
        """Test validation fails with both members and member_mappings."""
        config = StaticConsumerGroupConfig(
            max_members=3,
            members=["m1"],
            member_mappings=[MemberMapping(member="m2", partitions=[0])],
        )
        with pytest.raises(
            ValueError, match="either members or member mappings must be provided"
        ):
            validate_static_config(config)

    def test_member_mappings_too_many(self):
        """Test validation fails with too many member mappings."""
        config = StaticConsumerGroupConfig(
            max_members=2,
            member_mappings=[
                MemberMapping(member="m1", partitions=[0]),
                MemberMapping(member="m2", partitions=[1]),
                MemberMapping(member="m3", partitions=[]),
            ],
        )
        with pytest.raises(
            ValueError,
            match="number of member mappings must be between 1 and the max number",
        ):
            validate_static_config(config)

    def test_duplicate_member_names(self):
        """Test validation fails with duplicate member names in mappings."""
        config = StaticConsumerGroupConfig(
            max_members=3,
            member_mappings=[
                MemberMapping(member="m1", partitions=[0]),
                MemberMapping(member="m1", partitions=[1]),
                MemberMapping(member="m2", partitions=[2]),
            ],
        )
        with pytest.raises(ValueError, match="member names must be unique"):
            validate_static_config(config)

    def test_duplicate_partitions(self):
        """Test validation fails with duplicate partition numbers."""
        config = StaticConsumerGroupConfig(
            max_members=3,
            member_mappings=[
                MemberMapping(member="m1", partitions=[0, 1]),
                MemberMapping(member="m2", partitions=[1, 2]),
            ],
        )
        with pytest.raises(
            ValueError, match="partition numbers must be used only once"
        ):
            validate_static_config(config)

    def test_partition_out_of_range(self):
        """Test validation fails with partition number out of range."""
        config = StaticConsumerGroupConfig(
            max_members=3,
            member_mappings=[
                MemberMapping(member="m1", partitions=[0]),
                MemberMapping(member="m2", partitions=[3]),  # Out of range
                MemberMapping(member="m3", partitions=[2]),
            ],
        )
        with pytest.raises(
            ValueError, match="partition numbers must be between 0 and one less"
        ):
            validate_static_config(config)

    def test_missing_partitions(self):
        """Test validation fails when not all partitions are covered."""
        config = StaticConsumerGroupConfig(
            max_members=3,
            member_mappings=[
                MemberMapping(member="m1", partitions=[0]),
                MemberMapping(member="m2", partitions=[2]),
                # Missing partition 1
            ],
        )
        with pytest.raises(
            ValueError,
            match="number of unique partition numbers must be equal to the max",
        ):
            validate_static_config(config)

    def test_valid_complex_member_mappings(self):
        """Test validation with complex but valid member mappings."""
        config = StaticConsumerGroupConfig(
            max_members=6,
            member_mappings=[
                MemberMapping(member="m1", partitions=[0, 1, 2]),
                MemberMapping(member="m2", partitions=[3, 4, 5]),
            ],
        )
        validate_static_config(config)  # Should not raise
