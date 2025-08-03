"""Tests for topic data models."""

import pytest
from pydantic import ValidationError

from src.models.topic import (
    TopicConfig,
    TopicInfo,
    TopicListResponse,
    TopicCreateRequest,
    TopicDeleteRequest,
    TopicMetadata,
)


@pytest.mark.unit
class TestTopicConfig:
    """Test TopicConfig model."""

    def test_topic_config_minimal(self):
        """Test TopicConfig with minimal fields."""
        config = TopicConfig(name="test-topic")
        assert config.name == "test-topic"
        assert config.partitions == 1
        assert config.replication_factor == 1
        assert config.config == {}

    def test_topic_config_complete(self):
        """Test TopicConfig with all fields."""
        topic_config = {
            "retention.ms": "86400000",
            "max.message.bytes": "1000000"
        }
        config = TopicConfig(
            name="my-topic",
            partitions=3,
            replication_factor=2,
            config=topic_config
        )
        assert config.name == "my-topic"
        assert config.partitions == 3
        assert config.replication_factor == 2
        assert config.config == topic_config

    def test_topic_name_validation(self):
        """Test topic name validation."""
        # Empty name should fail
        with pytest.raises(ValidationError) as exc_info:
            TopicConfig(name="")
        assert "at least 1 character" in str(exc_info.value) or "Topic name cannot be empty" in str(exc_info.value)

        # Invalid characters should fail
        invalid_names = ["topic/with/slash", "topic:with:colon", "topic*with*star"]
        for name in invalid_names:
            with pytest.raises(ValidationError) as exc_info:
                TopicConfig(name=name)
            assert "invalid characters" in str(exc_info.value)

        # Reserved names should fail
        with pytest.raises(ValidationError) as exc_info:
            TopicConfig(name=".")
        assert "cannot be '.' or '..'" in str(exc_info.value)

        with pytest.raises(ValidationError) as exc_info:
            TopicConfig(name="..")
        assert "cannot be '.' or '..'" in str(exc_info.value)

        # Internal topic prefix should fail
        with pytest.raises(ValidationError) as exc_info:
            TopicConfig(name="__internal-topic")
        assert "reserved for internal topics" in str(exc_info.value)

        # Valid names should pass
        valid_names = ["test-topic", "my_topic", "topic123", "a" * 249]
        for name in valid_names:
            config = TopicConfig(name=name)
            assert config.name == name

    def test_topic_name_length_validation(self):
        """Test topic name length validation."""
        # Too long name should fail
        with pytest.raises(ValidationError):
            TopicConfig(name="a" * 250)

        # Maximum length should pass
        config = TopicConfig(name="a" * 249)
        assert len(config.name) == 249

    def test_partitions_validation(self):
        """Test partitions validation."""
        # Zero partitions should fail
        with pytest.raises(ValidationError):
            TopicConfig(name="test", partitions=0)

        # Negative partitions should fail
        with pytest.raises(ValidationError):
            TopicConfig(name="test", partitions=-1)

        # Too many partitions should fail
        with pytest.raises(ValidationError):
            TopicConfig(name="test", partitions=1001)

        # Valid partitions
        config = TopicConfig(name="test", partitions=100)
        assert config.partitions == 100

    def test_replication_factor_validation(self):
        """Test replication factor validation."""
        # Zero replication should fail
        with pytest.raises(ValidationError):
            TopicConfig(name="test", replication_factor=0)

        # Too high replication should fail
        with pytest.raises(ValidationError):
            TopicConfig(name="test", replication_factor=11)

        # Valid replication factor
        config = TopicConfig(name="test", replication_factor=3)
        assert config.replication_factor == 3

    def test_config_validation(self):
        """Test topic config validation."""
        # Valid numeric configs
        valid_config = {
            "retention.ms": "86400000",
            "max.message.bytes": "1000000",
            "segment.bytes": "1073741824"
        }
        config = TopicConfig(name="test", config=valid_config)
        assert config.config == valid_config

        # Invalid numeric config should fail
        with pytest.raises(ValidationError) as exc_info:
            TopicConfig(name="test", config={"retention.ms": "invalid"})
        assert "must be a valid integer" in str(exc_info.value)

        # Negative numeric config should fail
        with pytest.raises(ValidationError) as exc_info:
            TopicConfig(name="test", config={"retention.ms": "-1"})
        assert "must be non-negative" in str(exc_info.value) or "valid integer" in str(exc_info.value)


@pytest.mark.unit
class TestTopicInfo:
    """Test TopicInfo model."""

    def test_topic_info_minimal(self):
        """Test TopicInfo with minimal fields."""
        info = TopicInfo(
            name="test-topic",
            partitions=1,
            replication_factor=1
        )
        assert info.name == "test-topic"
        assert info.partitions == 1
        assert info.replication_factor == 1
        assert info.size_bytes is None
        assert info.config == {}

    def test_topic_info_complete(self):
        """Test TopicInfo with all fields."""
        partition_details = [
            {"partition": 0, "leader": 1, "replicas": [1]},
            {"partition": 1, "leader": 1, "replicas": [1]}
        ]
        info = TopicInfo(
            name="my-topic",
            partitions=2,
            replication_factor=1,
            size_bytes=1024000,
            config={"retention.ms": "86400000"},
            partition_details=partition_details
        )
        assert info.size_bytes == 1024000
        assert info.partition_details == partition_details

    def test_topic_info_validation(self):
        """Test TopicInfo validation."""
        # Negative size should fail
        with pytest.raises(ValidationError):
            TopicInfo(
                name="test",
                partitions=1,
                replication_factor=1,
                size_bytes=-1
            )

        # Zero partitions should fail
        with pytest.raises(ValidationError):
            TopicInfo(
                name="test",
                partitions=0,
                replication_factor=1
            )


@pytest.mark.unit
class TestTopicListResponse:
    """Test TopicListResponse model."""

    def test_topic_list_response_empty(self):
        """Test empty TopicListResponse."""
        response = TopicListResponse()
        assert response.topics == []
        assert response.total_count == 0

    def test_topic_list_response_with_topics(self):
        """Test TopicListResponse with topics."""
        topics = [
            TopicInfo(name="topic1", partitions=1, replication_factor=1),
            TopicInfo(name="topic2", partitions=2, replication_factor=1)
        ]
        response = TopicListResponse(topics=topics, total_count=2)
        assert len(response.topics) == 2
        assert response.total_count == 2


@pytest.mark.unit
class TestTopicCreateRequest:
    """Test TopicCreateRequest model."""

    def test_topic_create_request(self):
        """Test TopicCreateRequest inherits from TopicConfig."""
        request = TopicCreateRequest(name="new-topic", partitions=3)
        assert request.name == "new-topic"
        assert request.partitions == 3
        assert isinstance(request, TopicConfig)


@pytest.mark.unit
class TestTopicDeleteRequest:
    """Test TopicDeleteRequest model."""

    def test_topic_delete_request_default(self):
        """Test TopicDeleteRequest with default values."""
        request = TopicDeleteRequest()
        assert request.force is False

    def test_topic_delete_request_force(self):
        """Test TopicDeleteRequest with force=True."""
        request = TopicDeleteRequest(force=True)
        assert request.force is True


@pytest.mark.unit
class TestTopicMetadata:
    """Test TopicMetadata model."""

    def test_topic_metadata_minimal(self):
        """Test TopicMetadata with minimal fields."""
        metadata = TopicMetadata(name="test-topic")
        assert metadata.name == "test-topic"
        assert metadata.partitions == []
        assert metadata.config == {}
        assert metadata.consumer_groups == []
        assert metadata.message_count is None

    def test_topic_metadata_complete(self):
        """Test TopicMetadata with all fields."""
        partitions = [
            {"partition": 0, "leader": 1, "replicas": [1], "isr": [1]},
            {"partition": 1, "leader": 1, "replicas": [1], "isr": [1]}
        ]
        metadata = TopicMetadata(
            name="my-topic",
            partitions=partitions,
            config={"retention.ms": "86400000"},
            consumer_groups=["group1", "group2"],
            message_count=1000,
            earliest_offset=0,
            latest_offset=999
        )
        assert metadata.partitions == partitions
        assert metadata.consumer_groups == ["group1", "group2"]
        assert metadata.message_count == 1000
        assert metadata.earliest_offset == 0
        assert metadata.latest_offset == 999

    def test_topic_metadata_validation(self):
        """Test TopicMetadata validation."""
        # Negative message count should fail
        with pytest.raises(ValidationError):
            TopicMetadata(name="test", message_count=-1)

        # Negative offsets should fail
        with pytest.raises(ValidationError):
            TopicMetadata(name="test", earliest_offset=-1)

        with pytest.raises(ValidationError):
            TopicMetadata(name="test", latest_offset=-1)