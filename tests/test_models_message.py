"""Tests for message data models."""

import pytest
from pydantic import ValidationError

from src.models.message import (
    Message,
    ProduceRequest,
    ProduceResult,
    ConsumeRequest,
    ConsumeResponse,
    ConsumerGroup,
    ConsumerGroupListResponse,
)


@pytest.mark.unit
class TestMessage:
    """Test Message model."""

    def test_message_creation(self):
        """Test Message creation."""
        message = Message(
            topic="test-topic",
            partition=0,
            offset=123,
            key="user1",
            value={"action": "login", "user_id": 1},
            timestamp=1642234567000
        )
        assert message.topic == "test-topic"
        assert message.partition == 0
        assert message.offset == 123
        assert message.key == "user1"
        assert message.value == {"action": "login", "user_id": 1}
        assert message.timestamp == 1642234567000
        assert message.headers is None

    def test_message_with_headers(self):
        """Test Message with headers."""
        headers = {"source": "api", "version": "1.0"}
        message = Message(
            topic="test-topic",
            partition=0,
            offset=123,
            value={"data": "test"},
            timestamp=1642234567000,
            headers=headers
        )
        assert message.headers == headers

    def test_message_validation(self):
        """Test Message validation."""
        # Empty topic should fail
        with pytest.raises(ValidationError):
            Message(
                topic="",
                partition=0,
                offset=0,
                value={"test": "data"},
                timestamp=1642234567000
            )

        # Negative partition should fail
        with pytest.raises(ValidationError):
            Message(
                topic="test",
                partition=-1,
                offset=0,
                value={"test": "data"},
                timestamp=1642234567000
            )

        # Negative offset should fail
        with pytest.raises(ValidationError):
            Message(
                topic="test",
                partition=0,
                offset=-1,
                value={"test": "data"},
                timestamp=1642234567000
            )

    def test_message_timestamp_validation(self):
        """Test Message timestamp validation."""
        # Too old timestamp should fail
        with pytest.raises(ValidationError) as exc_info:
            Message(
                topic="test",
                partition=0,
                offset=0,
                value={"test": "data"},
                timestamp=999999999999  # Before 2001
            )
        assert "too old" in str(exc_info.value)

        # Too future timestamp should fail
        with pytest.raises(ValidationError) as exc_info:
            Message(
                topic="test",
                partition=0,
                offset=0,
                value={"test": "data"},
                timestamp=10000000000000  # After 2286
            )
        assert "too far in the future" in str(exc_info.value)

        # Valid timestamp should pass
        message = Message(
            topic="test",
            partition=0,
            offset=0,
            value={"test": "data"},
            timestamp=1642234567000  # Valid timestamp
        )
        assert message.timestamp == 1642234567000


@pytest.mark.unit
class TestProduceRequest:
    """Test ProduceRequest model."""

    def test_produce_request_minimal(self):
        """Test ProduceRequest with minimal fields."""
        request = ProduceRequest(
            topic="test-topic",
            value={"message": "hello"}
        )
        assert request.topic == "test-topic"
        assert request.value == {"message": "hello"}
        assert request.key is None
        assert request.partition is None
        assert request.headers is None

    def test_produce_request_complete(self):
        """Test ProduceRequest with all fields."""
        headers = {"source": "test"}
        request = ProduceRequest(
            topic="my-topic",
            key="user123",
            value={"action": "purchase", "amount": 99.99},
            partition=2,
            headers=headers
        )
        assert request.topic == "my-topic"
        assert request.key == "user123"
        assert request.value == {"action": "purchase", "amount": 99.99}
        assert request.partition == 2
        assert request.headers == headers

    def test_produce_request_validation(self):
        """Test ProduceRequest validation."""
        # Empty topic should fail
        with pytest.raises(ValidationError):
            ProduceRequest(topic="", value={"test": "data"})

        # Non-dict value should fail
        with pytest.raises(ValidationError) as exc_info:
            ProduceRequest(topic="test", value="string-value")
        assert "valid dictionary" in str(exc_info.value) or "must be a JSON object" in str(exc_info.value)

        # Negative partition should fail
        with pytest.raises(ValidationError):
            ProduceRequest(
                topic="test",
                value={"test": "data"},
                partition=-1
            )


@pytest.mark.unit
class TestProduceResult:
    """Test ProduceResult model."""

    def test_produce_result_creation(self):
        """Test ProduceResult creation."""
        result = ProduceResult(
            topic="test-topic",
            partition=0,
            offset=456,
            timestamp=1642234567000
        )
        assert result.topic == "test-topic"
        assert result.partition == 0
        assert result.offset == 456
        assert result.timestamp == 1642234567000
        assert result.key is None

    def test_produce_result_with_key(self):
        """Test ProduceResult with key."""
        result = ProduceResult(
            topic="test-topic",
            partition=0,
            offset=456,
            key="user123",
            timestamp=1642234567000
        )
        assert result.key == "user123"

    def test_produce_result_validation(self):
        """Test ProduceResult validation."""
        # Negative partition should fail
        with pytest.raises(ValidationError):
            ProduceResult(
                topic="test",
                partition=-1,
                offset=0,
                timestamp=1642234567000
            )

        # Negative offset should fail
        with pytest.raises(ValidationError):
            ProduceResult(
                topic="test",
                partition=0,
                offset=-1,
                timestamp=1642234567000
            )


@pytest.mark.unit
class TestConsumeRequest:
    """Test ConsumeRequest model."""

    def test_consume_request_minimal(self):
        """Test ConsumeRequest with minimal fields."""
        request = ConsumeRequest(
            topic="test-topic",
            consumer_group="test-group"
        )
        assert request.topic == "test-topic"
        assert request.consumer_group == "test-group"
        assert request.max_messages == 10
        assert request.timeout_ms == 5000
        assert request.from_beginning is False
        assert request.partition is None

    def test_consume_request_complete(self):
        """Test ConsumeRequest with all fields."""
        request = ConsumeRequest(
            topic="my-topic",
            consumer_group="my-group",
            max_messages=50,
            timeout_ms=10000,
            from_beginning=True,
            partition=1
        )
        assert request.max_messages == 50
        assert request.timeout_ms == 10000
        assert request.from_beginning is True
        assert request.partition == 1

    def test_consume_request_validation(self):
        """Test ConsumeRequest validation."""
        # Empty topic should fail
        with pytest.raises(ValidationError):
            ConsumeRequest(topic="", consumer_group="group")

        # Empty consumer group should fail
        with pytest.raises(ValidationError):
            ConsumeRequest(topic="topic", consumer_group="")

        # Whitespace-only consumer group should fail
        with pytest.raises(ValidationError) as exc_info:
            ConsumeRequest(topic="topic", consumer_group="   ")
        assert "cannot be empty or whitespace" in str(exc_info.value)

        # Invalid characters in consumer group should fail
        with pytest.raises(ValidationError) as exc_info:
            ConsumeRequest(topic="topic", consumer_group="group/with/slash")
        assert "invalid characters" in str(exc_info.value)

        # Too few max_messages should fail
        with pytest.raises(ValidationError):
            ConsumeRequest(
                topic="topic",
                consumer_group="group",
                max_messages=0
            )

        # Too many max_messages should fail
        with pytest.raises(ValidationError):
            ConsumeRequest(
                topic="topic",
                consumer_group="group",
                max_messages=1001
            )

        # Too low timeout should fail
        with pytest.raises(ValidationError):
            ConsumeRequest(
                topic="topic",
                consumer_group="group",
                timeout_ms=50
            )

        # Too high timeout should fail
        with pytest.raises(ValidationError):
            ConsumeRequest(
                topic="topic",
                consumer_group="group",
                timeout_ms=70000
            )

        # Negative partition should fail
        with pytest.raises(ValidationError):
            ConsumeRequest(
                topic="topic",
                consumer_group="group",
                partition=-1
            )


@pytest.mark.unit
class TestConsumeResponse:
    """Test ConsumeResponse model."""

    def test_consume_response_empty(self):
        """Test empty ConsumeResponse."""
        response = ConsumeResponse(
            consumer_group="test-group",
            topic="test-topic"
        )
        assert response.messages == []
        assert response.consumer_group == "test-group"
        assert response.topic == "test-topic"
        assert response.total_consumed == 0
        assert response.has_more is False

    def test_consume_response_with_messages(self):
        """Test ConsumeResponse with messages."""
        messages = [
            Message(
                topic="test-topic",
                partition=0,
                offset=1,
                value={"data": "test1"},
                timestamp=1642234567000
            ),
            Message(
                topic="test-topic",
                partition=0,
                offset=2,
                value={"data": "test2"},
                timestamp=1642234568000
            )
        ]
        response = ConsumeResponse(
            messages=messages,
            consumer_group="test-group",
            topic="test-topic",
            total_consumed=2,
            has_more=True
        )
        assert len(response.messages) == 2
        assert response.total_consumed == 2
        assert response.has_more is True


@pytest.mark.unit
class TestConsumerGroup:
    """Test ConsumerGroup model."""

    def test_consumer_group_minimal(self):
        """Test ConsumerGroup with minimal fields."""
        group = ConsumerGroup(
            group_id="test-group",
            state="Stable"
        )
        assert group.group_id == "test-group"
        assert group.state == "Stable"
        assert group.members == []
        assert group.coordinator is None
        assert group.partition_assignments == {}

    def test_consumer_group_complete(self):
        """Test ConsumerGroup with all fields."""
        members = [
            {"member_id": "consumer1", "client_id": "client1"},
            {"member_id": "consumer2", "client_id": "client2"}
        ]
        assignments = {
            "topic1": [0, 1],
            "topic2": [0]
        }
        group = ConsumerGroup(
            group_id="my-group",
            state="Stable",
            members=members,
            coordinator="broker1",
            partition_assignments=assignments
        )
        assert group.members == members
        assert group.coordinator == "broker1"
        assert group.partition_assignments == assignments


@pytest.mark.unit
class TestConsumerGroupListResponse:
    """Test ConsumerGroupListResponse model."""

    def test_consumer_group_list_response_empty(self):
        """Test empty ConsumerGroupListResponse."""
        response = ConsumerGroupListResponse()
        assert response.consumer_groups == []
        assert response.total_count == 0

    def test_consumer_group_list_response_with_groups(self):
        """Test ConsumerGroupListResponse with groups."""
        groups = [
            ConsumerGroup(group_id="group1", state="Stable"),
            ConsumerGroup(group_id="group2", state="Rebalancing")
        ]
        response = ConsumerGroupListResponse(
            consumer_groups=groups,
            total_count=2
        )
        assert len(response.consumer_groups) == 2
        assert response.total_count == 2