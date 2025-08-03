"""Unit tests for MessageManager."""

import pytest
import json
from unittest.mock import AsyncMock, Mock, patch
from datetime import datetime
import httpx

from src.services.message_manager import (
    MessageManager, MessageManagerError, KafkaRestProxyNotAvailableError
)
from src.models.message import (
    ProduceRequest, ProduceResult, ConsumeRequest, ConsumeResponse,
    Message, ConsumerGroup, ConsumerGroupListResponse
)


@pytest.mark.unit
class TestMessageManager:
    """Test cases for MessageManager class."""

    @pytest.fixture
    def message_manager(self):
        """Create a MessageManager instance for testing."""
        return MessageManager(
            rest_proxy_url="http://localhost:8082",
            request_timeout=30
        )

    @pytest.fixture
    def mock_http_client(self):
        """Create a mock HTTP client."""
        return AsyncMock(spec=httpx.AsyncClient)

    @pytest.fixture
    def sample_produce_request(self):
        """Create a sample produce request."""
        return ProduceRequest(
            topic="test-topic",
            key="test-key",
            value={"message": "Hello, Kafka!", "timestamp": 1642234567},
            headers={"source": "test"}
        )

    @pytest.fixture
    def sample_consume_request(self):
        """Create a sample consume request."""
        return ConsumeRequest(
            topic="test-topic",
            consumer_group="test-group",
            max_messages=10,
            timeout_ms=5000
        )

    def test_init(self):
        """Test MessageManager initialization."""
        manager = MessageManager(
            rest_proxy_url="http://localhost:8082",
            request_timeout=60
        )
        
        assert manager.rest_proxy_url == "http://localhost:8082"
        assert manager.request_timeout == 60
        assert manager._http_client is None

    def test_init_strips_trailing_slash(self):
        """Test that trailing slash is stripped from REST proxy URL."""
        manager = MessageManager(rest_proxy_url="http://localhost:8082/")
        assert manager.rest_proxy_url == "http://localhost:8082"

    @pytest.mark.asyncio
    async def test_http_client_property(self, message_manager):
        """Test HTTP client property creates client when needed."""
        assert message_manager._http_client is None
        
        client = message_manager.http_client
        assert client is not None
        assert isinstance(client, httpx.AsyncClient)
        assert message_manager._http_client is client
        
        # Should return same client on subsequent calls
        client2 = message_manager.http_client
        assert client2 is client

    @pytest.mark.asyncio
    async def test_check_rest_proxy_health_success(self, message_manager, mock_http_client):
        """Test successful REST proxy health check."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_http_client.get.return_value = mock_response
        
        message_manager._http_client = mock_http_client
        
        result = await message_manager._check_rest_proxy_health()
        
        assert result is True
        mock_http_client.get.assert_called_once_with("http://localhost:8082/")

    @pytest.mark.asyncio
    async def test_check_rest_proxy_health_failure(self, message_manager, mock_http_client):
        """Test failed REST proxy health check."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_http_client.get.return_value = mock_response
        
        message_manager._http_client = mock_http_client
        
        result = await message_manager._check_rest_proxy_health()
        
        assert result is False

    @pytest.mark.asyncio
    async def test_check_rest_proxy_health_exception(self, message_manager, mock_http_client):
        """Test REST proxy health check with exception."""
        mock_http_client.get.side_effect = httpx.ConnectError("Connection failed")
        message_manager._http_client = mock_http_client
        
        result = await message_manager._check_rest_proxy_health()
        
        assert result is False

    @pytest.mark.asyncio
    async def test_produce_message_success(self, message_manager, mock_http_client, sample_produce_request):
        """Test successful message production."""
        # Mock health check
        with patch.object(message_manager, '_check_rest_proxy_health', return_value=True):
            # Mock produce response
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "offsets": [
                    {
                        "partition": 0,
                        "offset": 123,
                        "error_code": None
                    }
                ]
            }
            mock_http_client.post.return_value = mock_response
            message_manager._http_client = mock_http_client
            
            result = await message_manager.produce_message(sample_produce_request)
            
            assert isinstance(result, ProduceResult)
            assert result.topic == "test-topic"
            assert result.partition == 0
            assert result.offset == 123
            assert result.key == "test-key"
            
            # Verify the request was made correctly
            mock_http_client.post.assert_called_once()
            call_args = mock_http_client.post.call_args
            assert call_args[0][0] == "http://localhost:8082/topics/test-topic"
            
            # Check payload
            payload = call_args[1]["json"]
            assert "records" in payload
            assert len(payload["records"]) == 1
            record = payload["records"][0]
            assert record["key"] == "test-key"
            assert record["value"] == {"message": "Hello, Kafka!", "timestamp": 1642234567}
            assert record["headers"] == {"source": "test"}

    @pytest.mark.asyncio
    async def test_produce_message_rest_proxy_unavailable(self, message_manager, sample_produce_request):
        """Test message production when REST proxy is unavailable."""
        with patch.object(message_manager, '_check_rest_proxy_health', return_value=False):
            with pytest.raises(KafkaRestProxyNotAvailableError):
                await message_manager.produce_message(sample_produce_request)

    @pytest.mark.asyncio
    async def test_produce_message_http_error(self, message_manager, mock_http_client, sample_produce_request):
        """Test message production with HTTP error."""
        with patch.object(message_manager, '_check_rest_proxy_health', return_value=True):
            mock_response = Mock()
            mock_response.status_code = 400
            mock_response.text = "Bad Request"
            mock_response.json.side_effect = ValueError("No JSON")
            mock_http_client.post.return_value = mock_response
            message_manager._http_client = mock_http_client
            
            with pytest.raises(MessageManagerError, match="Failed to produce message: HTTP 400"):
                await message_manager.produce_message(sample_produce_request)

    @pytest.mark.asyncio
    async def test_produce_message_kafka_error(self, message_manager, mock_http_client, sample_produce_request):
        """Test message production with Kafka error in response."""
        with patch.object(message_manager, '_check_rest_proxy_health', return_value=True):
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "offsets": [
                    {
                        "partition": 0,
                        "offset": -1,
                        "error_code": 2,
                        "error": "INVALID_TOPIC_EXCEPTION"
                    }
                ]
            }
            mock_http_client.post.return_value = mock_response
            message_manager._http_client = mock_http_client
            
            with pytest.raises(MessageManagerError, match="Kafka error: INVALID_TOPIC_EXCEPTION"):
                await message_manager.produce_message(sample_produce_request)

    @pytest.mark.asyncio
    async def test_produce_message_minimal_request(self, message_manager, mock_http_client):
        """Test message production with minimal request (no key, headers, partition)."""
        request = ProduceRequest(
            topic="test-topic",
            value={"message": "Hello"}
        )
        
        with patch.object(message_manager, '_check_rest_proxy_health', return_value=True):
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "offsets": [{"partition": 0, "offset": 456, "error_code": None}]
            }
            mock_http_client.post.return_value = mock_response
            message_manager._http_client = mock_http_client
            
            result = await message_manager.produce_message(request)
            
            assert result.topic == "test-topic"
            assert result.key is None
            
            # Check that payload doesn't include optional fields
            call_args = mock_http_client.post.call_args
            payload = call_args[1]["json"]
            record = payload["records"][0]
            assert "key" not in record
            assert "partition" not in record
            assert "headers" not in record

    @pytest.mark.asyncio
    async def test_consume_messages_success(self, message_manager, mock_http_client, sample_consume_request):
        """Test successful message consumption."""
        with patch.object(message_manager, '_check_rest_proxy_health', return_value=True):
            # Mock the consumer workflow
            with patch.object(message_manager, '_create_consumer_instance', return_value="http://localhost:8082/consumers/test-group/instances/consumer-123") as mock_create:
                with patch.object(message_manager, '_subscribe_consumer') as mock_subscribe:
                    with patch.object(message_manager, '_fetch_messages') as mock_fetch:
                        with patch.object(message_manager, '_delete_consumer_instance') as mock_delete:
                            
                            # Mock fetched messages
                            mock_fetch.return_value = [
                                {
                                    "topic": "test-topic",
                                    "partition": 0,
                                    "offset": 100,
                                    "key": "key1",
                                    "value": {"message": "Hello 1"},
                                    "timestamp": 1642234567000
                                },
                                {
                                    "topic": "test-topic",
                                    "partition": 0,
                                    "offset": 101,
                                    "key": "key2",
                                    "value": {"message": "Hello 2"},
                                    "timestamp": 1642234568000
                                }
                            ]
                            
                            result = await message_manager.consume_messages(sample_consume_request)
                            
                            assert isinstance(result, ConsumeResponse)
                            assert result.consumer_group == "test-group"
                            assert result.topic == "test-topic"
                            assert result.total_consumed == 2
                            assert len(result.messages) == 2
                            
                            # Check first message
                            msg1 = result.messages[0]
                            assert msg1.topic == "test-topic"
                            assert msg1.partition == 0
                            assert msg1.offset == 100
                            assert msg1.key == "key1"
                            assert msg1.value == {"message": "Hello 1"}
                            
                            # Verify workflow calls
                            mock_create.assert_called_once_with("test-group")
                            mock_subscribe.assert_called_once()
                            mock_fetch.assert_called_once_with(
                                "http://localhost:8082/consumers/test-group/instances/consumer-123",
                                10, 5000
                            )
                            mock_delete.assert_called_once()

    @pytest.mark.asyncio
    async def test_consume_messages_from_beginning(self, message_manager, mock_http_client, sample_consume_request):
        """Test message consumption from beginning."""
        sample_consume_request.from_beginning = True
        
        with patch.object(message_manager, '_check_rest_proxy_health', return_value=True):
            with patch.object(message_manager, '_create_consumer_instance', return_value="consumer-uri"):
                with patch.object(message_manager, '_subscribe_consumer'):
                    with patch.object(message_manager, '_seek_to_beginning') as mock_seek:
                        with patch.object(message_manager, '_fetch_messages', return_value=[]):
                            with patch.object(message_manager, '_delete_consumer_instance'):
                                
                                await message_manager.consume_messages(sample_consume_request)
                                
                                mock_seek.assert_called_once_with("consumer-uri", "test-topic")

    @pytest.mark.asyncio
    async def test_consume_messages_rest_proxy_unavailable(self, message_manager, sample_consume_request):
        """Test message consumption when REST proxy is unavailable."""
        with patch.object(message_manager, '_check_rest_proxy_health', return_value=False):
            with pytest.raises(KafkaRestProxyNotAvailableError):
                await message_manager.consume_messages(sample_consume_request)

    @pytest.mark.asyncio
    async def test_consume_messages_cleanup_on_error(self, message_manager, sample_consume_request):
        """Test that consumer instance is cleaned up even when errors occur."""
        with patch.object(message_manager, '_check_rest_proxy_health', return_value=True):
            with patch.object(message_manager, '_create_consumer_instance', return_value="consumer-uri"):
                with patch.object(message_manager, '_subscribe_consumer', side_effect=Exception("Subscribe failed")):
                    with patch.object(message_manager, '_delete_consumer_instance') as mock_delete:
                        
                        with pytest.raises(MessageManagerError):
                            await message_manager.consume_messages(sample_consume_request)
                        
                        # Ensure cleanup was called
                        mock_delete.assert_called_once_with("consumer-uri")

    @pytest.mark.asyncio
    async def test_create_consumer_instance_success(self, message_manager, mock_http_client):
        """Test successful consumer instance creation."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "instance_id": "consumer-123",
            "base_uri": "http://localhost:8082/consumers/test-group/instances/consumer-123"
        }
        mock_http_client.post.return_value = mock_response
        message_manager._http_client = mock_http_client
        
        result = await message_manager._create_consumer_instance("test-group")
        
        assert result == "http://localhost:8082/consumers/test-group/instances/consumer-123"
        
        # Verify request
        mock_http_client.post.assert_called_once()
        call_args = mock_http_client.post.call_args
        assert call_args[0][0] == "http://localhost:8082/consumers/test-group"
        
        payload = call_args[1]["json"]
        assert payload["format"] == "json"
        assert "name" in payload

    @pytest.mark.asyncio
    async def test_create_consumer_instance_failure(self, message_manager, mock_http_client):
        """Test consumer instance creation failure."""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_http_client.post.return_value = mock_response
        message_manager._http_client = mock_http_client
        
        with pytest.raises(MessageManagerError, match="Failed to create consumer instance"):
            await message_manager._create_consumer_instance("test-group")

    @pytest.mark.asyncio
    async def test_subscribe_consumer_to_topic(self, message_manager, mock_http_client):
        """Test subscribing consumer to topic."""
        mock_response = Mock()
        mock_response.status_code = 204
        mock_http_client.post.return_value = mock_response
        message_manager._http_client = mock_http_client
        
        await message_manager._subscribe_consumer("consumer-uri", "test-topic")
        
        # Verify subscription request
        mock_http_client.post.assert_called_once()
        call_args = mock_http_client.post.call_args
        assert call_args[0][0] == "consumer-uri/subscription"
        
        payload = call_args[1]["json"]
        assert payload["topics"] == ["test-topic"]

    @pytest.mark.asyncio
    async def test_subscribe_consumer_to_partition(self, message_manager, mock_http_client):
        """Test subscribing consumer to specific partition."""
        mock_response = Mock()
        mock_response.status_code = 204
        mock_http_client.post.return_value = mock_response
        message_manager._http_client = mock_http_client
        
        await message_manager._subscribe_consumer("consumer-uri", "test-topic", partition=2)
        
        # Verify assignment request
        call_args = mock_http_client.post.call_args
        assert call_args[0][0] == "consumer-uri/assignments"
        
        payload = call_args[1]["json"]
        assert payload["partitions"] == [{"topic": "test-topic", "partition": 2}]

    @pytest.mark.asyncio
    async def test_fetch_messages_success(self, message_manager, mock_http_client):
        """Test successful message fetching."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"topic": "test", "partition": 0, "offset": 1, "value": {"msg": "1"}},
            {"topic": "test", "partition": 0, "offset": 2, "value": {"msg": "2"}}
        ]
        mock_http_client.get.return_value = mock_response
        message_manager._http_client = mock_http_client
        
        result = await message_manager._fetch_messages("consumer-uri", 10, 5000)
        
        assert len(result) == 2
        assert result[0]["offset"] == 1
        assert result[1]["offset"] == 2

    @pytest.mark.asyncio
    async def test_fetch_messages_no_messages(self, message_manager, mock_http_client):
        """Test fetching when no messages are available."""
        mock_response = Mock()
        mock_response.status_code = 204
        mock_http_client.get.return_value = mock_response
        message_manager._http_client = mock_http_client
        
        result = await message_manager._fetch_messages("consumer-uri", 10, 5000)
        
        assert result == []

    @pytest.mark.asyncio
    async def test_fetch_messages_limits_results(self, message_manager, mock_http_client):
        """Test that fetch_messages respects max_messages limit."""
        # Return more messages than requested
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"offset": i, "value": {"msg": str(i)}} for i in range(15)
        ]
        mock_http_client.get.return_value = mock_response
        message_manager._http_client = mock_http_client
        
        result = await message_manager._fetch_messages("consumer-uri", 10, 5000)
        
        assert len(result) == 10  # Should be limited to max_messages

    @pytest.mark.asyncio
    async def test_get_consumer_groups_success(self, message_manager, mock_http_client):
        """Test successful consumer groups retrieval."""
        with patch.object(message_manager, '_check_rest_proxy_health', return_value=True):
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = ["group1", "group2", "group3"]
            mock_http_client.get.return_value = mock_response
            message_manager._http_client = mock_http_client
            
            result = await message_manager.get_consumer_groups()
            
            assert isinstance(result, ConsumerGroupListResponse)
            assert result.total_count == 3
            assert len(result.consumer_groups) == 3
            assert result.consumer_groups[0].group_id == "group1"

    @pytest.mark.asyncio
    async def test_get_consumer_groups_detailed(self, message_manager, mock_http_client):
        """Test consumer groups retrieval with detailed info."""
        with patch.object(message_manager, '_check_rest_proxy_health', return_value=True):
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = [
                {
                    "group_id": "detailed-group",
                    "state": "Stable",
                    "members": [{"member_id": "member1"}],
                    "coordinator": "broker1"
                }
            ]
            mock_http_client.get.return_value = mock_response
            message_manager._http_client = mock_http_client
            
            result = await message_manager.get_consumer_groups()
            
            group = result.consumer_groups[0]
            assert group.group_id == "detailed-group"
            assert group.state == "Stable"
            assert len(group.members) == 1

    @pytest.mark.asyncio
    async def test_get_consumer_groups_rest_proxy_unavailable(self, message_manager):
        """Test consumer groups retrieval when REST proxy is unavailable."""
        with patch.object(message_manager, '_check_rest_proxy_health', return_value=False):
            with pytest.raises(KafkaRestProxyNotAvailableError):
                await message_manager.get_consumer_groups()

    @pytest.mark.asyncio
    async def test_close(self, message_manager, mock_http_client):
        """Test closing the message manager."""
        message_manager._http_client = mock_http_client
        
        await message_manager.close()
        
        mock_http_client.aclose.assert_called_once()
        assert message_manager._http_client is None

    @pytest.mark.asyncio
    async def test_close_with_error(self, message_manager, mock_http_client):
        """Test closing with error doesn't raise exception."""
        mock_http_client.aclose.side_effect = Exception("Close error")
        message_manager._http_client = mock_http_client
        
        # Should not raise exception
        await message_manager.close()
        
        assert message_manager._http_client is None

    @pytest.mark.asyncio
    async def test_close_when_no_client(self, message_manager):
        """Test closing when no client exists."""
        assert message_manager._http_client is None
        
        # Should not raise exception
        await message_manager.close()
        
        assert message_manager._http_client is None


@pytest.mark.integration
class TestMessageManagerIntegration:
    """Integration-style tests for MessageManager."""

    @pytest.mark.asyncio
    async def test_produce_consume_workflow(self):
        """Test a complete produce-consume workflow with mocked REST proxy."""
        manager = MessageManager()
        
        # Mock the entire workflow
        with patch.object(manager, '_check_rest_proxy_health', return_value=True):
            mock_client = AsyncMock(spec=httpx.AsyncClient)
            manager._http_client = mock_client
            
            # Mock produce response
            produce_response = Mock()
            produce_response.status_code = 200
            produce_response.json.return_value = {
                "offsets": [{"partition": 0, "offset": 100, "error_code": None}]
            }
            
            # Mock consume workflow
            consumer_create_response = Mock()
            consumer_create_response.status_code = 200
            consumer_create_response.json.return_value = {
                "base_uri": "http://localhost:8082/consumers/test/instances/consumer-1"
            }
            
            subscribe_response = Mock()
            subscribe_response.status_code = 204
            
            fetch_response = Mock()
            fetch_response.status_code = 200
            fetch_response.json.return_value = [
                {
                    "topic": "test-topic",
                    "partition": 0,
                    "offset": 100,
                    "key": "test-key",
                    "value": {"message": "Hello, Kafka!"},
                    "timestamp": 1642234567000
                }
            ]
            
            delete_response = Mock()
            delete_response.status_code = 204
            
            # Configure mock client responses
            mock_client.post.side_effect = [
                produce_response,      # produce message
                consumer_create_response,  # create consumer
                subscribe_response,    # subscribe
            ]
            mock_client.get.return_value = fetch_response
            mock_client.delete.return_value = delete_response
            
            # Test produce
            produce_request = ProduceRequest(
                topic="test-topic",
                key="test-key",
                value={"message": "Hello, Kafka!"}
            )
            
            produce_result = await manager.produce_message(produce_request)
            assert produce_result.topic == "test-topic"
            assert produce_result.offset == 100
            
            # Test consume
            consume_request = ConsumeRequest(
                topic="test-topic",
                consumer_group="test-group",
                max_messages=1
            )
            
            consume_result = await manager.consume_messages(consume_request)
            assert consume_result.total_consumed == 1
            assert consume_result.messages[0].key == "test-key"
            assert consume_result.messages[0].value == {"message": "Hello, Kafka!"}