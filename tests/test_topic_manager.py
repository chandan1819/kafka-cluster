"""Unit tests for TopicManager."""

import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from kafka.errors import (
    KafkaError, 
    TopicAlreadyExistsError, 
    UnknownTopicOrPartitionError,
    KafkaConnectionError,
    KafkaTimeoutError
)
from kafka.admin import ConfigResource, ConfigResourceType
from kafka.structs import TopicPartition

from src.services.topic_manager import (
    TopicManager, 
    TopicManagerError, 
    KafkaNotAvailableError
)
from src.models.topic import TopicConfig, TopicInfo, TopicMetadata


@pytest.mark.unit
class TestTopicManager:
    """Test cases for TopicManager class."""

    @pytest.fixture
    def topic_manager(self):
        """Create a TopicManager instance for testing."""
        return TopicManager("localhost:9092")

    @pytest.fixture
    def mock_admin_client(self):
        """Create a mock Kafka admin client."""
        mock_client = Mock()
        mock_client.list_topics.return_value = {}
        return mock_client

    @pytest.fixture
    def mock_consumer(self):
        """Create a mock Kafka consumer."""
        mock_consumer = Mock()
        mock_consumer.partitions_for_topic.return_value = {0, 1, 2}
        mock_consumer.beginning_offsets.return_value = {
            ("test-topic", 0): 0,
            ("test-topic", 1): 0,
            ("test-topic", 2): 0
        }
        mock_consumer.end_offsets.return_value = {
            ("test-topic", 0): 100,
            ("test-topic", 1): 150,
            ("test-topic", 2): 200
        }
        mock_consumer.close = Mock()
        return mock_consumer

    @pytest.fixture
    def sample_topic_config(self):
        """Create a sample topic configuration."""
        return TopicConfig(
            name="test-topic",
            partitions=3,
            replication_factor=1,
            config={"retention.ms": "86400000"}
        )

    def test_init(self):
        """Test TopicManager initialization."""
        manager = TopicManager("custom:9092", 60000)
        assert manager.bootstrap_servers == "custom:9092"
        assert manager.request_timeout_ms == 60000
        assert manager._admin_client is None

    @patch('src.services.topic_manager.KafkaAdminClient')
    def test_admin_client_property_success(self, mock_admin_class, topic_manager):
        """Test successful admin client creation."""
        mock_admin = Mock()
        mock_admin.list_topics.return_value = {}
        mock_admin_class.return_value = mock_admin
        
        client = topic_manager.admin_client
        
        assert client == mock_admin
        mock_admin_class.assert_called_once_with(
            bootstrap_servers="localhost:9092",
            request_timeout_ms=30000,
            client_id="local-kafka-manager-admin"
        )
        mock_admin.list_topics.assert_called_once()

    @patch('src.services.topic_manager.KafkaAdminClient')
    def test_admin_client_property_connection_failure(self, mock_admin_class, topic_manager):
        """Test admin client creation with connection failure."""
        mock_admin_class.side_effect = KafkaConnectionError("Connection failed")
        
        with pytest.raises(KafkaNotAvailableError, match="Kafka is not available"):
            topic_manager.admin_client

    @patch('src.services.topic_manager.KafkaAdminClient')
    def test_admin_client_property_timeout(self, mock_admin_class, topic_manager):
        """Test admin client creation with timeout."""
        mock_admin = Mock()
        mock_admin.list_topics.side_effect = KafkaTimeoutError("Timeout")
        mock_admin_class.return_value = mock_admin
        
        with pytest.raises(KafkaNotAvailableError, match="Kafka is not available"):
            topic_manager.admin_client

    @pytest.mark.asyncio
    async def test_list_topics_success(self, topic_manager):
        """Test successful topic listing."""
        # Mock topic metadata
        mock_partition = Mock()
        mock_partition.partition = 0
        mock_partition.replicas = [1]
        
        mock_topic_metadata = Mock()
        mock_topic_metadata.partitions = [mock_partition, mock_partition, mock_partition]  # 3 partitions
        
        mock_admin = Mock()
        mock_admin.describe_topics.return_value = {
            "test-topic": mock_topic_metadata,
            "another-topic": mock_topic_metadata
        }
        
        # Mock config description
        mock_config_entry = Mock()
        mock_config_entry.name = "retention.ms"
        mock_config_entry.value = "86400000"
        mock_config_entry.is_default = False
        
        mock_config_resource = Mock()
        mock_config_resource.config_entries = {"retention.ms": mock_config_entry}
        
        # Mock describe_configs to return the config for any ConfigResource
        def mock_describe_configs(config_resources):
            result = {}
            for resource in config_resources:
                result[resource] = mock_config_resource
            return result
        
        mock_admin.describe_configs.side_effect = mock_describe_configs
        
        # Mock _get_topic_size
        topic_manager._admin_client = mock_admin
        with patch.object(topic_manager, '_get_topic_size', return_value=1024):
            topics = await topic_manager.list_topics()
        
        assert len(topics) == 2
        assert topics[0].name in ["test-topic", "another-topic"]
        assert topics[0].partitions == 3
        assert topics[0].replication_factor == 1
        assert topics[0].size_bytes == 1024
        assert topics[0].config == {"retention.ms": "86400000"}

    @pytest.mark.asyncio
    async def test_list_topics_exclude_internal(self, topic_manager):
        """Test topic listing excluding internal topics."""
        mock_partition = Mock()
        mock_partition.partition = 0
        mock_partition.replicas = [1]
        
        mock_topic_metadata = Mock()
        mock_topic_metadata.partitions = [mock_partition]
        
        mock_admin = Mock()
        mock_admin.describe_topics.return_value = {
            "test-topic": mock_topic_metadata,
            "__internal-topic": mock_topic_metadata
        }
        mock_admin.describe_configs.return_value = {}
        
        topic_manager._admin_client = mock_admin
        with patch.object(topic_manager, '_get_topic_size', return_value=None):
            topics = await topic_manager.list_topics(include_internal=False)
        
        assert len(topics) == 1
        assert topics[0].name == "test-topic"

    @pytest.mark.asyncio
    async def test_list_topics_include_internal(self, topic_manager):
        """Test topic listing including internal topics."""
        mock_partition = Mock()
        mock_partition.partition = 0
        mock_partition.replicas = [1]
        
        mock_topic_metadata = Mock()
        mock_topic_metadata.partitions = [mock_partition]
        
        mock_admin = Mock()
        mock_admin.describe_topics.return_value = {
            "test-topic": mock_topic_metadata,
            "__internal-topic": mock_topic_metadata
        }
        mock_admin.describe_configs.return_value = {}
        
        topic_manager._admin_client = mock_admin
        with patch.object(topic_manager, '_get_topic_size', return_value=None):
            topics = await topic_manager.list_topics(include_internal=True)
        
        assert len(topics) == 2
        topic_names = [topic.name for topic in topics]
        assert "test-topic" in topic_names
        assert "__internal-topic" in topic_names

    @pytest.mark.asyncio
    async def test_list_topics_failure(self, topic_manager):
        """Test topic listing failure."""
        mock_admin = Mock()
        mock_admin.describe_topics.side_effect = Exception("Kafka error")
        
        topic_manager._admin_client = mock_admin
        with pytest.raises(TopicManagerError, match="Failed to list topics"):
            await topic_manager.list_topics()

    @pytest.mark.asyncio
    async def test_create_topic_success(self, topic_manager, sample_topic_config):
        """Test successful topic creation."""
        mock_admin = Mock()
        
        # Mock successful creation
        mock_future = Mock()
        mock_future.result.return_value = None  # Success
        mock_admin.create_topics.return_value = {"test-topic": mock_future}
        
        topic_manager._admin_client = mock_admin
        result = await topic_manager.create_topic(sample_topic_config)
        
        assert result is True
        mock_admin.create_topics.assert_called_once()
        
        # Verify the NewTopic was created with correct parameters
        call_args = mock_admin.create_topics.call_args[0][0]  # First positional argument
        new_topic = call_args[0]  # First topic in the list
        assert new_topic.name == "test-topic"
        assert new_topic.num_partitions == 3
        assert new_topic.replication_factor == 1
        assert "retention.ms" in new_topic.topic_configs

    @pytest.mark.asyncio
    async def test_create_topic_already_exists(self, topic_manager, sample_topic_config):
        """Test topic creation when topic already exists."""
        mock_admin = Mock()
        
        # Mock topic already exists error
        mock_future = Mock()
        mock_future.result.side_effect = TopicAlreadyExistsError("Topic exists")
        mock_admin.create_topics.return_value = {"test-topic": mock_future}
        
        topic_manager._admin_client = mock_admin
        with pytest.raises(TopicManagerError, match="already exists"):
            await topic_manager.create_topic(sample_topic_config)

    @pytest.mark.asyncio
    async def test_create_topic_kafka_error(self, topic_manager, sample_topic_config):
        """Test topic creation with Kafka error."""
        mock_admin = Mock()
        
        # Mock Kafka error
        mock_future = Mock()
        mock_future.result.side_effect = KafkaError("Kafka error")
        mock_admin.create_topics.return_value = {"test-topic": mock_future}
        
        topic_manager._admin_client = mock_admin
        with pytest.raises(TopicManagerError, match="Failed to create topic"):
            await topic_manager.create_topic(sample_topic_config)

    @pytest.mark.asyncio
    async def test_delete_topic_success(self, topic_manager):
        """Test successful topic deletion."""
        mock_admin = Mock()
        
        # Mock existing topic
        with patch.object(topic_manager, 'list_topics') as mock_list:
            mock_list.return_value = [
                TopicInfo(name="test-topic", partitions=1, replication_factor=1)
            ]
            
            # Mock successful deletion
            mock_future = Mock()
            mock_future.result.return_value = None  # Success
            mock_admin.delete_topics.return_value = {"test-topic": mock_future}
            
            topic_manager._admin_client = mock_admin
            result = await topic_manager.delete_topic("test-topic")
            
            assert result is True
            mock_admin.delete_topics.assert_called_once_with(["test-topic"])

    @pytest.mark.asyncio
    async def test_delete_topic_not_exists(self, topic_manager):
        """Test topic deletion when topic doesn't exist."""
        # Mock no existing topics
        with patch.object(topic_manager, 'list_topics') as mock_list:
            mock_list.return_value = []
            
            with pytest.raises(TopicManagerError, match="does not exist"):
                await topic_manager.delete_topic("nonexistent-topic")

    @pytest.mark.asyncio
    async def test_delete_topic_unknown_topic_error(self, topic_manager):
        """Test topic deletion with unknown topic error."""
        mock_admin = Mock()
        
        # Mock existing topic
        with patch.object(topic_manager, 'list_topics') as mock_list:
            mock_list.return_value = [
                TopicInfo(name="test-topic", partitions=1, replication_factor=1)
            ]
            
            # Mock unknown topic error
            mock_future = Mock()
            mock_future.result.side_effect = UnknownTopicOrPartitionError("Unknown topic")
            mock_admin.delete_topics.return_value = {"test-topic": mock_future}
            
            topic_manager._admin_client = mock_admin
            with pytest.raises(TopicManagerError, match="does not exist"):
                await topic_manager.delete_topic("test-topic")

    @pytest.mark.asyncio
    async def test_delete_topic_kafka_error(self, topic_manager):
        """Test topic deletion with Kafka error."""
        mock_admin = Mock()
        
        # Mock existing topic
        with patch.object(topic_manager, 'list_topics') as mock_list:
            mock_list.return_value = [
                TopicInfo(name="test-topic", partitions=1, replication_factor=1)
            ]
            
            # Mock Kafka error
            mock_future = Mock()
            mock_future.result.side_effect = KafkaError("Kafka error")
            mock_admin.delete_topics.return_value = {"test-topic": mock_future}
            
            topic_manager._admin_client = mock_admin
            with pytest.raises(TopicManagerError, match="Failed to delete topic"):
                await topic_manager.delete_topic("test-topic")

    @pytest.mark.asyncio
    async def test_get_topic_metadata_success(self, topic_manager):
        """Test successful topic metadata retrieval."""
        mock_admin = Mock()
        
        # Mock partition info
        mock_partition = Mock()
        mock_partition.partition = 0
        mock_partition.leader = 1
        mock_partition.replicas = [1]
        mock_partition.isr = [1]
        
        mock_topic_metadata = Mock()
        mock_topic_metadata.partitions = [mock_partition]
        
        mock_admin.describe_topics.return_value = {"test-topic": mock_topic_metadata}
        
        # Mock config description
        mock_config_entry = Mock()
        mock_config_entry.name = "retention.ms"
        mock_config_entry.value = "86400000"
        
        mock_config_resource = Mock()
        mock_config_resource.config_entries = {"retention.ms": mock_config_entry}
        
        # Mock describe_configs to return the config for any ConfigResource
        def mock_describe_configs(config_resources):
            result = {}
            for resource in config_resources:
                result[resource] = mock_config_resource
            return result
        
        mock_admin.describe_configs.side_effect = mock_describe_configs
        
        # Mock offset information
        topic_manager._admin_client = mock_admin
        with patch.object(topic_manager, '_get_topic_offsets') as mock_offsets:
            mock_offsets.return_value = (0, 100, 100)
            
            metadata = await topic_manager.get_topic_metadata("test-topic")
        
        assert metadata.name == "test-topic"
        assert len(metadata.partitions) == 1
        assert metadata.partitions[0]["partition"] == 0
        assert metadata.partitions[0]["leader"] == 1
        assert metadata.config == {"retention.ms": "86400000"}
        assert metadata.message_count == 100
        assert metadata.earliest_offset == 0
        assert metadata.latest_offset == 100

    @pytest.mark.asyncio
    async def test_get_topic_metadata_not_found(self, topic_manager):
        """Test topic metadata retrieval for non-existent topic."""
        mock_admin = Mock()
        mock_admin.describe_topics.return_value = {}  # Topic not found
        
        topic_manager._admin_client = mock_admin
        with pytest.raises(TopicManagerError, match="not found"):
            await topic_manager.get_topic_metadata("nonexistent-topic")

    @pytest.mark.asyncio
    async def test_get_topic_metadata_failure(self, topic_manager):
        """Test topic metadata retrieval failure."""
        mock_admin = Mock()
        mock_admin.describe_topics.side_effect = Exception("Kafka error")
        
        topic_manager._admin_client = mock_admin
        with pytest.raises(TopicManagerError, match="Failed to get metadata"):
            await topic_manager.get_topic_metadata("test-topic")

    @pytest.mark.asyncio
    async def test_get_topic_size_returns_none(self, topic_manager):
        """Test _get_topic_size returns None (not implemented)."""
        size = await topic_manager._get_topic_size("test-topic")
        assert size is None

    @pytest.mark.asyncio
    @patch('src.services.topic_manager.KafkaConsumer')
    async def test_get_topic_offsets_success(self, mock_consumer_class, topic_manager, mock_consumer):
        """Test successful topic offset retrieval."""
        mock_consumer_class.return_value = mock_consumer
        
        earliest, latest, count = await topic_manager._get_topic_offsets("test-topic")
        
        assert earliest == 0  # Sum of beginning offsets
        assert latest == 450  # Sum of end offsets
        assert count == 450   # Difference
        mock_consumer.close.assert_called_once()

    @pytest.mark.asyncio
    @patch('src.services.topic_manager.KafkaConsumer')
    async def test_get_topic_offsets_no_partitions(self, mock_consumer_class, topic_manager):
        """Test topic offset retrieval when topic has no partitions."""
        mock_consumer = Mock()
        mock_consumer.partitions_for_topic.return_value = None
        mock_consumer.close = Mock()
        mock_consumer_class.return_value = mock_consumer
        
        earliest, latest, count = await topic_manager._get_topic_offsets("test-topic")
        
        assert earliest is None
        assert latest is None
        assert count is None

    @pytest.mark.asyncio
    @patch('src.services.topic_manager.KafkaConsumer')
    async def test_get_topic_offsets_exception(self, mock_consumer_class, topic_manager):
        """Test topic offset retrieval with exception."""
        mock_consumer_class.side_effect = Exception("Consumer error")
        
        earliest, latest, count = await topic_manager._get_topic_offsets("test-topic")
        
        assert earliest is None
        assert latest is None
        assert count is None

    def test_close_with_client(self, topic_manager):
        """Test close method when admin client exists."""
        mock_admin = Mock()
        topic_manager._admin_client = mock_admin
        
        topic_manager.close()
        
        mock_admin.close.assert_called_once()
        assert topic_manager._admin_client is None

    def test_close_without_client(self, topic_manager):
        """Test close method when no admin client exists."""
        # Should not raise an exception
        topic_manager.close()
        assert topic_manager._admin_client is None

    def test_close_with_exception(self, topic_manager):
        """Test close method with exception during close."""
        mock_admin = Mock()
        mock_admin.close.side_effect = Exception("Close error")
        topic_manager._admin_client = mock_admin
        
        # Should not raise an exception, just log warning
        topic_manager.close()
        assert topic_manager._admin_client is None

    @pytest.mark.asyncio
    async def test_kafka_not_available_propagation(self, topic_manager):
        """Test that KafkaNotAvailableError is properly propagated."""
        def raise_kafka_error():
            raise KafkaNotAvailableError("Kafka down")
        
        # Test by patching the property getter to raise the exception
        with patch('src.services.topic_manager.TopicManager.admin_client', new_callable=lambda: property(lambda self: raise_kafka_error())):
            with pytest.raises(KafkaNotAvailableError):
                await topic_manager.list_topics()
            
            with pytest.raises(KafkaNotAvailableError):
                await topic_manager.create_topic(TopicConfig(name="test"))
            
            with pytest.raises(KafkaNotAvailableError):
                await topic_manager.delete_topic("test")
            
            with pytest.raises(KafkaNotAvailableError):
                await topic_manager.get_topic_metadata("test")

    def test_default_topic_configs(self, topic_manager):
        """Test default topic configurations are set correctly."""
        expected_defaults = {
            "cleanup.policy": "delete",
            "retention.ms": "604800000",  # 7 days
            "segment.ms": "86400000",     # 1 day
            "max.message.bytes": "1000000"  # 1MB
        }
        
        assert topic_manager.default_topic_configs == expected_defaults

    @pytest.mark.asyncio
    async def test_create_topic_config_merging(self, topic_manager):
        """Test that topic configs are properly merged with defaults."""
        mock_admin = Mock()
        
        # Mock successful creation
        mock_future = Mock()
        mock_future.result.return_value = None
        mock_admin.create_topics.return_value = {"test-topic": mock_future}
        
        # Create topic with custom config
        topic_config = TopicConfig(
            name="test-topic",
            partitions=2,
            replication_factor=1,
            config={"retention.ms": "3600000", "custom.config": "value"}
        )
        
        topic_manager._admin_client = mock_admin
        await topic_manager.create_topic(topic_config)
        
        # Verify the merged config was used
        call_args = mock_admin.create_topics.call_args[0][0]
        new_topic = call_args[0]
        
        # Should have default configs plus custom ones
        assert "cleanup.policy" in new_topic.topic_configs  # Default
        assert new_topic.topic_configs["retention.ms"] == "3600000"  # Override
        assert new_topic.topic_configs["custom.config"] == "value"  # Custom