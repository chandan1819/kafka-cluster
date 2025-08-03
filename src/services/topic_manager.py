"""Topic Manager for Kafka topic lifecycle operations."""

import logging
from typing import Dict, List, Optional, Any
from kafka import KafkaAdminClient, KafkaConsumer
from kafka.admin import ConfigResource, ConfigResourceType, NewTopic
from kafka.errors import (
    KafkaError, 
    TopicAlreadyExistsError, 
    UnknownTopicOrPartitionError,
    KafkaConnectionError,
    KafkaTimeoutError
)

from ..models.topic import TopicConfig, TopicInfo, TopicMetadata
from ..models.base import ServiceStatus
from ..exceptions import (
    TopicManagerError,
    KafkaNotAvailableError,
    TopicNotFoundError,
    TopicAlreadyExistsError,
    TopicCreationError,
    TopicDeletionError,
    TopicConfigurationError,
    TimeoutError
)
from ..utils.retry import retry_async, RetryConfig, STANDARD_RETRY, NETWORK_RETRY


logger = logging.getLogger(__name__)


class TopicManager:
    """Manages Kafka topic lifecycle operations using Kafka Admin API."""
    
    def __init__(self, bootstrap_servers: Optional[str] = None, 
                 request_timeout_ms: Optional[int] = None):
        """Initialize the topic manager.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers (uses config if None)
            request_timeout_ms: Request timeout in milliseconds (uses config if None)
        """
        from ..config import settings
        
        self.bootstrap_servers = bootstrap_servers or settings.kafka.bootstrap_servers
        self.request_timeout_ms = request_timeout_ms or settings.kafka.request_timeout_ms
        self._admin_client: Optional[KafkaAdminClient] = None
        
        # Default topic configurations from settings
        self.default_topic_configs = settings.kafka.default_topic_configs

    @property
    def admin_client(self) -> KafkaAdminClient:
        """Get Kafka admin client, creating it if necessary."""
        if self._admin_client is None:
            try:
                self._admin_client = KafkaAdminClient(
                    bootstrap_servers=self.bootstrap_servers,
                    request_timeout_ms=self.request_timeout_ms,
                    client_id="local-kafka-manager-admin"
                )
                # Test connection by listing topics
                self._admin_client.list_topics()
            except (KafkaConnectionError, KafkaTimeoutError) as e:
                raise KafkaNotAvailableError(
                    message=f"Kafka is not available at {self.bootstrap_servers}: {e}",
                    cause=e
                )
            except Exception as e:
                raise TopicManagerError(
                    message=f"Failed to create Kafka admin client: {e}",
                    details={"bootstrap_servers": self.bootstrap_servers},
                    cause=e
                )
        return self._admin_client

    @retry_async(NETWORK_RETRY)
    async def list_topics(self, include_internal: bool = False) -> List[TopicInfo]:
        """List all Kafka topics.
        
        Args:
            include_internal: Whether to include internal topics (starting with __)
            
        Returns:
            List of TopicInfo objects
            
        Raises:
            TopicManagerError: If listing topics fails
            KafkaNotAvailableError: If Kafka is not available
        """
        logger.debug("Listing Kafka topics...")
        
        try:
            admin_client = self.admin_client
            
            # Get topic metadata
            metadata = admin_client.describe_topics()
            
            topics = []
            for topic_name, topic_metadata in metadata.items():
                # Skip internal topics unless requested
                if not include_internal and topic_name.startswith('__'):
                    continue
                
                # Get partition count and replication factor
                partitions = len(topic_metadata.partitions)
                replication_factor = len(topic_metadata.partitions[0].replicas) if topic_metadata.partitions else 1
                
                # Get topic configuration
                config_resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
                configs = admin_client.describe_configs([config_resource])
                topic_config = {}
                if config_resource in configs:
                    topic_config = {
                        entry.name: entry.value 
                        for entry in configs[config_resource].config_entries.values()
                        if not entry.is_default  # Only include non-default configs
                    }
                
                # Try to get topic size (approximate)
                size_bytes = await self._get_topic_size(topic_name)
                
                topic_info = TopicInfo(
                    name=topic_name,
                    partitions=partitions,
                    replication_factor=replication_factor,
                    size_bytes=size_bytes,
                    config=topic_config
                )
                topics.append(topic_info)
            
            logger.debug(f"Found {len(topics)} topics")
            return topics
            
        except KafkaNotAvailableError:
            raise
        except Exception as e:
            logger.error(f"Failed to list topics: {e}")
            raise TopicManagerError(
                message=f"Failed to list topics: {e}",
                details={"include_internal": include_internal},
                cause=e
            )

    async def create_topic(self, topic_config: TopicConfig) -> bool:
        """Create a new Kafka topic.
        
        Args:
            topic_config: Topic configuration
            
        Returns:
            True if topic was created successfully
            
        Raises:
            TopicManagerError: If topic creation fails
            KafkaNotAvailableError: If Kafka is not available
        """
        logger.info(f"Creating topic: {topic_config.name}")
        
        try:
            admin_client = self.admin_client
            
            # Merge default configs with provided configs
            merged_config = {**self.default_topic_configs, **topic_config.config}
            
            # Create NewTopic object
            new_topic = NewTopic(
                name=topic_config.name,
                num_partitions=topic_config.partitions,
                replication_factor=topic_config.replication_factor,
                topic_configs=merged_config
            )
            
            # Create the topic
            future_map = admin_client.create_topics([new_topic], validate_only=False)
            
            # Wait for creation to complete
            for topic_name, future in future_map.items():
                try:
                    future.result(timeout=30)  # 30 second timeout
                    logger.info(f"Topic '{topic_name}' created successfully")
                except TopicAlreadyExistsError as e:
                    logger.warning(f"Topic '{topic_name}' already exists")
                    raise TopicAlreadyExistsError(topic_name, cause=e)
                except Exception as e:
                    logger.error(f"Failed to create topic '{topic_name}': {e}")
                    raise TopicCreationError(topic_name, str(e), cause=e)
            
            return True
            
        except (TopicManagerError, KafkaNotAvailableError):
            raise
        except Exception as e:
            logger.error(f"Failed to create topic {topic_config.name}: {e}")
            raise TopicCreationError(
                topic_name=topic_config.name,
                reason=str(e),
                cause=e
            )

    async def delete_topic(self, topic_name: str, force: bool = False) -> bool:
        """Delete a Kafka topic.
        
        Args:
            topic_name: Name of the topic to delete
            force: Force deletion (currently not used, for future implementation)
            
        Returns:
            True if topic was deleted successfully
            
        Raises:
            TopicManagerError: If topic deletion fails
            KafkaNotAvailableError: If Kafka is not available
        """
        logger.info(f"Deleting topic: {topic_name}")
        
        try:
            admin_client = self.admin_client
            
            # Check if topic exists first
            existing_topics = await self.list_topics(include_internal=True)
            topic_exists = any(topic.name == topic_name for topic in existing_topics)
            
            if not topic_exists:
                logger.warning(f"Topic '{topic_name}' does not exist")
                raise TopicNotFoundError(topic_name)
            
            # Delete the topic
            future_map = admin_client.delete_topics([topic_name])
            
            # Wait for deletion to complete
            for topic, future in future_map.items():
                try:
                    future.result(timeout=30)  # 30 second timeout
                    logger.info(f"Topic '{topic}' deleted successfully")
                except UnknownTopicOrPartitionError as e:
                    logger.warning(f"Topic '{topic}' does not exist")
                    raise TopicNotFoundError(topic, cause=e)
                except Exception as e:
                    logger.error(f"Failed to delete topic '{topic}': {e}")
                    raise TopicDeletionError(topic, str(e), cause=e)
            
            return True
            
        except (TopicManagerError, KafkaNotAvailableError):
            raise
        except Exception as e:
            logger.error(f"Failed to delete topic {topic_name}: {e}")
            raise TopicDeletionError(
                topic_name=topic_name,
                reason=str(e),
                cause=e
            )

    async def get_topic_metadata(self, topic_name: str) -> TopicMetadata:
        """Get detailed metadata for a specific topic.
        
        Args:
            topic_name: Name of the topic
            
        Returns:
            TopicMetadata object with detailed information
            
        Raises:
            TopicManagerError: If getting metadata fails
            KafkaNotAvailableError: If Kafka is not available
        """
        logger.debug(f"Getting metadata for topic: {topic_name}")
        
        try:
            admin_client = self.admin_client
            
            # Get topic metadata
            metadata = admin_client.describe_topics([topic_name])
            
            if topic_name not in metadata:
                raise TopicNotFoundError(topic_name)
            
            topic_metadata = metadata[topic_name]
            
            # Build partition details
            partition_details = []
            for partition in topic_metadata.partitions:
                partition_info = {
                    "partition": partition.partition,
                    "leader": partition.leader,
                    "replicas": partition.replicas,
                    "isr": partition.isr  # In-sync replicas
                }
                partition_details.append(partition_info)
            
            # Get topic configuration
            config_resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
            configs = admin_client.describe_configs([config_resource])
            topic_config = {}
            if config_resource in configs:
                topic_config = {
                    entry.name: entry.value 
                    for entry in configs[config_resource].config_entries.values()
                }
            
            # Get consumer groups (this would require additional API calls)
            # For now, return empty list - this can be enhanced later
            consumer_groups = []
            
            # Get offset information
            earliest_offset, latest_offset, message_count = await self._get_topic_offsets(topic_name)
            
            return TopicMetadata(
                name=topic_name,
                partitions=partition_details,
                config=topic_config,
                consumer_groups=consumer_groups,
                message_count=message_count,
                earliest_offset=earliest_offset,
                latest_offset=latest_offset
            )
            
        except (TopicManagerError, KafkaNotAvailableError):
            raise
        except Exception as e:
            logger.error(f"Failed to get metadata for topic {topic_name}: {e}")
            raise TopicManagerError(
                message=f"Failed to get metadata for topic {topic_name}: {e}",
                details={"topic": topic_name},
                cause=e
            )

    async def _get_topic_size(self, topic_name: str) -> Optional[int]:
        """Get approximate size of a topic in bytes.
        
        Args:
            topic_name: Name of the topic
            
        Returns:
            Size in bytes or None if unable to determine
        """
        try:
            # This is a simplified implementation
            # In a real scenario, you'd need to sum up log segment sizes
            # For now, return None to indicate size is not available
            return None
        except Exception as e:
            logger.debug(f"Could not get size for topic {topic_name}: {e}")
            return None

    async def _get_topic_offsets(self, topic_name: str) -> tuple[Optional[int], Optional[int], Optional[int]]:
        """Get offset information for a topic.
        
        Args:
            topic_name: Name of the topic
            
        Returns:
            Tuple of (earliest_offset, latest_offset, message_count)
        """
        try:
            # Create a consumer to get offset information
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                consumer_timeout_ms=5000,
                enable_auto_commit=False
            )
            
            # Get topic partitions
            partitions = consumer.partitions_for_topic(topic_name)
            if not partitions:
                return None, None, None
            
            # Get beginning and end offsets for all partitions
            topic_partitions = [
                (topic_name, partition) for partition in partitions
            ]
            
            beginning_offsets = consumer.beginning_offsets(topic_partitions)
            end_offsets = consumer.end_offsets(topic_partitions)
            
            # Calculate totals
            total_earliest = sum(beginning_offsets.values()) if beginning_offsets else None
            total_latest = sum(end_offsets.values()) if end_offsets else None
            
            # Message count is the difference
            message_count = None
            if total_earliest is not None and total_latest is not None:
                message_count = total_latest - total_earliest
            
            consumer.close()
            
            return total_earliest, total_latest, message_count
            
        except Exception as e:
            logger.debug(f"Could not get offsets for topic {topic_name}: {e}")
            return None, None, None

    def close(self) -> None:
        """Close the admin client connection."""
        if self._admin_client:
            try:
                self._admin_client.close()
            except Exception as e:
                logger.warning(f"Error closing admin client: {e}")
            finally:
                self._admin_client = None