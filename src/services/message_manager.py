"""Message Manager for Kafka message production and consumption operations."""

import json
import logging
import asyncio
from typing import Dict, List, Optional, Any
import httpx
from datetime import datetime

from ..models.message import (
    Message, ProduceRequest, ProduceResult, ConsumeRequest, 
    ConsumeResponse, ConsumerGroup, ConsumerGroupListResponse
)
from ..models.base import ServiceStatus
from ..exceptions import (
    MessageManagerError,
    KafkaRestProxyNotAvailableError,
    MessageProduceError,
    MessageConsumeError,
    ConsumerGroupError,
    MessageSerializationError,
    TimeoutError
)
from ..utils.retry import retry_async, RetryConfig, NETWORK_RETRY, STANDARD_RETRY


logger = logging.getLogger(__name__)


class MessageManager:
    """Manages Kafka message operations using Kafka REST Proxy."""
    
    def __init__(self, rest_proxy_url: Optional[str] = None, 
                 request_timeout: Optional[int] = None,
                 cluster_id: Optional[str] = None):
        """Initialize the message manager.
        
        Args:
            rest_proxy_url: Kafka REST Proxy URL (uses config if None)
            request_timeout: Request timeout in seconds (uses config if None)
            cluster_id: Cluster ID for multi-cluster operations (uses default if None)
        """
        from ..config import settings
        
        self.cluster_id = cluster_id or "default"
        self.rest_proxy_url = (rest_proxy_url or settings.get_rest_proxy_url()).rstrip('/')
        self.request_timeout = request_timeout or settings.kafka_rest_proxy.request_timeout
        self._http_client: Optional[httpx.AsyncClient] = None
        
        # Content type headers for REST Proxy
        self.produce_headers = {
            "Content-Type": "application/vnd.kafka.json.v2+json",
            "Accept": "application/vnd.kafka.v2+json"
        }
        
        self.consume_headers = {
            "Content-Type": "application/vnd.kafka.v2+json",
            "Accept": "application/vnd.kafka.json.v2+json"
        }

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Get HTTP client, creating it if necessary."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.request_timeout),
                headers={"User-Agent": "local-kafka-manager/1.0"}
            )
        return self._http_client

    @retry_async(RetryConfig(max_attempts=2, base_delay=0.5))
    async def _check_rest_proxy_health(self) -> bool:
        """Check if Kafka REST Proxy is available and healthy."""
        try:
            client = self.http_client
            response = await client.get(f"{self.rest_proxy_url}/")
            return response.status_code == 200
        except Exception as e:
            logger.debug(f"REST Proxy health check failed: {e}")
            return False

    @retry_async(NETWORK_RETRY)
    async def produce_message(self, request: ProduceRequest) -> ProduceResult:
        """Produce a message to Kafka via REST Proxy.
        
        Args:
            request: Message production request
            
        Returns:
            ProduceResult with production details
            
        Raises:
            MessageProduceError: If message production fails
            KafkaRestProxyNotAvailableError: If REST Proxy is not available
        """
        logger.info(f"Producing message to topic: {request.topic} in cluster: {self.cluster_id}")
        
        # Check REST Proxy availability
        if not await self._check_rest_proxy_health():
            raise KafkaRestProxyNotAvailableError(
                message=f"Kafka REST Proxy for cluster {self.cluster_id} is not available at {self.rest_proxy_url}"
            )
        
        try:
            client = self.http_client
            
            # Prepare the message payload for REST Proxy
            message_data = {
                "value": request.value
            }
            
            # Add key if provided
            if request.key is not None:
                message_data["key"] = request.key
            
            # Add partition if specified
            if request.partition is not None:
                message_data["partition"] = request.partition
            
            # Add headers if provided
            if request.headers:
                message_data["headers"] = request.headers
            
            # Prepare the request payload
            payload = {
                "records": [message_data]
            }
            
            # Send the produce request
            url = f"{self.rest_proxy_url}/topics/{request.topic}"
            response = await client.post(
                url,
                json=payload,
                headers=self.produce_headers
            )
            
            if response.status_code not in [200, 201]:
                error_detail = response.text
                try:
                    error_json = response.json()
                    error_detail = error_json.get("message", error_detail)
                except:
                    pass
                
                raise MessageProduceError(
                    topic=request.topic,
                    reason=f"Cluster {self.cluster_id}: HTTP {response.status_code} - {error_detail}"
                )
            
            # Parse the response
            result = response.json()
            
            if "offsets" not in result or not result["offsets"]:
                raise MessageProduceError(
                    topic=request.topic,
                    reason="Invalid response from REST Proxy: missing offsets"
                )
            
            offset_info = result["offsets"][0]
            
            # Check for errors in the response
            if "error_code" in offset_info and offset_info["error_code"] is not None:
                error_msg = offset_info.get("error", "Unknown error")
                raise MessageProduceError(
                    topic=request.topic,
                    reason=f"Kafka error: {error_msg}"
                )
            
            # Create the result
            produce_result = ProduceResult(
                topic=request.topic,
                partition=offset_info["partition"],
                offset=offset_info["offset"],
                key=request.key,
                timestamp=int(datetime.utcnow().timestamp() * 1000)  # Convert to milliseconds as int
            )
            
            logger.info(f"Message produced successfully to {request.topic}:{offset_info['partition']}:{offset_info['offset']}")
            return produce_result
            
        except (KafkaRestProxyNotAvailableError, MessageProduceError):
            raise
        except Exception as e:
            logger.error(f"Failed to produce message to topic {request.topic}: {e}")
            raise MessageProduceError(
                topic=request.topic,
                reason=str(e),
                cause=e
            )

    @retry_async(NETWORK_RETRY)
    async def consume_messages(self, request: ConsumeRequest) -> ConsumeResponse:
        """Consume messages from Kafka via REST Proxy.
        
        Args:
            request: Message consumption request
            
        Returns:
            ConsumeResponse with consumed messages
            
        Raises:
            MessageConsumeError: If message consumption fails
            KafkaRestProxyNotAvailableError: If REST Proxy is not available
        """
        logger.info(f"Consuming messages from topic: {request.topic}, group: {request.consumer_group}")
        
        # Check REST Proxy availability
        if not await self._check_rest_proxy_health():
            raise KafkaRestProxyNotAvailableError(
                message=f"Kafka REST Proxy is not available at {self.rest_proxy_url}"
            )
        
        consumer_instance = None
        try:
            client = self.http_client
            
            # Step 1: Create a consumer instance
            consumer_instance = await self._create_consumer_instance(request.consumer_group)
            
            # Step 2: Subscribe to the topic
            await self._subscribe_consumer(consumer_instance, request.topic, request.partition)
            
            # Step 3: Set consumer position if needed
            if request.from_beginning:
                await self._seek_to_beginning(consumer_instance, request.topic)
            
            # Step 4: Consume messages
            messages = await self._fetch_messages(consumer_instance, request.max_messages, request.timeout_ms)
            
            # Step 5: Parse messages
            parsed_messages = []
            for msg_data in messages:
                try:
                    message = Message(
                        topic=msg_data.get("topic", request.topic),
                        partition=msg_data.get("partition", 0),
                        offset=msg_data.get("offset", 0),
                        key=msg_data.get("key"),
                        value=msg_data.get("value", {}),
                        timestamp=msg_data.get("timestamp", int(datetime.utcnow().timestamp() * 1000)),
                        headers=msg_data.get("headers")
                    )
                    parsed_messages.append(message)
                except Exception as e:
                    logger.warning(f"Failed to parse message: {e}, data: {msg_data}")
                    continue
            
            # Determine if there are more messages
            has_more = len(messages) == request.max_messages
            
            response = ConsumeResponse(
                messages=parsed_messages,
                consumer_group=request.consumer_group,
                topic=request.topic,
                total_consumed=len(parsed_messages),
                has_more=has_more
            )
            
            logger.info(f"Consumed {len(parsed_messages)} messages from {request.topic}")
            return response
            
        except (KafkaRestProxyNotAvailableError, MessageConsumeError):
            raise
        except Exception as e:
            logger.error(f"Failed to consume messages from topic {request.topic}: {e}")
            raise MessageConsumeError(
                topic=request.topic,
                consumer_group=request.consumer_group,
                reason=str(e),
                cause=e
            )
        finally:
            # Clean up consumer instance
            if consumer_instance:
                try:
                    await self._delete_consumer_instance(consumer_instance)
                except Exception as e:
                    logger.warning(f"Failed to clean up consumer instance: {e}")

    async def _create_consumer_instance(self, consumer_group: str) -> str:
        """Create a consumer instance in the REST Proxy."""
        client = self.http_client
        
        # Generate a unique consumer instance name
        instance_id = f"consumer-{int(datetime.utcnow().timestamp())}"
        
        payload = {
            "name": instance_id,
            "format": "json",
            "auto.offset.reset": "earliest",
            "auto.commit.enable": "false"
        }
        
        url = f"{self.rest_proxy_url}/consumers/{consumer_group}"
        response = await client.post(
            url,
            json=payload,
            headers=self.consume_headers
        )
        
        if response.status_code not in [200, 201]:
            raise ConsumerGroupError(
                consumer_group=consumer_group,
                operation="create_instance",
                reason=f"HTTP {response.status_code}"
            )
        
        result = response.json()
        base_uri = result.get("base_uri")
        if not base_uri:
            raise ConsumerGroupError(
                consumer_group=consumer_group,
                operation="create_instance",
                reason="Invalid response: missing base_uri"
            )
        
        logger.debug(f"Created consumer instance: {base_uri}")
        return base_uri

    async def _subscribe_consumer(self, consumer_instance: str, topic: str, partition: Optional[int] = None):
        """Subscribe consumer to a topic."""
        client = self.http_client
        
        if partition is not None:
            # Subscribe to specific partition
            payload = {
                "partitions": [
                    {"topic": topic, "partition": partition}
                ]
            }
            url = f"{consumer_instance}/assignments"
        else:
            # Subscribe to topic (all partitions)
            payload = {
                "topics": [topic]
            }
            url = f"{consumer_instance}/subscription"
        
        response = await client.post(
            url,
            json=payload,
            headers=self.consume_headers
        )
        
        if response.status_code not in [200, 204]:
            raise ConsumerGroupError(
                consumer_group="unknown",
                operation="subscribe",
                reason=f"HTTP {response.status_code}"
            )
        
        logger.debug(f"Subscribed consumer to topic: {topic}")

    async def _seek_to_beginning(self, consumer_instance: str, topic: str):
        """Seek consumer to the beginning of the topic."""
        client = self.http_client
        
        # First, get the current assignments to know which partitions to seek
        assignments_url = f"{consumer_instance}/assignments"
        response = await client.get(assignments_url, headers=self.consume_headers)
        
        if response.status_code != 200:
            logger.warning("Could not get consumer assignments for seeking")
            return
        
        assignments = response.json()
        partitions = assignments.get("partitions", [])
        
        if not partitions:
            logger.warning("No partition assignments found for seeking")
            return
        
        # Seek to beginning for each partition
        seek_payload = {
            "partitions": [
                {"topic": p["topic"], "partition": p["partition"], "offset": 0}
                for p in partitions if p["topic"] == topic
            ]
        }
        
        if not seek_payload["partitions"]:
            return
        
        seek_url = f"{consumer_instance}/positions/beginning"
        response = await client.post(
            seek_url,
            json=seek_payload,
            headers=self.consume_headers
        )
        
        if response.status_code not in [200, 204]:
            logger.warning(f"Failed to seek to beginning: HTTP {response.status_code}")
        else:
            logger.debug("Seeked consumer to beginning")

    async def _fetch_messages(self, consumer_instance: str, max_messages: int, timeout_ms: int) -> List[Dict[str, Any]]:
        """Fetch messages from the consumer."""
        client = self.http_client
        
        url = f"{consumer_instance}/records"
        params = {
            "timeout": timeout_ms,
            "max_bytes": 1024 * 1024  # 1MB max
        }
        
        # Use a longer timeout for the HTTP request to account for Kafka timeout
        http_timeout = (timeout_ms / 1000) + 5  # Add 5 seconds buffer
        
        response = await client.get(
            url,
            params=params,
            headers=self.consume_headers,
            timeout=http_timeout
        )
        
        if response.status_code == 200:
            messages = response.json()
            # Limit to max_messages
            return messages[:max_messages] if len(messages) > max_messages else messages
        elif response.status_code == 204:
            # No messages available
            return []
        else:
            raise MessageConsumeError(
                topic="unknown",
                consumer_group="unknown",
                reason=f"Failed to fetch messages: HTTP {response.status_code}"
            )

    async def _delete_consumer_instance(self, consumer_instance: str):
        """Delete the consumer instance."""
        client = self.http_client
        
        response = await client.delete(
            consumer_instance,
            headers=self.consume_headers
        )
        
        if response.status_code not in [200, 204]:
            logger.warning(f"Failed to delete consumer instance: HTTP {response.status_code}")
        else:
            logger.debug("Deleted consumer instance")

    async def get_consumer_groups(self) -> ConsumerGroupListResponse:
        """Get list of consumer groups.
        
        Returns:
            ConsumerGroupListResponse with list of consumer groups
            
        Raises:
            MessageManagerError: If getting consumer groups fails
            KafkaRestProxyNotAvailableError: If REST Proxy is not available
        """
        logger.debug("Getting consumer groups...")
        
        # Check REST Proxy availability
        if not await self._check_rest_proxy_health():
            raise KafkaRestProxyNotAvailableError(
                message=f"Kafka REST Proxy is not available at {self.rest_proxy_url}"
            )
        
        try:
            client = self.http_client
            
            # Get consumer groups from REST Proxy
            url = f"{self.rest_proxy_url}/consumers"
            response = await client.get(url, headers=self.consume_headers)
            
            if response.status_code != 200:
                raise ConsumerGroupError(
                    consumer_group="all",
                    operation="list",
                    reason=f"HTTP {response.status_code}"
                )
            
            # Parse response - this is a simplified implementation
            # The actual REST Proxy API might return different format
            groups_data = response.json()
            
            consumer_groups = []
            if isinstance(groups_data, list):
                for group_data in groups_data:
                    if isinstance(group_data, str):
                        # Simple group name
                        group = ConsumerGroup(
                            group_id=group_data,
                            state="Unknown",
                            members=[],
                            coordinator=None,
                            partition_assignments={}
                        )
                    else:
                        # Detailed group info
                        group = ConsumerGroup(
                            group_id=group_data.get("group_id", "unknown"),
                            state=group_data.get("state", "Unknown"),
                            members=group_data.get("members", []),
                            coordinator=group_data.get("coordinator"),
                            partition_assignments=group_data.get("partition_assignments", {})
                        )
                    consumer_groups.append(group)
            
            return ConsumerGroupListResponse(
                consumer_groups=consumer_groups,
                total_count=len(consumer_groups)
            )
            
        except (KafkaRestProxyNotAvailableError, ConsumerGroupError):
            raise
        except Exception as e:
            logger.error(f"Failed to get consumer groups: {e}")
            raise ConsumerGroupError(
                consumer_group="all",
                operation="list",
                reason=str(e),
                cause=e
            )

    async def close(self) -> None:
        """Close the HTTP client connection."""
        if self._http_client:
            try:
                await self._http_client.aclose()
            except Exception as e:
                logger.warning(f"Error closing HTTP client: {e}")
            finally:
                self._http_client = None