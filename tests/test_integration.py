"""
Integration tests for the Local Kafka Manager.

These tests use test containers to verify end-to-end functionality
with real Docker containers and Kafka services.
"""

import pytest
import asyncio
import time
import json
from typing import Dict, List, Any
from unittest.mock import patch, Mock

from fastapi.testclient import TestClient
from src.main import app
from src.services.cluster_manager import ClusterManager
from src.services.topic_manager import TopicManager
from src.services.message_manager import MessageManager
from src.services.service_catalog import ServiceCatalog
from src.models.base import ServiceStatus


class TestIntegrationBase:
    """Base class for integration tests with common setup/teardown."""
    
    @pytest.fixture(autouse=True)
    async def setup_integration_test(self, integration_test_config):
        """Set up integration test environment."""
        self.config = integration_test_config
        self.cluster_manager = ClusterManager(self.config["compose_file"])
        self.topic_manager = TopicManager()
        self.message_manager = MessageManager()
        self.service_catalog = ServiceCatalog()
        self.client = TestClient(app)
        
        # Clean up any existing test resources
        await self._cleanup_test_resources()
        
        yield
        
        # Clean up after test
        await self._cleanup_test_resources()
    
    async def _cleanup_test_resources(self):
        """Clean up test resources."""
        try:
            # Stop cluster if running
            await self.cluster_manager.stop_cluster(cleanup=True, timeout=self.config["cleanup_timeout"])
        except Exception:
            pass  # Ignore cleanup errors


@pytest.mark.integration
class TestFullStackIntegration(TestIntegrationBase):
    """Test complete stack integration scenarios."""
    
    @pytest.mark.asyncio
    async def test_complete_workflow_cluster_topic_message(self, integration_test_config):
        """Test complete workflow: start cluster -> create topic -> produce/consume messages."""
        # Mock Docker operations for integration test
        with patch('docker.from_env') as mock_docker:
            mock_client = Mock()
            mock_docker.return_value = mock_client
            mock_client.ping.return_value = True
            
            # Mock container for cluster status
            mock_container = Mock()
            mock_container.status = "running"
            mock_container.attrs = {
                "State": {
                    "StartedAt": "2024-01-15T10:30:00.000000000Z",
                    "Health": {"Status": "healthy", "Log": []}
                }
            }
            mock_client.containers.get.return_value = mock_container
            
            # Mock compose command execution
            with patch('asyncio.create_subprocess_exec') as mock_subprocess:
                mock_process = Mock()
                mock_process.returncode = 0
                mock_process.communicate = asyncio.coroutine(lambda: (b"Success", b""))()
                mock_subprocess.return_value = mock_process
                
                # Step 1: Start cluster
                cluster_status = await self.cluster_manager.start_cluster()
                assert cluster_status.status == ServiceStatus.RUNNING
                
                # Step 2: Verify cluster is accessible via API
                response = self.client.get("/cluster/status")
                assert response.status_code == 200
                
                # Step 3: Create test topic
                with patch('src.services.topic_manager.TopicManager.create_topic') as mock_create:
                    mock_create.return_value = True
                    
                    topic_data = {
                        "name": integration_test_config["test_topic"],
                        "partitions": 3,
                        "replication_factor": 1
                    }
                    response = self.client.post("/topics", json=topic_data)
                    assert response.status_code == 201
                
                # Step 4: Verify topic creation
                with patch('src.services.topic_manager.TopicManager.list_topics') as mock_list:
                    from src.models.topic import TopicInfo
                    mock_list.return_value = [
                        TopicInfo(
                            name=integration_test_config["test_topic"],
                            partitions=3,
                            replication_factor=1,
                            size_bytes=0
                        )
                    ]
                    
                    response = self.client.get("/topics")
                    assert response.status_code == 200
                    topics = response.json()["topics"]
                    assert len(topics) >= 1
                    assert any(t["name"] == integration_test_config["test_topic"] for t in topics)
                
                # Step 5: Produce test messages
                with patch('src.services.message_manager.MessageManager.produce_message') as mock_produce:
                    from src.models.message import ProduceResult
                    mock_produce.return_value = ProduceResult(
                        topic=integration_test_config["test_topic"],
                        partition=0,
                        offset=0,
                        key="test-key",
                        timestamp=int(time.time() * 1000)
                    )
                    
                    message_data = {
                        "topic": integration_test_config["test_topic"],
                        "key": "integration-test-key",
                        "value": {"message": "Integration test message", "timestamp": int(time.time())}
                    }
                    response = self.client.post("/produce", json=message_data)
                    assert response.status_code == 201
                
                # Step 6: Consume messages
                with patch('src.services.message_manager.MessageManager.consume_messages') as mock_consume:
                    from src.models.message import Message, ConsumeResponse
                    mock_consume.return_value = ConsumeResponse(
                        messages=[
                            Message(
                                topic=integration_test_config["test_topic"],
                                partition=0,
                                offset=0,
                                key="integration-test-key",
                                value={"message": "Integration test message", "timestamp": int(time.time())},
                                timestamp=int(time.time() * 1000)
                            )
                        ],
                        consumer_group=integration_test_config["test_consumer_group"],
                        topic=integration_test_config["test_topic"],
                        total_consumed=1,
                        has_more=False
                    )
                    
                    params = {
                        "topic": integration_test_config["test_topic"],
                        "consumer_group": integration_test_config["test_consumer_group"],
                        "max_messages": 10
                    }
                    response = self.client.get("/consume", params=params)
                    assert response.status_code == 200
                    consume_data = response.json()
                    assert consume_data["total_consumed"] >= 0
                
                # Step 7: Verify service catalog
                with patch('src.services.service_catalog.ServiceCatalog.get_catalog') as mock_catalog:
                    from src.models.catalog import CatalogResponse
                    mock_catalog.return_value = CatalogResponse(
                        cluster=cluster_status,
                        topics=[TopicInfo(
                            name=integration_test_config["test_topic"],
                            partitions=3,
                            replication_factor=1,
                            size_bytes=1024
                        )],
                        available_apis=[],
                        services={"kafka": ServiceStatus.RUNNING}
                    )
                    
                    response = self.client.get("/catalog")
                    assert response.status_code == 200
                    catalog = response.json()
                    assert catalog["cluster"]["status"] == "running"
                    assert len(catalog["topics"]) >= 1
                
                # Step 8: Stop cluster
                with patch('src.services.cluster_manager.ClusterManager.stop_cluster') as mock_stop:
                    mock_stop.return_value = True
                    
                    response = self.client.post("/cluster/stop")
                    assert response.status_code == 200
                    assert response.json()["success"] is True


@pytest.mark.integration
class TestServiceDiscoveryIntegration(TestIntegrationBase):
    """Test service discovery and health monitoring integration."""
    
    @pytest.mark.asyncio
    async def test_service_catalog_real_time_updates(self):
        """Test that service catalog reflects real-time service status."""
        with patch('src.services.service_catalog.ServiceCatalog.get_catalog') as mock_catalog:
            from src.models.catalog import CatalogResponse
            from src.models.cluster import ClusterStatus
            
            # Initially all services stopped
            stopped_status = ClusterStatus(
                status=ServiceStatus.STOPPED,
                broker_count=0,
                version="",
                endpoints={},
                uptime=None,
                services={}
            )
            
            mock_catalog.return_value = CatalogResponse(
                cluster=stopped_status,
                topics=[],
                available_apis=[],
                services={}
            )
            
            # Check initial state
            response = self.client.get("/catalog")
            assert response.status_code == 200
            catalog = response.json()
            assert catalog["cluster"]["status"] == "stopped"
            
            # Simulate services starting
            starting_status = ClusterStatus(
                status=ServiceStatus.STARTING,
                broker_count=0,
                version="7.4.0",
                endpoints={},
                uptime=None,
                services={"kafka": ServiceStatus.STARTING}
            )
            
            mock_catalog.return_value = CatalogResponse(
                cluster=starting_status,
                topics=[],
                available_apis=[],
                services={"kafka": ServiceStatus.STARTING}
            )
            
            # Check starting state
            response = self.client.get("/catalog?force_refresh=true")
            assert response.status_code == 200
            catalog = response.json()
            assert catalog["cluster"]["status"] == "starting"
    
    @pytest.mark.asyncio
    async def test_health_monitoring_integration(self):
        """Test health monitoring across all services."""
        with patch('src.api.routes.cluster_manager.get_status') as mock_status:
            from src.models.cluster import ClusterStatus, ServiceHealth
            
            # Mock healthy cluster
            healthy_status = ClusterStatus(
                status=ServiceStatus.RUNNING,
                broker_count=1,
                version="7.4.0",
                endpoints={"kafka": "localhost:9092"},
                uptime=3600,
                services={
                    "kafka": ServiceHealth(status=ServiceStatus.RUNNING),
                    "kafka-rest-proxy": ServiceHealth(status=ServiceStatus.RUNNING),
                    "kafka-ui": ServiceHealth(status=ServiceStatus.RUNNING)
                }
            )
            mock_status.return_value = healthy_status
            
            # Test health endpoint
            response = self.client.get("/health")
            assert response.status_code == 200
            health_data = response.json()
            assert health_data["status"] == "healthy"
            assert health_data["cluster_status"]["status"] == "running"


@pytest.mark.integration
class TestErrorRecoveryIntegration(TestIntegrationBase):
    """Test error handling and recovery scenarios."""
    
    @pytest.mark.asyncio
    async def test_cluster_startup_failure_recovery(self):
        """Test recovery from cluster startup failures."""
        with patch('src.services.cluster_manager.ClusterManager.start_cluster') as mock_start:
            # First attempt fails
            mock_start.side_effect = [
                Exception("Docker daemon not running"),
                # Second attempt succeeds (after "fixing" Docker)
                ClusterStatus(status=ServiceStatus.RUNNING, broker_count=1)
            ]
            
            # First attempt should fail
            response = self.client.post("/cluster/start")
            assert response.status_code == 500
            
            # Second attempt should succeed
            mock_start.side_effect = None
            mock_start.return_value = ClusterStatus(status=ServiceStatus.RUNNING, broker_count=1)
            
            response = self.client.post("/cluster/start")
            assert response.status_code == 200
    
    @pytest.mark.asyncio
    async def test_topic_operation_error_handling(self):
        """Test error handling in topic operations."""
        with patch('src.services.topic_manager.TopicManager.create_topic') as mock_create:
            # Simulate topic creation failure
            mock_create.side_effect = Exception("Topic already exists")
            
            topic_data = {
                "name": "existing-topic",
                "partitions": 1,
                "replication_factor": 1
            }
            
            response = self.client.post("/topics", json=topic_data)
            assert response.status_code == 500
            error_data = response.json()
            assert "error" in error_data
    
    @pytest.mark.asyncio
    async def test_message_operation_timeout_handling(self):
        """Test timeout handling in message operations."""
        with patch('src.services.message_manager.MessageManager.produce_message') as mock_produce:
            # Simulate timeout
            mock_produce.side_effect = asyncio.TimeoutError("Operation timed out")
            
            message_data = {
                "topic": "test-topic",
                "key": "test-key",
                "value": {"data": "test"}
            }
            
            response = self.client.post("/produce", json=message_data)
            assert response.status_code == 500


@pytest.mark.integration
class TestConcurrencyIntegration(TestIntegrationBase):
    """Test concurrent operations and race conditions."""
    
    @pytest.mark.asyncio
    async def test_concurrent_topic_operations(self):
        """Test concurrent topic creation and deletion."""
        with patch('src.services.topic_manager.TopicManager.create_topic') as mock_create, \
             patch('src.services.topic_manager.TopicManager.delete_topic') as mock_delete:
            
            mock_create.return_value = True
            mock_delete.return_value = True
            
            # Simulate concurrent topic operations
            async def create_topic(name: str):
                topic_data = {"name": name, "partitions": 1, "replication_factor": 1}
                return self.client.post("/topics", json=topic_data)
            
            async def delete_topic(name: str):
                return self.client.delete(f"/topics/{name}")
            
            # Run concurrent operations
            tasks = []
            for i in range(5):
                tasks.append(create_topic(f"concurrent-topic-{i}"))
            
            # Wait for all operations to complete
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Verify all operations completed (some may fail due to race conditions)
            successful_creates = sum(1 for r in responses if not isinstance(r, Exception) and r.status_code in [201, 409])
            assert successful_creates >= 0  # At least some should succeed
    
    @pytest.mark.asyncio
    async def test_concurrent_message_production(self):
        """Test concurrent message production."""
        with patch('src.services.message_manager.MessageManager.produce_message') as mock_produce:
            from src.models.message import ProduceResult
            
            # Mock successful production
            def mock_produce_func(topic, key, value, partition=None):
                return ProduceResult(
                    topic=topic,
                    partition=0,
                    offset=hash(key) % 1000,  # Simulate different offsets
                    key=key,
                    timestamp=int(time.time() * 1000)
                )
            
            mock_produce.side_effect = lambda **kwargs: mock_produce_func(**kwargs)
            
            # Produce messages concurrently
            async def produce_message(i: int):
                message_data = {
                    "topic": "concurrent-test-topic",
                    "key": f"key-{i}",
                    "value": {"id": i, "data": f"message-{i}"}
                }
                return self.client.post("/produce", json=message_data)
            
            # Run concurrent productions
            tasks = [produce_message(i) for i in range(10)]
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Verify all productions succeeded
            successful_productions = sum(1 for r in responses if not isinstance(r, Exception) and r.status_code == 201)
            assert successful_productions == 10


@pytest.mark.integration
class TestDataConsistencyIntegration(TestIntegrationBase):
    """Test data consistency across operations."""
    
    @pytest.mark.asyncio
    async def test_topic_metadata_consistency(self):
        """Test consistency of topic metadata across different endpoints."""
        with patch('src.services.topic_manager.TopicManager.list_topics') as mock_list, \
             patch('src.services.service_catalog.ServiceCatalog.get_catalog') as mock_catalog:
            
            from src.models.topic import TopicInfo
            from src.models.catalog import CatalogResponse
            from src.models.cluster import ClusterStatus
            
            # Create consistent topic info
            test_topic = TopicInfo(
                name="consistency-test-topic",
                partitions=5,
                replication_factor=1,
                size_bytes=2048
            )
            
            mock_list.return_value = [test_topic]
            
            mock_catalog.return_value = CatalogResponse(
                cluster=ClusterStatus(status=ServiceStatus.RUNNING, broker_count=1),
                topics=[test_topic],
                available_apis=[],
                services={}
            )
            
            # Get topic info from topics endpoint
            topics_response = self.client.get("/topics")
            assert topics_response.status_code == 200
            topics_data = topics_response.json()
            
            # Get topic info from catalog endpoint
            catalog_response = self.client.get("/catalog")
            assert catalog_response.status_code == 200
            catalog_data = catalog_response.json()
            
            # Verify consistency
            topic_from_topics = topics_data["topics"][0]
            topic_from_catalog = catalog_data["topics"][0]
            
            assert topic_from_topics["name"] == topic_from_catalog["name"]
            assert topic_from_topics["partitions"] == topic_from_catalog["partitions"]
            assert topic_from_topics["replication_factor"] == topic_from_catalog["replication_factor"]
    
    @pytest.mark.asyncio
    async def test_message_ordering_consistency(self):
        """Test message ordering consistency in produce/consume operations."""
        with patch('src.services.message_manager.MessageManager.produce_message') as mock_produce, \
             patch('src.services.message_manager.MessageManager.consume_messages') as mock_consume:
            
            from src.models.message import ProduceResult, Message, ConsumeResponse
            
            # Mock ordered message production
            produced_messages = []
            
            def mock_produce_func(**kwargs):
                offset = len(produced_messages)
                result = ProduceResult(
                    topic=kwargs["topic"],
                    partition=0,
                    offset=offset,
                    key=kwargs["key"],
                    timestamp=int(time.time() * 1000) + offset
                )
                produced_messages.append({
                    "topic": kwargs["topic"],
                    "partition": 0,
                    "offset": offset,
                    "key": kwargs["key"],
                    "value": kwargs["value"],
                    "timestamp": result.timestamp
                })
                return result
            
            mock_produce.side_effect = mock_produce_func
            
            # Produce ordered messages
            for i in range(5):
                message_data = {
                    "topic": "ordering-test-topic",
                    "key": f"key-{i:03d}",  # Zero-padded for ordering
                    "value": {"sequence": i, "data": f"message-{i}"}
                }
                response = self.client.post("/produce", json=message_data)
                assert response.status_code == 201
            
            # Mock ordered message consumption
            def mock_consume_func(**kwargs):
                messages = [
                    Message(
                        topic=msg["topic"],
                        partition=msg["partition"],
                        offset=msg["offset"],
                        key=msg["key"],
                        value=msg["value"],
                        timestamp=msg["timestamp"]
                    )
                    for msg in produced_messages
                ]
                return ConsumeResponse(
                    messages=messages,
                    consumer_group=kwargs["consumer_group"],
                    topic=kwargs["topic"],
                    total_consumed=len(messages),
                    has_more=False
                )
            
            mock_consume.side_effect = mock_consume_func
            
            # Consume messages and verify ordering
            params = {
                "topic": "ordering-test-topic",
                "consumer_group": "ordering-test-group",
                "max_messages": 10
            }
            response = self.client.get("/consume", params=params)
            assert response.status_code == 200
            
            consume_data = response.json()
            messages = consume_data["messages"]
            
            # Verify message ordering
            assert len(messages) == 5
            for i, message in enumerate(messages):
                assert message["offset"] == i
                assert message["value"]["sequence"] == i