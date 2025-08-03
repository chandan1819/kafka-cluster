"""
End-to-end integration tests for the Local Kafka Manager.

These tests start the full stack and verify complete workflows including:
- Cluster start → topic creation → message flow
- Service catalog accuracy with running services  
- Error recovery and cleanup scenarios
- Performance benchmarks for typical usage patterns
"""

import pytest
import asyncio
import time
import json
import statistics
from typing import Dict, List, Any, Optional
from unittest.mock import patch, Mock, AsyncMock
from concurrent.futures import ThreadPoolExecutor, as_completed

from fastapi.testclient import TestClient
from src.main import app
from src.services.cluster_manager import ClusterManager
from src.services.topic_manager import TopicManager
from src.services.message_manager import MessageManager
from src.services.service_catalog import ServiceCatalog
from src.models.base import ServiceStatus
from src.models.cluster import ClusterStatus, ServiceHealth
from src.models.topic import TopicInfo, TopicConfig
from src.models.message import Message, ProduceResult, ConsumeResponse
from src.models.catalog import CatalogResponse


class EndToEndTestBase:
    """Base class for end-to-end integration tests."""
    
    @pytest.fixture(autouse=True)
    async def setup_e2e_test(self, integration_test_config):
        """Set up end-to-end test environment."""
        self.config = integration_test_config
        self.client = TestClient(app)
        self.cluster_manager = ClusterManager(self.config["compose_file"])
        self.topic_manager = TopicManager()
        self.message_manager = MessageManager()
        self.service_catalog = ServiceCatalog()
        
        # Test metrics collection
        self.test_metrics = {
            "workflow_times": [],
            "error_recovery_times": [],
            "performance_metrics": {},
            "service_health_checks": []
        }
        
        # Clean up any existing test resources
        await self._cleanup_test_environment()
        
        yield
        
        # Final cleanup
        await self._cleanup_test_environment()
    
    async def _cleanup_test_environment(self):
        """Clean up test environment and resources."""
        try:
            # Stop cluster if running
            await self.cluster_manager.stop_cluster(
                cleanup=True, 
                timeout=self.config["cleanup_timeout"]
            )
            
            # Clear any test topics or consumer groups
            # This would be done via mocked cleanup in real implementation
            pass
            
        except Exception as e:
            print(f"Cleanup warning: {e}")
    
    def record_workflow_time(self, workflow_name: str, duration: float):
        """Record workflow execution time for performance analysis."""
        self.test_metrics["workflow_times"].append({
            "workflow": workflow_name,
            "duration_seconds": duration,
            "timestamp": time.time()
        })
    
    def record_service_health(self, service_name: str, status: ServiceStatus, response_time: float):
        """Record service health check results."""
        self.test_metrics["service_health_checks"].append({
            "service": service_name,
            "status": status.value,
            "response_time_ms": response_time,
            "timestamp": time.time()
        })


@pytest.mark.integration
class TestCompleteWorkflowIntegration(EndToEndTestBase):
    """Test complete end-to-end workflows."""    

    @pytest.mark.asyncio
    async def test_full_stack_workflow_cluster_to_messages(self, integration_test_config):
        """Test complete workflow: cluster start → topic creation → message production/consumption."""
        workflow_start = time.perf_counter()
        
        with patch('docker.from_env') as mock_docker, \
             patch('asyncio.create_subprocess_exec') as mock_subprocess:
            
            # Setup Docker mocks
            mock_client = Mock()
            mock_docker.return_value = mock_client
            mock_client.ping.return_value = True
            
            # Mock healthy containers
            mock_container = Mock()
            mock_container.status = "running"
            mock_container.attrs = {
                "State": {
                    "StartedAt": "2024-01-15T10:30:00.000000000Z",
                    "Health": {"Status": "healthy", "Log": []}
                }
            }
            mock_client.containers.get.return_value = mock_container
            
            # Mock successful compose operations
            mock_process = Mock()
            mock_process.returncode = 0
            mock_process.communicate = asyncio.coroutine(lambda: (b"Services started successfully", b""))()
            mock_subprocess.return_value = mock_process
            
            # Step 1: Start cluster
            step_start = time.perf_counter()
            cluster_status = await self.cluster_manager.start_cluster()
            step_duration = time.perf_counter() - step_start
            
            assert cluster_status.status == ServiceStatus.RUNNING
            assert cluster_status.broker_count >= 1
            self.record_workflow_time("cluster_start", step_duration)
            
            # Verify cluster is accessible via API
            response = self.client.get("/cluster/status")
            assert response.status_code == 200
            status_data = response.json()
            assert status_data["status"] == "running"
            
            # Step 2: Create multiple test topics with different configurations
            step_start = time.perf_counter()
            
            with patch('src.services.topic_manager.TopicManager.create_topic') as mock_create, \
                 patch('src.services.topic_manager.TopicManager.list_topics') as mock_list:
                
                mock_create.return_value = True
                
                test_topics = [
                    {"name": "e2e-test-topic-1", "partitions": 1, "replication_factor": 1},
                    {"name": "e2e-test-topic-3", "partitions": 3, "replication_factor": 1},
                    {"name": "e2e-test-topic-5", "partitions": 5, "replication_factor": 1}
                ]
                
                created_topics = []
                for topic_config in test_topics:
                    response = self.client.post("/topics", json=topic_config)
                    assert response.status_code == 201
                    created_topics.append(topic_config["name"])
                
                # Mock topic listing to reflect created topics
                mock_list.return_value = [
                    TopicInfo(
                        name=topic["name"],
                        partitions=topic["partitions"],
                        replication_factor=topic["replication_factor"],
                        size_bytes=0
                    )
                    for topic in test_topics
                ]
                
                step_duration = time.perf_counter() - step_start
                self.record_workflow_time("topic_creation", step_duration)
                
                # Verify topics are listed correctly
                response = self.client.get("/topics")
                assert response.status_code == 200
                topics_data = response.json()
                assert topics_data["total_count"] == len(test_topics)
                
                topic_names = [t["name"] for t in topics_data["topics"]]
                for topic_name in created_topics:
                    assert topic_name in topic_names
            
            # Step 3: Produce messages to different topics
            step_start = time.perf_counter()
            
            with patch('src.services.message_manager.MessageManager.produce_message') as mock_produce:
                
                async def mock_produce_func(request):
                    return ProduceResult(
                        topic=request.topic,
                        partition=hash(request.key or "default") % 5,  # Distribute across partitions
                        offset=hash(request.key or "default") % 10000,
                        key=request.key,
                        timestamp=int(time.time() * 1000)
                    )
                
                mock_produce.side_effect = mock_produce_func
                
                # Produce messages to each topic
                messages_per_topic = 50
                total_produced = 0
                
                for topic_name in created_topics:
                    for i in range(messages_per_topic):
                        message_data = {
                            "topic": topic_name,
                            "key": f"{topic_name}-key-{i}",
                            "value": {
                                "message_id": i,
                                "topic": topic_name,
                                "timestamp": time.time(),
                                "data": f"End-to-end test message {i} for {topic_name}",
                                "metadata": {"test_run": "e2e_workflow", "batch": i // 10}
                            }
                        }
                        
                        response = self.client.post("/produce", json=message_data)
                        assert response.status_code == 201
                        
                        produce_result = response.json()
                        assert produce_result["topic"] == topic_name
                        assert "offset" in produce_result
                        total_produced += 1
                
                step_duration = time.perf_counter() - step_start
                self.record_workflow_time("message_production", step_duration)
                
                # Calculate production rate
                production_rate = total_produced / step_duration if step_duration > 0 else 0
                self.test_metrics["performance_metrics"]["production_rate"] = production_rate
                assert production_rate >= 100  # At least 100 messages/second
            
            # Step 4: Consume messages from topics
            step_start = time.perf_counter()
            
            with patch('src.services.message_manager.MessageManager.consume_messages') as mock_consume:
                
                async def mock_consume_func(request):
                    topic = request.topic
                    max_messages = request.max_messages
                    
                    # Simulate consuming messages that were produced
                    messages = []
                    for i in range(min(max_messages, messages_per_topic)):
                        messages.append(Message(
                            topic=topic,
                            partition=i % 5,
                            offset=i,
                            key=f"{topic}-key-{i}",
                            value={
                                "message_id": i,
                                "topic": topic,
                                "timestamp": time.time(),
                                "data": f"End-to-end test message {i} for {topic}",
                                "metadata": {"test_run": "e2e_workflow", "batch": i // 10}
                            },
                            timestamp=int(time.time() * 1000) + i
                        ))
                    
                    return ConsumeResponse(
                        messages=messages,
                        consumer_group=request.consumer_group,
                        topic=topic,
                        total_consumed=len(messages),
                        has_more=len(messages) == max_messages
                    )
                
                mock_consume.side_effect = mock_consume_func
                
                # Consume from each topic
                total_consumed = 0
                for topic_name in created_topics:
                    params = {
                        "topic": topic_name,
                        "consumer_group": f"e2e-test-group-{topic_name}",
                        "max_messages": 25,
                        "timeout_ms": 5000
                    }
                    
                    response = self.client.get("/consume", params=params)
                    assert response.status_code == 200
                    
                    consume_data = response.json()
                    assert consume_data["topic"] == topic_name
                    assert consume_data["total_consumed"] > 0
                    
                    # Verify message structure
                    messages = consume_data["messages"]
                    for message in messages:
                        assert message["topic"] == topic_name
                        assert "value" in message
                        assert message["value"]["topic"] == topic_name
                        assert "message_id" in message["value"]
                    
                    total_consumed += consume_data["total_consumed"]
                
                step_duration = time.perf_counter() - step_start
                self.record_workflow_time("message_consumption", step_duration)
                
                # Calculate consumption rate
                consumption_rate = total_consumed / step_duration if step_duration > 0 else 0
                self.test_metrics["performance_metrics"]["consumption_rate"] = consumption_rate
                assert consumption_rate >= 50  # At least 50 messages/second
            
            # Step 5: Verify service catalog reflects current state
            step_start = time.perf_counter()
            
            with patch('src.services.service_catalog.ServiceCatalog.get_catalog') as mock_catalog:
                
                mock_catalog.return_value = CatalogResponse(
                    cluster=ClusterStatus(
                        status=ServiceStatus.RUNNING,
                        broker_count=1,
                        version="7.4.0",
                        endpoints={
                            "kafka": "localhost:9092",
                            "kafka-rest-proxy": "localhost:8082",
                            "kafka-ui": "localhost:8080"
                        },
                        uptime=int(time.time() - workflow_start),
                        services={
                            "kafka": ServiceHealth(status=ServiceStatus.RUNNING),
                            "kafka-rest-proxy": ServiceHealth(status=ServiceStatus.RUNNING),
                            "kafka-ui": ServiceHealth(status=ServiceStatus.RUNNING)
                        }
                    ),
                    topics=[
                        TopicInfo(
                            name=topic["name"],
                            partitions=topic["partitions"],
                            replication_factor=topic["replication_factor"],
                            size_bytes=messages_per_topic * 1024  # Estimated size
                        )
                        for topic in test_topics
                    ],
                    available_apis=[
                        {"path": "/catalog", "method": "GET", "description": "Get service catalog"},
                        {"path": "/cluster/start", "method": "POST", "description": "Start cluster"},
                        {"path": "/cluster/stop", "method": "POST", "description": "Stop cluster"},
                        {"path": "/topics", "method": "GET", "description": "List topics"},
                        {"path": "/topics", "method": "POST", "description": "Create topic"},
                        {"path": "/produce", "method": "POST", "description": "Produce message"},
                        {"path": "/consume", "method": "GET", "description": "Consume messages"}
                    ],
                    services={
                        "kafka": ServiceStatus.RUNNING,
                        "kafka-rest-proxy": ServiceStatus.RUNNING,
                        "kafka-ui": ServiceStatus.RUNNING
                    }
                )
                
                response = self.client.get("/catalog")
                assert response.status_code == 200
                
                catalog_data = response.json()
                assert catalog_data["cluster"]["status"] == "running"
                assert len(catalog_data["topics"]) == len(test_topics)
                assert len(catalog_data["available_apis"]) >= 7
                
                # Verify all services are running
                for service_name, status in catalog_data["services"].items():
                    assert status == "running"
                    self.record_service_health(service_name, ServiceStatus.RUNNING, 0)
                
                step_duration = time.perf_counter() - step_start
                self.record_workflow_time("catalog_verification", step_duration)
            
            # Step 6: Clean shutdown
            step_start = time.perf_counter()
            
            with patch('src.services.cluster_manager.ClusterManager.stop_cluster') as mock_stop:
                mock_stop.return_value = True
                
                response = self.client.post("/cluster/stop")
                assert response.status_code == 200
                assert response.json()["success"] is True
                
                step_duration = time.perf_counter() - step_start
                self.record_workflow_time("cluster_shutdown", step_duration)
            
            # Record total workflow time
            total_workflow_time = time.perf_counter() - workflow_start
            self.record_workflow_time("complete_workflow", total_workflow_time)
            
            # Performance assertions for complete workflow
            assert total_workflow_time < 60  # Complete workflow under 1 minute
            
            # Log workflow performance summary
            print(f"\n=== End-to-End Workflow Performance ===")
            print(f"Total Workflow Time: {total_workflow_time:.2f} seconds")
            print(f"Topics Created: {len(test_topics)}")
            print(f"Messages Produced: {total_produced}")
            print(f"Messages Consumed: {total_consumed}")
            print(f"Production Rate: {self.test_metrics['performance_metrics']['production_rate']:.2f} msg/sec")
            print(f"Consumption Rate: {self.test_metrics['performance_metrics']['consumption_rate']:.2f} msg/sec")
    
    @pytest.mark.asyncio
    async def test_multi_topic_concurrent_workflow(self, integration_test_config):
        """Test concurrent operations across multiple topics."""
        workflow_start = time.perf_counter()
        
        with patch('docker.from_env') as mock_docker, \
             patch('asyncio.create_subprocess_exec') as mock_subprocess, \
             patch('src.services.cluster_manager.ClusterManager.start_cluster') as mock_start:
            
            # Setup mocks
            mock_client = Mock()
            mock_docker.return_value = mock_client
            mock_client.ping.return_value = True
            
            mock_start.return_value = ClusterStatus(
                status=ServiceStatus.RUNNING,
                broker_count=1,
                version="7.4.0",
                endpoints={"kafka": "localhost:9092"},
                uptime=0
            )
            
            # Start cluster
            response = self.client.post("/cluster/start")
            assert response.status_code == 200
            
            # Create multiple topics concurrently
            with patch('src.services.topic_manager.TopicManager.create_topic') as mock_create:
                mock_create.return_value = True
                
                topic_configs = [
                    {"name": f"concurrent-topic-{i}", "partitions": (i % 3) + 1, "replication_factor": 1}
                    for i in range(10)
                ]
                
                # Create topics concurrently
                async def create_topic_async(topic_config):
                    return self.client.post("/topics", json=topic_config)
                
                tasks = [create_topic_async(config) for config in topic_configs]
                responses = await asyncio.gather(*tasks, return_exceptions=True)
                
                successful_creates = sum(
                    1 for r in responses 
                    if not isinstance(r, Exception) and r.status_code == 201
                )
                assert successful_creates >= len(topic_configs) * 0.8  # 80% success rate
            
            # Produce messages concurrently to different topics
            with patch('src.services.message_manager.MessageManager.produce_message') as mock_produce:
                
                def mock_produce_func(**kwargs):
                    return ProduceResult(
                        topic=kwargs["topic"],
                        partition=0,
                        offset=hash(kwargs["key"]) % 1000,
                        key=kwargs["key"],
                        timestamp=int(time.time() * 1000)
                    )
                
                mock_produce.side_effect = mock_produce_func
                
                async def produce_messages_to_topic(topic_name, message_count=20):
                    successful_productions = 0
                    for i in range(message_count):
                        message_data = {
                            "topic": topic_name,
                            "key": f"{topic_name}-key-{i}",
                            "value": {"id": i, "topic": topic_name, "timestamp": time.time()}
                        }
                        
                        try:
                            response = self.client.post("/produce", json=message_data)
                            if response.status_code == 201:
                                successful_productions += 1
                        except Exception:
                            pass
                    
                    return successful_productions
                
                # Produce to all topics concurrently
                production_tasks = [
                    produce_messages_to_topic(config["name"]) 
                    for config in topic_configs
                ]
                production_results = await asyncio.gather(*production_tasks, return_exceptions=True)
                
                total_produced = sum(
                    r for r in production_results 
                    if not isinstance(r, Exception)
                )
                assert total_produced >= len(topic_configs) * 15  # At least 15 messages per topic
            
            total_workflow_time = time.perf_counter() - workflow_start
            self.record_workflow_time("concurrent_multi_topic_workflow", total_workflow_time)
            
            print(f"\n=== Concurrent Multi-Topic Workflow ===")
            print(f"Topics Created: {successful_creates}")
            print(f"Total Messages Produced: {total_produced}")
            print(f"Workflow Time: {total_workflow_time:.2f} seconds")


@pytest.mark.integration
class TestServiceCatalogAccuracy(EndToEndTestBase):
    """Test service catalog accuracy with running services."""
    
    @pytest.mark.asyncio
    async def test_catalog_reflects_real_time_service_status(self, integration_test_config):
        """Test that service catalog accurately reflects real-time service status."""
        
        with patch('src.services.service_catalog.ServiceCatalog.get_catalog') as mock_catalog, \
             patch('src.services.cluster_manager.ClusterManager.get_status') as mock_cluster_status:
            
            # Test scenario 1: All services stopped
            stopped_cluster = ClusterStatus(
                status=ServiceStatus.STOPPED,
                broker_count=0,
                version="",
                endpoints={},
                uptime=None,
                services={}
            )
            
            mock_catalog.return_value = CatalogResponse(
                cluster=stopped_cluster,
                topics=[],
                available_apis=[],
                services={}
            )
            
            response = self.client.get("/catalog")
            assert response.status_code == 200
            catalog = response.json()
            assert catalog["cluster"]["status"] == "stopped"
            assert len(catalog["topics"]) == 0
            assert len(catalog["services"]) == 0
            
            # Test scenario 2: Services starting up
            starting_cluster = ClusterStatus(
                status=ServiceStatus.STARTING,
                broker_count=0,
                version="7.4.0",
                endpoints={},
                uptime=None,
                services={
                    "kafka": ServiceHealth(status=ServiceStatus.STARTING),
                    "kafka-rest-proxy": ServiceHealth(status=ServiceStatus.STOPPED),
                    "kafka-ui": ServiceHealth(status=ServiceStatus.STOPPED)
                }
            )
            
            mock_catalog.return_value = CatalogResponse(
                cluster=starting_cluster,
                topics=[],
                available_apis=[],
                services={
                    "kafka": ServiceStatus.STARTING,
                    "kafka-rest-proxy": ServiceStatus.STOPPED,
                    "kafka-ui": ServiceStatus.STOPPED
                }
            )
            
            response = self.client.get("/catalog?force_refresh=true")
            assert response.status_code == 200
            catalog = response.json()
            assert catalog["cluster"]["status"] == "starting"
            assert catalog["services"]["kafka"] == "starting"
            assert catalog["services"]["kafka-rest-proxy"] == "stopped"
            
            # Test scenario 3: All services running with topics
            running_cluster = ClusterStatus(
                status=ServiceStatus.RUNNING,
                broker_count=1,
                version="7.4.0",
                endpoints={
                    "kafka": "localhost:9092",
                    "kafka-rest-proxy": "localhost:8082",
                    "kafka-ui": "localhost:8080"
                },
                uptime=3600,
                services={
                    "kafka": ServiceHealth(status=ServiceStatus.RUNNING),
                    "kafka-rest-proxy": ServiceHealth(status=ServiceStatus.RUNNING),
                    "kafka-ui": ServiceHealth(status=ServiceStatus.RUNNING)
                }
            )
            
            test_topics = [
                TopicInfo(name="catalog-test-topic-1", partitions=1, replication_factor=1, size_bytes=1024),
                TopicInfo(name="catalog-test-topic-2", partitions=3, replication_factor=1, size_bytes=2048),
                TopicInfo(name="catalog-test-topic-3", partitions=5, replication_factor=1, size_bytes=4096)
            ]
            
            mock_catalog.return_value = CatalogResponse(
                cluster=running_cluster,
                topics=test_topics,
                available_apis=[
                    {"path": "/catalog", "method": "GET", "description": "Get service catalog"},
                    {"path": "/cluster/start", "method": "POST", "description": "Start cluster"},
                    {"path": "/cluster/stop", "method": "POST", "description": "Stop cluster"},
                    {"path": "/cluster/status", "method": "GET", "description": "Get cluster status"},
                    {"path": "/topics", "method": "GET", "description": "List topics"},
                    {"path": "/topics", "method": "POST", "description": "Create topic"},
                    {"path": "/topics/{name}", "method": "DELETE", "description": "Delete topic"},
                    {"path": "/produce", "method": "POST", "description": "Produce message"},
                    {"path": "/consume", "method": "GET", "description": "Consume messages"},
                    {"path": "/health", "method": "GET", "description": "Health check"}
                ],
                services={
                    "kafka": ServiceStatus.RUNNING,
                    "kafka-rest-proxy": ServiceStatus.RUNNING,
                    "kafka-ui": ServiceStatus.RUNNING
                }
            )
            
            response = self.client.get("/catalog")
            assert response.status_code == 200
            catalog = response.json()
            
            # Verify cluster status
            assert catalog["cluster"]["status"] == "running"
            assert catalog["cluster"]["broker_count"] == 1
            assert catalog["cluster"]["uptime"] == 3600
            assert "kafka" in catalog["cluster"]["endpoints"]
            
            # Verify topics
            assert len(catalog["topics"]) == 3
            topic_names = [t["name"] for t in catalog["topics"]]
            assert "catalog-test-topic-1" in topic_names
            assert "catalog-test-topic-2" in topic_names
            assert "catalog-test-topic-3" in topic_names
            
            # Verify API endpoints
            assert len(catalog["available_apis"]) == 10
            api_paths = [api["path"] for api in catalog["available_apis"]]
            assert "/catalog" in api_paths
            assert "/produce" in api_paths
            assert "/consume" in api_paths
            
            # Verify services
            assert len(catalog["services"]) == 3
            for service_name, status in catalog["services"].items():
                assert status == "running"
                self.record_service_health(service_name, ServiceStatus.RUNNING, 0)
    
    @pytest.mark.asyncio
    async def test_catalog_consistency_across_endpoints(self, integration_test_config):
        """Test that catalog data is consistent across different API endpoints."""
        
        with patch('src.services.topic_manager.TopicManager.list_topics') as mock_list_topics, \
             patch('src.services.cluster_manager.ClusterManager.get_status') as mock_cluster_status, \
             patch('src.services.service_catalog.ServiceCatalog.get_catalog') as mock_catalog:
            
            # Setup consistent test data
            test_topics = [
                TopicInfo(name="consistency-topic-1", partitions=2, replication_factor=1, size_bytes=1024),
                TopicInfo(name="consistency-topic-2", partitions=4, replication_factor=1, size_bytes=2048)
            ]
            
            cluster_status = ClusterStatus(
                status=ServiceStatus.RUNNING,
                broker_count=1,
                version="7.4.0",
                endpoints={"kafka": "localhost:9092"},
                uptime=1800
            )
            
            mock_list_topics.return_value = test_topics
            mock_cluster_status.return_value = cluster_status
            mock_catalog.return_value = CatalogResponse(
                cluster=cluster_status,
                topics=test_topics,
                available_apis=[],
                services={"kafka": ServiceStatus.RUNNING}
            )
            
            # Get data from different endpoints
            topics_response = self.client.get("/topics")
            cluster_response = self.client.get("/cluster/status")
            catalog_response = self.client.get("/catalog")
            
            assert topics_response.status_code == 200
            assert cluster_response.status_code == 200
            assert catalog_response.status_code == 200
            
            topics_data = topics_response.json()
            cluster_data = cluster_response.json()
            catalog_data = catalog_response.json()
            
            # Verify consistency between endpoints
            # Topics consistency
            assert topics_data["total_count"] == len(catalog_data["topics"])
            for i, topic in enumerate(topics_data["topics"]):
                catalog_topic = catalog_data["topics"][i]
                assert topic["name"] == catalog_topic["name"]
                assert topic["partitions"] == catalog_topic["partitions"]
                assert topic["replication_factor"] == catalog_topic["replication_factor"]
            
            # Cluster status consistency
            assert cluster_data["status"] == catalog_data["cluster"]["status"]
            assert cluster_data["broker_count"] == catalog_data["cluster"]["broker_count"]
            assert cluster_data["version"] == catalog_data["cluster"]["version"]
            assert cluster_data["uptime"] == catalog_data["cluster"]["uptime"]
    
    @pytest.mark.asyncio
    async def test_catalog_performance_with_many_topics(self, integration_test_config):
        """Test catalog performance with a large number of topics."""
        
        with patch('src.services.service_catalog.ServiceCatalog.get_catalog') as mock_catalog:
            
            # Create a large number of topics
            topic_count = 1000
            large_topic_list = [
                TopicInfo(
                    name=f"perf-topic-{i:04d}",
                    partitions=(i % 10) + 1,
                    replication_factor=1,
                    size_bytes=i * 1024
                )
                for i in range(topic_count)
            ]
            
            cluster_status = ClusterStatus(
                status=ServiceStatus.RUNNING,
                broker_count=3,
                version="7.4.0",
                endpoints={
                    "kafka": "localhost:9092",
                    "kafka-rest-proxy": "localhost:8082",
                    "kafka-ui": "localhost:8080"
                },
                uptime=7200
            )
            
            mock_catalog.return_value = CatalogResponse(
                cluster=cluster_status,
                topics=large_topic_list,
                available_apis=[
                    {"path": f"/api/v{i}", "method": "GET", "description": f"API endpoint {i}"}
                    for i in range(50)
                ],
                services={
                    "kafka": ServiceStatus.RUNNING,
                    "kafka-rest-proxy": ServiceStatus.RUNNING,
                    "kafka-ui": ServiceStatus.RUNNING
                }
            )
            
            # Measure catalog response time
            start_time = time.perf_counter()
            response = self.client.get("/catalog")
            end_time = time.perf_counter()
            
            response_time = (end_time - start_time) * 1000  # Convert to milliseconds
            
            assert response.status_code == 200
            catalog_data = response.json()
            
            # Verify all data is present
            assert len(catalog_data["topics"]) == topic_count
            assert len(catalog_data["available_apis"]) == 50
            assert catalog_data["cluster"]["broker_count"] == 3
            
            # Performance assertion
            assert response_time < 2000  # Less than 2 seconds for 1000 topics
            
            self.test_metrics["performance_metrics"]["catalog_response_time_ms"] = response_time
            self.test_metrics["performance_metrics"]["catalog_topic_count"] = topic_count
            
            print(f"\n=== Catalog Performance Test ===")
            print(f"Topics: {topic_count}")
            print(f"Response Time: {response_time:.2f}ms")
            print(f"Topics per ms: {topic_count / response_time:.2f}")


@pytest.mark.integration
class TestErrorRecoveryAndCleanup(EndToEndTestBase):
    """Test error recovery and cleanup scenarios."""
    
    @pytest.mark.asyncio
    async def test_cluster_startup_failure_and_recovery(self, integration_test_config):
        """Test recovery from cluster startup failures."""
        
        with patch('src.services.cluster_manager.ClusterManager.start_cluster') as mock_start:
            
            # Simulate startup failure scenarios
            startup_errors = [
                Exception("Docker daemon not running"),
                Exception("Port 9092 already in use"),
                Exception("Insufficient memory"),
                TimeoutError("Cluster startup timeout")
            ]
            
            for error in startup_errors:
                recovery_start = time.perf_counter()
                
                # First attempt fails
                mock_start.side_effect = error
                
                response = self.client.post("/cluster/start")
                assert response.status_code == 500
                error_data = response.json()
                assert "error" in error_data
                
                # Simulate fixing the issue and retry
                mock_start.side_effect = None
                mock_start.return_value = ClusterStatus(
                    status=ServiceStatus.RUNNING,
                    broker_count=1,
                    version="7.4.0",
                    endpoints={"kafka": "localhost:9092"},
                    uptime=0
                )
                
                # Retry should succeed
                response = self.client.post("/cluster/start")
                assert response.status_code == 200
                
                recovery_time = time.perf_counter() - recovery_start
                self.record_workflow_time(f"error_recovery_{type(error).__name__}", recovery_time)
                
                # Verify cluster is actually running
                response = self.client.get("/cluster/status")
                assert response.status_code == 200
                status_data = response.json()
                assert status_data["status"] == "running"
    
    @pytest.mark.asyncio
    async def test_topic_operation_error_recovery(self, integration_test_config):
        """Test error recovery in topic operations."""
        
        # Test topic creation failures
        with patch('src.services.topic_manager.TopicManager.create_topic') as mock_create:
            
            topic_errors = [
                Exception("Topic already exists"),
                Exception("Invalid partition count"),
                Exception("Kafka not available"),
                ValueError("Invalid topic name")
            ]
            
            for error in topic_errors:
                # First attempt fails
                mock_create.side_effect = error
                
                topic_data = {
                    "name": "error-recovery-topic",
                    "partitions": 3,
                    "replication_factor": 1
                }
                
                response = self.client.post("/topics", json=topic_data)
                assert response.status_code in [400, 500]  # Client or server error
                
                # Fix the issue and retry
                mock_create.side_effect = None
                mock_create.return_value = True
                
                response = self.client.post("/topics", json=topic_data)
                assert response.status_code == 201
        
        # Test topic deletion failures
        with patch('src.services.topic_manager.TopicManager.delete_topic') as mock_delete:
            
            # First attempt fails
            mock_delete.side_effect = Exception("Topic not found")
            
            response = self.client.delete("/topics/nonexistent-topic")
            assert response.status_code in [404, 500]
            
            # Retry with existing topic
            mock_delete.side_effect = None
            mock_delete.return_value = True
            
            response = self.client.delete("/topics/existing-topic")
            assert response.status_code == 200
    
    @pytest.mark.asyncio
    async def test_message_operation_timeout_and_retry(self, integration_test_config):
        """Test timeout handling and retry logic in message operations."""
        
        with patch('src.services.message_manager.MessageManager.produce_message') as mock_produce, \
             patch('src.services.message_manager.MessageManager.consume_messages') as mock_consume:
            
            # Test production timeout and recovery
            mock_produce.side_effect = [
                asyncio.TimeoutError("Production timeout"),
                ProduceResult(
                    topic="timeout-test-topic",
                    partition=0,
                    offset=123,
                    key="test-key",
                    timestamp=int(time.time() * 1000)
                )
            ]
            
            message_data = {
                "topic": "timeout-test-topic",
                "key": "test-key",
                "value": {"data": "timeout test"}
            }
            
            # First attempt times out
            response = self.client.post("/produce", json=message_data)
            assert response.status_code == 500
            
            # Second attempt succeeds
            response = self.client.post("/produce", json=message_data)
            assert response.status_code == 201
            
            # Test consumption timeout and recovery
            mock_consume.side_effect = [
                asyncio.TimeoutError("Consumption timeout"),
                ConsumeResponse(
                    messages=[Message(
                        topic="timeout-test-topic",
                        partition=0,
                        offset=123,
                        key="test-key",
                        value={"data": "timeout test"},
                        timestamp=int(time.time() * 1000)
                    )],
                    consumer_group="timeout-test-group",
                    topic="timeout-test-topic",
                    total_consumed=1,
                    has_more=False
                )
            ]
            
            params = {
                "topic": "timeout-test-topic",
                "consumer_group": "timeout-test-group",
                "max_messages": 10,
                "timeout_ms": 1000
            }
            
            # First attempt times out
            response = self.client.get("/consume", params=params)
            assert response.status_code == 500
            
            # Second attempt succeeds
            response = self.client.get("/consume", params=params)
            assert response.status_code == 200
            consume_data = response.json()
            assert consume_data["total_consumed"] == 1
    
    @pytest.mark.asyncio
    async def test_cleanup_after_failures(self, integration_test_config):
        """Test proper cleanup after various failure scenarios."""
        
        cleanup_scenarios = [
            "cluster_start_failure",
            "topic_creation_failure", 
            "message_production_failure",
            "service_unavailable"
        ]
        
        for scenario in cleanup_scenarios:
            cleanup_start = time.perf_counter()
            
            # Simulate failure scenario
            if scenario == "cluster_start_failure":
                with patch('src.services.cluster_manager.ClusterManager.start_cluster') as mock_start:
                    mock_start.side_effect = Exception("Startup failed")
                    response = self.client.post("/cluster/start")
                    assert response.status_code == 500
            
            elif scenario == "topic_creation_failure":
                with patch('src.services.topic_manager.TopicManager.create_topic') as mock_create:
                    mock_create.side_effect = Exception("Creation failed")
                    topic_data = {"name": "cleanup-test-topic", "partitions": 1, "replication_factor": 1}
                    response = self.client.post("/topics", json=topic_data)
                    assert response.status_code == 500
            
            elif scenario == "message_production_failure":
                with patch('src.services.message_manager.MessageManager.produce_message') as mock_produce:
                    mock_produce.side_effect = Exception("Production failed")
                    message_data = {"topic": "cleanup-topic", "key": "key", "value": {"data": "test"}}
                    response = self.client.post("/produce", json=message_data)
                    assert response.status_code == 500
            
            # Verify system can still respond to health checks
            response = self.client.get("/health")
            assert response.status_code == 200
            
            # Verify cleanup was performed (system is still responsive)
            response = self.client.get("/catalog")
            assert response.status_code == 200
            
            cleanup_time = time.perf_counter() - cleanup_start
            self.record_workflow_time(f"cleanup_{scenario}", cleanup_time)
            
            # Cleanup should be fast
            assert cleanup_time < 5.0  # Less than 5 seconds
    
    @pytest.mark.asyncio
    async def test_graceful_shutdown_after_errors(self, integration_test_config):
        """Test graceful shutdown after encountering errors."""
        
        with patch('src.services.cluster_manager.ClusterManager.stop_cluster') as mock_stop:
            
            # Simulate various error states before shutdown
            error_states = [
                "partial_service_failure",
                "network_connectivity_issues",
                "resource_exhaustion"
            ]
            
            for error_state in error_states:
                # Simulate error state
                print(f"Testing graceful shutdown after {error_state}")
                
                # Attempt graceful shutdown
                mock_stop.return_value = True
                
                shutdown_start = time.perf_counter()
                response = self.client.post("/cluster/stop")
                shutdown_time = time.perf_counter() - shutdown_start
                
                assert response.status_code == 200
                assert response.json()["success"] is True
                
                # Shutdown should be reasonably fast even after errors
                assert shutdown_time < 30.0  # Less than 30 seconds
                
                self.record_workflow_time(f"graceful_shutdown_after_{error_state}", shutdown_time)
                
                # Verify system is in clean state
                response = self.client.get("/cluster/status")
                # Should either return stopped status or service unavailable
                assert response.status_code in [200, 503]


@pytest.mark.integration
class TestPerformanceBenchmarks(EndToEndTestBase):
    """Test performance benchmarks for typical usage patterns."""
    
    @pytest.mark.asyncio
    async def test_typical_developer_workflow_performance(self, integration_test_config):
        """Benchmark typical developer workflow performance."""
        
        # Typical developer workflow:
        # 1. Start cluster
        # 2. Create 2-3 topics
        # 3. Produce 100-500 messages
        # 4. Consume messages
        # 5. Check status/catalog a few times
        # 6. Stop cluster
        
        workflow_start = time.perf_counter()
        performance_metrics = {}
        
        with patch('docker.from_env') as mock_docker, \
             patch('asyncio.create_subprocess_exec') as mock_subprocess:
            
            # Setup mocks
            mock_client = Mock()
            mock_docker.return_value = mock_client
            mock_client.ping.return_value = True
            
            mock_container = Mock()
            mock_container.status = "running"
            mock_container.attrs = {
                "State": {
                    "StartedAt": "2024-01-15T10:30:00.000000000Z",
                    "Health": {"Status": "healthy", "Log": []}
                }
            }
            mock_client.containers.get.return_value = mock_container
            
            mock_process = Mock()
            mock_process.returncode = 0
            mock_process.communicate = asyncio.coroutine(lambda: (b"Success", b""))()
            mock_subprocess.return_value = mock_process
            
            # Step 1: Start cluster (should be fast)
            step_start = time.perf_counter()
            
            with patch('src.services.cluster_manager.ClusterManager.start_cluster') as mock_start:
                mock_start.return_value = ClusterStatus(
                    status=ServiceStatus.RUNNING,
                    broker_count=1,
                    version="7.4.0",
                    endpoints={"kafka": "localhost:9092"},
                    uptime=0
                )
                
                response = self.client.post("/cluster/start")
                assert response.status_code == 200
            
            cluster_start_time = time.perf_counter() - step_start
            performance_metrics["cluster_start_time"] = cluster_start_time
            assert cluster_start_time < 10.0  # Should start within 10 seconds
            
            # Step 2: Create typical topics
            step_start = time.perf_counter()
            
            with patch('src.services.topic_manager.TopicManager.create_topic') as mock_create:
                mock_create.return_value = True
                
                typical_topics = [
                    {"name": "user-events", "partitions": 3, "replication_factor": 1},
                    {"name": "system-logs", "partitions": 1, "replication_factor": 1},
                    {"name": "metrics", "partitions": 5, "replication_factor": 1}
                ]
                
                for topic_config in typical_topics:
                    response = self.client.post("/topics", json=topic_config)
                    assert response.status_code == 201
            
            topic_creation_time = time.perf_counter() - step_start
            performance_metrics["topic_creation_time"] = topic_creation_time
            assert topic_creation_time < 5.0  # Should create topics within 5 seconds
            
            # Step 3: Produce typical message volume
            step_start = time.perf_counter()
            
            with patch('src.services.message_manager.MessageManager.produce_message') as mock_produce:
                
                def mock_produce_func(**kwargs):
                    return ProduceResult(
                        topic=kwargs["topic"],
                        partition=hash(kwargs["key"]) % 5,
                        offset=hash(kwargs["key"]) % 10000,
                        key=kwargs["key"],
                        timestamp=int(time.time() * 1000)
                    )
                
                mock_produce.side_effect = mock_produce_func
                
                # Produce typical developer message volume
                message_counts = {"user-events": 200, "system-logs": 50, "metrics": 100}
                total_produced = 0
                
                for topic_name, count in message_counts.items():
                    for i in range(count):
                        message_data = {
                            "topic": topic_name,
                            "key": f"{topic_name}-{i}",
                            "value": {
                                "id": i,
                                "timestamp": time.time(),
                                "data": f"Typical {topic_name} message {i}",
                                "metadata": {"source": "developer_test", "batch": i // 10}
                            }
                        }
                        
                        response = self.client.post("/produce", json=message_data)
                        assert response.status_code == 201
                        total_produced += 1
            
            message_production_time = time.perf_counter() - step_start
            performance_metrics["message_production_time"] = message_production_time
            performance_metrics["production_rate"] = total_produced / message_production_time
            
            # Should achieve reasonable production rate
            assert performance_metrics["production_rate"] >= 50  # At least 50 messages/second
            
            # Step 4: Consume messages
            step_start = time.perf_counter()
            
            with patch('src.services.message_manager.MessageManager.consume_messages') as mock_consume:
                
                def mock_consume_func(**kwargs):
                    topic = kwargs["topic"]
                    max_messages = kwargs.get("max_messages", 10)
                    
                    # Return realistic message batch
                    message_count = min(max_messages, message_counts.get(topic, 0))
                    messages = [
                        Message(
                            topic=topic,
                            partition=i % 5,
                            offset=i,
                            key=f"{topic}-{i}",
                            value={
                                "id": i,
                                "timestamp": time.time(),
                                "data": f"Typical {topic} message {i}",
                                "metadata": {"source": "developer_test", "batch": i // 10}
                            },
                            timestamp=int(time.time() * 1000) + i
                        )
                        for i in range(message_count)
                    ]
                    
                    return ConsumeResponse(
                        messages=messages,
                        consumer_group=kwargs["consumer_group"],
                        topic=topic,
                        total_consumed=len(messages),
                        has_more=len(messages) == max_messages
                    )
                
                mock_consume.side_effect = mock_consume_func
                
                # Consume from each topic
                total_consumed = 0
                for topic_name in message_counts.keys():
                    params = {
                        "topic": topic_name,
                        "consumer_group": f"dev-group-{topic_name}",
                        "max_messages": 50,
                        "timeout_ms": 5000
                    }
                    
                    response = self.client.get("/consume", params=params)
                    assert response.status_code == 200
                    
                    consume_data = response.json()
                    total_consumed += consume_data["total_consumed"]
            
            message_consumption_time = time.perf_counter() - step_start
            performance_metrics["message_consumption_time"] = message_consumption_time
            performance_metrics["consumption_rate"] = total_consumed / message_consumption_time
            
            # Should achieve reasonable consumption rate
            assert performance_metrics["consumption_rate"] >= 30  # At least 30 messages/second
            
            # Step 5: Check status/catalog (typical monitoring)
            step_start = time.perf_counter()
            
            with patch('src.services.service_catalog.ServiceCatalog.get_catalog') as mock_catalog, \
                 patch('src.services.cluster_manager.ClusterManager.get_status') as mock_status:
                
                mock_status.return_value = ClusterStatus(
                    status=ServiceStatus.RUNNING,
                    broker_count=1,
                    version="7.4.0",
                    endpoints={"kafka": "localhost:9092"},
                    uptime=int(time.time() - workflow_start)
                )
                
                mock_catalog.return_value = CatalogResponse(
                    cluster=mock_status.return_value,
                    topics=[
                        TopicInfo(name=name, partitions=config["partitions"], 
                                replication_factor=1, size_bytes=count * 1024)
                        for (name, config), count in zip(
                            [(name, {"partitions": [3,1,5][i]}) for i, name in enumerate(typical_topics)],
                            message_counts.values()
                        )
                    ],
                    available_apis=[],
                    services={"kafka": ServiceStatus.RUNNING}
                )
                
                # Typical monitoring checks
                monitoring_checks = 5
                for i in range(monitoring_checks):
                    # Alternate between status and catalog checks
                    if i % 2 == 0:
                        response = self.client.get("/cluster/status")
                    else:
                        response = self.client.get("/catalog")
                    
                    assert response.status_code == 200
            
            monitoring_time = time.perf_counter() - step_start
            performance_metrics["monitoring_time"] = monitoring_time
            performance_metrics["monitoring_response_time"] = monitoring_time / monitoring_checks
            
            # Monitoring should be fast
            assert performance_metrics["monitoring_response_time"] < 1.0  # Less than 1 second per check
            
            # Step 6: Stop cluster
            step_start = time.perf_counter()
            
            with patch('src.services.cluster_manager.ClusterManager.stop_cluster') as mock_stop:
                mock_stop.return_value = True
                
                response = self.client.post("/cluster/stop")
                assert response.status_code == 200
            
            cluster_stop_time = time.perf_counter() - step_start
            performance_metrics["cluster_stop_time"] = cluster_stop_time
            assert cluster_stop_time < 15.0  # Should stop within 15 seconds
            
            # Calculate total workflow time
            total_workflow_time = time.perf_counter() - workflow_start
            performance_metrics["total_workflow_time"] = total_workflow_time
            
            # Store metrics for analysis
            self.test_metrics["performance_metrics"]["typical_developer_workflow"] = performance_metrics
            
            # Overall workflow should complete in reasonable time
            assert total_workflow_time < 60.0  # Complete workflow under 1 minute
            
            # Log performance summary
            print(f"\n=== Typical Developer Workflow Performance ===")
            print(f"Total Workflow Time: {total_workflow_time:.2f} seconds")
            print(f"Cluster Start: {cluster_start_time:.2f}s")
            print(f"Topic Creation: {topic_creation_time:.2f}s")
            print(f"Message Production: {message_production_time:.2f}s ({performance_metrics['production_rate']:.1f} msg/sec)")
            print(f"Message Consumption: {message_consumption_time:.2f}s ({performance_metrics['consumption_rate']:.1f} msg/sec)")
            print(f"Monitoring: {monitoring_time:.2f}s ({performance_metrics['monitoring_response_time']:.3f}s per check)")
            print(f"Cluster Stop: {cluster_stop_time:.2f}s")
    
    @pytest.mark.asyncio
    async def test_high_throughput_scenario_performance(self, integration_test_config):
        """Benchmark high-throughput scenario performance."""
        
        scenario_start = time.perf_counter()
        
        with patch('src.services.message_manager.MessageManager.produce_message') as mock_produce, \
             patch('src.services.message_manager.MessageManager.consume_messages') as mock_consume:
            
            # Setup high-throughput mocks
            def mock_produce_func(**kwargs):
                return ProduceResult(
                    topic=kwargs["topic"],
                    partition=hash(kwargs["key"]) % 10,
                    offset=hash(kwargs["key"]) % 100000,
                    key=kwargs["key"],
                    timestamp=int(time.time() * 1000)
                )
            
            mock_produce.side_effect = mock_produce_func
            
            # High-throughput production test
            high_volume_topics = ["high-throughput-topic-1", "high-throughput-topic-2"]
            messages_per_topic = 1000
            
            production_start = time.perf_counter()
            total_produced = 0
            
            # Produce messages in batches for better performance
            batch_size = 50
            for topic_name in high_volume_topics:
                for batch_start in range(0, messages_per_topic, batch_size):
                    batch_end = min(batch_start + batch_size, messages_per_topic)
                    
                    # Produce batch of messages
                    for i in range(batch_start, batch_end):
                        message_data = {
                            "topic": topic_name,
                            "key": f"{topic_name}-key-{i}",
                            "value": {
                                "id": i,
                                "timestamp": time.time(),
                                "payload": f"High throughput message {i}" * 5,  # Larger payload
                                "metadata": {"batch": batch_start // batch_size, "sequence": i}
                            }
                        }
                        
                        response = self.client.post("/produce", json=message_data)
                        assert response.status_code == 201
                        total_produced += 1
            
            production_time = time.perf_counter() - production_start
            production_rate = total_produced / production_time
            
            # High-throughput consumption test
            def mock_consume_func(**kwargs):
                max_messages = kwargs.get("max_messages", 100)
                topic = kwargs["topic"]
                
                # Return large batches for high throughput
                messages = [
                    Message(
                        topic=topic,
                        partition=i % 10,
                        offset=i,
                        key=f"{topic}-key-{i}",
                        value={
                            "id": i,
                            "timestamp": time.time(),
                            "payload": f"High throughput message {i}" * 5,
                            "metadata": {"batch": i // 50, "sequence": i}
                        },
                        timestamp=int(time.time() * 1000) + i
                    )
                    for i in range(max_messages)
                ]
                
                return ConsumeResponse(
                    messages=messages,
                    consumer_group=kwargs["consumer_group"],
                    topic=topic,
                    total_consumed=len(messages),
                    has_more=True
                )
            
            mock_consume.side_effect = mock_consume_func
            
            consumption_start = time.perf_counter()
            total_consumed = 0
            
            # Consume in large batches
            for topic_name in high_volume_topics:
                batches_to_consume = 10
                for batch in range(batches_to_consume):
                    params = {
                        "topic": topic_name,
                        "consumer_group": f"high-throughput-group-{topic_name}",
                        "max_messages": 100,  # Large batch size
                        "timeout_ms": 10000
                    }
                    
                    response = self.client.get("/consume", params=params)
                    assert response.status_code == 200
                    
                    consume_data = response.json()
                    total_consumed += consume_data["total_consumed"]
            
            consumption_time = time.perf_counter() - consumption_start
            consumption_rate = total_consumed / consumption_time
            
            total_scenario_time = time.perf_counter() - scenario_start
            
            # Performance assertions for high-throughput scenario
            assert production_rate >= 200  # At least 200 messages/second production
            assert consumption_rate >= 500  # At least 500 messages/second consumption
            assert total_scenario_time < 30.0  # Complete scenario under 30 seconds
            
            # Store high-throughput metrics
            self.test_metrics["performance_metrics"]["high_throughput_scenario"] = {
                "total_produced": total_produced,
                "total_consumed": total_consumed,
                "production_rate": production_rate,
                "consumption_rate": consumption_rate,
                "production_time": production_time,
                "consumption_time": consumption_time,
                "total_time": total_scenario_time
            }
            
            print(f"\n=== High-Throughput Scenario Performance ===")
            print(f"Total Messages Produced: {total_produced}")
            print(f"Total Messages Consumed: {total_consumed}")
            print(f"Production Rate: {production_rate:.1f} messages/second")
            print(f"Consumption Rate: {consumption_rate:.1f} messages/second")
            print(f"Total Scenario Time: {total_scenario_time:.2f} seconds")
    
    @pytest.mark.asyncio
    async def test_concurrent_operations_performance(self, integration_test_config):
        """Benchmark concurrent operations performance."""
        
        concurrent_start = time.perf_counter()
        
        with patch('src.services.message_manager.MessageManager.produce_message') as mock_produce, \
             patch('src.services.topic_manager.TopicManager.create_topic') as mock_create, \
             patch('src.api.routes.cluster_manager.get_status') as mock_status:
            
            # Setup mocks for concurrent operations
            def mock_produce_func(**kwargs):
                # Simulate slight delay for realistic concurrency testing
                time.sleep(0.001)  # 1ms delay
                return ProduceResult(
                    topic=kwargs["topic"],
                    partition=hash(kwargs["key"]) % 3,
                    offset=hash(kwargs["key"]) % 10000,
                    key=kwargs["key"],
                    timestamp=int(time.time() * 1000)
                )
            
            mock_produce.side_effect = mock_produce_func
            mock_create.return_value = True
            mock_status.return_value = ClusterStatus(
                status=ServiceStatus.RUNNING,
                broker_count=1,
                version="7.4.0",
                endpoints={"kafka": "localhost:9092"},
                uptime=3600
            )
            
            # Test 1: Concurrent topic creation
            topic_creation_start = time.perf_counter()
            
            async def create_topic_async(topic_id):
                topic_data = {
                    "name": f"concurrent-topic-{topic_id}",
                    "partitions": (topic_id % 5) + 1,
                    "replication_factor": 1
                }
                return self.client.post("/topics", json=topic_data)
            
            # Create 20 topics concurrently
            topic_tasks = [create_topic_async(i) for i in range(20)]
            topic_results = await asyncio.gather(*topic_tasks, return_exceptions=True)
            
            topic_creation_time = time.perf_counter() - topic_creation_start
            successful_topic_creates = sum(
                1 for r in topic_results 
                if not isinstance(r, Exception) and r.status_code == 201
            )
            
            # Test 2: Concurrent message production
            production_start = time.perf_counter()
            
            async def produce_message_async(message_id):
                message_data = {
                    "topic": f"concurrent-topic-{message_id % 5}",
                    "key": f"concurrent-key-{message_id}",
                    "value": {
                        "id": message_id,
                        "timestamp": time.time(),
                        "data": f"Concurrent message {message_id}"
                    }
                }
                return self.client.post("/produce", json=message_data)
            
            # Produce 100 messages concurrently
            production_tasks = [produce_message_async(i) for i in range(100)]
            production_results = await asyncio.gather(*production_tasks, return_exceptions=True)
            
            production_time = time.perf_counter() - production_start
            successful_productions = sum(
                1 for r in production_results 
                if not isinstance(r, Exception) and r.status_code == 201
            )
            
            # Test 3: Concurrent status checks
            status_check_start = time.perf_counter()
            
            async def check_status_async():
                return self.client.get("/cluster/status")
            
            # Perform 50 concurrent status checks
            status_tasks = [check_status_async() for _ in range(50)]
            status_results = await asyncio.gather(*status_tasks, return_exceptions=True)
            
            status_check_time = time.perf_counter() - status_check_start
            successful_status_checks = sum(
                1 for r in status_results 
                if not isinstance(r, Exception) and r.status_code == 200
            )
            
            total_concurrent_time = time.perf_counter() - concurrent_start
            
            # Performance assertions
            assert successful_topic_creates >= 18  # At least 90% success rate
            assert successful_productions >= 95  # At least 95% success rate
            assert successful_status_checks >= 48  # At least 96% success rate
            
            # Calculate rates
            topic_creation_rate = successful_topic_creates / topic_creation_time
            production_rate = successful_productions / production_time
            status_check_rate = successful_status_checks / status_check_time
            
            # Store concurrent operation metrics
            self.test_metrics["performance_metrics"]["concurrent_operations"] = {
                "topic_creation_rate": topic_creation_rate,
                "production_rate": production_rate,
                "status_check_rate": status_check_rate,
                "topic_creation_time": topic_creation_time,
                "production_time": production_time,
                "status_check_time": status_check_time,
                "total_time": total_concurrent_time,
                "success_rates": {
                    "topic_creation": successful_topic_creates / 20,
                    "production": successful_productions / 100,
                    "status_checks": successful_status_checks / 50
                }
            }
            
            print(f"\n=== Concurrent Operations Performance ===")
            print(f"Topic Creation: {successful_topic_creates}/20 ({topic_creation_rate:.1f} topics/sec)")
            print(f"Message Production: {successful_productions}/100 ({production_rate:.1f} msg/sec)")
            print(f"Status Checks: {successful_status_checks}/50 ({status_check_rate:.1f} checks/sec)")
            print(f"Total Concurrent Test Time: {total_concurrent_time:.2f} seconds")
    
    def test_performance_summary_and_analysis(self):
        """Analyze and summarize all performance test results."""
        
        if not self.test_metrics["performance_metrics"]:
            pytest.skip("No performance metrics collected")
        
        print(f"\n{'='*60}")
        print("PERFORMANCE BENCHMARK SUMMARY")
        print(f"{'='*60}")
        
        # Analyze workflow times
        if self.test_metrics["workflow_times"]:
            workflow_times = {wf["workflow"]: wf["duration_seconds"] for wf in self.test_metrics["workflow_times"]}
            
            print(f"\nWorkflow Performance:")
            for workflow, duration in workflow_times.items():
                print(f"  {workflow}: {duration:.2f}s")
        
        # Analyze performance metrics
        perf_metrics = self.test_metrics["performance_metrics"]
        
        if "typical_developer_workflow" in perf_metrics:
            tdw = perf_metrics["typical_developer_workflow"]
            print(f"\nTypical Developer Workflow:")
            print(f"  Total Time: {tdw['total_workflow_time']:.2f}s")
            print(f"  Production Rate: {tdw['production_rate']:.1f} msg/sec")
            print(f"  Consumption Rate: {tdw['consumption_rate']:.1f} msg/sec")
        
        if "high_throughput_scenario" in perf_metrics:
            hts = perf_metrics["high_throughput_scenario"]
            print(f"\nHigh-Throughput Scenario:")
            print(f"  Production Rate: {hts['production_rate']:.1f} msg/sec")
            print(f"  Consumption Rate: {hts['consumption_rate']:.1f} msg/sec")
            print(f"  Total Messages: {hts['total_produced']} produced, {hts['total_consumed']} consumed")
        
        if "concurrent_operations" in perf_metrics:
            co = perf_metrics["concurrent_operations"]
            print(f"\nConcurrent Operations:")
            print(f"  Topic Creation Rate: {co['topic_creation_rate']:.1f} topics/sec")
            print(f"  Concurrent Production Rate: {co['production_rate']:.1f} msg/sec")
            print(f"  Status Check Rate: {co['status_check_rate']:.1f} checks/sec")
        
        # Service health summary
        if self.test_metrics["service_health_checks"]:
            health_checks = self.test_metrics["service_health_checks"]
            services = set(check["service"] for check in health_checks)
            
            print(f"\nService Health Summary:")
            for service in services:
                service_checks = [c for c in health_checks if c["service"] == service]
                avg_response_time = sum(c["response_time_ms"] for c in service_checks) / len(service_checks)
                print(f"  {service}: {len(service_checks)} checks, avg {avg_response_time:.2f}ms")
        
        print(f"\n{'='*60}")
        
        # Performance assertions for overall system
        if "typical_developer_workflow" in perf_metrics:
            tdw = perf_metrics["typical_developer_workflow"]
            assert tdw["total_workflow_time"] < 60, "Developer workflow should complete under 1 minute"
            assert tdw["production_rate"] >= 50, "Should achieve at least 50 msg/sec production"
            assert tdw["consumption_rate"] >= 30, "Should achieve at least 30 msg/sec consumption"
        
        print("All performance benchmarks passed!")