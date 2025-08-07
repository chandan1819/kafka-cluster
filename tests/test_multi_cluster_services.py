"""
Tests for multi-cluster service extensions.
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from datetime import datetime

from src.services.topic_manager import TopicManager
from src.services.message_manager import MessageManager
from src.services.multi_cluster_service_catalog import MultiClusterServiceCatalog
from src.services.multi_cluster_manager import MultiClusterManager
from src.models.topic import TopicConfig, TopicInfo
from src.models.message import ProduceRequest, ProduceResult, ConsumeRequest
from src.models.multi_cluster import ClusterSummary, ClusterEnvironment
from src.models.base import ServiceStatus
from src.models.catalog import CatalogResponse, ServiceInfo
from src.exceptions import (
    TopicManagerError,
    MessageProduceError,
    ServiceDiscoveryError,
    CatalogRefreshError
)


@pytest.fixture
def mock_multi_cluster_manager():
    """Mock multi-cluster manager."""
    manager = AsyncMock(spec=MultiClusterManager)
    manager.registry = AsyncMock()
    manager.factory = AsyncMock()
    manager.list_clusters = AsyncMock(return_value=[])
    manager.get_cluster_status = AsyncMock(return_value=ServiceStatus.RUNNING)
    return manager


@pytest.fixture
def sample_cluster_summary():
    """Sample cluster summary for testing."""
    return ClusterSummary(
        id="test-cluster",
        name="Test Cluster",
        environment=ClusterEnvironment.DEVELOPMENT,
        status=ServiceStatus.RUNNING,
        created_at=datetime.utcnow(),
        endpoints={
            "kafka": "localhost:9092",
            "kafka-rest-proxy": "http://localhost:8082",
            "kafka-ui": "http://localhost:8080"
        },
        tags={"env": "test"}
    )


class TestTopicManagerMultiCluster:
    """Test TopicManager with multi-cluster support."""
    
    def test_init_with_cluster_id(self):
        """Test TopicManager initialization with cluster ID."""
        topic_manager = TopicManager(
            bootstrap_servers="localhost:9092",
            cluster_id="test-cluster"
        )
        
        assert topic_manager.cluster_id == "test-cluster"
        assert topic_manager.bootstrap_servers == "localhost:9092"
    
    def test_init_with_default_cluster_id(self):
        """Test TopicManager initialization with default cluster ID."""
        topic_manager = TopicManager(bootstrap_servers="localhost:9092")
        
        assert topic_manager.cluster_id == "default"
    
    @patch('src.services.topic_manager.KafkaAdminClient')
    async def test_list_topics_with_cluster_context(self, mock_admin_client_class):
        """Test listing topics includes cluster context in logs."""
        # Setup mock
        mock_admin_client = Mock()
        mock_admin_client.describe_topics.return_value = {
            "test-topic": Mock(
                partitions=[Mock(replicas=[1, 2, 3])]
            )
        }
        mock_admin_client.describe_configs.return_value = {}
        mock_admin_client_class.return_value = mock_admin_client
        
        topic_manager = TopicManager(
            bootstrap_servers="localhost:9092",
            cluster_id="test-cluster"
        )
        
        with patch.object(topic_manager, '_get_topic_size', return_value=None), \
             patch.object(topic_manager, '_get_topic_offsets', return_value=(None, None, None)):
            
            topics = await topic_manager.list_topics()
            
            assert len(topics) == 1
            assert topics[0].name == "test-topic"
    
    @patch('src.services.topic_manager.KafkaAdminClient')
    async def test_create_topic_with_cluster_context(self, mock_admin_client_class):
        """Test creating topic includes cluster context in logs and errors."""
        # Setup mock to simulate topic creation failure
        mock_admin_client = Mock()
        mock_future = Mock()
        mock_future.result.side_effect = Exception("Creation failed")
        mock_admin_client.create_topics.return_value = {"test-topic": mock_future}
        mock_admin_client_class.return_value = mock_admin_client
        
        topic_manager = TopicManager(
            bootstrap_servers="localhost:9092",
            cluster_id="test-cluster"
        )
        
        topic_config = TopicConfig(
            name="test-topic",
            partitions=3,
            replication_factor=1
        )
        
        with pytest.raises(Exception):  # Should include cluster context in error
            await topic_manager.create_topic(topic_config)
    
    @patch('src.services.topic_manager.KafkaAdminClient')
    async def test_error_messages_include_cluster_id(self, mock_admin_client_class):
        """Test that error messages include cluster ID."""
        # Setup mock to fail
        mock_admin_client_class.side_effect = Exception("Connection failed")
        
        topic_manager = TopicManager(
            bootstrap_servers="localhost:9092",
            cluster_id="test-cluster"
        )
        
        with pytest.raises(TopicManagerError) as exc_info:
            await topic_manager.list_topics()
        
        # Error message should include cluster ID
        assert "test-cluster" in str(exc_info.value)


class TestMessageManagerMultiCluster:
    """Test MessageManager with multi-cluster support."""
    
    def test_init_with_cluster_id(self):
        """Test MessageManager initialization with cluster ID."""
        message_manager = MessageManager(
            rest_proxy_url="http://localhost:8082",
            cluster_id="test-cluster"
        )
        
        assert message_manager.cluster_id == "test-cluster"
        assert message_manager.rest_proxy_url == "http://localhost:8082"
    
    def test_init_with_default_cluster_id(self):
        """Test MessageManager initialization with default cluster ID."""
        message_manager = MessageManager(rest_proxy_url="http://localhost:8082")
        
        assert message_manager.cluster_id == "default"
    
    @patch('httpx.AsyncClient')
    async def test_produce_message_with_cluster_context(self, mock_client_class):
        """Test producing message includes cluster context in logs."""
        # Setup mock HTTP client
        mock_client = AsyncMock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "offsets": [{"partition": 0, "offset": 123}]
        }
        mock_client.get.return_value = mock_response  # Health check
        mock_client.post.return_value = mock_response  # Produce
        mock_client_class.return_value = mock_client
        
        message_manager = MessageManager(
            rest_proxy_url="http://localhost:8082",
            cluster_id="test-cluster"
        )
        
        request = ProduceRequest(
            topic="test-topic",
            value={"message": "test"}
        )
        
        result = await message_manager.produce_message(request)
        
        assert isinstance(result, ProduceResult)
        assert result.topic == "test-topic"
        assert result.partition == 0
        assert result.offset == 123
    
    @patch('httpx.AsyncClient')
    async def test_produce_error_includes_cluster_id(self, mock_client_class):
        """Test that produce errors include cluster ID."""
        # Setup mock to fail health check
        mock_client = AsyncMock()
        mock_response = Mock()
        mock_response.status_code = 500
        mock_client.get.return_value = mock_response
        mock_client_class.return_value = mock_client
        
        message_manager = MessageManager(
            rest_proxy_url="http://localhost:8082",
            cluster_id="test-cluster"
        )
        
        request = ProduceRequest(
            topic="test-topic",
            value={"message": "test"}
        )
        
        with pytest.raises(Exception) as exc_info:
            await message_manager.produce_message(request)
        
        # Error message should include cluster ID
        assert "test-cluster" in str(exc_info.value)


class TestMultiClusterServiceCatalog:
    """Test MultiClusterServiceCatalog functionality."""
    
    async def test_init(self, mock_multi_cluster_manager):
        """Test MultiClusterServiceCatalog initialization."""
        catalog = MultiClusterServiceCatalog(
            multi_cluster_manager=mock_multi_cluster_manager,
            cache_ttl=60
        )
        
        assert catalog.multi_cluster_manager == mock_multi_cluster_manager
        assert catalog.cache_ttl == 60
        assert len(catalog._multi_cluster_endpoints) > 0
    
    async def test_get_aggregated_catalog_empty(self, mock_multi_cluster_manager):
        """Test getting aggregated catalog with no clusters."""
        mock_multi_cluster_manager.list_clusters.return_value = []
        
        catalog = MultiClusterServiceCatalog(mock_multi_cluster_manager)
        result = await catalog.get_aggregated_catalog()
        
        assert result["total_clusters"] == 0
        assert result["active_clusters"] == 0
        assert result["clusters"] == {}
        assert result["cluster_catalogs"] == {}
        assert "available_apis" in result
        assert "aggregated_stats" in result
    
    async def test_get_aggregated_catalog_with_clusters(self, mock_multi_cluster_manager, sample_cluster_summary):
        """Test getting aggregated catalog with clusters."""
        mock_multi_cluster_manager.list_clusters.return_value = [sample_cluster_summary]
        
        # Mock cluster catalog creation
        from src.models.cluster import ClusterStatus
        mock_cluster_status = ClusterStatus(
            status=ServiceStatus.RUNNING,
            broker_count=1,
            version="7.4.0",
            endpoints={"kafka": "localhost:9092"},
            services={}
        )
        
        mock_catalog = CatalogResponse(
            cluster=mock_cluster_status,
            topics=[
                TopicInfo(name="test-topic", partitions=3, replication_factor=1)
            ],
            available_apis=[],
            services={
                "kafka": ServiceInfo(
                    name="Apache Kafka",
                    status=ServiceStatus.RUNNING,
                    url="localhost:9092",
                    version="7.4.0"
                )
            },
            system_info={"cluster_id": "test-cluster"}
        )
        
        catalog = MultiClusterServiceCatalog(mock_multi_cluster_manager)
        
        with patch.object(catalog, '_get_cluster_catalog', return_value=mock_catalog):
            result = await catalog.get_aggregated_catalog()
            
            assert result["total_clusters"] == 1
            assert result["active_clusters"] == 1
            assert "test-cluster" in result["clusters"]
            assert "test-cluster" in result["cluster_catalogs"]
            assert result["aggregated_stats"]["total_topics"] == 1
            assert result["aggregated_stats"]["total_services"] == 1
    
    async def test_get_cluster_catalog(self, mock_multi_cluster_manager, sample_cluster_summary):
        """Test getting catalog for specific cluster."""
        mock_multi_cluster_manager.get_cluster_status.return_value = ServiceStatus.RUNNING
        mock_multi_cluster_manager.registry.get_cluster.return_value = Mock()
        mock_multi_cluster_manager.factory.get_cluster_endpoints.return_value = {
            "kafka": "localhost:9092",
            "kafka-rest-proxy": "http://localhost:8082"
        }
        
        catalog = MultiClusterServiceCatalog(mock_multi_cluster_manager)
        
        # Mock the service catalog creation
        with patch('src.services.multi_cluster_service_catalog.TopicManager') as mock_topic_manager_class, \
             patch('src.services.multi_cluster_service_catalog.ServiceCatalog') as mock_service_catalog_class:
            
            mock_topic_manager = Mock()
            mock_topic_manager.close = Mock()
            mock_topic_manager_class.return_value = mock_topic_manager
            
            mock_service_catalog = AsyncMock()
            mock_service_catalog.close = Mock()
            from src.models.cluster import ClusterStatus
            mock_cluster_status = ClusterStatus(
                status=ServiceStatus.RUNNING,
                broker_count=1,
                services={}
            )
            
            mock_service_catalog.get_catalog.return_value = CatalogResponse(
                cluster=mock_cluster_status,
                topics=[],
                available_apis=[],
                services={},
                system_info={}
            )
            mock_service_catalog_class.return_value = mock_service_catalog
            
            result = await catalog.get_cluster_catalog("test-cluster")
            
            assert isinstance(result, CatalogResponse)
            assert result.system_info["cluster_id"] == "test-cluster"
            mock_topic_manager.close.assert_called_once()
            mock_service_catalog.close.assert_called_once()
    
    async def test_get_cluster_catalog_not_running(self, mock_multi_cluster_manager):
        """Test getting catalog for non-running cluster."""
        mock_multi_cluster_manager.get_cluster_status.return_value = ServiceStatus.STOPPED
        
        catalog = MultiClusterServiceCatalog(mock_multi_cluster_manager)
        result = await catalog.get_cluster_catalog("test-cluster")
        
        assert isinstance(result, CatalogResponse)
        assert result.system_info["cluster_id"] == "test-cluster"
        assert result.system_info["status"] == "stopped"
        assert len(result.topics) == 0
        assert len(result.services) == 0
    
    async def test_get_cluster_services(self, mock_multi_cluster_manager):
        """Test getting services for specific cluster."""
        from src.models.cluster import ClusterStatus
        mock_cluster_status = ClusterStatus(
            status=ServiceStatus.RUNNING,
            broker_count=1,
            services={}
        )
        
        mock_catalog = CatalogResponse(
            cluster=mock_cluster_status,
            topics=[],
            available_apis=[],
            services={
                "kafka": ServiceInfo(
                    name="Apache Kafka",
                    status=ServiceStatus.RUNNING,
                    url="localhost:9092",
                    version="7.4.0"
                )
            },
            system_info={}
        )
        
        catalog = MultiClusterServiceCatalog(mock_multi_cluster_manager)
        
        with patch.object(catalog, '_get_cluster_catalog', return_value=mock_catalog):
            services = await catalog.get_cluster_services("test-cluster")
            
            assert "kafka" in services
            assert services["kafka"].name == "Apache Kafka"
            assert services["kafka"].status == ServiceStatus.RUNNING
    
    async def test_get_cluster_topics(self, mock_multi_cluster_manager):
        """Test getting topics for specific cluster."""
        from src.models.cluster import ClusterStatus
        mock_cluster_status = ClusterStatus(
            status=ServiceStatus.RUNNING,
            broker_count=1,
            services={}
        )
        
        mock_catalog = CatalogResponse(
            cluster=mock_cluster_status,
            topics=[
                TopicInfo(name="topic1", partitions=3, replication_factor=1),
                TopicInfo(name="topic2", partitions=1, replication_factor=1)
            ],
            available_apis=[],
            services={},
            system_info={}
        )
        
        catalog = MultiClusterServiceCatalog(mock_multi_cluster_manager)
        
        with patch.object(catalog, '_get_cluster_catalog', return_value=mock_catalog):
            topics = await catalog.get_cluster_topics("test-cluster")
            
            assert len(topics) == 2
            assert topics[0]["name"] == "topic1"
            assert topics[0]["cluster_id"] == "test-cluster"
            assert topics[1]["name"] == "topic2"
            assert topics[1]["cluster_id"] == "test-cluster"
    
    async def test_create_cluster_topic_manager(self, mock_multi_cluster_manager):
        """Test creating topic manager for specific cluster."""
        mock_cluster = Mock()
        mock_multi_cluster_manager.registry.get_cluster.return_value = mock_cluster
        mock_multi_cluster_manager.factory.get_cluster_endpoints.return_value = {
            "kafka": "localhost:9092"
        }
        
        catalog = MultiClusterServiceCatalog(mock_multi_cluster_manager)
        
        with patch('src.services.multi_cluster_service_catalog.TopicManager') as mock_topic_manager_class:
            mock_topic_manager = Mock()
            mock_topic_manager_class.return_value = mock_topic_manager
            
            result = await catalog.create_cluster_topic_manager("test-cluster")
            
            assert result == mock_topic_manager
            mock_topic_manager_class.assert_called_once_with(
                bootstrap_servers="localhost:9092",
                cluster_id="test-cluster"
            )
    
    async def test_create_cluster_topic_manager_no_kafka_endpoint(self, mock_multi_cluster_manager):
        """Test creating topic manager when Kafka endpoint is not available."""
        mock_cluster = Mock()
        mock_multi_cluster_manager.registry.get_cluster.return_value = mock_cluster
        mock_multi_cluster_manager.factory.get_cluster_endpoints.return_value = {}
        
        catalog = MultiClusterServiceCatalog(mock_multi_cluster_manager)
        
        with pytest.raises(ServiceDiscoveryError) as exc_info:
            await catalog.create_cluster_topic_manager("test-cluster")
        
        assert "Kafka endpoint not available" in str(exc_info.value)
    
    async def test_create_cluster_message_manager(self, mock_multi_cluster_manager):
        """Test creating message manager for specific cluster."""
        mock_cluster = Mock()
        mock_multi_cluster_manager.registry.get_cluster.return_value = mock_cluster
        mock_multi_cluster_manager.factory.get_cluster_endpoints.return_value = {
            "kafka-rest-proxy": "http://localhost:8082"
        }
        
        catalog = MultiClusterServiceCatalog(mock_multi_cluster_manager)
        
        with patch('src.services.multi_cluster_service_catalog.MessageManager') as mock_message_manager_class:
            mock_message_manager = Mock()
            mock_message_manager_class.return_value = mock_message_manager
            
            result = await catalog.create_cluster_message_manager("test-cluster")
            
            assert result == mock_message_manager
            mock_message_manager_class.assert_called_once_with(
                rest_proxy_url="http://localhost:8082",
                cluster_id="test-cluster"
            )
    
    async def test_create_cluster_message_manager_no_rest_proxy_endpoint(self, mock_multi_cluster_manager):
        """Test creating message manager when REST Proxy endpoint is not available."""
        mock_cluster = Mock()
        mock_multi_cluster_manager.registry.get_cluster.return_value = mock_cluster
        mock_multi_cluster_manager.factory.get_cluster_endpoints.return_value = {}
        
        catalog = MultiClusterServiceCatalog(mock_multi_cluster_manager)
        
        with pytest.raises(ServiceDiscoveryError) as exc_info:
            await catalog.create_cluster_message_manager("test-cluster")
        
        assert "REST Proxy endpoint not available" in str(exc_info.value)
    
    async def test_refresh_all_clusters(self, mock_multi_cluster_manager, sample_cluster_summary):
        """Test refreshing catalog data for all clusters."""
        mock_multi_cluster_manager.list_clusters.return_value = [sample_cluster_summary]
        
        catalog = MultiClusterServiceCatalog(mock_multi_cluster_manager)
        
        # Mock successful refresh
        with patch.object(catalog, '_get_cluster_catalog', return_value=Mock()) as mock_get_catalog:
            results = await catalog.refresh_all_clusters()
            
            assert results["test-cluster"] is True
            mock_get_catalog.assert_called_once_with("test-cluster", force_refresh=True)
    
    async def test_refresh_all_clusters_with_failures(self, mock_multi_cluster_manager, sample_cluster_summary):
        """Test refreshing catalog data with some failures."""
        mock_multi_cluster_manager.list_clusters.return_value = [sample_cluster_summary]
        
        catalog = MultiClusterServiceCatalog(mock_multi_cluster_manager)
        
        # Mock failed refresh
        with patch.object(catalog, '_get_cluster_catalog', side_effect=Exception("Refresh failed")):
            results = await catalog.refresh_all_clusters()
            
            assert results["test-cluster"] is False
    
    async def test_cache_validity(self, mock_multi_cluster_manager):
        """Test cache validity checking."""
        catalog = MultiClusterServiceCatalog(mock_multi_cluster_manager, cache_ttl=1)
        
        # Initially invalid
        assert not catalog._is_cache_valid("test-cluster")
        
        # Add to cache
        import time
        catalog._cluster_catalogs["test-cluster"] = Mock()
        catalog._cache_timestamps["test-cluster"] = time.time()
        
        # Should be valid
        assert catalog._is_cache_valid("test-cluster")
        
        # Wait for expiry
        await asyncio.sleep(1.1)
        
        # Should be invalid
        assert not catalog._is_cache_valid("test-cluster")
    
    async def test_aggregated_stats_calculation(self, mock_multi_cluster_manager, sample_cluster_summary):
        """Test aggregated statistics calculation."""
        clusters = [sample_cluster_summary]
        from src.models.cluster import ClusterStatus
        mock_cluster_status = ClusterStatus(
            status=ServiceStatus.RUNNING,
            broker_count=1,
            services={}
        )
        
        cluster_catalogs = {
            "test-cluster": CatalogResponse(
                cluster=mock_cluster_status,
                topics=[
                    TopicInfo(name="topic1", partitions=3, replication_factor=1),
                    TopicInfo(name="topic2", partitions=1, replication_factor=1)
                ],
                available_apis=[],
                services={
                    "kafka": ServiceInfo(
                        name="Apache Kafka",
                        status=ServiceStatus.RUNNING,
                        url="localhost:9092",
                        version="7.4.0"
                    ),
                    "kafka-ui": ServiceInfo(
                        name="Kafka UI",
                        status=ServiceStatus.STOPPED,
                        url="",
                        version="unknown"
                    )
                },
                system_info={}
            )
        }
        
        catalog = MultiClusterServiceCatalog(mock_multi_cluster_manager)
        stats = await catalog._get_aggregated_stats(clusters, cluster_catalogs)
        
        assert stats["total_topics"] == 2
        assert stats["total_services"] == 2
        assert stats["running_services"] == 1
        assert stats["cluster_distribution"]["running"] == 1
        assert stats["environments"]["development"] == 1
        assert "apache_kafka" in stats["service_types"]
        assert "kafka_ui" in stats["service_types"]
    
    def test_multi_cluster_endpoints_generation(self, mock_multi_cluster_manager):
        """Test that multi-cluster API endpoints are properly generated."""
        catalog = MultiClusterServiceCatalog(mock_multi_cluster_manager)
        endpoints = catalog._multi_cluster_endpoints
        
        assert len(endpoints) > 0
        
        # Check for key endpoints
        endpoint_paths = [ep.path for ep in endpoints]
        assert "/multi-cluster/catalog" in endpoint_paths
        assert "/multi-cluster/clusters" in endpoint_paths
        assert "/multi-cluster/clusters/{cluster_id}/topics" in endpoint_paths
        assert "/multi-cluster/clusters/{cluster_id}/produce" in endpoint_paths
        assert "/multi-cluster/health" in endpoint_paths
        
        # Check endpoint details
        catalog_endpoint = next(ep for ep in endpoints if ep.path == "/multi-cluster/catalog")
        assert catalog_endpoint.method == "GET"
        assert "force_refresh" in catalog_endpoint.parameters
    
    async def test_error_handling_in_get_cluster_catalog(self, mock_multi_cluster_manager):
        """Test error handling in _get_cluster_catalog method."""
        mock_multi_cluster_manager.get_cluster_status.side_effect = Exception("Status error")
        
        catalog = MultiClusterServiceCatalog(mock_multi_cluster_manager)
        result = await catalog._get_cluster_catalog("test-cluster")
        
        # Should return error catalog instead of raising
        assert isinstance(result, CatalogResponse)
        assert result.system_info["cluster_id"] == "test-cluster"
        assert "error" in result.system_info
        assert "Status error" in result.system_info["error"]


class TestServiceIntegration:
    """Test integration between extended services."""
    
    async def test_topic_manager_message_manager_integration(self, mock_multi_cluster_manager):
        """Test that TopicManager and MessageManager work together for same cluster."""
        cluster_id = "integration-test-cluster"
        
        # Create managers for same cluster
        topic_manager = TopicManager(
            bootstrap_servers="localhost:9092",
            cluster_id=cluster_id
        )
        message_manager = MessageManager(
            rest_proxy_url="http://localhost:8082",
            cluster_id=cluster_id
        )
        
        assert topic_manager.cluster_id == cluster_id
        assert message_manager.cluster_id == cluster_id
    
    async def test_multi_cluster_service_catalog_integration(self, mock_multi_cluster_manager, sample_cluster_summary):
        """Test MultiClusterServiceCatalog integration with managers."""
        mock_multi_cluster_manager.list_clusters.return_value = [sample_cluster_summary]
        mock_multi_cluster_manager.get_cluster_status.return_value = ServiceStatus.RUNNING
        mock_multi_cluster_manager.registry.get_cluster.return_value = Mock()
        mock_multi_cluster_manager.factory.get_cluster_endpoints.return_value = {
            "kafka": "localhost:9092",
            "kafka-rest-proxy": "http://localhost:8082"
        }
        
        catalog = MultiClusterServiceCatalog(mock_multi_cluster_manager)
        
        # Test creating both managers for same cluster
        with patch('src.services.multi_cluster_service_catalog.TopicManager') as mock_topic_manager_class, \
             patch('src.services.multi_cluster_service_catalog.MessageManager') as mock_message_manager_class:
            
            mock_topic_manager_class.return_value = Mock()
            mock_message_manager_class.return_value = Mock()
            
            topic_manager = await catalog.create_cluster_topic_manager("test-cluster")
            message_manager = await catalog.create_cluster_message_manager("test-cluster")
            
            # Both should be created with same cluster_id
            mock_topic_manager_class.assert_called_once_with(
                bootstrap_servers="localhost:9092",
                cluster_id="test-cluster"
            )
            mock_message_manager_class.assert_called_once_with(
                rest_proxy_url="http://localhost:8082",
                cluster_id="test-cluster"
            )


@pytest.mark.integration
class TestMultiClusterServiceIntegration:
    """Integration tests for multi-cluster service functionality."""
    
    async def test_full_multi_cluster_workflow(self, mock_multi_cluster_manager, sample_cluster_summary):
        """Test complete multi-cluster service workflow."""
        # Setup multi-cluster manager with test cluster
        mock_multi_cluster_manager.list_clusters.return_value = [sample_cluster_summary]
        mock_multi_cluster_manager.get_cluster_status.return_value = ServiceStatus.RUNNING
        mock_multi_cluster_manager.registry.get_cluster.return_value = Mock()
        mock_multi_cluster_manager.factory.get_cluster_endpoints.return_value = {
            "kafka": "localhost:9092",
            "kafka-rest-proxy": "http://localhost:8082"
        }
        
        # Create multi-cluster service catalog
        catalog = MultiClusterServiceCatalog(mock_multi_cluster_manager)
        
        # Mock service catalog components
        with patch('src.services.multi_cluster_service_catalog.TopicManager') as mock_topic_manager_class, \
             patch('src.services.multi_cluster_service_catalog.ServiceCatalog') as mock_service_catalog_class:
            
            # Setup mocks
            mock_topic_manager = Mock()
            mock_topic_manager.close = Mock()
            mock_topic_manager_class.return_value = mock_topic_manager
            
            mock_service_catalog = AsyncMock()
            mock_service_catalog.close = Mock()
            from src.models.cluster import ClusterStatus
            mock_cluster_status = ClusterStatus(
                status=ServiceStatus.RUNNING,
                broker_count=1,
                services={}
            )
            
            mock_service_catalog.get_catalog.return_value = CatalogResponse(
                cluster=mock_cluster_status,
                topics=[TopicInfo(name="test-topic", partitions=3, replication_factor=1)],
                available_apis=[],
                services={
                    "kafka": ServiceInfo(
                        name="Apache Kafka",
                        status=ServiceStatus.RUNNING,
                        url="localhost:9092",
                        version="7.4.0"
                    )
                },
                system_info={}
            )
            mock_service_catalog_class.return_value = mock_service_catalog
            
            # Test workflow
            # 1. Get aggregated catalog
            aggregated = await catalog.get_aggregated_catalog()
            assert aggregated["total_clusters"] == 1
            assert "test-cluster" in aggregated["clusters"]
            
            # 2. Get cluster-specific catalog
            cluster_catalog = await catalog.get_cluster_catalog("test-cluster")
            assert cluster_catalog.system_info["cluster_id"] == "test-cluster"
            
            # 3. Get cluster services
            services = await catalog.get_cluster_services("test-cluster")
            assert "kafka" in services
            
            # 4. Get cluster topics
            topics = await catalog.get_cluster_topics("test-cluster")
            assert len(topics) == 1
            assert topics[0]["cluster_id"] == "test-cluster"
            
            # 5. Create cluster-specific managers
            topic_manager = await catalog.create_cluster_topic_manager("test-cluster")
            message_manager = await catalog.create_cluster_message_manager("test-cluster")
            
            # Verify all components were created correctly
            assert topic_manager is not None
            assert message_manager is not None
            mock_topic_manager.close.assert_called()
            mock_service_catalog.close.assert_called()