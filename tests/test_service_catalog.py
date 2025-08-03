"""Unit tests for Service Catalog component."""

import pytest
import time
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime

from src.services.service_catalog import ServiceCatalog, ServiceCatalogError
from src.services.cluster_manager import ClusterManager, ClusterManagerError
from src.services.topic_manager import TopicManager, TopicManagerError, KafkaNotAvailableError
from src.models.base import ServiceStatus
from src.models.cluster import ClusterStatus, ServiceHealth
from src.models.topic import TopicInfo
from src.models.catalog import CatalogResponse, APIEndpoint, ServiceInfo


@pytest.mark.unit
class TestServiceCatalog:
    """Test cases for ServiceCatalog class."""

    @pytest.fixture
    def mock_cluster_manager(self):
        """Create a mock cluster manager."""
        manager = Mock(spec=ClusterManager)
        manager.get_status = AsyncMock()
        return manager

    @pytest.fixture
    def mock_topic_manager(self):
        """Create a mock topic manager."""
        manager = Mock(spec=TopicManager)
        manager.list_topics = AsyncMock()
        manager.close = Mock()
        return manager

    @pytest.fixture
    def service_catalog(self, mock_cluster_manager, mock_topic_manager):
        """Create a ServiceCatalog instance with mocked dependencies."""
        return ServiceCatalog(
            cluster_manager=mock_cluster_manager,
            topic_manager=mock_topic_manager,
            cache_ttl=30
        )

    @pytest.fixture
    def sample_cluster_status(self):
        """Create a sample cluster status."""
        return ClusterStatus(
            status=ServiceStatus.RUNNING,
            broker_count=1,
            version="7.4.0",
            endpoints={
                "kafka": "localhost:9092",
                "kafka-rest-proxy": "http://localhost:8082",
                "kafka-ui": "http://localhost:8080"
            },
            uptime=3600,
            services={
                "kafka": ServiceHealth(
                    status=ServiceStatus.RUNNING,
                    uptime=3600,
                    last_check="2024-01-15T10:30:00"
                ),
                "kafka-rest-proxy": ServiceHealth(
                    status=ServiceStatus.RUNNING,
                    uptime=3500,
                    last_check="2024-01-15T10:30:00"
                ),
                "kafka-ui": ServiceHealth(
                    status=ServiceStatus.RUNNING,
                    uptime=3400,
                    last_check="2024-01-15T10:30:00"
                )
            }
        )

    @pytest.fixture
    def sample_topics(self):
        """Create sample topics list."""
        return [
            TopicInfo(
                name="test-topic-1",
                partitions=3,
                replication_factor=1,
                size_bytes=1024,
                config={"retention.ms": "604800000"}
            ),
            TopicInfo(
                name="test-topic-2", 
                partitions=1,
                replication_factor=1,
                size_bytes=512,
                config={}
            )
        ]

    async def test_get_catalog_success(self, service_catalog, mock_cluster_manager, 
                                     mock_topic_manager, sample_cluster_status, sample_topics):
        """Test successful catalog generation."""
        # Setup mocks
        mock_cluster_manager.get_status.return_value = sample_cluster_status
        mock_topic_manager.list_topics.return_value = sample_topics
        
        # Mock Docker client for system info
        with patch.object(service_catalog.cluster_manager, 'docker_client') as mock_docker:
            mock_docker.info.return_value = {
                "ServerVersion": "20.10.0",
                "ContainersRunning": 3,
                "Containers": 3
            }
            
            # Get catalog
            catalog = await service_catalog.get_catalog()
        
        # Verify result
        assert isinstance(catalog, CatalogResponse)
        assert catalog.cluster == sample_cluster_status
        assert catalog.topics == sample_topics
        assert len(catalog.available_apis) > 0
        assert len(catalog.services) > 0
        assert "local-kafka-manager" in catalog.services
        assert catalog.services["local-kafka-manager"].status == ServiceStatus.RUNNING
        assert "docker" in catalog.system_info
        
        # Verify API endpoints are present
        api_paths = [endpoint.path for endpoint in catalog.available_apis]
        assert "/catalog" in api_paths
        assert "/cluster/start" in api_paths
        assert "/topics" in api_paths
        assert "/produce" in api_paths

    async def test_get_catalog_with_cluster_error(self, service_catalog, mock_cluster_manager,
                                                mock_topic_manager, sample_topics):
        """Test catalog generation when cluster manager fails."""
        # Setup mocks
        mock_cluster_manager.get_status.side_effect = ClusterManagerError("Docker not available")
        mock_topic_manager.list_topics.return_value = sample_topics
        
        # Get catalog
        catalog = await service_catalog.get_catalog()
        
        # Verify result - should return error status but still include topics and APIs
        assert isinstance(catalog, CatalogResponse)
        assert catalog.cluster.status == ServiceStatus.ERROR
        assert catalog.topics == sample_topics
        assert len(catalog.available_apis) > 0

    async def test_get_catalog_with_topic_error(self, service_catalog, mock_cluster_manager,
                                              mock_topic_manager, sample_cluster_status):
        """Test catalog generation when topic manager fails."""
        # Setup mocks
        mock_cluster_manager.get_status.return_value = sample_cluster_status
        mock_topic_manager.list_topics.side_effect = KafkaNotAvailableError("Kafka not available")
        
        # Get catalog
        catalog = await service_catalog.get_catalog()
        
        # Verify result - should return cluster status but empty topics
        assert isinstance(catalog, CatalogResponse)
        assert catalog.cluster == sample_cluster_status
        assert catalog.topics == []
        assert len(catalog.available_apis) > 0

    async def test_get_catalog_with_complete_failure(self, service_catalog, mock_cluster_manager,
                                                   mock_topic_manager):
        """Test catalog generation when everything fails."""
        # Setup mocks to fail
        mock_cluster_manager.get_status.side_effect = Exception("Complete failure")
        mock_topic_manager.list_topics.side_effect = Exception("Complete failure")
        
        # Get catalog
        catalog = await service_catalog.get_catalog()
        
        # Verify result - should return minimal error catalog
        assert isinstance(catalog, CatalogResponse)
        assert catalog.cluster.status == ServiceStatus.ERROR
        assert catalog.topics == []
        assert len(catalog.available_apis) > 0  # API endpoints should still be available
        assert "error" in catalog.services
        assert "error" in catalog.system_info

    async def test_caching_mechanism(self, service_catalog, mock_cluster_manager,
                                   mock_topic_manager, sample_cluster_status, sample_topics):
        """Test that caching works correctly."""
        # Setup mocks
        mock_cluster_manager.get_status.return_value = sample_cluster_status
        mock_topic_manager.list_topics.return_value = sample_topics
        
        # First call
        catalog1 = await service_catalog.get_catalog()
        
        # Second call (should use cache)
        catalog2 = await service_catalog.get_catalog()
        
        # Verify mocks were called only once (due to caching)
        assert mock_cluster_manager.get_status.call_count == 1
        assert mock_topic_manager.list_topics.call_count == 1
        
        # Results should be identical
        assert catalog1.cluster == catalog2.cluster
        assert catalog1.topics == catalog2.topics

    async def test_force_refresh(self, service_catalog, mock_cluster_manager,
                                mock_topic_manager, sample_cluster_status, sample_topics):
        """Test force refresh bypasses cache."""
        # Setup mocks
        mock_cluster_manager.get_status.return_value = sample_cluster_status
        mock_topic_manager.list_topics.return_value = sample_topics
        
        # First call
        await service_catalog.get_catalog()
        
        # Second call with force refresh
        await service_catalog.get_catalog(force_refresh=True)
        
        # Verify mocks were called twice (cache bypassed)
        assert mock_cluster_manager.get_status.call_count == 2
        assert mock_topic_manager.list_topics.call_count == 2

    async def test_cache_expiration(self, service_catalog, mock_cluster_manager,
                                  mock_topic_manager, sample_cluster_status, sample_topics):
        """Test that cache expires after TTL."""
        # Set short cache TTL
        service_catalog.cache_ttl = 1
        
        # Setup mocks
        mock_cluster_manager.get_status.return_value = sample_cluster_status
        mock_topic_manager.list_topics.return_value = sample_topics
        
        # First call
        await service_catalog.get_catalog()
        
        # Wait for cache to expire
        time.sleep(1.1)
        
        # Second call (should refresh cache)
        await service_catalog.get_catalog()
        
        # Verify mocks were called twice (cache expired)
        assert mock_cluster_manager.get_status.call_count == 2
        assert mock_topic_manager.list_topics.call_count == 2

    async def test_refresh_status(self, service_catalog, mock_cluster_manager,
                                mock_topic_manager, sample_cluster_status, sample_topics):
        """Test manual status refresh."""
        # Setup mocks
        mock_cluster_manager.get_status.return_value = sample_cluster_status
        mock_topic_manager.list_topics.return_value = sample_topics
        
        # Populate cache
        await service_catalog.get_catalog()
        
        # Refresh status
        await service_catalog.refresh_status()
        
        # Verify cache was cleared and repopulated
        assert mock_cluster_manager.get_status.call_count == 2
        assert mock_topic_manager.list_topics.call_count == 2

    async def test_refresh_status_with_errors(self, service_catalog, mock_cluster_manager,
                                            mock_topic_manager):
        """Test refresh status handles errors gracefully."""
        # Setup mocks to fail
        mock_cluster_manager.get_status.side_effect = ClusterManagerError("Error")
        mock_topic_manager.list_topics.side_effect = TopicManagerError("Error")
        
        # Should not raise exception
        await service_catalog.refresh_status()
        
        # Verify methods were called
        assert mock_cluster_manager.get_status.call_count == 1
        assert mock_topic_manager.list_topics.call_count == 1

    def test_get_api_specs(self, service_catalog):
        """Test API specifications grouping."""
        specs = service_catalog.get_api_specs()
        
        # Verify grouping
        assert isinstance(specs, dict)
        assert "catalog" in specs
        assert "cluster" in specs
        assert "topics" in specs
        assert "produce" in specs
        assert "consume" in specs
        
        # Verify each group contains APIEndpoint objects
        for category, endpoints in specs.items():
            assert isinstance(endpoints, list)
            for endpoint in endpoints:
                assert isinstance(endpoint, APIEndpoint)

    def test_api_endpoints_structure(self, service_catalog):
        """Test that API endpoints are properly structured."""
        endpoints = service_catalog._api_endpoints
        
        # Verify we have expected endpoints
        paths = [ep.path for ep in endpoints]
        expected_paths = [
            "/catalog", "/cluster/start", "/cluster/stop", "/cluster/status",
            "/topics", "/topics/{name}", "/topics/{name}/metadata",
            "/produce", "/consume", "/health", "/docs", "/openapi.json"
        ]
        
        for expected_path in expected_paths:
            assert expected_path in paths
        
        # Verify endpoint structure
        for endpoint in endpoints:
            assert isinstance(endpoint, APIEndpoint)
            assert endpoint.path
            assert endpoint.method in ["GET", "POST", "DELETE"]
            assert endpoint.description
            assert isinstance(endpoint.parameters, dict)

    async def test_services_info_generation(self, service_catalog, sample_cluster_status):
        """Test service information generation."""
        services = await service_catalog._get_services_info(sample_cluster_status)
        
        # Verify all expected services are present
        expected_services = ["kafka", "kafka-rest-proxy", "kafka-ui", "local-kafka-manager"]
        for service_name in expected_services:
            assert service_name in services
            assert isinstance(services[service_name], ServiceInfo)
        
        # Verify local-kafka-manager is always running
        assert services["local-kafka-manager"].status == ServiceStatus.RUNNING
        assert services["local-kafka-manager"].url == "http://localhost:8000"
        
        # Verify Kafka services match cluster status
        assert services["kafka"].status == ServiceStatus.RUNNING
        assert services["kafka"].url == "localhost:9092"

    async def test_services_info_with_stopped_cluster(self, service_catalog):
        """Test service information when cluster is stopped."""
        stopped_cluster = ClusterStatus(
            status=ServiceStatus.STOPPED,
            broker_count=0,
            services={}
        )
        
        services = await service_catalog._get_services_info(stopped_cluster)
        
        # Verify Kafka services are stopped
        assert services["kafka"].status == ServiceStatus.STOPPED
        assert services["kafka"].url == ""
        
        # Verify local-kafka-manager is still running
        assert services["local-kafka-manager"].status == ServiceStatus.RUNNING

    async def test_system_info_generation(self, service_catalog):
        """Test system information generation."""
        with patch.object(service_catalog.cluster_manager, 'docker_client') as mock_docker:
            mock_docker.info.return_value = {
                "ServerVersion": "20.10.0",
                "ContainersRunning": 3,
                "Containers": 5
            }
            
            system_info = await service_catalog._get_system_info()
        
        # Verify system info structure
        assert "timestamp" in system_info
        assert "cache_ttl" in system_info
        assert "api_version" in system_info
        assert "supported_kafka_version" in system_info
        assert "docker" in system_info
        
        # Verify Docker info
        docker_info = system_info["docker"]
        assert docker_info["version"] == "20.10.0"
        assert docker_info["containers_running"] == 3
        assert docker_info["containers_total"] == 5

    async def test_system_info_without_docker(self, service_catalog):
        """Test system information when Docker is not available."""
        with patch.object(service_catalog.cluster_manager, 'docker_client') as mock_docker:
            mock_docker.info.side_effect = Exception("Docker not available")
            
            system_info = await service_catalog._get_system_info()
        
        # Verify system info still generated
        assert "timestamp" in system_info
        assert "docker" in system_info
        assert system_info["docker"]["error"] == "Docker not available"

    def test_cache_validity_check(self, service_catalog):
        """Test cache validity checking."""
        # Test with no cache
        assert not service_catalog._is_cache_valid("nonexistent")
        
        # Test with valid cache
        service_catalog._status_cache["test"] = "data"
        service_catalog._cache_timestamps["test"] = time.time()
        assert service_catalog._is_cache_valid("test")
        
        # Test with expired cache
        service_catalog._cache_timestamps["test"] = time.time() - 100
        assert not service_catalog._is_cache_valid("test")

    def test_close_method(self, service_catalog, mock_topic_manager):
        """Test close method."""
        service_catalog.close()
        
        # Verify topic manager close was called
        mock_topic_manager.close.assert_called_once()

    def test_close_method_with_error(self, service_catalog, mock_topic_manager):
        """Test close method handles errors gracefully."""
        mock_topic_manager.close.side_effect = Exception("Close error")
        
        # Should not raise exception
        service_catalog.close()
        
        # Verify close was attempted
        mock_topic_manager.close.assert_called_once()

    async def test_cached_cluster_status_with_error_fallback(self, service_catalog, 
                                                           mock_cluster_manager, sample_cluster_status):
        """Test cached cluster status falls back to cache on error."""
        # First populate cache
        mock_cluster_manager.get_status.return_value = sample_cluster_status
        status1 = await service_catalog._get_cached_cluster_status()
        
        # Then make it fail
        mock_cluster_manager.get_status.side_effect = ClusterManagerError("Error")
        status2 = await service_catalog._get_cached_cluster_status()
        
        # Should return cached data
        assert status1 == status2
        assert status2 == sample_cluster_status

    async def test_cached_topics_with_error_fallback(self, service_catalog, 
                                                   mock_topic_manager, sample_topics):
        """Test cached topics falls back to cache on error."""
        # First populate cache
        mock_topic_manager.list_topics.return_value = sample_topics
        topics1 = await service_catalog._get_cached_topics()
        
        # Then make it fail
        mock_topic_manager.list_topics.side_effect = KafkaNotAvailableError("Error")
        topics2 = await service_catalog._get_cached_topics()
        
        # Should return cached data
        assert topics1 == topics2
        assert topics2 == sample_topics

    async def test_cached_cluster_status_no_fallback(self, service_catalog, mock_cluster_manager):
        """Test cached cluster status returns error status when no cache available."""
        # Make it fail with no cache
        mock_cluster_manager.get_status.side_effect = ClusterManagerError("Error")
        status = await service_catalog._get_cached_cluster_status()
        
        # Should return error status
        assert status.status == ServiceStatus.ERROR
        assert status.broker_count == 0

    async def test_cached_topics_no_fallback(self, service_catalog, mock_topic_manager):
        """Test cached topics returns empty list when no cache available."""
        # Make it fail with no cache
        mock_topic_manager.list_topics.side_effect = KafkaNotAvailableError("Error")
        topics = await service_catalog._get_cached_topics()
        
        # Should return empty list
        assert topics == []