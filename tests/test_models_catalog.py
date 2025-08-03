"""Tests for catalog data models."""

import pytest
from pydantic import ValidationError

from src.models.base import ServiceStatus
from src.models.cluster import ClusterStatus
from src.models.topic import TopicInfo
from src.models.catalog import (
    APIEndpoint,
    ServiceInfo,
    CatalogResponse,
)


@pytest.mark.unit
class TestAPIEndpoint:
    """Test APIEndpoint model."""

    def test_api_endpoint_creation(self):
        """Test APIEndpoint creation."""
        endpoint = APIEndpoint(
            path="/topics",
            method="GET",
            description="List all topics"
        )
        assert endpoint.path == "/topics"
        assert endpoint.method == "GET"
        assert endpoint.description == "List all topics"
        assert endpoint.parameters == {}

    def test_api_endpoint_with_parameters(self):
        """Test APIEndpoint with parameters."""
        parameters = {
            "topic_name": {"type": "string", "required": True},
            "partition": {"type": "integer", "required": False}
        }
        endpoint = APIEndpoint(
            path="/topics/{topic_name}",
            method="POST",
            description="Create a topic",
            parameters=parameters
        )
        assert endpoint.parameters == parameters

    def test_api_endpoint_validation(self):
        """Test APIEndpoint validation."""
        # Missing required fields should fail
        with pytest.raises(ValidationError):
            APIEndpoint()

        with pytest.raises(ValidationError):
            APIEndpoint(path="/test")  # Missing method and description


@pytest.mark.unit
class TestServiceInfo:
    """Test ServiceInfo model."""

    def test_service_info_minimal(self):
        """Test ServiceInfo with minimal fields."""
        service = ServiceInfo(
            name="kafka",
            status=ServiceStatus.RUNNING,
            url="localhost:9092"
        )
        assert service.name == "kafka"
        assert service.status == ServiceStatus.RUNNING
        assert service.url == "localhost:9092"
        assert service.version == "unknown"
        assert service.health_check_url == ""

    def test_service_info_complete(self):
        """Test ServiceInfo with all fields."""
        service = ServiceInfo(
            name="kafka-rest-proxy",
            status=ServiceStatus.RUNNING,
            url="http://localhost:8082",
            version="7.4.0",
            health_check_url="http://localhost:8082/health"
        )
        assert service.name == "kafka-rest-proxy"
        assert service.version == "7.4.0"
        assert service.health_check_url == "http://localhost:8082/health"

    def test_service_info_validation(self):
        """Test ServiceInfo validation."""
        # Missing required fields should fail
        with pytest.raises(ValidationError):
            ServiceInfo()

        with pytest.raises(ValidationError):
            ServiceInfo(name="test")  # Missing status and url


@pytest.mark.unit
class TestCatalogResponse:
    """Test CatalogResponse model."""

    def test_catalog_response_minimal(self):
        """Test CatalogResponse with minimal fields."""
        cluster_status = ClusterStatus(status=ServiceStatus.RUNNING)
        catalog = CatalogResponse(cluster=cluster_status)
        
        assert catalog.cluster == cluster_status
        assert catalog.topics == []
        assert catalog.available_apis == []
        assert catalog.services == {}
        assert catalog.system_info == {}

    def test_catalog_response_complete(self):
        """Test CatalogResponse with all fields."""
        # Create cluster status
        cluster_status = ClusterStatus(
            status=ServiceStatus.RUNNING,
            broker_count=1,
            version="7.4.0"
        )
        
        # Create topics
        topics = [
            TopicInfo(name="topic1", partitions=1, replication_factor=1),
            TopicInfo(name="topic2", partitions=3, replication_factor=1)
        ]
        
        # Create API endpoints
        apis = [
            APIEndpoint(
                path="/topics",
                method="GET",
                description="List topics"
            ),
            APIEndpoint(
                path="/cluster/status",
                method="GET",
                description="Get cluster status"
            )
        ]
        
        # Create services
        services = {
            "kafka": ServiceInfo(
                name="kafka",
                status=ServiceStatus.RUNNING,
                url="localhost:9092",
                version="7.4.0"
            ),
            "rest-proxy": ServiceInfo(
                name="kafka-rest-proxy",
                status=ServiceStatus.RUNNING,
                url="http://localhost:8082",
                version="7.4.0"
            )
        }
        
        # Create system info
        system_info = {
            "docker_version": "24.0.0",
            "compose_version": "2.20.0",
            "python_version": "3.11.0"
        }
        
        catalog = CatalogResponse(
            cluster=cluster_status,
            topics=topics,
            available_apis=apis,
            services=services,
            system_info=system_info
        )
        
        assert catalog.cluster == cluster_status
        assert len(catalog.topics) == 2
        assert len(catalog.available_apis) == 2
        assert len(catalog.services) == 2
        assert catalog.system_info == system_info

    def test_catalog_response_validation(self):
        """Test CatalogResponse validation."""
        # Missing cluster should fail
        with pytest.raises(ValidationError):
            CatalogResponse()

    def test_catalog_response_json_serialization(self):
        """Test CatalogResponse JSON serialization."""
        cluster_status = ClusterStatus(status=ServiceStatus.RUNNING)
        topics = [TopicInfo(name="test-topic", partitions=1, replication_factor=1)]
        
        catalog = CatalogResponse(
            cluster=cluster_status,
            topics=topics
        )
        
        json_data = catalog.model_dump()
        assert "cluster" in json_data
        assert "topics" in json_data
        assert "available_apis" in json_data
        assert "services" in json_data
        assert "system_info" in json_data
        assert "timestamp" in json_data  # From BaseResponse
        
        # Verify nested data structure
        assert json_data["cluster"]["status"] == "running"
        assert len(json_data["topics"]) == 1
        assert json_data["topics"][0]["name"] == "test-topic"