"""
Unit tests for multi-cluster data models.
"""

import pytest
from datetime import datetime, timedelta
from pydantic import ValidationError

from src.models.multi_cluster import (
    PortAllocation,
    KafkaConfig,
    RestProxyConfig,
    UIConfig,
    RetentionPolicy,
    ClusterDefinition,
    ClusterTemplate,
    CrossClusterOperation,
    ClusterComparisonResult,
    CreateClusterRequest,
    UpdateClusterRequest,
    ClusterSummary,
    MigrateDataRequest,
    CreateTemplateRequest,
    ClusterEnvironment,
    OperationStatus,
    TemplateCategory
)
from src.models.base import ServiceStatus


class TestPortAllocation:
    """Test PortAllocation data class."""
    
    def test_valid_port_allocation(self):
        """Test creating valid port allocation."""
        allocation = PortAllocation(
            kafka_port=9092,
            rest_proxy_port=8082,
            ui_port=8080,
            jmx_port=9999
        )
        
        assert allocation.kafka_port == 9092
        assert allocation.rest_proxy_port == 8082
        assert allocation.ui_port == 8080
        assert allocation.jmx_port == 9999
    
    def test_port_allocation_without_jmx(self):
        """Test port allocation without JMX port."""
        allocation = PortAllocation(
            kafka_port=9092,
            rest_proxy_port=8082,
            ui_port=8080
        )
        
        assert allocation.jmx_port is None
        assert len(allocation.get_all_ports()) == 3
    
    def test_duplicate_ports_validation(self):
        """Test validation of duplicate ports."""
        with pytest.raises(ValueError, match="duplicate ports"):
            PortAllocation(
                kafka_port=9092,
                rest_proxy_port=9092,  # Duplicate
                ui_port=8080
            )
    
    def test_invalid_port_range(self):
        """Test validation of port ranges."""
        with pytest.raises(ValueError, match="outside valid range"):
            PortAllocation(
                kafka_port=80,  # Too low
                rest_proxy_port=8082,
                ui_port=8080
            )
        
        with pytest.raises(ValueError, match="outside valid range"):
            PortAllocation(
                kafka_port=9092,
                rest_proxy_port=70000,  # Too high
                ui_port=8080
            )
    
    def test_get_all_ports(self):
        """Test getting all allocated ports."""
        allocation = PortAllocation(
            kafka_port=9092,
            rest_proxy_port=8082,
            ui_port=8080,
            jmx_port=9999
        )
        
        ports = allocation.get_all_ports()
        assert len(ports) == 4
        assert 9092 in ports
        assert 8082 in ports
        assert 8080 in ports
        assert 9999 in ports
    
    def test_to_dict(self):
        """Test converting to dictionary."""
        allocation = PortAllocation(
            kafka_port=9092,
            rest_proxy_port=8082,
            ui_port=8080,
            jmx_port=9999
        )
        
        result = allocation.to_dict()
        expected = {
            "kafka_port": 9092,
            "rest_proxy_port": 8082,
            "ui_port": 8080,
            "jmx_port": 9999
        }
        assert result == expected


class TestKafkaConfig:
    """Test KafkaConfig model."""
    
    def test_default_kafka_config(self):
        """Test default Kafka configuration."""
        config = KafkaConfig()
        
        assert config.heap_size == "1G"
        assert config.log_retention_hours == 168
        assert config.num_partitions == 3
        assert config.default_replication_factor == 1
        assert config.custom_properties == {}
    
    def test_custom_kafka_config(self):
        """Test custom Kafka configuration."""
        config = KafkaConfig(
            heap_size="2G",
            log_retention_hours=72,
            num_partitions=6,
            custom_properties={"auto.create.topics.enable": "false"}
        )
        
        assert config.heap_size == "2G"
        assert config.log_retention_hours == 72
        assert config.num_partitions == 6
        assert config.custom_properties["auto.create.topics.enable"] == "false"
    
    def test_invalid_heap_size(self):
        """Test validation of heap size format."""
        with pytest.raises(ValidationError, match="Heap size must be in format"):
            KafkaConfig(heap_size="invalid")
        
        with pytest.raises(ValidationError, match="Heap size must be in format"):
            KafkaConfig(heap_size="1GB")  # Should be 1G
    
    def test_valid_heap_sizes(self):
        """Test valid heap size formats."""
        valid_sizes = ["512M", "1G", "2G", "4G", "8G"]
        
        for size in valid_sizes:
            config = KafkaConfig(heap_size=size)
            assert config.heap_size == size
    
    def test_validation_constraints(self):
        """Test field validation constraints."""
        # Test minimum values
        with pytest.raises(ValidationError):
            KafkaConfig(log_retention_hours=0)
        
        with pytest.raises(ValidationError):
            KafkaConfig(num_partitions=0)
        
        with pytest.raises(ValidationError):
            KafkaConfig(num_network_threads=0)


class TestClusterDefinition:
    """Test ClusterDefinition model."""
    
    def test_minimal_cluster_definition(self):
        """Test creating minimal cluster definition."""
        definition = ClusterDefinition(
            id="test-cluster",
            name="Test Cluster",
            environment=ClusterEnvironment.DEVELOPMENT
        )
        
        assert definition.id == "test-cluster"
        assert definition.name == "Test Cluster"
        assert definition.environment == ClusterEnvironment.DEVELOPMENT
        assert definition.status == ServiceStatus.STOPPED
        assert isinstance(definition.kafka_config, KafkaConfig)
        assert isinstance(definition.created_at, datetime)
    
    def test_full_cluster_definition(self):
        """Test creating full cluster definition."""
        port_allocation = PortAllocation(
            kafka_port=9092,
            rest_proxy_port=8082,
            ui_port=8080
        )
        
        definition = ClusterDefinition(
            id="full-cluster",
            name="Full Test Cluster",
            description="A comprehensive test cluster",
            environment=ClusterEnvironment.TESTING,
            port_allocation=port_allocation,
            network_name="test-network",
            data_directory="/data/test-cluster",
            tags={"team": "engineering", "purpose": "testing"}
        )
        
        assert definition.description == "A comprehensive test cluster"
        assert definition.port_allocation == port_allocation
        assert definition.network_name == "test-network"
        assert definition.data_directory == "/data/test-cluster"
        assert definition.tags["team"] == "engineering"
    
    def test_cluster_id_validation(self):
        """Test cluster ID validation."""
        # Valid IDs
        valid_ids = ["test-cluster", "dev-env-1", "prod-kafka-01"]
        for cluster_id in valid_ids:
            definition = ClusterDefinition(
                id=cluster_id,
                name="Test",
                environment=ClusterEnvironment.DEVELOPMENT
            )
            assert definition.id == cluster_id
        
        # Invalid IDs
        invalid_ids = [
            "Test-Cluster",  # Uppercase
            "-test-cluster",  # Starts with hyphen
            "test-cluster-",  # Ends with hyphen
            "test_cluster",  # Underscore
            "tc",  # Too short
            "a" * 64  # Too long
        ]
        
        for cluster_id in invalid_ids:
            with pytest.raises(ValidationError):
                ClusterDefinition(
                    id=cluster_id,
                    name="Test",
                    environment=ClusterEnvironment.DEVELOPMENT
                )
    
    def test_cluster_name_validation(self):
        """Test cluster name validation."""
        # Valid names
        definition = ClusterDefinition(
            id="test-cluster",
            name="  Test Cluster  ",  # Should be trimmed
            environment=ClusterEnvironment.DEVELOPMENT
        )
        assert definition.name == "Test Cluster"
        
        # Invalid names
        with pytest.raises(ValidationError):
            ClusterDefinition(
                id="test-cluster",
                name="",  # Empty
                environment=ClusterEnvironment.DEVELOPMENT
            )
        
        with pytest.raises(ValidationError):
            ClusterDefinition(
                id="test-cluster",
                name="a" * 101,  # Too long
                environment=ClusterEnvironment.DEVELOPMENT
            )
    
    def test_update_timestamp(self):
        """Test updating timestamp."""
        definition = ClusterDefinition(
            id="test-cluster",
            name="Test Cluster",
            environment=ClusterEnvironment.DEVELOPMENT
        )
        
        original_time = definition.updated_at
        definition.update_timestamp()
        
        assert definition.updated_at > original_time


class TestClusterTemplate:
    """Test ClusterTemplate model."""
    
    def test_basic_template(self):
        """Test creating basic template."""
        template = ClusterTemplate(
            id="dev-template",
            name="Development Template",
            description="Template for development clusters",
            category=TemplateCategory.DEVELOPMENT
        )
        
        assert template.id == "dev-template"
        assert template.name == "Development Template"
        assert template.category == TemplateCategory.DEVELOPMENT
        assert template.version == "1.0.0"
        assert not template.is_builtin
    
    def test_template_with_custom_config(self):
        """Test template with custom configuration."""
        kafka_config = KafkaConfig(heap_size="2G", num_partitions=6)
        
        template = ClusterTemplate(
            id="high-perf-template",
            name="High Performance Template",
            description="Template for high performance clusters",
            category=TemplateCategory.HIGH_THROUGHPUT,
            default_kafka_config=kafka_config,
            min_memory_mb=4096,
            recommended_memory_mb=8192
        )
        
        assert template.default_kafka_config.heap_size == "2G"
        assert template.default_kafka_config.num_partitions == 6
        assert template.min_memory_mb == 4096
        assert template.recommended_memory_mb == 8192
    
    def test_template_id_validation(self):
        """Test template ID validation."""
        # Valid IDs
        valid_ids = ["dev-template", "prod-v2", "custom-01"]
        for template_id in valid_ids:
            template = ClusterTemplate(
                id=template_id,
                name="Test Template",
                description="Test",
                category=TemplateCategory.CUSTOM
            )
            assert template.id == template_id
        
        # Invalid IDs
        invalid_ids = ["Dev-Template", "-dev", "dev-", "dev_template"]
        for template_id in invalid_ids:
            with pytest.raises(ValidationError):
                ClusterTemplate(
                    id=template_id,
                    name="Test Template",
                    description="Test",
                    category=TemplateCategory.CUSTOM
                )
    
    def test_version_validation(self):
        """Test version validation."""
        # Valid versions
        valid_versions = ["1.0.0", "2.1.3", "10.20.30"]
        for version in valid_versions:
            template = ClusterTemplate(
                id="test-template",
                name="Test Template",
                description="Test",
                category=TemplateCategory.CUSTOM,
                version=version
            )
            assert template.version == version
        
        # Invalid versions
        invalid_versions = ["1.0", "v1.0.0", "1.0.0-beta"]
        for version in invalid_versions:
            with pytest.raises(ValidationError):
                ClusterTemplate(
                    id="test-template",
                    name="Test Template",
                    description="Test",
                    category=TemplateCategory.CUSTOM,
                    version=version
                )
    
    def test_apply_to_definition(self):
        """Test applying template to create cluster definition."""
        kafka_config = KafkaConfig(heap_size="2G", num_partitions=6)
        template = ClusterTemplate(
            id="test-template",
            name="Test Template",
            description="Test template",
            category=TemplateCategory.DEVELOPMENT,
            default_kafka_config=kafka_config
        )
        
        definition = template.apply_to_definition(
            cluster_id="test-cluster",
            cluster_name="Test Cluster",
            environment=ClusterEnvironment.DEVELOPMENT
        )
        
        assert definition.id == "test-cluster"
        assert definition.name == "Test Cluster"
        assert definition.template_id == "test-template"
        assert definition.kafka_config.heap_size == "2G"
        assert definition.kafka_config.num_partitions == 6
        assert definition.tags["template"] == "test-template"
        assert definition.tags["category"] == "development"
    
    def test_apply_with_overrides(self):
        """Test applying template with overrides."""
        template = ClusterTemplate(
            id="test-template",
            name="Test Template",
            description="Test template",
            category=TemplateCategory.DEVELOPMENT
        )
        
        overrides = {
            "description": "Custom description",
            "tags": {"custom": "value"}
        }
        
        definition = template.apply_to_definition(
            cluster_id="test-cluster",
            cluster_name="Test Cluster",
            environment=ClusterEnvironment.DEVELOPMENT,
            overrides=overrides
        )
        
        assert definition.description == "Custom description"
        assert definition.tags["custom"] == "value"


class TestCrossClusterOperation:
    """Test CrossClusterOperation model."""
    
    def test_basic_operation(self):
        """Test creating basic cross-cluster operation."""
        operation = CrossClusterOperation(
            id="op-001",
            operation_type="migrate",
            source_cluster_id="source-cluster",
            target_cluster_ids=["target-cluster"]
        )
        
        assert operation.id == "op-001"
        assert operation.operation_type == "migrate"
        assert operation.source_cluster_id == "source-cluster"
        assert operation.target_cluster_ids == ["target-cluster"]
        assert operation.status == OperationStatus.PENDING
        assert operation.progress_percent == 0.0
    
    def test_operation_type_validation(self):
        """Test operation type validation."""
        valid_types = ["migrate", "replicate", "compare", "sync"]
        for op_type in valid_types:
            operation = CrossClusterOperation(
                id="op-001",
                operation_type=op_type,
                source_cluster_id="source",
                target_cluster_ids=["target"]
            )
            assert operation.operation_type == op_type
        
        with pytest.raises(ValidationError, match="Operation type must be one of"):
            CrossClusterOperation(
                id="op-001",
                operation_type="invalid",
                source_cluster_id="source",
                target_cluster_ids=["target"]
            )
    
    def test_update_progress(self):
        """Test updating operation progress."""
        operation = CrossClusterOperation(
            id="op-001",
            operation_type="migrate",
            source_cluster_id="source",
            target_cluster_ids=["target"]
        )
        
        # Test starting operation
        operation.update_progress(10.0, OperationStatus.RUNNING)
        assert operation.progress_percent == 10.0
        assert operation.status == OperationStatus.RUNNING
        assert operation.started_at is not None
        assert operation.completed_at is None
        
        # Test completing operation
        operation.update_progress(100.0, OperationStatus.COMPLETED)
        assert operation.progress_percent == 100.0
        assert operation.status == OperationStatus.COMPLETED
        assert operation.completed_at is not None
    
    def test_progress_bounds(self):
        """Test progress percentage bounds."""
        operation = CrossClusterOperation(
            id="op-001",
            operation_type="migrate",
            source_cluster_id="source",
            target_cluster_ids=["target"]
        )
        
        # Test lower bound
        operation.update_progress(-10.0)
        assert operation.progress_percent == 0.0
        
        # Test upper bound
        operation.update_progress(150.0)
        assert operation.progress_percent == 100.0


class TestClusterComparisonResult:
    """Test ClusterComparisonResult model."""
    
    def test_basic_comparison_result(self):
        """Test creating basic comparison result."""
        result = ClusterComparisonResult(
            source_cluster="cluster-a",
            target_cluster="cluster-b"
        )
        
        assert result.source_cluster == "cluster-a"
        assert result.target_cluster == "cluster-b"
        assert isinstance(result.comparison_timestamp, datetime)
        assert result.is_identical()  # Empty differences = identical
    
    def test_comparison_with_differences(self):
        """Test comparison result with differences."""
        result = ClusterComparisonResult(
            source_cluster="cluster-a",
            target_cluster="cluster-b",
            config_differences={"kafka.heap_size": {"source": "1G", "target": "2G"}},
            topics_only_in_source=["topic-1"],
            topics_only_in_target=["topic-2"],
            topics_with_different_config=["topic-3"]
        )
        
        assert not result.is_identical()
        assert "kafka.heap_size" in result.config_differences
        assert "topic-1" in result.topics_only_in_source
        assert "topic-2" in result.topics_only_in_target
        assert "topic-3" in result.topics_with_different_config


class TestRequestModels:
    """Test API request models."""
    
    def test_create_cluster_request(self):
        """Test CreateClusterRequest model."""
        request = CreateClusterRequest(
            name="Test Cluster",
            environment=ClusterEnvironment.DEVELOPMENT,
            template_id="dev-template",
            tags={"team": "engineering"}
        )
        
        assert request.name == "Test Cluster"
        assert request.environment == ClusterEnvironment.DEVELOPMENT
        assert request.template_id == "dev-template"
        assert request.auto_start is True
        assert request.tags["team"] == "engineering"
    
    def test_update_cluster_request(self):
        """Test UpdateClusterRequest model."""
        kafka_config = KafkaConfig(heap_size="2G")
        
        request = UpdateClusterRequest(
            name="Updated Cluster",
            kafka_config=kafka_config,
            tags={"updated": "true"}
        )
        
        assert request.name == "Updated Cluster"
        assert request.kafka_config.heap_size == "2G"
        assert request.tags["updated"] == "true"
    
    def test_migrate_data_request(self):
        """Test MigrateDataRequest model."""
        request = MigrateDataRequest(
            source_cluster_id="source",
            target_cluster_id="target",
            topics=["topic-1", "topic-2"],
            include_consumer_groups=True,
            dry_run=True
        )
        
        assert request.source_cluster_id == "source"
        assert request.target_cluster_id == "target"
        assert request.topics == ["topic-1", "topic-2"]
        assert request.include_consumer_groups is True
        assert request.dry_run is True
    
    def test_create_template_request(self):
        """Test CreateTemplateRequest model."""
        kafka_config = KafkaConfig(heap_size="2G")
        
        request = CreateTemplateRequest(
            name="Custom Template",
            description="A custom template",
            category=TemplateCategory.CUSTOM,
            kafka_config=kafka_config,
            min_memory_mb=4096,
            tags=["custom", "high-performance"]
        )
        
        assert request.name == "Custom Template"
        assert request.category == TemplateCategory.CUSTOM
        assert request.kafka_config.heap_size == "2G"
        assert request.min_memory_mb == 4096
        assert "custom" in request.tags


class TestEnums:
    """Test enum values."""
    
    def test_cluster_environment_enum(self):
        """Test ClusterEnvironment enum."""
        assert ClusterEnvironment.DEVELOPMENT == "development"
        assert ClusterEnvironment.TESTING == "testing"
        assert ClusterEnvironment.STAGING == "staging"
        assert ClusterEnvironment.PRODUCTION == "production"
    
    def test_operation_status_enum(self):
        """Test OperationStatus enum."""
        assert OperationStatus.PENDING == "pending"
        assert OperationStatus.RUNNING == "running"
        assert OperationStatus.COMPLETED == "completed"
        assert OperationStatus.FAILED == "failed"
        assert OperationStatus.CANCELLED == "cancelled"
    
    def test_template_category_enum(self):
        """Test TemplateCategory enum."""
        assert TemplateCategory.DEVELOPMENT == "development"
        assert TemplateCategory.TESTING == "testing"
        assert TemplateCategory.PRODUCTION == "production"
        assert TemplateCategory.HIGH_THROUGHPUT == "high-throughput"
        assert TemplateCategory.CUSTOM == "custom"


class TestRetentionPolicy:
    """Test RetentionPolicy model."""
    
    def test_default_retention_policy(self):
        """Test default retention policy."""
        policy = RetentionPolicy()
        
        assert policy.log_retention_hours == 168
        assert policy.log_retention_bytes is None
        assert policy.cleanup_policy == "delete"
    
    def test_custom_retention_policy(self):
        """Test custom retention policy."""
        policy = RetentionPolicy(
            log_retention_hours=72,
            log_retention_bytes=1073741824,
            cleanup_policy="compact"
        )
        
        assert policy.log_retention_hours == 72
        assert policy.log_retention_bytes == 1073741824
        assert policy.cleanup_policy == "compact"
    
    def test_cleanup_policy_validation(self):
        """Test cleanup policy validation."""
        valid_policies = ["delete", "compact", "delete,compact"]
        for policy in valid_policies:
            retention = RetentionPolicy(cleanup_policy=policy)
            assert retention.cleanup_policy == policy
        
        with pytest.raises(ValidationError, match="Cleanup policy must be"):
            RetentionPolicy(cleanup_policy="invalid")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])