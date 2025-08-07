"""
Unit tests for template manager.
"""

import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock
from typing import List, Optional, Dict, Any

from src.services.template_manager import (
    TemplateManager,
    TemplateManagerError,
    TemplateNotFoundError,
    TemplateAlreadyExistsError,
    TemplateValidationError
)
from src.models.multi_cluster import (
    ClusterTemplate,
    ClusterDefinition,
    TemplateCategory,
    ClusterEnvironment,
    KafkaConfig,
    RestProxyConfig,
    UIConfig,
    RetentionPolicy,
    ServiceStatus
)
from src.storage.base import StorageBackend


class MockStorageBackend(StorageBackend):
    """Mock storage backend for testing."""
    
    def __init__(self):
        self.templates: Dict[str, ClusterTemplate] = {}
        self.clusters: Dict[str, ClusterDefinition] = {}
        self.initialized = False
    
    async def initialize(self) -> bool:
        self.initialized = True
        return True
    
    async def close(self) -> None:
        self.initialized = False
    
    # Template operations
    async def save_template(self, template: ClusterTemplate) -> bool:
        if template.id in self.templates:
            return False  # Already exists
        self.templates[template.id] = template
        return True
    
    async def load_template(self, template_id: str) -> Optional[ClusterTemplate]:
        template = self.templates.get(template_id)
        if template:
            # Return a copy to avoid modifying the stored template
            return template.copy(deep=True)
        return None
    
    async def update_template(self, template: ClusterTemplate) -> bool:
        if template.id not in self.templates:
            return False  # Doesn't exist
        self.templates[template.id] = template
        return True
    
    async def delete_template(self, template_id: str) -> bool:
        if template_id not in self.templates:
            return False  # Doesn't exist
        del self.templates[template_id]
        return True
    
    async def list_templates(self, filters: Optional[Dict[str, Any]] = None) -> List[ClusterTemplate]:
        templates = list(self.templates.values())
        
        if filters:
            if "category" in filters:
                templates = [t for t in templates if t.category.value == filters["category"]]
        
        return templates
    
    async def template_exists(self, template_id: str) -> bool:
        return template_id in self.templates
    
    # Cluster operations (minimal implementation for testing)
    async def save_cluster(self, cluster: ClusterDefinition) -> bool:
        self.clusters[cluster.id] = cluster
        return True
    
    async def load_cluster(self, cluster_id: str) -> Optional[ClusterDefinition]:
        return self.clusters.get(cluster_id)
    
    async def update_cluster(self, cluster: ClusterDefinition) -> bool:
        if cluster.id not in self.clusters:
            return False
        self.clusters[cluster.id] = cluster
        return True
    
    async def delete_cluster(self, cluster_id: str) -> bool:
        if cluster_id not in self.clusters:
            return False
        del self.clusters[cluster_id]
        return True
    
    async def list_clusters(self, filters: Optional[Dict[str, Any]] = None) -> List[ClusterDefinition]:
        clusters = list(self.clusters.values())
        
        if filters:
            if "template_id" in filters:
                clusters = [c for c in clusters if c.template_id == filters["template_id"]]
        
        return clusters
    
    async def cluster_exists(self, cluster_id: str) -> bool:
        return cluster_id in self.clusters
    
    # Utility methods
    async def health_check(self) -> Dict[str, Any]:
        return {"status": "healthy"}
    
    async def get_stats(self) -> Dict[str, Any]:
        return {"templates": len(self.templates), "clusters": len(self.clusters)}
    
    async def backup(self, backup_path: str) -> bool:
        return True
    
    async def restore(self, backup_path: str) -> bool:
        return True


@pytest.fixture
def mock_storage():
    """Create mock storage backend."""
    return MockStorageBackend()


@pytest.fixture
def template_manager(mock_storage):
    """Create template manager with mock storage."""
    return TemplateManager(mock_storage)


@pytest.fixture
def sample_template():
    """Create sample template for testing."""
    return ClusterTemplate(
        id="test-template",
        name="Test Template",
        description="Template for testing",
        category=TemplateCategory.CUSTOM,
        default_kafka_config=KafkaConfig(heap_size="1G"),
        default_rest_proxy_config=RestProxyConfig(heap_size="512M"),
        default_ui_config=UIConfig(heap_size="512M"),
        default_retention_policy=RetentionPolicy(),
        min_memory_mb=2048,
        min_disk_gb=5,
        recommended_memory_mb=4096,
        recommended_disk_gb=10,
        version="1.0.0",
        is_builtin=False,
        tags=["test"]
    )


@pytest.fixture
def sample_cluster():
    """Create sample cluster for testing."""
    return ClusterDefinition(
        id="test-cluster",
        name="Test Cluster",
        environment=ClusterEnvironment.DEVELOPMENT,
        kafka_config=KafkaConfig(heap_size="2G"),
        rest_proxy_config=RestProxyConfig(heap_size="1G"),
        ui_config=UIConfig(heap_size="1G"),
        retention_policy=RetentionPolicy(log_retention_hours=48)
    )


class TestTemplateManager:
    """Test template manager functionality."""
    
    @pytest.mark.asyncio
    async def test_initialization(self, template_manager, mock_storage):
        """Test template manager initialization."""
        assert not template_manager._initialized
        
        await template_manager.initialize()
        
        assert template_manager._initialized
        assert mock_storage.initialized
        
        # Check that built-in templates were loaded
        builtin_templates = await template_manager.get_builtin_templates()
        assert len(builtin_templates) == 4  # dev, test, prod, high-throughput
        
        # Verify built-in template IDs
        builtin_ids = {t.id for t in builtin_templates}
        expected_ids = {"development", "testing", "production", "high-throughput"}
        assert builtin_ids == expected_ids
    
    @pytest.mark.asyncio
    async def test_builtin_templates_content(self, template_manager):
        """Test built-in templates have correct configurations."""
        await template_manager.initialize()
        
        # Test development template
        dev_template = await template_manager.get_template("development")
        assert dev_template.category == TemplateCategory.DEVELOPMENT
        assert dev_template.default_kafka_config.heap_size == "512M"
        assert dev_template.default_kafka_config.log_retention_hours == 24
        assert dev_template.default_kafka_config.num_partitions == 1
        assert dev_template.is_builtin
        
        # Test production template
        prod_template = await template_manager.get_template("production")
        assert prod_template.category == TemplateCategory.PRODUCTION
        assert prod_template.default_kafka_config.heap_size == "2G"
        assert prod_template.default_kafka_config.log_retention_hours == 168
        assert prod_template.default_kafka_config.num_partitions == 6
        assert prod_template.default_kafka_config.default_replication_factor == 3
        assert prod_template.is_builtin
        
        # Test high-throughput template
        ht_template = await template_manager.get_template("high-throughput")
        assert ht_template.category == TemplateCategory.HIGH_THROUGHPUT
        assert ht_template.default_kafka_config.heap_size == "4G"
        assert ht_template.default_kafka_config.num_partitions == 12
        assert "batch.size" in ht_template.default_kafka_config.custom_properties
        assert ht_template.is_builtin
    
    @pytest.mark.asyncio
    async def test_create_template(self, template_manager, sample_template):
        """Test creating a new template."""
        await template_manager.initialize()
        
        created_template = await template_manager.create_template(sample_template)
        
        assert created_template.id == sample_template.id
        assert created_template.name == sample_template.name
        assert not created_template.is_builtin
        assert created_template.created_at is not None
        
        # Verify template was saved
        retrieved_template = await template_manager.get_template(sample_template.id)
        assert retrieved_template.id == sample_template.id
    
    @pytest.mark.asyncio
    async def test_create_duplicate_template(self, template_manager, sample_template):
        """Test creating duplicate template raises error."""
        await template_manager.initialize()
        
        # Create template first time
        await template_manager.create_template(sample_template)
        
        # Try to create again
        with pytest.raises(TemplateAlreadyExistsError) as exc_info:
            await template_manager.create_template(sample_template)
        
        assert exc_info.value.template_id == sample_template.id
    
    @pytest.mark.asyncio
    async def test_get_template_not_found(self, template_manager):
        """Test getting non-existent template raises error."""
        await template_manager.initialize()
        
        with pytest.raises(TemplateNotFoundError) as exc_info:
            await template_manager.get_template("non-existent")
        
        assert exc_info.value.template_id == "non-existent"
    
    @pytest.mark.asyncio
    async def test_update_template(self, template_manager, sample_template):
        """Test updating existing template."""
        await template_manager.initialize()
        
        # Create template
        await template_manager.create_template(sample_template)
        
        # Update template
        sample_template.name = "Updated Template"
        sample_template.description = "Updated description"
        
        updated_template = await template_manager.update_template(sample_template)
        
        assert updated_template.name == "Updated Template"
        assert updated_template.description == "Updated description"
        
        # Verify update was saved
        retrieved_template = await template_manager.get_template(sample_template.id)
        assert retrieved_template.name == "Updated Template"
    
    @pytest.mark.asyncio
    async def test_update_nonexistent_template(self, template_manager, sample_template):
        """Test updating non-existent template raises error."""
        await template_manager.initialize()
        
        with pytest.raises(TemplateNotFoundError):
            await template_manager.update_template(sample_template)
    
    @pytest.mark.asyncio
    async def test_update_builtin_template_forbidden(self, template_manager):
        """Test updating built-in template is forbidden."""
        await template_manager.initialize()
        
        # Get built-in template
        dev_template = await template_manager.get_template("development")
        
        # Try to update it
        dev_template.name = "Modified Development"
        dev_template.is_builtin = False  # Try to change built-in status
        
        with pytest.raises(TemplateValidationError):
            await template_manager.update_template(dev_template)
    
    @pytest.mark.asyncio
    async def test_delete_template(self, template_manager, sample_template):
        """Test deleting template."""
        await template_manager.initialize()
        
        # Create template
        await template_manager.create_template(sample_template)
        
        # Delete template
        result = await template_manager.delete_template(sample_template.id)
        assert result is True
        
        # Verify template was deleted
        with pytest.raises(TemplateNotFoundError):
            await template_manager.get_template(sample_template.id)
    
    @pytest.mark.asyncio
    async def test_delete_nonexistent_template(self, template_manager):
        """Test deleting non-existent template raises error."""
        await template_manager.initialize()
        
        with pytest.raises(TemplateNotFoundError):
            await template_manager.delete_template("non-existent")
    
    @pytest.mark.asyncio
    async def test_delete_builtin_template_forbidden(self, template_manager):
        """Test deleting built-in template is forbidden."""
        await template_manager.initialize()
        
        with pytest.raises(TemplateValidationError):
            await template_manager.delete_template("development")
    
    @pytest.mark.asyncio
    async def test_list_templates(self, template_manager, sample_template):
        """Test listing templates."""
        await template_manager.initialize()
        
        # Create custom template
        await template_manager.create_template(sample_template)
        
        # List all templates
        all_templates = await template_manager.list_templates()
        assert len(all_templates) == 5  # 4 built-in + 1 custom
        
        # List by category
        dev_templates = await template_manager.list_templates(TemplateCategory.DEVELOPMENT)
        assert len(dev_templates) == 1
        assert dev_templates[0].id == "development"
        
        custom_templates = await template_manager.list_templates(TemplateCategory.CUSTOM)
        assert len(custom_templates) == 1
        assert custom_templates[0].id == sample_template.id
        
        # List excluding built-in
        custom_only = await template_manager.list_templates(include_builtin=False)
        assert len(custom_only) == 1
        assert custom_only[0].id == sample_template.id
    
    @pytest.mark.asyncio
    async def test_apply_template(self, template_manager):
        """Test applying template to create cluster definition."""
        await template_manager.initialize()
        
        cluster_def = await template_manager.apply_template(
            template_id="development",
            cluster_id="test-cluster",
            cluster_name="Test Cluster",
            environment=ClusterEnvironment.DEVELOPMENT
        )
        
        assert cluster_def.id == "test-cluster"
        assert cluster_def.name == "Test Cluster"
        assert cluster_def.environment == ClusterEnvironment.DEVELOPMENT
        assert cluster_def.template_id == "development"
        assert cluster_def.kafka_config.heap_size == "512M"  # From dev template
        assert "template" in cluster_def.tags
        assert cluster_def.tags["template"] == "development"
    
    @pytest.mark.asyncio
    async def test_apply_template_with_overrides(self, template_manager):
        """Test applying template with configuration overrides."""
        await template_manager.initialize()
        
        overrides = {
            "kafka_config": KafkaConfig(heap_size="2G", num_partitions=6),
            "description": "Custom description"
        }
        
        cluster_def = await template_manager.apply_template(
            template_id="development",
            cluster_id="test-cluster",
            cluster_name="Test Cluster",
            environment=ClusterEnvironment.DEVELOPMENT,
            overrides=overrides
        )
        
        assert cluster_def.kafka_config.heap_size == "2G"  # Override applied
        assert cluster_def.kafka_config.num_partitions == 6  # Override applied
        assert cluster_def.description == "Custom description"  # Override applied
    
    @pytest.mark.asyncio
    async def test_apply_nonexistent_template(self, template_manager):
        """Test applying non-existent template raises error."""
        await template_manager.initialize()
        
        with pytest.raises(TemplateNotFoundError):
            await template_manager.apply_template(
                template_id="non-existent",
                cluster_id="test-cluster",
                cluster_name="Test Cluster",
                environment=ClusterEnvironment.DEVELOPMENT
            )
    
    @pytest.mark.asyncio
    async def test_create_template_from_cluster(self, template_manager, sample_cluster):
        """Test creating template from existing cluster."""
        await template_manager.initialize()
        
        template = await template_manager.create_template_from_cluster(
            cluster=sample_cluster,
            template_id="cluster-based-template",
            template_name="Cluster Based Template",
            template_description="Template created from cluster",
            category=TemplateCategory.CUSTOM
        )
        
        assert template.id == "cluster-based-template"
        assert template.name == "Cluster Based Template"
        assert template.category == TemplateCategory.CUSTOM
        assert template.default_kafka_config.heap_size == sample_cluster.kafka_config.heap_size
        assert template.default_retention_policy.log_retention_hours == sample_cluster.retention_policy.log_retention_hours
        assert not template.is_builtin
        assert f"from-cluster-{sample_cluster.id}" in template.tags
    
    @pytest.mark.asyncio
    async def test_template_validation(self, template_manager):
        """Test template validation."""
        await template_manager.initialize()
        
        # Create invalid template (recommended < minimum)
        invalid_template = ClusterTemplate(
            id="invalid-template",
            name="Invalid Template",
            description="Invalid template for testing",
            category=TemplateCategory.CUSTOM,
            min_memory_mb=4096,
            recommended_memory_mb=2048,  # Less than minimum
            min_disk_gb=10,
            recommended_disk_gb=5,  # Less than minimum
            version="1.0.0"
        )
        
        with pytest.raises(TemplateValidationError):
            await template_manager.create_template(invalid_template)
    
    @pytest.mark.asyncio
    async def test_get_template_usage_stats(self, template_manager, mock_storage):
        """Test getting template usage statistics."""
        await template_manager.initialize()
        
        # Create some clusters using the development template
        cluster1 = ClusterDefinition(
            id="cluster1",
            name="Cluster 1",
            environment=ClusterEnvironment.DEVELOPMENT,
            template_id="development"
        )
        cluster2 = ClusterDefinition(
            id="cluster2",
            name="Cluster 2",
            environment=ClusterEnvironment.TESTING,
            template_id="development"
        )
        
        await mock_storage.save_cluster(cluster1)
        await mock_storage.save_cluster(cluster2)
        
        stats = await template_manager.get_template_usage_stats("development")
        
        assert stats["template_id"] == "development"
        assert stats["template_name"] == "Development"
        assert stats["usage_count"] == 2
        assert "cluster1" in stats["clusters_using_template"]
        assert "cluster2" in stats["clusters_using_template"]
        assert "development" in stats["environments"]
        assert "testing" in stats["environments"]
        assert stats["is_builtin"] is True
    
    @pytest.mark.asyncio
    async def test_get_usage_stats_nonexistent_template(self, template_manager):
        """Test getting usage stats for non-existent template."""
        await template_manager.initialize()
        
        with pytest.raises(TemplateNotFoundError):
            await template_manager.get_template_usage_stats("non-existent")
    
    @pytest.mark.asyncio
    async def test_memory_estimation(self, template_manager):
        """Test memory requirement estimation."""
        # Test with different heap sizes
        kafka_config_1g = KafkaConfig(heap_size="1G")
        kafka_config_512m = KafkaConfig(heap_size="512M")
        kafka_config_4g = KafkaConfig(heap_size="4G")
        
        # Access private method for testing
        estimate_1g = template_manager._estimate_memory_requirements(kafka_config_1g)
        estimate_512m = template_manager._estimate_memory_requirements(kafka_config_512m)
        estimate_4g = template_manager._estimate_memory_requirements(kafka_config_4g)
        
        assert estimate_1g == 2048  # 1GB + 1GB overhead = 2GB
        assert estimate_512m == 2048  # 512MB + 1GB overhead = 1.5GB, but minimum 2GB
        assert estimate_4g == 5120  # 4GB + 1GB overhead = 5GB
    
    @pytest.mark.asyncio
    async def test_close(self, template_manager, mock_storage):
        """Test closing template manager."""
        await template_manager.initialize()
        assert template_manager._initialized
        assert mock_storage.initialized
        
        await template_manager.close()
        assert not template_manager._initialized
        assert not mock_storage.initialized


class TestBuiltinTemplates:
    """Test built-in template configurations."""
    
    @pytest.mark.asyncio
    async def test_development_template(self, template_manager):
        """Test development template configuration."""
        await template_manager.initialize()
        
        template = await template_manager.get_template("development")
        
        assert template.category == TemplateCategory.DEVELOPMENT
        assert template.is_builtin
        assert template.default_kafka_config.heap_size == "512M"
        assert template.default_kafka_config.log_retention_hours == 24
        assert template.default_kafka_config.num_partitions == 1
        assert template.default_kafka_config.default_replication_factor == 1
        assert template.min_memory_mb == 1024
        assert template.recommended_memory_mb == 2048
        assert "development" in template.tags
        assert "lightweight" in template.tags
    
    @pytest.mark.asyncio
    async def test_testing_template(self, template_manager):
        """Test testing template configuration."""
        await template_manager.initialize()
        
        template = await template_manager.get_template("testing")
        
        assert template.category == TemplateCategory.TESTING
        assert template.is_builtin
        assert template.default_kafka_config.heap_size == "1G"
        assert template.default_kafka_config.log_retention_hours == 72
        assert template.default_kafka_config.num_partitions == 3
        assert template.default_kafka_config.default_replication_factor == 1
        assert template.min_memory_mb == 2048
        assert template.recommended_memory_mb == 4096
        assert "testing" in template.tags
        assert "balanced" in template.tags
    
    @pytest.mark.asyncio
    async def test_production_template(self, template_manager):
        """Test production template configuration."""
        await template_manager.initialize()
        
        template = await template_manager.get_template("production")
        
        assert template.category == TemplateCategory.PRODUCTION
        assert template.is_builtin
        assert template.default_kafka_config.heap_size == "2G"
        assert template.default_kafka_config.log_retention_hours == 168
        assert template.default_kafka_config.num_partitions == 6
        assert template.default_kafka_config.default_replication_factor == 3
        assert template.default_ui_config.topic_creation_enabled is False
        assert template.default_ui_config.topic_deletion_enabled is False
        assert template.min_memory_mb == 8192
        assert template.recommended_memory_mb == 16384
        assert "production" in template.tags
        assert "robust" in template.tags
    
    @pytest.mark.asyncio
    async def test_high_throughput_template(self, template_manager):
        """Test high-throughput template configuration."""
        await template_manager.initialize()
        
        template = await template_manager.get_template("high-throughput")
        
        assert template.category == TemplateCategory.HIGH_THROUGHPUT
        assert template.is_builtin
        assert template.default_kafka_config.heap_size == "4G"
        assert template.default_kafka_config.log_retention_hours == 48
        assert template.default_kafka_config.num_partitions == 12
        assert template.default_kafka_config.default_replication_factor == 2
        assert template.default_kafka_config.log_segment_bytes == 2147483648  # 2GB
        assert "batch.size" in template.default_kafka_config.custom_properties
        assert "linger.ms" in template.default_kafka_config.custom_properties
        assert "compression.type" in template.default_kafka_config.custom_properties
        assert template.min_memory_mb == 16384
        assert template.recommended_memory_mb == 32768
        assert "high-throughput" in template.tags
        assert "performance" in template.tags


class TestTemplateApplication:
    """Test template application to cluster definitions."""
    
    @pytest.mark.asyncio
    async def test_apply_development_template(self, template_manager):
        """Test applying development template."""
        await template_manager.initialize()
        
        cluster_def = await template_manager.apply_template(
            template_id="development",
            cluster_id="dev-cluster",
            cluster_name="Development Cluster",
            environment=ClusterEnvironment.DEVELOPMENT
        )
        
        assert cluster_def.id == "dev-cluster"
        assert cluster_def.name == "Development Cluster"
        assert cluster_def.environment == ClusterEnvironment.DEVELOPMENT
        assert cluster_def.template_id == "development"
        assert cluster_def.kafka_config.heap_size == "512M"
        assert cluster_def.kafka_config.log_retention_hours == 24
        assert cluster_def.retention_policy.log_retention_hours == 24
        assert cluster_def.tags["template"] == "development"
        assert cluster_def.tags["category"] == "development"
    
    @pytest.mark.asyncio
    async def test_apply_production_template(self, template_manager):
        """Test applying production template."""
        await template_manager.initialize()
        
        cluster_def = await template_manager.apply_template(
            template_id="production",
            cluster_id="prod-cluster",
            cluster_name="Production Cluster",
            environment=ClusterEnvironment.PRODUCTION
        )
        
        assert cluster_def.id == "prod-cluster"
        assert cluster_def.name == "Production Cluster"
        assert cluster_def.environment == ClusterEnvironment.PRODUCTION
        assert cluster_def.template_id == "production"
        assert cluster_def.kafka_config.heap_size == "2G"
        assert cluster_def.kafka_config.log_retention_hours == 168
        assert cluster_def.kafka_config.default_replication_factor == 3
        assert cluster_def.ui_config.topic_creation_enabled is False
        assert cluster_def.ui_config.topic_deletion_enabled is False
    
    @pytest.mark.asyncio
    async def test_template_overrides(self, template_manager):
        """Test template application with overrides."""
        await template_manager.initialize()
        
        overrides = {
            "description": "Custom development cluster",
            "kafka_config": KafkaConfig(
                heap_size="1G",
                num_partitions=6,
                log_retention_hours=48
            ),
            "tags": {"custom": "true", "team": "backend"}
        }
        
        cluster_def = await template_manager.apply_template(
            template_id="development",
            cluster_id="custom-dev",
            cluster_name="Custom Dev Cluster",
            environment=ClusterEnvironment.DEVELOPMENT,
            overrides=overrides
        )
        
        assert cluster_def.description == "Custom development cluster"
        assert cluster_def.kafka_config.heap_size == "1G"  # Override
        assert cluster_def.kafka_config.num_partitions == 6  # Override
        assert cluster_def.kafka_config.log_retention_hours == 48  # Override
        assert cluster_def.tags["custom"] == "true"  # Override
        assert cluster_def.tags["team"] == "backend"  # Override
        # Original template tags should still be present
        assert cluster_def.tags["template"] == "development"