"""
Unit tests for storage backends.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime
from unittest.mock import AsyncMock, patch

from src.storage.base import StorageBackend
from src.storage.file_backend import FileStorageBackend
from src.storage.database_backend import DatabaseStorageBackend
from src.models.multi_cluster import (
    ClusterDefinition,
    ClusterTemplate,
    ClusterEnvironment,
    TemplateCategory,
    KafkaConfig,
    PortAllocation
)
from src.exceptions import (
    StorageBackendError,
    ClusterNotFoundError,
    ClusterAlreadyExistsError,
    TemplateNotFoundError,
    TemplateAlreadyExistsError
)


@pytest.fixture
def temp_dir():
    """Create temporary directory for tests."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_cluster():
    """Create sample cluster definition."""
    return ClusterDefinition(
        id="test-cluster",
        name="Test Cluster",
        description="A test cluster",
        environment=ClusterEnvironment.DEVELOPMENT,
        port_allocation=PortAllocation(
            kafka_port=9092,
            rest_proxy_port=8082,
            ui_port=8080
        ),
        tags={"team": "engineering", "purpose": "testing"}
    )


@pytest.fixture
def sample_template():
    """Create sample cluster template."""
    return ClusterTemplate(
        id="test-template",
        name="Test Template",
        description="A test template",
        category=TemplateCategory.DEVELOPMENT,
        default_kafka_config=KafkaConfig(heap_size="2G"),
        tags=["test", "development"]
    )


class TestStorageBackendInterface:
    """Test StorageBackend abstract interface."""
    
    def test_storage_backend_is_abstract(self):
        """Test that StorageBackend cannot be instantiated directly."""
        with pytest.raises(TypeError):
            StorageBackend()
    
    def test_storage_backend_methods_are_abstract(self):
        """Test that all required methods are abstract."""
        abstract_methods = {
            'initialize', 'close', 'save_cluster', 'load_cluster', 
            'update_cluster', 'delete_cluster', 'list_clusters', 'cluster_exists',
            'save_template', 'load_template', 'update_template', 'delete_template', 
            'list_templates', 'template_exists', 'health_check', 'get_stats',
            'backup', 'restore'
        }
        
        assert StorageBackend.__abstractmethods__ == abstract_methods


class TestFileStorageBackend:
    """Test FileStorageBackend implementation."""
    
    @pytest.fixture
    async def file_backend(self, temp_dir):
        """Create and initialize file storage backend."""
        backend = FileStorageBackend(storage_dir=temp_dir)
        await backend.initialize()
        yield backend
        await backend.close()
    
    async def test_initialization(self, temp_dir):
        """Test backend initialization."""
        backend = FileStorageBackend(storage_dir=temp_dir)
        
        # Test initialization
        result = await backend.initialize()
        assert result is True
        
        # Check directories were created
        storage_path = Path(temp_dir)
        assert (storage_path / "clusters").exists()
        assert (storage_path / "templates").exists()
        assert (storage_path / "backups").exists()
        
        # Check index files were created
        assert (storage_path / "clusters" / "index.json").exists()
        assert (storage_path / "templates" / "index.json").exists()
        
        await backend.close()
    
    async def test_cluster_operations(self, file_backend, sample_cluster):
        """Test cluster CRUD operations."""
        # Test save
        result = await file_backend.save_cluster(sample_cluster)
        assert result is True
        
        # Test exists
        exists = await file_backend.cluster_exists(sample_cluster.id)
        assert exists is True
        
        # Test load
        loaded_cluster = await file_backend.load_cluster(sample_cluster.id)
        assert loaded_cluster is not None
        assert loaded_cluster.id == sample_cluster.id
        assert loaded_cluster.name == sample_cluster.name
        assert loaded_cluster.environment == sample_cluster.environment
        
        # Test list
        clusters = await file_backend.list_clusters()
        assert len(clusters) == 1
        assert clusters[0].id == sample_cluster.id
        
        # Test update
        sample_cluster.name = "Updated Test Cluster"
        result = await file_backend.update_cluster(sample_cluster)
        assert result is True
        
        # Verify update
        updated_cluster = await file_backend.load_cluster(sample_cluster.id)
        assert updated_cluster.name == "Updated Test Cluster"
        
        # Test delete
        result = await file_backend.delete_cluster(sample_cluster.id)
        assert result is True
        
        # Verify deletion
        exists = await file_backend.cluster_exists(sample_cluster.id)
        assert exists is False
    
    async def test_cluster_not_found_errors(self, file_backend):
        """Test cluster not found error handling."""
        # Test load non-existent cluster
        cluster = await file_backend.load_cluster("non-existent")
        assert cluster is None
        
        # Test update non-existent cluster
        sample_cluster = ClusterDefinition(
            id="non-existent",
            name="Non-existent",
            environment=ClusterEnvironment.DEVELOPMENT
        )
        
        with pytest.raises(ClusterNotFoundError):
            await file_backend.update_cluster(sample_cluster)
        
        # Test delete non-existent cluster
        with pytest.raises(ClusterNotFoundError):
            await file_backend.delete_cluster("non-existent")
    
    async def test_cluster_already_exists_error(self, file_backend, sample_cluster):
        """Test cluster already exists error handling."""
        # Save cluster first
        await file_backend.save_cluster(sample_cluster)
        
        # Try to save again
        with pytest.raises(ClusterAlreadyExistsError):
            await file_backend.save_cluster(sample_cluster)
    
    async def test_template_operations(self, file_backend, sample_template):
        """Test template CRUD operations."""
        # Test save
        result = await file_backend.save_template(sample_template)
        assert result is True
        
        # Test exists
        exists = await file_backend.template_exists(sample_template.id)
        assert exists is True
        
        # Test load
        loaded_template = await file_backend.load_template(sample_template.id)
        assert loaded_template is not None
        assert loaded_template.id == sample_template.id
        assert loaded_template.name == sample_template.name
        assert loaded_template.category == sample_template.category
        
        # Test list
        templates = await file_backend.list_templates()
        assert len(templates) == 1
        assert templates[0].id == sample_template.id
        
        # Test update
        sample_template.name = "Updated Test Template"
        result = await file_backend.update_template(sample_template)
        assert result is True
        
        # Verify update
        updated_template = await file_backend.load_template(sample_template.id)
        assert updated_template.name == "Updated Test Template"
        
        # Test delete
        result = await file_backend.delete_template(sample_template.id)
        assert result is True
        
        # Verify deletion
        exists = await file_backend.template_exists(sample_template.id)
        assert exists is False
    
    async def test_template_not_found_errors(self, file_backend):
        """Test template not found error handling."""
        # Test load non-existent template
        template = await file_backend.load_template("non-existent")
        assert template is None
        
        # Test update non-existent template
        sample_template = ClusterTemplate(
            id="non-existent",
            name="Non-existent",
            description="Non-existent template",
            category=TemplateCategory.CUSTOM
        )
        
        with pytest.raises(TemplateNotFoundError):
            await file_backend.update_template(sample_template)
        
        # Test delete non-existent template
        with pytest.raises(TemplateNotFoundError):
            await file_backend.delete_template("non-existent")
    
    async def test_template_already_exists_error(self, file_backend, sample_template):
        """Test template already exists error handling."""
        # Save template first
        await file_backend.save_template(sample_template)
        
        # Try to save again
        with pytest.raises(TemplateAlreadyExistsError):
            await file_backend.save_template(sample_template)
    
    async def test_filtering(self, file_backend):
        """Test filtering functionality."""
        # Create multiple clusters with different environments
        cluster1 = ClusterDefinition(
            id="dev-cluster",
            name="Dev Cluster",
            environment=ClusterEnvironment.DEVELOPMENT
        )
        cluster2 = ClusterDefinition(
            id="prod-cluster",
            name="Prod Cluster",
            environment=ClusterEnvironment.PRODUCTION
        )
        
        await file_backend.save_cluster(cluster1)
        await file_backend.save_cluster(cluster2)
        
        # Test filtering by environment
        dev_clusters = await file_backend.list_clusters({"environment": "development"})
        assert len(dev_clusters) == 1
        assert dev_clusters[0].id == "dev-cluster"
        
        prod_clusters = await file_backend.list_clusters({"environment": "production"})
        assert len(prod_clusters) == 1
        assert prod_clusters[0].id == "prod-cluster"
        
        # Test no filter
        all_clusters = await file_backend.list_clusters()
        assert len(all_clusters) == 2
    
    async def test_health_check(self, file_backend):
        """Test health check functionality."""
        health = await file_backend.health_check()
        
        assert health["status"] == "healthy"
        assert health["backend_type"] == "file"
        assert "storage_dir" in health
        assert "cluster_count" in health
        assert "template_count" in health
        assert "free_space_mb" in health
        assert "last_check" in health
    
    async def test_get_stats(self, file_backend, sample_cluster, sample_template):
        """Test statistics functionality."""
        # Add some data
        await file_backend.save_cluster(sample_cluster)
        await file_backend.save_template(sample_template)
        
        stats = await file_backend.get_stats()
        
        assert stats["backend_type"] == "file"
        assert stats["cluster_count"] == 1
        assert stats["template_count"] == 1
        assert "total_size_bytes" in stats
        assert "disk_total_gb" in stats
        assert "timestamp" in stats
    
    async def test_backup_and_restore(self, file_backend, sample_cluster, temp_dir):
        """Test backup and restore functionality."""
        # Add some data
        await file_backend.save_cluster(sample_cluster)
        
        # Create backup
        backup_path = Path(temp_dir) / "backup.tar.gz"
        result = await file_backend.backup(str(backup_path))
        assert result is True
        assert backup_path.exists()
        
        # Delete original data
        await file_backend.delete_cluster(sample_cluster.id)
        clusters = await file_backend.list_clusters()
        assert len(clusters) == 0
        
        # Restore backup
        result = await file_backend.restore(str(backup_path))
        assert result is True
        
        # Verify data is restored
        clusters = await file_backend.list_clusters()
        assert len(clusters) == 1
        assert clusters[0].id == sample_cluster.id


class TestDatabaseStorageBackend:
    """Test DatabaseStorageBackend implementation."""
    
    @pytest.fixture
    async def db_backend(self, temp_dir):
        """Create and initialize database storage backend."""
        db_path = Path(temp_dir) / "test.db"
        backend = DatabaseStorageBackend(database_path=str(db_path))
        await backend.initialize()
        yield backend
        await backend.close()
    
    async def test_initialization(self, temp_dir):
        """Test backend initialization."""
        db_path = Path(temp_dir) / "test.db"
        backend = DatabaseStorageBackend(database_path=str(db_path))
        
        # Test initialization
        result = await backend.initialize()
        assert result is True
        
        # Check database file was created
        assert db_path.exists()
        
        await backend.close()
    
    async def test_cluster_operations(self, db_backend, sample_cluster):
        """Test cluster CRUD operations."""
        # Test save
        result = await db_backend.save_cluster(sample_cluster)
        assert result is True
        
        # Test exists
        exists = await db_backend.cluster_exists(sample_cluster.id)
        assert exists is True
        
        # Test load
        loaded_cluster = await db_backend.load_cluster(sample_cluster.id)
        assert loaded_cluster is not None
        assert loaded_cluster.id == sample_cluster.id
        assert loaded_cluster.name == sample_cluster.name
        assert loaded_cluster.environment == sample_cluster.environment
        
        # Test list
        clusters = await db_backend.list_clusters()
        assert len(clusters) == 1
        assert clusters[0].id == sample_cluster.id
        
        # Test update
        sample_cluster.name = "Updated Test Cluster"
        result = await db_backend.update_cluster(sample_cluster)
        assert result is True
        
        # Verify update
        updated_cluster = await db_backend.load_cluster(sample_cluster.id)
        assert updated_cluster.name == "Updated Test Cluster"
        
        # Test delete
        result = await db_backend.delete_cluster(sample_cluster.id)
        assert result is True
        
        # Verify deletion
        exists = await db_backend.cluster_exists(sample_cluster.id)
        assert exists is False
    
    async def test_cluster_not_found_errors(self, db_backend):
        """Test cluster not found error handling."""
        # Test load non-existent cluster
        cluster = await db_backend.load_cluster("non-existent")
        assert cluster is None
        
        # Test update non-existent cluster
        sample_cluster = ClusterDefinition(
            id="non-existent",
            name="Non-existent",
            environment=ClusterEnvironment.DEVELOPMENT
        )
        
        with pytest.raises(ClusterNotFoundError):
            await db_backend.update_cluster(sample_cluster)
        
        # Test delete non-existent cluster
        with pytest.raises(ClusterNotFoundError):
            await db_backend.delete_cluster("non-existent")
    
    async def test_cluster_already_exists_error(self, db_backend, sample_cluster):
        """Test cluster already exists error handling."""
        # Save cluster first
        await db_backend.save_cluster(sample_cluster)
        
        # Try to save again
        with pytest.raises(ClusterAlreadyExistsError):
            await db_backend.save_cluster(sample_cluster)
    
    async def test_template_operations(self, db_backend, sample_template):
        """Test template CRUD operations."""
        # Test save
        result = await db_backend.save_template(sample_template)
        assert result is True
        
        # Test exists
        exists = await db_backend.template_exists(sample_template.id)
        assert exists is True
        
        # Test load
        loaded_template = await db_backend.load_template(sample_template.id)
        assert loaded_template is not None
        assert loaded_template.id == sample_template.id
        assert loaded_template.name == sample_template.name
        assert loaded_template.category == sample_template.category
        
        # Test list
        templates = await db_backend.list_templates()
        assert len(templates) == 1
        assert templates[0].id == sample_template.id
        
        # Test update
        sample_template.name = "Updated Test Template"
        result = await db_backend.update_template(sample_template)
        assert result is True
        
        # Verify update
        updated_template = await db_backend.load_template(sample_template.id)
        assert updated_template.name == "Updated Test Template"
        
        # Test delete
        result = await db_backend.delete_template(sample_template.id)
        assert result is True
        
        # Verify deletion
        exists = await db_backend.template_exists(sample_template.id)
        assert exists is False
    
    async def test_filtering(self, db_backend):
        """Test filtering functionality."""
        # Create multiple clusters with different environments
        cluster1 = ClusterDefinition(
            id="dev-cluster",
            name="Dev Cluster",
            environment=ClusterEnvironment.DEVELOPMENT
        )
        cluster2 = ClusterDefinition(
            id="prod-cluster",
            name="Prod Cluster",
            environment=ClusterEnvironment.PRODUCTION
        )
        
        await db_backend.save_cluster(cluster1)
        await db_backend.save_cluster(cluster2)
        
        # Test filtering by environment
        dev_clusters = await db_backend.list_clusters({"environment": "development"})
        assert len(dev_clusters) == 1
        assert dev_clusters[0].id == "dev-cluster"
        
        prod_clusters = await db_backend.list_clusters({"environment": "production"})
        assert len(prod_clusters) == 1
        assert prod_clusters[0].id == "prod-cluster"
        
        # Test no filter
        all_clusters = await db_backend.list_clusters()
        assert len(all_clusters) == 2
    
    async def test_health_check(self, db_backend):
        """Test health check functionality."""
        health = await db_backend.health_check()
        
        assert health["status"] == "healthy"
        assert health["backend_type"] == "database"
        assert "database_path" in health
        assert "cluster_count" in health
        assert "template_count" in health
        assert "database_size_bytes" in health
        assert "last_check" in health
    
    async def test_get_stats(self, db_backend, sample_cluster, sample_template):
        """Test statistics functionality."""
        # Add some data
        await db_backend.save_cluster(sample_cluster)
        await db_backend.save_template(sample_template)
        
        stats = await db_backend.get_stats()
        
        assert stats["backend_type"] == "database"
        assert stats["cluster_count"] == 1
        assert stats["template_count"] == 1
        assert "database_size_bytes" in stats
        assert "timestamp" in stats
    
    async def test_backup_and_restore(self, db_backend, sample_cluster, temp_dir):
        """Test backup and restore functionality."""
        # Add some data
        await db_backend.save_cluster(sample_cluster)
        
        # Create backup
        backup_path = Path(temp_dir) / "backup.db"
        result = await db_backend.backup(str(backup_path))
        assert result is True
        assert backup_path.exists()
        
        # Delete original data
        await db_backend.delete_cluster(sample_cluster.id)
        clusters = await db_backend.list_clusters()
        assert len(clusters) == 0
        
        # Restore backup
        result = await db_backend.restore(str(backup_path))
        assert result is True
        
        # Verify data is restored
        clusters = await db_backend.list_clusters()
        assert len(clusters) == 1
        assert clusters[0].id == sample_cluster.id


class TestStorageBackendErrorHandling:
    """Test error handling in storage backends."""
    
    async def test_file_backend_initialization_error(self, temp_dir):
        """Test file backend initialization error handling."""
        # Create a file where directory should be
        invalid_path = Path(temp_dir) / "invalid"
        invalid_path.touch()
        
        backend = FileStorageBackend(storage_dir=str(invalid_path))
        
        with pytest.raises(StorageBackendError):
            await backend.initialize()
    
    async def test_database_backend_initialization_error(self):
        """Test database backend initialization error handling."""
        # Use invalid database path
        backend = DatabaseStorageBackend(database_path="/invalid/path/test.db")
        
        with pytest.raises(StorageBackendError):
            await backend.initialize()
    
    async def test_file_backend_corrupted_data_handling(self, temp_dir):
        """Test file backend handling of corrupted data."""
        backend = FileStorageBackend(storage_dir=temp_dir)
        await backend.initialize()
        
        # Create corrupted cluster file
        cluster_file = Path(temp_dir) / "clusters" / "corrupted.json"
        cluster_file.write_text("invalid json content")
        
        # List should skip corrupted files
        clusters = await backend.list_clusters()
        assert len(clusters) == 0
        
        await backend.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])