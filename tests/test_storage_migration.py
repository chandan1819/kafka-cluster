"""
Unit tests for storage migration utilities.
"""

import pytest
import tempfile
import shutil
from pathlib import Path

from src.storage.file_backend import FileStorageBackend
from src.storage.database_backend import DatabaseStorageBackend
from src.storage.migration import StorageMigrator, migrate_storage, validate_storage_migration
from src.models.multi_cluster import (
    ClusterDefinition,
    ClusterTemplate,
    ClusterEnvironment,
    TemplateCategory,
    KafkaConfig
)
from src.exceptions import StorageBackendError


@pytest.fixture
def temp_dir():
    """Create temporary directory for tests."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_clusters():
    """Create sample cluster definitions."""
    return [
        ClusterDefinition(
            id="dev-cluster",
            name="Development Cluster",
            environment=ClusterEnvironment.DEVELOPMENT,
            tags={"team": "engineering"}
        ),
        ClusterDefinition(
            id="test-cluster",
            name="Test Cluster",
            environment=ClusterEnvironment.TESTING,
            tags={"team": "qa"}
        ),
        ClusterDefinition(
            id="prod-cluster",
            name="Production Cluster",
            environment=ClusterEnvironment.PRODUCTION,
            tags={"team": "ops"}
        )
    ]


@pytest.fixture
def sample_templates():
    """Create sample cluster templates."""
    return [
        ClusterTemplate(
            id="dev-template",
            name="Development Template",
            description="Template for development clusters",
            category=TemplateCategory.DEVELOPMENT,
            default_kafka_config=KafkaConfig(heap_size="1G"),
            tags=["development"]
        ),
        ClusterTemplate(
            id="prod-template",
            name="Production Template",
            description="Template for production clusters",
            category=TemplateCategory.PRODUCTION,
            default_kafka_config=KafkaConfig(heap_size="4G"),
            tags=["production"]
        )
    ]


@pytest.fixture
async def populated_file_backend(temp_dir, sample_clusters, sample_templates):
    """Create file backend with sample data."""
    backend = FileStorageBackend(storage_dir=f"{temp_dir}/source")
    await backend.initialize()
    
    # Add sample data
    for cluster in sample_clusters:
        await backend.save_cluster(cluster)
    
    for template in sample_templates:
        await backend.save_template(template)
    
    yield backend
    await backend.close()


@pytest.fixture
async def empty_db_backend(temp_dir):
    """Create empty database backend."""
    db_path = Path(temp_dir) / "target.db"
    backend = DatabaseStorageBackend(database_path=str(db_path))
    await backend.initialize()
    yield backend
    await backend.close()


class TestStorageMigrator:
    """Test StorageMigrator class."""
    
    async def test_migrate_all_dry_run(self, populated_file_backend, empty_db_backend):
        """Test dry run migration."""
        migrator = StorageMigrator(populated_file_backend, empty_db_backend)
        
        results = await migrator.migrate_all(dry_run=True)
        
        # Check results
        assert results["dry_run"] is True
        assert results["source_backend"] == "FileStorageBackend"
        assert results["target_backend"] == "DatabaseStorageBackend"
        assert results["clusters"]["total"] == 3
        assert results["clusters"]["migrated"] == 3
        assert results["templates"]["total"] == 2
        assert results["templates"]["migrated"] == 2
        assert len(results["clusters"]["errors"]) == 0
        assert len(results["templates"]["errors"]) == 0
        assert results["duration_seconds"] > 0
        
        # Verify target backend is still empty (dry run)
        target_clusters = await empty_db_backend.list_clusters()
        target_templates = await empty_db_backend.list_templates()
        assert len(target_clusters) == 0
        assert len(target_templates) == 0
    
    async def test_migrate_all_actual(self, populated_file_backend, empty_db_backend):
        """Test actual migration."""
        migrator = StorageMigrator(populated_file_backend, empty_db_backend)
        
        results = await migrator.migrate_all(dry_run=False)
        
        # Check results
        assert results["dry_run"] is False
        assert results["clusters"]["total"] == 3
        assert results["clusters"]["migrated"] == 3
        assert results["templates"]["total"] == 2
        assert results["templates"]["migrated"] == 2
        assert len(results["clusters"]["errors"]) == 0
        assert len(results["templates"]["errors"]) == 0
        
        # Verify data was migrated
        target_clusters = await empty_db_backend.list_clusters()
        target_templates = await empty_db_backend.list_templates()
        assert len(target_clusters) == 3
        assert len(target_templates) == 2
        
        # Verify cluster data integrity
        cluster_ids = {cluster.id for cluster in target_clusters}
        assert cluster_ids == {"dev-cluster", "test-cluster", "prod-cluster"}
        
        # Verify template data integrity
        template_ids = {template.id for template in target_templates}
        assert template_ids == {"dev-template", "prod-template"}
    
    async def test_migrate_clusters_only(self, populated_file_backend, empty_db_backend):
        """Test migrating only clusters."""
        migrator = StorageMigrator(populated_file_backend, empty_db_backend)
        
        results = await migrator.migrate_clusters_only(dry_run=False)
        
        # Check results
        assert results["total"] == 3
        assert results["migrated"] == 3
        assert len(results["errors"]) == 0
        
        # Verify only clusters were migrated
        target_clusters = await empty_db_backend.list_clusters()
        target_templates = await empty_db_backend.list_templates()
        assert len(target_clusters) == 3
        assert len(target_templates) == 0
    
    async def test_migrate_templates_only(self, populated_file_backend, empty_db_backend):
        """Test migrating only templates."""
        migrator = StorageMigrator(populated_file_backend, empty_db_backend)
        
        results = await migrator.migrate_templates_only(dry_run=False)
        
        # Check results
        assert results["total"] == 2
        assert results["migrated"] == 2
        assert len(results["errors"]) == 0
        
        # Verify only templates were migrated
        target_clusters = await empty_db_backend.list_clusters()
        target_templates = await empty_db_backend.list_templates()
        assert len(target_clusters) == 0
        assert len(target_templates) == 2
    
    async def test_validate_migration_success(self, populated_file_backend, empty_db_backend):
        """Test successful migration validation."""
        migrator = StorageMigrator(populated_file_backend, empty_db_backend)
        
        # Perform migration first
        await migrator.migrate_all(dry_run=False)
        
        # Validate migration
        validation_results = await migrator.validate_migration()
        
        # Check validation results
        assert validation_results["validation_passed"] is True
        assert validation_results["clusters"]["source_count"] == 3
        assert validation_results["clusters"]["target_count"] == 3
        assert len(validation_results["clusters"]["missing"]) == 0
        assert len(validation_results["clusters"]["mismatched"]) == 0
        assert validation_results["templates"]["source_count"] == 2
        assert validation_results["templates"]["target_count"] == 2
        assert len(validation_results["templates"]["missing"]) == 0
        assert len(validation_results["templates"]["mismatched"]) == 0
    
    async def test_validate_migration_missing_data(self, populated_file_backend, empty_db_backend, sample_clusters):
        """Test validation with missing data."""
        migrator = StorageMigrator(populated_file_backend, empty_db_backend)
        
        # Partially migrate (only some clusters)
        await empty_db_backend.save_cluster(sample_clusters[0])  # Only migrate first cluster
        
        # Validate migration
        validation_results = await migrator.validate_migration()
        
        # Check validation results
        assert validation_results["validation_passed"] is False
        assert validation_results["clusters"]["source_count"] == 3
        assert validation_results["clusters"]["target_count"] == 1
        assert len(validation_results["clusters"]["missing"]) == 2
        assert "test-cluster" in validation_results["clusters"]["missing"]
        assert "prod-cluster" in validation_results["clusters"]["missing"]
    
    async def test_migration_with_batch_size(self, populated_file_backend, empty_db_backend):
        """Test migration with custom batch size."""
        migrator = StorageMigrator(populated_file_backend, empty_db_backend)
        
        results = await migrator.migrate_all(dry_run=False, batch_size=1)
        
        # Should still migrate all data
        assert results["clusters"]["migrated"] == 3
        assert results["templates"]["migrated"] == 2
        
        # Verify data was migrated
        target_clusters = await empty_db_backend.list_clusters()
        target_templates = await empty_db_backend.list_templates()
        assert len(target_clusters) == 3
        assert len(target_templates) == 2
    
    async def test_migration_skip_existing(self, populated_file_backend, empty_db_backend, sample_clusters):
        """Test migration skips existing items."""
        # Pre-populate target with one cluster
        await empty_db_backend.save_cluster(sample_clusters[0])
        
        migrator = StorageMigrator(populated_file_backend, empty_db_backend)
        
        results = await migrator.migrate_all(dry_run=False)
        
        # Should migrate only the non-existing items
        assert results["clusters"]["total"] == 3
        assert results["clusters"]["migrated"] == 2  # Skipped existing one
        assert results["templates"]["migrated"] == 2
        
        # Verify final state
        target_clusters = await empty_db_backend.list_clusters()
        assert len(target_clusters) == 3


class TestMigrationConvenienceFunctions:
    """Test convenience functions for migration."""
    
    async def test_migrate_storage_function(self, populated_file_backend, empty_db_backend):
        """Test migrate_storage convenience function."""
        results = await migrate_storage(
            populated_file_backend, 
            empty_db_backend, 
            dry_run=False
        )
        
        # Check results
        assert results["clusters"]["migrated"] == 3
        assert results["templates"]["migrated"] == 2
        
        # Verify data was migrated
        target_clusters = await empty_db_backend.list_clusters()
        target_templates = await empty_db_backend.list_templates()
        assert len(target_clusters) == 3
        assert len(target_templates) == 2
    
    async def test_validate_storage_migration_function(self, populated_file_backend, empty_db_backend):
        """Test validate_storage_migration convenience function."""
        # Migrate first
        await migrate_storage(populated_file_backend, empty_db_backend, dry_run=False)
        
        # Validate
        validation_results = await validate_storage_migration(
            populated_file_backend, 
            empty_db_backend
        )
        
        # Check validation results
        assert validation_results["validation_passed"] is True
        assert validation_results["clusters"]["source_count"] == 3
        assert validation_results["clusters"]["target_count"] == 3


class TestMigrationErrorHandling:
    """Test error handling in migration."""
    
    async def test_migration_with_backend_errors(self, temp_dir):
        """Test migration handles backend errors gracefully."""
        # Create backends that will fail
        source_backend = FileStorageBackend(storage_dir=f"{temp_dir}/source")
        target_backend = DatabaseStorageBackend(database_path="/invalid/path/test.db")
        
        migrator = StorageMigrator(source_backend, target_backend)
        
        # Migration should raise StorageBackendError
        with pytest.raises(StorageBackendError):
            await migrator.migrate_all(dry_run=False)
    
    async def test_migration_partial_failure(self, populated_file_backend, temp_dir):
        """Test migration handles partial failures."""
        # Create a target backend that will fail for some operations
        target_backend = DatabaseStorageBackend(database_path=f"{temp_dir}/target.db")
        await target_backend.initialize()
        
        # Close the connection to simulate failures
        await target_backend.close()
        
        migrator = StorageMigrator(populated_file_backend, target_backend)
        
        # Migration should handle errors and continue
        results = await migrator.migrate_all(dry_run=False)
        
        # Should have errors recorded
        assert len(results["clusters"]["errors"]) > 0 or len(results["templates"]["errors"]) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])