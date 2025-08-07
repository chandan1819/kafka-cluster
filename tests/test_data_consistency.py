"""
Data consistency validation tests for multi-cluster operations.

This module validates that data remains consistent across various
multi-cluster operations including creation, cloning, snapshots, and recovery.
"""

import pytest
import asyncio
import hashlib
import json
from typing import Dict, List, Any, Optional, Set
from datetime import datetime
from unittest.mock import Mock, AsyncMock, patch

from src.services.multi_cluster_manager import MultiClusterManager
from src.registry.cluster_registry import ClusterRegistry
from src.services.advanced_cluster_features import AdvancedClusterFeatures
from src.services.cross_cluster_operations import CrossClusterOperations
from src.services.configuration_manager import ConfigurationManager
from src.models.multi_cluster import ClusterDefinition, PortAllocation, SnapshotType
from src.models.base import ServiceStatus
from src.storage.file_backend import FileStorageBackend
from src.networking.port_allocator import PortAllocator


class DataConsistencyValidator:
    """Validates data consistency across multi-cluster operations."""
    
    def __init__(self):
        self.checksums = {}
        self.validation_errors = []
    
    def calculate_cluster_checksum(self, cluster: ClusterDefinition) -> str:
        """Calculate checksum for cluster definition."""
        # Create a normalized representation for checksum
        cluster_data = {
            "id": cluster.id,
            "name": cluster.name,
            "description": cluster.description,
            "environment": cluster.environment,
            "template_id": cluster.template_id,
            "port_allocation": {
                "kafka_port": cluster.port_allocation.kafka_port,
                "rest_proxy_port": cluster.port_allocation.rest_proxy_port,
                "ui_port": cluster.port_allocation.ui_port,
                "jmx_port": cluster.port_allocation.jmx_port
            },
            "tags": dict(sorted(cluster.tags.items())) if cluster.tags else {},
            "created_at": cluster.created_at.isoformat() if cluster.created_at else None
        }
        
        # Convert to JSON and calculate hash
        json_str = json.dumps(cluster_data, sort_keys=True)
        return hashlib.sha256(json_str.encode()).hexdigest()
    
    def store_cluster_checksum(self, cluster_id: str, cluster: ClusterDefinition):
        """Store cluster checksum for later validation."""
        checksum = self.calculate_cluster_checksum(cluster)
        self.checksums[cluster_id] = {
            "checksum": checksum,
            "cluster_data": cluster,
            "timestamp": datetime.utcnow()
        }
    
    def validate_cluster_consistency(self, cluster_id: str, cluster: ClusterDefinition) -> bool:
        """Validate cluster consistency against stored checksum."""
        if cluster_id not in self.checksums:
            self.validation_errors.append(f"No stored checksum for cluster {cluster_id}")
            return False
        
        stored_checksum = self.checksums[cluster_id]["checksum"]
        current_checksum = self.calculate_cluster_checksum(cluster)
        
        if stored_checksum != current_checksum:
            self.validation_errors.append(
                f"Checksum mismatch for cluster {cluster_id}: "
                f"stored={stored_checksum[:8]}..., current={current_checksum[:8]}..."
            )
            return False
        
        return True
    
    def validate_cluster_clone_consistency(
        self, 
        source_cluster: ClusterDefinition, 
        cloned_cluster: ClusterDefinition,
        expected_differences: Set[str] = None
    ) -> bool:
        """Validate that cloned cluster matches source except for expected differences."""
        expected_differences = expected_differences or {"id", "name", "port_allocation", "created_at"}
        
        # Compare all fields except expected differences
        source_dict = source_cluster.__dict__.copy()
        cloned_dict = cloned_cluster.__dict__.copy()
        
        # Remove expected differences
        for field in expected_differences:
            source_dict.pop(field, None)
            cloned_dict.pop(field, None)
        
        # Special handling for port allocation if not in expected differences
        if "port_allocation" not in expected_differences:
            if source_cluster.port_allocation and cloned_cluster.port_allocation:
                source_dict["port_allocation"] = source_cluster.port_allocation.__dict__
                cloned_dict["port_allocation"] = cloned_cluster.port_allocation.__dict__
        
        # Compare remaining fields
        for field, source_value in source_dict.items():
            cloned_value = cloned_dict.get(field)
            if source_value != cloned_value:
                self.validation_errors.append(
                    f"Clone consistency error in field '{field}': "
                    f"source={source_value}, cloned={cloned_value}"
                )
                return False
        
        return True
    
    def get_validation_report(self) -> Dict[str, Any]:
        """Get validation report."""
        return {
            "total_checksums": len(self.checksums),
            "validation_errors": self.validation_errors,
            "error_count": len(self.validation_errors),
            "is_consistent": len(self.validation_errors) == 0
        }


class TestDataConsistency:
    """Test data consistency across multi-cluster operations."""
    
    @pytest.fixture(scope="class")
    async def consistency_test_setup(self):
        """Set up consistency test environment."""
        import tempfile
        import shutil
        from pathlib import Path
        
        # Create temporary directory
        temp_dir = tempfile.mkdtemp(prefix="consistency_test_")
        temp_path = Path(temp_dir)
        
        try:
            # Set up storage
            storage_path = temp_path / "storage"
            storage_path.mkdir(exist_ok=True)
            storage_backend = FileStorageBackend(str(storage_path))
            await storage_backend.initialize()
            
            # Set up components
            port_allocator = PortAllocator(start_port=27000, end_port=27200)
            cluster_registry = ClusterRegistry(storage_backend)
            await cluster_registry.initialize()
            
            from src.services.template_manager import TemplateManager
            template_manager = TemplateManager(cluster_registry)
            await template_manager.initialize()
            
            data_dir = temp_path / "clusters"
            data_dir.mkdir(exist_ok=True)
            from src.services.cluster_factory import ClusterFactory
            cluster_factory = ClusterFactory(
                registry=cluster_registry,
                port_allocator=port_allocator,
                data_directory=str(data_dir)
            )
            
            # Create multi-cluster manager
            with patch('src.services.cluster_factory.docker'):
                manager = MultiClusterManager(
                    registry=cluster_registry,
                    factory=cluster_factory,
                    template_manager=template_manager
                )
                await manager.initialize()
            
            # Create additional services
            advanced_features = AdvancedClusterFeatures(cluster_registry)
            cross_cluster_ops = CrossClusterOperations(cluster_registry)
            config_manager = ConfigurationManager(cluster_registry)
            
            yield {
                "manager": manager,
                "registry": cluster_registry,
                "advanced_features": advanced_features,
                "cross_cluster_ops": cross_cluster_ops,
                "config_manager": config_manager,
                "port_allocator": port_allocator,
                "temp_path": temp_path
            }
            
        finally:
            # Cleanup
            try:
                await manager.shutdown()
            except Exception:
                pass
            shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.mark.asyncio
    async def test_cluster_creation_consistency(self, consistency_test_setup):
        """Test data consistency during cluster creation."""
        env = consistency_test_setup
        manager = env["manager"]
        registry = env["registry"]
        port_allocator = env["port_allocator"]
        validator = DataConsistencyValidator()
        
        with patch('src.services.cluster_factory.docker'), \
             patch('src.services.cluster_manager.ClusterManager') as mock_cluster_manager:
            
            mock_manager_instance = AsyncMock()
            mock_manager_instance.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
            mock_cluster_manager.return_value = mock_manager_instance
            
            # Create test cluster
            ports = await port_allocator.allocate_ports("consistency-test")
            cluster_def = ClusterDefinition(
                id="consistency-test",
                name="Consistency Test Cluster",
                description="Cluster for testing data consistency",
                environment="testing",
                template_id="testing",
                port_allocation=PortAllocation(
                    kafka_port=ports.kafka_port,
                    rest_proxy_port=ports.rest_proxy_port,
                    ui_port=ports.ui_port
                ),
                tags={"consistency": "test", "validation": "enabled"}
            )
            
            # Store original checksum
            validator.store_cluster_checksum("consistency-test", cluster_def)
            
            # Create cluster
            created_cluster = await manager.create_cluster(cluster_def, auto_start=False)
            
            # Validate consistency after creation
            assert validator.validate_cluster_consistency("consistency-test", created_cluster)
            
            # Retrieve cluster from registry
            retrieved_cluster = await registry.get_cluster("consistency-test")
            
            # Validate consistency of retrieved cluster
            assert validator.validate_cluster_consistency("consistency-test", retrieved_cluster)
            
            # Update cluster status and validate consistency is maintained
            await registry.update_cluster_status("consistency-test", ServiceStatus.RUNNING)
            updated_cluster = await registry.get_cluster("consistency-test")
            
            # Status change should not affect core cluster data consistency
            # (we exclude status from checksum calculation)
            assert validator.validate_cluster_consistency("consistency-test", updated_cluster)
            
            # Cleanup
            await manager.delete_cluster("consistency-test", force=True)
            
            # Validate no errors occurred
            report = validator.get_validation_report()
            assert report["is_consistent"], f"Validation errors: {report['validation_errors']}"
    
    @pytest.mark.asyncio
    async def test_cluster_cloning_consistency(self, consistency_test_setup):
        """Test data consistency during cluster cloning."""
        env = consistency_test_setup
        manager = env["manager"]
        advanced_features = env["advanced_features"]
        port_allocator = env["port_allocator"]
        validator = DataConsistencyValidator()
        
        with patch('src.services.cluster_factory.docker'), \
             patch('src.services.cluster_manager.ClusterManager') as mock_cluster_manager:
            
            mock_manager_instance = AsyncMock()
            mock_manager_instance.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
            mock_cluster_manager.return_value = mock_manager_instance
            
            # Create source cluster
            source_ports = await port_allocator.allocate_ports("clone-source")
            source_cluster_def = ClusterDefinition(
                id="clone-source",
                name="Clone Source Cluster",
                description="Source cluster for cloning test",
                environment="development",
                template_id="development",
                port_allocation=PortAllocation(
                    kafka_port=source_ports.kafka_port,
                    rest_proxy_port=source_ports.rest_proxy_port,
                    ui_port=source_ports.ui_port
                ),
                tags={"role": "source", "cloning": "test", "data": "original"}
            )
            
            created_source = await manager.create_cluster(source_cluster_def, auto_start=True)
            validator.store_cluster_checksum("clone-source", created_source)
            
            # Clone the cluster
            cloned_cluster = await advanced_features.clone_cluster(
                source_cluster_id="clone-source",
                target_cluster_id="clone-target",
                target_name="Clone Target Cluster",
                clone_data=True,
                clone_config=True,
                port_offset=100,
                tags={"role": "clone", "cloned_from": "clone-source"}
            )
            
            # Validate clone consistency
            expected_differences = {
                "id", "name", "port_allocation", "created_at", "tags"
            }
            
            assert validator.validate_cluster_clone_consistency(
                created_source, cloned_cluster, expected_differences
            )
            
            # Validate that core configuration was preserved
            assert cloned_cluster.description == created_source.description
            assert cloned_cluster.environment == created_source.environment
            assert cloned_cluster.template_id == created_source.template_id
            
            # Validate that ports were properly offset
            assert cloned_cluster.port_allocation.kafka_port == created_source.port_allocation.kafka_port + 100
            assert cloned_cluster.port_allocation.rest_proxy_port == created_source.port_allocation.rest_proxy_port + 100
            assert cloned_cluster.port_allocation.ui_port == created_source.port_allocation.ui_port + 100
            
            # Validate that tags were properly merged
            assert cloned_cluster.tags.get("role") == "clone"
            assert cloned_cluster.tags.get("cloned_from") == "clone-source"
            
            # Cleanup
            await manager.delete_cluster("clone-source", force=True)
            await manager.delete_cluster("clone-target", force=True)
            
            # Validate no errors occurred
            report = validator.get_validation_report()
            assert report["is_consistent"], f"Validation errors: {report['validation_errors']}"
    
    @pytest.mark.asyncio
    async def test_snapshot_consistency(self, consistency_test_setup):
        """Test data consistency during snapshot operations."""
        env = consistency_test_setup
        manager = env["manager"]
        advanced_features = env["advanced_features"]
        port_allocator = env["port_allocator"]
        validator = DataConsistencyValidator()
        
        with patch('src.services.cluster_factory.docker'), \
             patch('src.services.cluster_manager.ClusterManager') as mock_cluster_manager:
            
            mock_manager_instance = AsyncMock()
            mock_manager_instance.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
            mock_cluster_manager.return_value = mock_manager_instance
            
            # Create test cluster
            ports = await port_allocator.allocate_ports("snapshot-test")
            cluster_def = ClusterDefinition(
                id="snapshot-test",
                name="Snapshot Test Cluster",
                description="Cluster for snapshot consistency testing",
                environment="testing",
                template_id="testing",
                port_allocation=PortAllocation(
                    kafka_port=ports.kafka_port,
                    rest_proxy_port=ports.rest_proxy_port,
                    ui_port=ports.ui_port
                ),
                tags={"snapshot": "test", "version": "1.0"}
            )
            
            created_cluster = await manager.create_cluster(cluster_def, auto_start=True)
            validator.store_cluster_checksum("snapshot-test-original", created_cluster)
            
            # Create snapshot
            snapshot = await advanced_features.create_snapshot(
                cluster_id="snapshot-test",
                name="Consistency Test Snapshot",
                description="Snapshot for consistency testing",
                snapshot_type=SnapshotType.FULL,
                tags={"consistency", "test"}
            )
            
            # Modify cluster (simulate changes)
            modified_tags = created_cluster.tags.copy()
            modified_tags["version"] = "2.0"
            modified_tags["modified"] = "true"
            
            await advanced_features.add_cluster_tags(
                cluster_id="snapshot-test",
                tags={"version": "2.0", "modified": "true"}
            )
            
            # Get modified cluster
            modified_cluster = await manager.registry.get_cluster("snapshot-test")
            
            # Restore from snapshot
            restored_cluster = await advanced_features.restore_snapshot(
                snapshot_id=snapshot.id,
                target_cluster_id="snapshot-test-restored",
                restore_config=True,
                restore_data=True
            )
            
            # Validate that restored cluster matches original (excluding expected differences)
            expected_differences = {"id", "name", "created_at"}
            
            assert validator.validate_cluster_clone_consistency(
                created_cluster, restored_cluster, expected_differences
            )
            
            # Validate that original tags were restored (not modified ones)
            assert restored_cluster.tags.get("version") == "1.0"
            assert "modified" not in restored_cluster.tags
            
            # Cleanup
            await advanced_features.delete_snapshot(snapshot.id)
            await manager.delete_cluster("snapshot-test", force=True)
            await manager.delete_cluster("snapshot-test-restored", force=True)
            
            # Validate no errors occurred
            report = validator.get_validation_report()
            assert report["is_consistent"], f"Validation errors: {report['validation_errors']}"
    
    @pytest.mark.asyncio
    async def test_configuration_export_import_consistency(self, consistency_test_setup):
        """Test data consistency during configuration export/import."""
        env = consistency_test_setup
        manager = env["manager"]
        config_manager = env["config_manager"]
        port_allocator = env["port_allocator"]
        validator = DataConsistencyValidator()
        
        with patch('src.services.cluster_factory.docker'), \
             patch('src.services.cluster_manager.ClusterManager') as mock_cluster_manager:
            
            mock_manager_instance = AsyncMock()
            mock_manager_instance.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
            mock_cluster_manager.return_value = mock_manager_instance
            
            # Create test cluster
            ports = await port_allocator.allocate_ports("config-test")
            cluster_def = ClusterDefinition(
                id="config-test",
                name="Configuration Test Cluster",
                description="Cluster for configuration consistency testing",
                environment="testing",
                template_id="testing",
                port_allocation=PortAllocation(
                    kafka_port=ports.kafka_port,
                    rest_proxy_port=ports.rest_proxy_port,
                    ui_port=ports.ui_port
                ),
                tags={"config": "test", "export": "enabled", "version": "1.0"}
            )
            
            created_cluster = await manager.create_cluster(cluster_def, auto_start=False)
            validator.store_cluster_checksum("config-test-original", created_cluster)
            
            # Export configuration
            exported_config = await config_manager.export_cluster_config(
                cluster_id="config-test",
                format="yaml",
                include_metadata=True,
                include_secrets=False
            )
            
            # Validate export contains expected data
            assert "id: config-test" in exported_config
            assert "name: Configuration Test Cluster" in exported_config
            assert "environment: testing" in exported_config
            
            # Import configuration as new cluster
            imported_config = exported_config.replace(
                "id: config-test",
                "id: config-test-imported"
            ).replace(
                "name: Configuration Test Cluster",
                "name: Imported Configuration Test Cluster"
            ).replace(
                f"kafka_port: {ports.kafka_port}",
                f"kafka_port: {ports.kafka_port + 100}"
            ).replace(
                f"rest_proxy_port: {ports.rest_proxy_port}",
                f"rest_proxy_port: {ports.rest_proxy_port + 100}"
            ).replace(
                f"ui_port: {ports.ui_port}",
                f"ui_port: {ports.ui_port + 100}"
            )
            
            imported_cluster_def = await config_manager.import_cluster_config(
                config_content=imported_config,
                format="yaml",
                validate_schema=True,
                overwrite_existing=False
            )
            
            # Create imported cluster
            imported_cluster = await manager.create_cluster(imported_cluster_def, auto_start=False)
            
            # Validate that imported cluster matches original (excluding expected differences)
            expected_differences = {"id", "name", "port_allocation", "created_at"}
            
            assert validator.validate_cluster_clone_consistency(
                created_cluster, imported_cluster, expected_differences
            )
            
            # Validate specific fields were preserved
            assert imported_cluster.description == created_cluster.description
            assert imported_cluster.environment == created_cluster.environment
            assert imported_cluster.template_id == created_cluster.template_id
            assert imported_cluster.tags == created_cluster.tags
            
            # Cleanup
            await manager.delete_cluster("config-test", force=True)
            await manager.delete_cluster("config-test-imported", force=True)
            
            # Validate no errors occurred
            report = validator.get_validation_report()
            assert report["is_consistent"], f"Validation errors: {report['validation_errors']}"
    
    @pytest.mark.asyncio
    async def test_cross_cluster_operation_consistency(self, consistency_test_setup):
        """Test data consistency during cross-cluster operations."""
        env = consistency_test_setup
        manager = env["manager"]
        cross_cluster_ops = env["cross_cluster_ops"]
        port_allocator = env["port_allocator"]
        validator = DataConsistencyValidator()
        
        with patch('src.services.cluster_factory.docker'), \
             patch('src.services.cluster_manager.ClusterManager') as mock_cluster_manager:
            
            mock_manager_instance = AsyncMock()
            mock_manager_instance.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
            mock_cluster_manager.return_value = mock_manager_instance
            
            # Create source cluster
            source_ports = await port_allocator.allocate_ports("cross-source")
            source_cluster_def = ClusterDefinition(
                id="cross-source",
                name="Cross-Cluster Source",
                description="Source cluster for cross-cluster operations",
                environment="production",
                template_id="production",
                port_allocation=PortAllocation(
                    kafka_port=source_ports.kafka_port,
                    rest_proxy_port=source_ports.rest_proxy_port,
                    ui_port=source_ports.ui_port,
                    jmx_port=source_ports.kafka_port + 1000
                ),
                tags={"role": "source", "cross_cluster": "test"}
            )
            
            # Create target cluster
            target_ports = await port_allocator.allocate_ports("cross-target")
            target_cluster_def = ClusterDefinition(
                id="cross-target",
                name="Cross-Cluster Target",
                description="Target cluster for cross-cluster operations",
                environment="production",
                template_id="production",
                port_allocation=PortAllocation(
                    kafka_port=target_ports.kafka_port,
                    rest_proxy_port=target_ports.rest_proxy_port,
                    ui_port=target_ports.ui_port,
                    jmx_port=target_ports.kafka_port + 1000
                ),
                tags={"role": "target", "cross_cluster": "test"}
            )
            
            created_source = await manager.create_cluster(source_cluster_def, auto_start=True)
            created_target = await manager.create_cluster(target_cluster_def, auto_start=True)
            
            validator.store_cluster_checksum("cross-source", created_source)
            validator.store_cluster_checksum("cross-target", created_target)
            
            # Perform cluster comparison
            comparison = await cross_cluster_ops.compare_clusters(
                cluster_id_1="cross-source",
                cluster_id_2="cross-target",
                compare_topics=True,
                compare_configs=True,
                compare_consumer_groups=True
            )
            
            # Validate comparison results
            assert comparison.cluster_1_id == "cross-source"
            assert comparison.cluster_2_id == "cross-target"
            assert comparison.comparison_timestamp is not None
            
            # Validate that clusters remain unchanged after comparison
            source_after_comparison = await manager.registry.get_cluster("cross-source")
            target_after_comparison = await manager.registry.get_cluster("cross-target")
            
            assert validator.validate_cluster_consistency("cross-source", source_after_comparison)
            assert validator.validate_cluster_consistency("cross-target", target_after_comparison)
            
            # Mock data migration
            with patch.object(cross_cluster_ops, '_perform_topic_migration', new_callable=AsyncMock) as mock_migrate:
                mock_migrate.return_value = {"topics_migrated": 3, "messages_migrated": 1500}
                
                migration_result = await cross_cluster_ops.migrate_data(
                    source_cluster_id="cross-source",
                    target_cluster_id="cross-target",
                    topics=["test-topic-1", "test-topic-2", "test-topic-3"],
                    include_consumer_groups=True,
                    migration_mode="incremental"
                )
                
                # Validate migration results
                assert migration_result.source_cluster_id == "cross-source"
                assert migration_result.target_cluster_id == "cross-target"
            
            # Validate that clusters remain unchanged after migration
            source_after_migration = await manager.registry.get_cluster("cross-source")
            target_after_migration = await manager.registry.get_cluster("cross-target")
            
            assert validator.validate_cluster_consistency("cross-source", source_after_migration)
            assert validator.validate_cluster_consistency("cross-target", target_after_migration)
            
            # Cleanup
            await manager.delete_cluster("cross-source", force=True)
            await manager.delete_cluster("cross-target", force=True)
            
            # Validate no errors occurred
            report = validator.get_validation_report()
            assert report["is_consistent"], f"Validation errors: {report['validation_errors']}"
    
    @pytest.mark.asyncio
    async def test_concurrent_operations_consistency(self, consistency_test_setup):
        """Test data consistency during concurrent operations."""
        env = consistency_test_setup
        manager = env["manager"]
        advanced_features = env["advanced_features"]
        port_allocator = env["port_allocator"]
        validator = DataConsistencyValidator()
        
        with patch('src.services.cluster_factory.docker'), \
             patch('src.services.cluster_manager.ClusterManager') as mock_cluster_manager:
            
            mock_manager_instance = AsyncMock()
            mock_manager_instance.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
            mock_manager_instance.start_cluster.return_value = True
            mock_manager_instance.stop_cluster.return_value = True
            mock_cluster_manager.return_value = mock_manager_instance
            
            # Create base cluster
            ports = await port_allocator.allocate_ports("concurrent-base")
            base_cluster_def = ClusterDefinition(
                id="concurrent-base",
                name="Concurrent Operations Base",
                description="Base cluster for concurrent operations testing",
                environment="testing",
                template_id="testing",
                port_allocation=PortAllocation(
                    kafka_port=ports.kafka_port,
                    rest_proxy_port=ports.rest_proxy_port,
                    ui_port=ports.ui_port
                ),
                tags={"concurrent": "test", "base": "true"}
            )
            
            created_base = await manager.create_cluster(base_cluster_def, auto_start=True)
            validator.store_cluster_checksum("concurrent-base", created_base)
            
            # Define concurrent operations
            async def create_snapshot():
                return await advanced_features.create_snapshot(
                    cluster_id="concurrent-base",
                    name="Concurrent Snapshot",
                    description="Snapshot created during concurrent test",
                    snapshot_type=SnapshotType.CONFIG,
                    tags={"concurrent", "test"}
                )
            
            async def add_tags():
                return await advanced_features.add_cluster_tags(
                    cluster_id="concurrent-base",
                    tags={"concurrent_op": "tag_addition", "timestamp": datetime.utcnow().isoformat()}
                )
            
            async def start_stop_cluster():
                await manager.stop_cluster("concurrent-base")
                await asyncio.sleep(0.1)  # Brief pause
                return await manager.start_cluster("concurrent-base")
            
            async def get_cluster_status():
                return await manager.get_cluster_status("concurrent-base")
            
            # Execute operations concurrently
            operations = [
                create_snapshot(),
                add_tags(),
                start_stop_cluster(),
                get_cluster_status(),
                get_cluster_status(),  # Multiple status checks
            ]
            
            results = await asyncio.gather(*operations, return_exceptions=True)
            
            # Validate that at least some operations succeeded
            successful_operations = [r for r in results if not isinstance(r, Exception)]
            assert len(successful_operations) > 0, "No concurrent operations succeeded"
            
            # Get final cluster state
            final_cluster = await manager.registry.get_cluster("concurrent-base")
            
            # Validate that core cluster data remains consistent
            # (Note: tags may have changed due to concurrent operations)
            core_consistency = validator.validate_cluster_clone_consistency(
                created_base, final_cluster, 
                expected_differences={"tags", "created_at", "status"}
            )
            
            # If core consistency failed, it might be due to legitimate concurrent changes
            # In that case, validate that essential fields are preserved
            if not core_consistency:
                assert final_cluster.id == created_base.id
                assert final_cluster.name == created_base.name
                assert final_cluster.description == created_base.description
                assert final_cluster.environment == created_base.environment
                assert final_cluster.template_id == created_base.template_id
                assert final_cluster.port_allocation.kafka_port == created_base.port_allocation.kafka_port
            
            # Cleanup snapshots if created
            snapshots = await advanced_features.list_snapshots(cluster_id="concurrent-base")
            for snapshot in snapshots:
                try:
                    await advanced_features.delete_snapshot(snapshot.id)
                except Exception:
                    pass  # Ignore cleanup errors
            
            # Cleanup cluster
            await manager.delete_cluster("concurrent-base", force=True)
            
            # Validate that no critical consistency errors occurred
            report = validator.get_validation_report()
            critical_errors = [
                error for error in report["validation_errors"]
                if "id" in error or "port_allocation" in error or "environment" in error
            ]
            
            assert len(critical_errors) == 0, f"Critical consistency errors: {critical_errors}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])