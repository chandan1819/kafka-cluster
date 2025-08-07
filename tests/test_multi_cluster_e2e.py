"""
End-to-end integration tests for multi-cluster Kafka manager.

This module contains comprehensive integration tests that validate complete
multi-cluster workflows from cluster creation to cross-cluster operations.
"""

import pytest
import asyncio
import tempfile
import shutil
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch

from src.services.multi_cluster_manager import MultiClusterManager
from src.registry.cluster_registry import ClusterRegistry
from src.services.cluster_factory import ClusterFactory
from src.services.template_manager import TemplateManager
from src.services.advanced_cluster_features import AdvancedClusterFeatures
from src.services.cross_cluster_operations import CrossClusterOperations
from src.services.configuration_manager import ConfigurationManager
from src.services.resource_manager import ResourceManager
from src.monitoring.enhanced_health_monitor import EnhancedHealthMonitor
from src.security.access_control import AccessControl
from src.models.multi_cluster import (
    ClusterDefinition, PortAllocation, ClusterTemplate,
    SnapshotType, ScheduleType, ScheduleFrequency, AccessLevel
)
from src.models.base import ServiceStatus
from src.storage.file_backend import FileStorageBackend
from src.networking.port_allocator import PortAllocator
from src.exceptions.multi_cluster_exceptions import (
    ClusterCreationError, ClusterStartupError, ClusterUnavailableError
)


class TestMultiClusterE2E:
    """End-to-end integration tests for multi-cluster system."""
    
    @pytest.fixture(scope="class")
    async def temp_directory(self):
        """Create temporary directory for test data."""
        temp_dir = tempfile.mkdtemp(prefix="multi_cluster_test_")
        yield Path(temp_dir)
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.fixture(scope="class")
    async def storage_backend(self, temp_directory):
        """Create file storage backend for testing."""
        storage_path = temp_directory / "storage"
        storage_path.mkdir(exist_ok=True)
        backend = FileStorageBackend(str(storage_path))
        await backend.initialize()
        return backend
    
    @pytest.fixture(scope="class")
    async def port_allocator(self):
        """Create port allocator for testing."""
        return PortAllocator(start_port=19092, end_port=19200)
    
    @pytest.fixture(scope="class")
    async def cluster_registry(self, storage_backend):
        """Create cluster registry for testing."""
        registry = ClusterRegistry(storage_backend)
        await registry.initialize()
        return registry
    
    @pytest.fixture(scope="class")
    async def template_manager(self, cluster_registry):
        """Create template manager for testing."""
        manager = TemplateManager(cluster_registry)
        await manager.initialize()
        return manager
    
    @pytest.fixture(scope="class")
    async def cluster_factory(self, cluster_registry, port_allocator, temp_directory):
        """Create cluster factory for testing."""
        data_dir = temp_directory / "clusters"
        data_dir.mkdir(exist_ok=True)
        
        factory = ClusterFactory(
            registry=cluster_registry,
            port_allocator=port_allocator,
            data_directory=str(data_dir)
        )
        return factory
    
    @pytest.fixture(scope="class")
    async def multi_cluster_manager(self, cluster_registry, cluster_factory, template_manager):
        """Create multi-cluster manager for testing."""
        manager = MultiClusterManager(
            registry=cluster_registry,
            factory=cluster_factory,
            template_manager=template_manager
        )
        
        # Mock Docker operations for testing
        with patch('src.services.cluster_factory.docker'):
            await manager.initialize()
        
        yield manager
        
        # Cleanup
        try:
            await manager.shutdown()
        except Exception:
            pass
    
    @pytest.fixture
    async def sample_cluster_definitions(self, port_allocator) -> List[ClusterDefinition]:
        """Create sample cluster definitions for testing."""
        clusters = []
        
        # Development cluster
        dev_ports = await port_allocator.allocate_ports("dev-cluster")
        clusters.append(ClusterDefinition(
            id="dev-cluster",
            name="Development Cluster",
            description="Development environment cluster",
            environment="development",
            template_id="development",
            port_allocation=PortAllocation(
                kafka_port=dev_ports.kafka_port,
                rest_proxy_port=dev_ports.rest_proxy_port,
                ui_port=dev_ports.ui_port
            ),
            tags={"environment": "development", "team": "backend"}
        ))
        
        # Staging cluster
        staging_ports = await port_allocator.allocate_ports("staging-cluster")
        clusters.append(ClusterDefinition(
            id="staging-cluster",
            name="Staging Cluster",
            description="Staging environment cluster",
            environment="staging",
            template_id="staging",
            port_allocation=PortAllocation(
                kafka_port=staging_ports.kafka_port,
                rest_proxy_port=staging_ports.rest_proxy_port,
                ui_port=staging_ports.ui_port
            ),
            tags={"environment": "staging", "team": "backend"}
        ))
        
        # Production cluster
        prod_ports = await port_allocator.allocate_ports("prod-cluster")
        clusters.append(ClusterDefinition(
            id="prod-cluster",
            name="Production Cluster",
            description="Production environment cluster",
            environment="production",
            template_id="production",
            port_allocation=PortAllocation(
                kafka_port=prod_ports.kafka_port,
                rest_proxy_port=prod_ports.rest_proxy_port,
                ui_port=prod_ports.ui_port,
                jmx_port=prod_ports.kafka_port + 1000
            ),
            tags={"environment": "production", "team": "backend", "critical": "true"}
        ))
        
        return clusters
    
    @pytest.mark.asyncio
    async def test_complete_cluster_lifecycle(self, multi_cluster_manager, sample_cluster_definitions):
        """Test complete cluster lifecycle: create, start, stop, delete."""
        cluster_def = sample_cluster_definitions[0]  # Use dev cluster
        
        # Mock Docker operations
        with patch('src.services.cluster_factory.docker') as mock_docker, \
             patch('src.services.cluster_manager.ClusterManager') as mock_cluster_manager:
            
            # Mock cluster manager
            mock_manager_instance = AsyncMock()
            mock_manager_instance.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
            mock_manager_instance.start_cluster.return_value = True
            mock_manager_instance.stop_cluster.return_value = True
            mock_cluster_manager.return_value = mock_manager_instance
            
            # 1. Create cluster
            created_cluster = await multi_cluster_manager.create_cluster(cluster_def, auto_start=False)
            assert created_cluster.id == cluster_def.id
            assert created_cluster.name == cluster_def.name
            
            # 2. Start cluster
            status = await multi_cluster_manager.start_cluster(cluster_def.id)
            assert status == ServiceStatus.RUNNING
            
            # 3. Verify cluster is in registry
            clusters = await multi_cluster_manager.list_clusters()
            assert any(c.id == cluster_def.id for c in clusters)
            
            # 4. Get cluster status
            cluster_status = await multi_cluster_manager.get_cluster_status(cluster_def.id)
            assert cluster_status.status == ServiceStatus.RUNNING
            
            # 5. Stop cluster
            stopped = await multi_cluster_manager.stop_cluster(cluster_def.id)
            assert stopped is True
            
            # 6. Delete cluster
            deleted = await multi_cluster_manager.delete_cluster(cluster_def.id)
            assert deleted is True
            
            # 7. Verify cluster is removed
            with pytest.raises(Exception):  # Should raise ClusterNotFoundError
                await multi_cluster_manager.get_cluster_status(cluster_def.id)
    
    @pytest.mark.asyncio
    async def test_multiple_cluster_management(self, multi_cluster_manager, sample_cluster_definitions):
        """Test managing multiple clusters simultaneously."""
        
        with patch('src.services.cluster_factory.docker') as mock_docker, \
             patch('src.services.cluster_manager.ClusterManager') as mock_cluster_manager:
            
            # Mock cluster manager
            mock_manager_instance = AsyncMock()
            mock_manager_instance.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
            mock_manager_instance.start_cluster.return_value = True
            mock_manager_instance.stop_cluster.return_value = True
            mock_cluster_manager.return_value = mock_manager_instance
            
            created_clusters = []
            
            # Create all clusters
            for cluster_def in sample_cluster_definitions:
                created_cluster = await multi_cluster_manager.create_cluster(cluster_def, auto_start=False)
                created_clusters.append(created_cluster)
                assert created_cluster.id == cluster_def.id
            
            # Start all clusters
            for cluster in created_clusters:
                status = await multi_cluster_manager.start_cluster(cluster.id)
                assert status == ServiceStatus.RUNNING
            
            # Verify all clusters are running
            all_clusters = await multi_cluster_manager.list_clusters()
            running_clusters = [c for c in all_clusters if c.status == ServiceStatus.RUNNING]
            assert len(running_clusters) == len(sample_cluster_definitions)
            
            # Get system overview
            overview = await multi_cluster_manager.get_system_overview()
            assert overview["total_clusters"] == len(sample_cluster_definitions)
            assert overview["running_clusters"] == len(sample_cluster_definitions)
            
            # Stop all clusters
            for cluster in created_clusters:
                stopped = await multi_cluster_manager.stop_cluster(cluster.id)
                assert stopped is True
            
            # Delete all clusters
            for cluster in created_clusters:
                deleted = await multi_cluster_manager.delete_cluster(cluster.id)
                assert deleted is True
    
    @pytest.mark.asyncio
    async def test_template_based_cluster_creation(self, multi_cluster_manager, template_manager):
        """Test creating clusters from templates."""
        
        with patch('src.services.cluster_factory.docker') as mock_docker, \
             patch('src.services.cluster_manager.ClusterManager') as mock_cluster_manager:
            
            mock_manager_instance = AsyncMock()
            mock_manager_instance.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
            mock_cluster_manager.return_value = mock_manager_instance
            
            # Get available templates
            templates = await template_manager.list_templates()
            assert len(templates) > 0
            
            # Use development template
            dev_template = next((t for t in templates if t.id == "development"), None)
            assert dev_template is not None
            
            # Create cluster from template
            cluster_def = await template_manager.create_cluster_from_template(
                template_id="development",
                cluster_id="template-test-cluster",
                cluster_name="Template Test Cluster",
                overrides={"description": "Created from template"}
            )
            
            # Create the cluster
            created_cluster = await multi_cluster_manager.create_cluster(cluster_def, auto_start=False)
            assert created_cluster.id == "template-test-cluster"
            assert created_cluster.template_id == "development"
            assert "Created from template" in created_cluster.description
            
            # Cleanup
            await multi_cluster_manager.delete_cluster(created_cluster.id, force=True)
    
    @pytest.mark.asyncio
    async def test_advanced_cluster_features(self, multi_cluster_manager, sample_cluster_definitions):
        """Test advanced cluster features like cloning and snapshots."""
        
        with patch('src.services.cluster_factory.docker') as mock_docker, \
             patch('src.services.cluster_manager.ClusterManager') as mock_cluster_manager:
            
            mock_manager_instance = AsyncMock()
            mock_manager_instance.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
            mock_cluster_manager.return_value = mock_manager_instance
            
            # Create source cluster
            source_cluster = sample_cluster_definitions[0]
            created_source = await multi_cluster_manager.create_cluster(source_cluster, auto_start=True)
            
            # Initialize advanced features
            advanced_features = AdvancedClusterFeatures(multi_cluster_manager.registry)
            
            # Create snapshot
            snapshot = await advanced_features.create_snapshot(
                cluster_id=created_source.id,
                name="Test Snapshot",
                description="Test snapshot for E2E testing",
                snapshot_type=SnapshotType.FULL,
                tags={"test", "e2e"}
            )
            assert snapshot.cluster_id == created_source.id
            assert snapshot.name == "Test Snapshot"
            
            # List snapshots
            snapshots = await advanced_features.list_snapshots(cluster_id=created_source.id)
            assert len(snapshots) == 1
            assert snapshots[0].id == snapshot.id
            
            # Clone cluster
            cloned_cluster = await advanced_features.clone_cluster(
                source_cluster_id=created_source.id,
                target_cluster_id="cloned-cluster",
                target_name="Cloned Cluster",
                clone_data=False,  # Skip data cloning for test
                clone_config=True,
                port_offset=10,
                tags={"cloned": "true", "source": created_source.id}
            )
            assert cloned_cluster.id == "cloned-cluster"
            assert cloned_cluster.name == "Cloned Cluster"
            assert cloned_cluster.tags.get("cloned") == "true"
            
            # Create schedule
            schedule = await advanced_features.create_schedule(
                cluster_id=created_source.id,
                name="Test Schedule",
                schedule_type=ScheduleType.BACKUP,
                frequency=ScheduleFrequency.DAILY,
                schedule_expression="02:00",
                description="Daily backup schedule",
                enabled=True,
                tags={"backup", "automated"}
            )
            assert schedule.cluster_id == created_source.id
            assert schedule.name == "Test Schedule"
            
            # Cleanup
            await advanced_features.delete_snapshot(snapshot.id)
            await advanced_features.delete_schedule(schedule.id)
            await multi_cluster_manager.delete_cluster(created_source.id, force=True)
            await multi_cluster_manager.delete_cluster(cloned_cluster.id, force=True)
    
    @pytest.mark.asyncio
    async def test_cross_cluster_operations(self, multi_cluster_manager, sample_cluster_definitions):
        """Test cross-cluster operations like data migration and comparison."""
        
        with patch('src.services.cluster_factory.docker') as mock_docker, \
             patch('src.services.cluster_manager.ClusterManager') as mock_cluster_manager:
            
            mock_manager_instance = AsyncMock()
            mock_manager_instance.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
            mock_cluster_manager.return_value = mock_manager_instance
            
            # Create source and target clusters
            source_cluster = sample_cluster_definitions[0]
            target_cluster = sample_cluster_definitions[1]
            
            created_source = await multi_cluster_manager.create_cluster(source_cluster, auto_start=True)
            created_target = await multi_cluster_manager.create_cluster(target_cluster, auto_start=True)
            
            # Initialize cross-cluster operations
            cross_cluster_ops = CrossClusterOperations(multi_cluster_manager.registry)
            
            # Compare clusters
            comparison = await cross_cluster_ops.compare_clusters(
                cluster_id_1=created_source.id,
                cluster_id_2=created_target.id,
                compare_topics=True,
                compare_configs=True,
                compare_consumer_groups=True
            )
            assert comparison.cluster_1_id == created_source.id
            assert comparison.cluster_2_id == created_target.id
            assert comparison.comparison_timestamp is not None
            
            # Mock data migration (would normally involve actual Kafka operations)
            with patch.object(cross_cluster_ops, '_perform_topic_migration', new_callable=AsyncMock) as mock_migrate:
                mock_migrate.return_value = {"topics_migrated": 5, "messages_migrated": 1000}
                
                migration_result = await cross_cluster_ops.migrate_data(
                    source_cluster_id=created_source.id,
                    target_cluster_id=created_target.id,
                    topics=["test-topic-1", "test-topic-2"],
                    include_consumer_groups=True,
                    migration_mode="incremental"
                )
                assert migration_result.source_cluster_id == created_source.id
                assert migration_result.target_cluster_id == created_target.id
                assert migration_result.status.value in ["completed", "in_progress"]
            
            # Set up replication
            with patch.object(cross_cluster_ops, '_setup_kafka_replication', new_callable=AsyncMock) as mock_replication:
                mock_replication.return_value = {"replication_id": "test-replication-123"}
                
                replication = await cross_cluster_ops.setup_replication(
                    source_cluster_id=created_source.id,
                    target_cluster_ids=[created_target.id],
                    topics=["important-topic"],
                    replication_mode="async"
                )
                assert replication.source_cluster_id == created_source.id
                assert created_target.id in replication.target_cluster_ids
            
            # Cleanup
            await multi_cluster_manager.delete_cluster(created_source.id, force=True)
            await multi_cluster_manager.delete_cluster(created_target.id, force=True)
    
    @pytest.mark.asyncio
    async def test_configuration_management(self, multi_cluster_manager, sample_cluster_definitions):
        """Test configuration management features."""
        
        with patch('src.services.cluster_factory.docker') as mock_docker, \
             patch('src.services.cluster_manager.ClusterManager') as mock_cluster_manager:
            
            mock_manager_instance = AsyncMock()
            mock_manager_instance.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
            mock_cluster_manager.return_value = mock_manager_instance
            
            # Create test cluster
            test_cluster = sample_cluster_definitions[0]
            created_cluster = await multi_cluster_manager.create_cluster(test_cluster, auto_start=True)
            
            # Initialize configuration manager
            config_manager = ConfigurationManager(multi_cluster_manager.registry)
            
            # Export configuration
            exported_config = await config_manager.export_cluster_config(
                cluster_id=created_cluster.id,
                format="yaml",
                include_metadata=True,
                include_secrets=False
            )
            assert isinstance(exported_config, str)
            assert "id: " + created_cluster.id in exported_config
            
            # Validate configuration
            validation_result = await config_manager.validate_configuration(
                config_content=exported_config,
                format="yaml"
            )
            assert validation_result.is_valid is True
            assert len(validation_result.errors) == 0
            
            # List configuration versions
            versions = await config_manager.list_config_versions(created_cluster.id)
            assert len(versions) >= 1  # At least the initial version
            
            # Import modified configuration
            modified_config = exported_config.replace(
                test_cluster.description,
                "Modified description for testing"
            )
            
            imported_cluster = await config_manager.import_cluster_config(
                config_content=modified_config,
                format="yaml",
                validate_schema=True,
                overwrite_existing=False
            )
            assert "Modified description for testing" in imported_cluster.description
            
            # Cleanup
            await multi_cluster_manager.delete_cluster(created_cluster.id, force=True)
    
    @pytest.mark.asyncio
    async def test_security_and_access_control(self, multi_cluster_manager, sample_cluster_definitions):
        """Test security and access control features."""
        
        with patch('src.services.cluster_factory.docker') as mock_docker, \
             patch('src.services.cluster_manager.ClusterManager') as mock_cluster_manager:
            
            mock_manager_instance = AsyncMock()
            mock_manager_instance.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
            mock_cluster_manager.return_value = mock_manager_instance
            
            # Create test cluster
            test_cluster = sample_cluster_definitions[0]
            created_cluster = await multi_cluster_manager.create_cluster(test_cluster, auto_start=True)
            
            # Initialize access control
            access_control = AccessControl()
            await access_control.initialize()
            
            # Create test user
            user = await access_control.create_user(
                username="test_user",
                email="test@example.com",
                password="secure_password",
                is_admin=False
            )
            assert user.username == "test_user"
            assert user.is_admin is False
            
            # Create admin user
            admin = await access_control.create_user(
                username="test_admin",
                email="admin@example.com",
                password="admin_password",
                is_admin=True
            )
            assert admin.is_admin is True
            
            # Grant cluster permission
            permission = await access_control.grant_cluster_permission(
                user_id=user.id,
                cluster_id=created_cluster.id,
                access_level=AccessLevel.READ,
                granted_by=admin.id
            )
            assert permission.user_id == user.id
            assert permission.cluster_id == created_cluster.id
            assert permission.access_level == AccessLevel.READ
            
            # Check permission
            has_permission = await access_control.check_cluster_permission(
                user.id,
                created_cluster.id,
                "cluster_view"
            )
            assert has_permission is True
            
            # Create API key
            api_key, raw_key = await access_control.create_api_key(
                user_id=user.id,
                name="test_api_key",
                cluster_permissions={created_cluster.id: AccessLevel.READ},
                expires_at=datetime.utcnow() + timedelta(days=30)
            )
            assert api_key.name == "test_api_key"
            assert raw_key is not None
            
            # Verify API key
            verified_key = await access_control.verify_api_key(raw_key)
            assert verified_key.id == api_key.id
            
            # Get audit log
            audit_entries = await access_control.get_audit_log(
                user_id=user.id,
                limit=10
            )
            assert len(audit_entries) > 0
            
            # Cleanup
            await access_control.delete_api_key(api_key.id)
            await access_control.delete_user(user.id)
            await access_control.delete_user(admin.id)
            await multi_cluster_manager.delete_cluster(created_cluster.id, force=True)
    
    @pytest.mark.asyncio
    async def test_monitoring_and_health_checks(self, multi_cluster_manager, sample_cluster_definitions):
        """Test monitoring and health check functionality."""
        
        with patch('src.services.cluster_factory.docker') as mock_docker, \
             patch('src.services.cluster_manager.ClusterManager') as mock_cluster_manager:
            
            mock_manager_instance = AsyncMock()
            mock_manager_instance.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
            mock_cluster_manager.return_value = mock_manager_instance
            
            # Create test cluster
            test_cluster = sample_cluster_definitions[0]
            created_cluster = await multi_cluster_manager.create_cluster(test_cluster, auto_start=True)
            
            # Initialize health monitor
            health_monitor = EnhancedHealthMonitor()
            
            # Get system overview
            overview = await health_monitor.get_system_overview()
            assert "clusters" in overview
            assert "system_resources" in overview
            assert overview["clusters"]["total"] >= 1
            
            # Get cluster-specific health
            cluster_health = await health_monitor.get_cluster_specific_health(created_cluster.id)
            assert cluster_health["cluster_id"] == created_cluster.id
            assert "health" in cluster_health
            assert "alerts" in cluster_health
            
            # Get system metrics
            metrics = await health_monitor.get_system_metrics()
            assert isinstance(metrics, list)
            
            # Test alert functionality
            alerts = await health_monitor.get_active_alerts()
            assert isinstance(alerts, list)
            
            # Cleanup
            await multi_cluster_manager.delete_cluster(created_cluster.id, force=True)
    
    @pytest.mark.asyncio
    async def test_resource_management(self, multi_cluster_manager, sample_cluster_definitions):
        """Test resource management and optimization."""
        
        with patch('src.services.cluster_factory.docker') as mock_docker, \
             patch('src.services.cluster_manager.ClusterManager') as mock_cluster_manager:
            
            mock_manager_instance = AsyncMock()
            mock_manager_instance.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
            mock_cluster_manager.return_value = mock_manager_instance
            
            # Create test clusters
            created_clusters = []
            for cluster_def in sample_cluster_definitions[:2]:  # Create 2 clusters
                created_cluster = await multi_cluster_manager.create_cluster(cluster_def, auto_start=True)
                created_clusters.append(created_cluster)
            
            # Initialize resource manager
            resource_manager = ResourceManager()
            
            # Get system resource usage
            system_usage = await resource_manager.get_system_resource_usage()
            assert "cpu" in system_usage
            assert "memory" in system_usage
            assert "disk" in system_usage
            
            # Get cluster resource usage
            for cluster in created_clusters:
                cluster_usage = await resource_manager.get_cluster_resource_usage(cluster.id)
                assert "disk" in cluster_usage
                assert "ports" in cluster_usage
            
            # Get scaling recommendations
            recommendations = await resource_manager.get_scaling_recommendations()
            assert isinstance(recommendations, list)
            
            # Run resource optimization
            optimization = await resource_manager.optimize_resource_allocation()
            assert "system_optimization" in optimization
            assert "cluster_optimizations" in optimization
            
            # Test cleanup (dry run)
            cleanup_result = await resource_manager.run_cleanup(dry_run=True)
            assert "clusters_to_clean" in cleanup_result
            assert "space_to_free_bytes" in cleanup_result
            
            # Cleanup
            for cluster in created_clusters:
                await multi_cluster_manager.delete_cluster(cluster.id, force=True)
    
    @pytest.mark.asyncio
    async def test_error_handling_and_recovery(self, multi_cluster_manager, sample_cluster_definitions):
        """Test error handling and recovery mechanisms."""
        
        with patch('src.services.cluster_factory.docker') as mock_docker, \
             patch('src.services.cluster_manager.ClusterManager') as mock_cluster_manager:
            
            # Mock cluster manager to simulate failures
            mock_manager_instance = AsyncMock()
            mock_manager_instance.get_status.return_value = Mock(status=ServiceStatus.ERROR)
            mock_manager_instance.start_cluster.side_effect = Exception("Simulated startup failure")
            mock_cluster_manager.return_value = mock_manager_instance
            
            # Test cluster creation with failure
            test_cluster = sample_cluster_definitions[0]
            
            # This should trigger error recovery
            with pytest.raises(Exception):  # Should raise ClusterCreationError or similar
                await multi_cluster_manager.create_cluster(test_cluster, auto_start=True)
            
            # Test error recovery statistics
            from src.recovery.error_recovery import error_recovery_manager
            stats = error_recovery_manager.get_recovery_statistics()
            assert isinstance(stats, dict)
            assert "total_recoveries" in stats
            
            # Test graceful degradation
            from src.services.graceful_degradation import graceful_degradation_service
            
            # Force degradation for testing
            await graceful_degradation_service.force_degradation(
                test_cluster.id,
                graceful_degradation_service.DegradationLevel.MODERATE,
                "E2E test degradation"
            )
            
            # Check degradation status
            is_degraded = graceful_degradation_service.is_cluster_degraded(test_cluster.id)
            assert is_degraded is True
            
            # Get degradation statistics
            degradation_stats = graceful_degradation_service.get_degradation_statistics()
            assert degradation_stats["degraded_clusters"] >= 1
            
            # Restore from degradation
            await graceful_degradation_service.force_restoration(test_cluster.id)
            is_degraded_after = graceful_degradation_service.is_cluster_degraded(test_cluster.id)
            assert is_degraded_after is False
    
    @pytest.mark.asyncio
    async def test_concurrent_operations(self, multi_cluster_manager, sample_cluster_definitions):
        """Test concurrent cluster operations."""
        
        with patch('src.services.cluster_factory.docker') as mock_docker, \
             patch('src.services.cluster_manager.ClusterManager') as mock_cluster_manager:
            
            mock_manager_instance = AsyncMock()
            mock_manager_instance.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
            mock_manager_instance.start_cluster.return_value = True
            mock_manager_instance.stop_cluster.return_value = True
            mock_cluster_manager.return_value = mock_manager_instance
            
            # Create multiple clusters concurrently
            create_tasks = []
            for i, cluster_def in enumerate(sample_cluster_definitions):
                # Modify cluster ID to avoid conflicts
                modified_cluster = ClusterDefinition(
                    id=f"concurrent-{i}-{cluster_def.id}",
                    name=f"Concurrent {i} {cluster_def.name}",
                    description=cluster_def.description,
                    environment=cluster_def.environment,
                    template_id=cluster_def.template_id,
                    port_allocation=cluster_def.port_allocation,
                    tags=cluster_def.tags
                )
                task = multi_cluster_manager.create_cluster(modified_cluster, auto_start=False)
                create_tasks.append(task)
            
            # Wait for all creations to complete
            created_clusters = await asyncio.gather(*create_tasks, return_exceptions=True)
            
            # Filter out exceptions and get successful creations
            successful_clusters = [c for c in created_clusters if not isinstance(c, Exception)]
            assert len(successful_clusters) > 0
            
            # Start clusters concurrently
            start_tasks = []
            for cluster in successful_clusters:
                task = multi_cluster_manager.start_cluster(cluster.id)
                start_tasks.append(task)
            
            start_results = await asyncio.gather(*start_tasks, return_exceptions=True)
            successful_starts = [r for r in start_results if not isinstance(r, Exception)]
            assert len(successful_starts) > 0
            
            # Stop and delete clusters concurrently
            cleanup_tasks = []
            for cluster in successful_clusters:
                task = multi_cluster_manager.delete_cluster(cluster.id, force=True)
                cleanup_tasks.append(task)
            
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)
    
    @pytest.mark.asyncio
    async def test_data_consistency_and_persistence(self, multi_cluster_manager, sample_cluster_definitions, temp_directory):
        """Test data consistency and persistence across restarts."""
        
        with patch('src.services.cluster_factory.docker') as mock_docker, \
             patch('src.services.cluster_manager.ClusterManager') as mock_cluster_manager:
            
            mock_manager_instance = AsyncMock()
            mock_manager_instance.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
            mock_cluster_manager.return_value = mock_manager_instance
            
            # Create test cluster
            test_cluster = sample_cluster_definitions[0]
            created_cluster = await multi_cluster_manager.create_cluster(test_cluster, auto_start=True)
            
            # Verify cluster is persisted
            clusters_before = await multi_cluster_manager.list_clusters()
            assert any(c.id == created_cluster.id for c in clusters_before)
            
            # Simulate manager restart by creating new instance with same storage
            storage_backend = FileStorageBackend(str(temp_directory / "storage"))
            await storage_backend.initialize()
            
            new_registry = ClusterRegistry(storage_backend)
            await new_registry.initialize()
            
            # Verify cluster still exists after "restart"
            clusters_after = await new_registry.list_clusters()
            assert any(c.id == created_cluster.id for c in clusters_after)
            
            # Verify cluster data integrity
            restored_cluster = await new_registry.get_cluster(created_cluster.id)
            assert restored_cluster.id == created_cluster.id
            assert restored_cluster.name == created_cluster.name
            assert restored_cluster.description == created_cluster.description
            assert restored_cluster.environment == created_cluster.environment
            
            # Cleanup
            await multi_cluster_manager.delete_cluster(created_cluster.id, force=True)
    
    @pytest.mark.asyncio
    async def test_performance_under_load(self, multi_cluster_manager, sample_cluster_definitions):
        """Test system performance under load."""
        
        with patch('src.services.cluster_factory.docker') as mock_docker, \
             patch('src.services.cluster_manager.ClusterManager') as mock_cluster_manager:
            
            mock_manager_instance = AsyncMock()
            mock_manager_instance.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
            mock_manager_instance.start_cluster.return_value = True
            mock_cluster_manager.return_value = mock_manager_instance
            
            # Create many clusters to test performance
            num_clusters = 10
            cluster_tasks = []
            
            start_time = datetime.utcnow()
            
            for i in range(num_clusters):
                # Create unique cluster definition
                cluster_def = ClusterDefinition(
                    id=f"perf-test-{i}",
                    name=f"Performance Test Cluster {i}",
                    description=f"Performance test cluster {i}",
                    environment="testing",
                    template_id="testing",
                    port_allocation=PortAllocation(
                        kafka_port=20000 + i,
                        rest_proxy_port=21000 + i,
                        ui_port=22000 + i
                    ),
                    tags={"performance": "test", "batch": str(i // 5)}
                )
                
                task = multi_cluster_manager.create_cluster(cluster_def, auto_start=True)
                cluster_tasks.append(task)
            
            # Wait for all clusters to be created
            results = await asyncio.gather(*cluster_tasks, return_exceptions=True)
            successful_clusters = [r for r in results if not isinstance(r, Exception)]
            
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            # Performance assertions
            assert len(successful_clusters) >= num_clusters * 0.8  # At least 80% success rate
            assert duration < 60  # Should complete within 60 seconds
            
            # Test list performance
            list_start = datetime.utcnow()
            all_clusters = await multi_cluster_manager.list_clusters()
            list_end = datetime.utcnow()
            list_duration = (list_end - list_start).total_seconds()
            
            assert len(all_clusters) >= len(successful_clusters)
            assert list_duration < 5  # List operation should be fast
            
            # Cleanup
            cleanup_tasks = []
            for cluster in successful_clusters:
                task = multi_cluster_manager.delete_cluster(cluster.id, force=True)
                cleanup_tasks.append(task)
            
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])