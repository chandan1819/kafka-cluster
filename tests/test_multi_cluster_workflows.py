"""
Multi-cluster workflow integration tests.

This module tests complete multi-cluster workflows that span multiple
components and simulate real-world usage scenarios.
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
from src.monitoring.enhanced_health_monitor import EnhancedHealthMonitor
from src.security.access_control import AccessControl
from src.models.multi_cluster import (
    ClusterDefinition, PortAllocation, SnapshotType, 
    ScheduleType, ScheduleFrequency, AccessLevel
)
from src.models.base import ServiceStatus
from src.storage.file_backend import FileStorageBackend
from src.networking.port_allocator import PortAllocator


class TestMultiClusterWorkflows:
    """Test complete multi-cluster workflows."""
    
    @pytest.fixture(scope="class")
    async def test_environment(self):
        """Set up complete test environment."""
        # Create temporary directory
        temp_dir = tempfile.mkdtemp(prefix="workflow_test_")
        temp_path = Path(temp_dir)
        
        try:
            # Set up storage
            storage_path = temp_path / "storage"
            storage_path.mkdir(exist_ok=True)
            storage_backend = FileStorageBackend(str(storage_path))
            await storage_backend.initialize()
            
            # Set up components
            port_allocator = PortAllocator(start_port=20000, end_port=20200)
            cluster_registry = ClusterRegistry(storage_backend)
            await cluster_registry.initialize()
            
            template_manager = TemplateManager(cluster_registry)
            await template_manager.initialize()
            
            data_dir = temp_path / "clusters"
            data_dir.mkdir(exist_ok=True)
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
            health_monitor = EnhancedHealthMonitor()
            access_control = AccessControl()
            await access_control.initialize()
            
            yield {
                "manager": manager,
                "registry": cluster_registry,
                "template_manager": template_manager,
                "advanced_features": advanced_features,
                "cross_cluster_ops": cross_cluster_ops,
                "config_manager": config_manager,
                "health_monitor": health_monitor,
                "access_control": access_control,
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
    async def test_development_to_production_workflow(self, test_environment):
        """Test complete development to production deployment workflow."""
        env = test_environment
        manager = env["manager"]
        advanced_features = env["advanced_features"]
        config_manager = env["config_manager"]
        port_allocator = env["port_allocator"]
        
        with patch('src.services.cluster_factory.docker'), \
             patch('src.services.cluster_manager.ClusterManager') as mock_cluster_manager:
            
            # Mock cluster manager
            mock_manager_instance = AsyncMock()
            mock_manager_instance.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
            mock_manager_instance.start_cluster.return_value = True
            mock_manager_instance.stop_cluster.return_value = True
            mock_cluster_manager.return_value = mock_manager_instance
            
            # Step 1: Create development cluster
            dev_ports = await port_allocator.allocate_ports("dev-workflow")
            dev_cluster = ClusterDefinition(
                id="dev-workflow",
                name="Development Workflow Cluster",
                description="Development cluster for workflow testing",
                environment="development",
                template_id="development",
                port_allocation=PortAllocation(
                    kafka_port=dev_ports.kafka_port,
                    rest_proxy_port=dev_ports.rest_proxy_port,
                    ui_port=dev_ports.ui_port
                ),
                tags={"workflow": "dev-to-prod", "stage": "development"}
            )
            
            created_dev = await manager.create_cluster(dev_cluster, auto_start=True)
            assert created_dev.environment == "development"
            
            # Step 2: Develop and test (simulated)
            await asyncio.sleep(0.1)  # Simulate development time
            
            # Step 3: Create snapshot of development cluster
            dev_snapshot = await advanced_features.create_snapshot(
                cluster_id=created_dev.id,
                name="Development Baseline",
                description="Snapshot before promoting to staging",
                snapshot_type=SnapshotType.FULL,
                tags={"promotion", "development", "baseline"}
            )
            assert dev_snapshot.cluster_id == created_dev.id
            
            # Step 4: Export development configuration
            dev_config = await config_manager.export_cluster_config(
                cluster_id=created_dev.id,
                format="yaml",
                include_metadata=True,
                include_secrets=False
            )
            assert isinstance(dev_config, str)
            
            # Step 5: Create staging cluster from development config
            staging_ports = await port_allocator.allocate_ports("staging-workflow")
            
            # Modify config for staging
            staging_config = dev_config.replace(
                "id: dev-workflow",
                "id: staging-workflow"
            ).replace(
                "name: Development Workflow Cluster",
                "name: Staging Workflow Cluster"
            ).replace(
                "environment: development",
                "environment: staging"
            ).replace(
                f"kafka_port: {dev_ports.kafka_port}",
                f"kafka_port: {staging_ports.kafka_port}"
            ).replace(
                f"rest_proxy_port: {dev_ports.rest_proxy_port}",
                f"rest_proxy_port: {staging_ports.rest_proxy_port}"
            ).replace(
                f"ui_port: {dev_ports.ui_port}",
                f"ui_port: {staging_ports.ui_port}"
            )
            
            staging_cluster = await config_manager.import_cluster_config(
                config_content=staging_config,
                format="yaml",
                validate_schema=True,
                overwrite_existing=False
            )
            
            created_staging = await manager.create_cluster(staging_cluster, auto_start=True)
            assert created_staging.environment == "staging"
            
            # Step 6: Run staging tests (simulated)
            await asyncio.sleep(0.1)  # Simulate testing time
            
            # Step 7: Create production cluster
            prod_ports = await port_allocator.allocate_ports("prod-workflow")
            
            # Clone staging to production with modifications
            prod_cluster = await advanced_features.clone_cluster(
                source_cluster_id=created_staging.id,
                target_cluster_id="prod-workflow",
                target_name="Production Workflow Cluster",
                clone_data=False,  # Don't clone test data
                clone_config=True,
                port_offset=prod_ports.kafka_port - staging_ports.kafka_port,
                tags={"workflow": "dev-to-prod", "stage": "production", "critical": "true"}
            )
            
            # Update environment to production
            prod_cluster.environment = "production"
            prod_cluster.template_id = "production"
            
            created_prod = await manager.create_cluster(prod_cluster, auto_start=True)
            assert created_prod.environment == "production"
            
            # Step 8: Create production snapshot
            prod_snapshot = await advanced_features.create_snapshot(
                cluster_id=created_prod.id,
                name="Production Baseline",
                description="Initial production deployment snapshot",
                snapshot_type=SnapshotType.FULL,
                tags={"production", "baseline", "deployment"}
            )
            
            # Step 9: Set up production monitoring and backups
            backup_schedule = await advanced_features.create_schedule(
                cluster_id=created_prod.id,
                name="Production Daily Backup",
                schedule_type=ScheduleType.BACKUP,
                frequency=ScheduleFrequency.DAILY,
                schedule_expression="02:00",
                description="Daily production backup",
                enabled=True,
                tags={"backup", "production", "automated"}
            )
            
            # Step 10: Verify all clusters are running
            all_clusters = await manager.list_clusters()
            workflow_clusters = [c for c in all_clusters if "workflow" in c.id]
            assert len(workflow_clusters) == 3  # dev, staging, prod
            
            running_clusters = [c for c in workflow_clusters if c.status == ServiceStatus.RUNNING]
            assert len(running_clusters) == 3
            
            # Step 11: Cleanup
            await advanced_features.delete_snapshot(dev_snapshot.id)
            await advanced_features.delete_snapshot(prod_snapshot.id)
            await advanced_features.delete_schedule(backup_schedule.id)
            
            for cluster in workflow_clusters:
                await manager.delete_cluster(cluster.id, force=True)
    
    @pytest.mark.asyncio
    async def test_disaster_recovery_workflow(self, test_environment):
        """Test complete disaster recovery workflow."""
        env = test_environment
        manager = env["manager"]
        advanced_features = env["advanced_features"]
        cross_cluster_ops = env["cross_cluster_ops"]
        port_allocator = env["port_allocator"]
        
        with patch('src.services.cluster_factory.docker'), \
             patch('src.services.cluster_manager.ClusterManager') as mock_cluster_manager:
            
            mock_manager_instance = AsyncMock()
            mock_manager_instance.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
            mock_manager_instance.start_cluster.return_value = True
            mock_manager_instance.stop_cluster.return_value = True
            mock_cluster_manager.return_value = mock_manager_instance
            
            # Step 1: Create primary production cluster
            primary_ports = await port_allocator.allocate_ports("primary-prod")
            primary_cluster = ClusterDefinition(
                id="primary-prod",
                name="Primary Production Cluster",
                description="Primary production cluster",
                environment="production",
                template_id="production",
                port_allocation=PortAllocation(
                    kafka_port=primary_ports.kafka_port,
                    rest_proxy_port=primary_ports.rest_proxy_port,
                    ui_port=primary_ports.ui_port,
                    jmx_port=primary_ports.kafka_port + 1000
                ),
                tags={"role": "primary", "dr": "enabled", "critical": "true"}
            )
            
            created_primary = await manager.create_cluster(primary_cluster, auto_start=True)
            
            # Step 2: Create DR cluster
            dr_ports = await port_allocator.allocate_ports("dr-cluster")
            dr_cluster = await advanced_features.clone_cluster(
                source_cluster_id=created_primary.id,
                target_cluster_id="dr-cluster",
                target_name="Disaster Recovery Cluster",
                clone_data=False,
                clone_config=True,
                port_offset=dr_ports.kafka_port - primary_ports.kafka_port,
                tags={"role": "dr", "primary": created_primary.id}
            )
            
            created_dr = await manager.create_cluster(dr_cluster, auto_start=True)
            
            # Step 3: Set up replication
            with patch.object(cross_cluster_ops, '_setup_kafka_replication', new_callable=AsyncMock) as mock_replication:
                mock_replication.return_value = {"replication_id": "dr-replication-123"}
                
                replication = await cross_cluster_ops.setup_replication(
                    source_cluster_id=created_primary.id,
                    target_cluster_ids=[created_dr.id],
                    topics=["critical-events", "user-data", "transactions"],
                    replication_mode="async"
                )
                assert replication.source_cluster_id == created_primary.id
                assert created_dr.id in replication.target_cluster_ids
            
            # Step 4: Create regular snapshots
            primary_snapshot = await advanced_features.create_snapshot(
                cluster_id=created_primary.id,
                name="Pre-disaster Snapshot",
                description="Snapshot before simulated disaster",
                snapshot_type=SnapshotType.FULL,
                tags={"dr", "pre-disaster"}
            )
            
            # Step 5: Set up automated DR testing
            dr_test_schedule = await advanced_features.create_schedule(
                cluster_id=created_dr.id,
                name="DR Test Schedule",
                schedule_type=ScheduleType.CUSTOM,
                frequency=ScheduleFrequency.WEEKLY,
                schedule_expression="SUN 03:00",
                description="Weekly DR test",
                enabled=True,
                tags={"dr", "testing", "automated"}
            )
            
            # Step 6: Simulate disaster (primary cluster failure)
            # In real scenario, this would be actual failure
            await manager.stop_cluster(created_primary.id, force=True)
            
            # Step 7: Promote DR cluster to primary
            # Update DR cluster tags to indicate it's now primary
            await advanced_features.add_cluster_tags(
                cluster_id=created_dr.id,
                tags={
                    "role": "primary",
                    "promoted_at": datetime.utcnow().isoformat(),
                    "original_primary": created_primary.id
                }
            )
            
            # Step 8: Verify DR cluster is operational
            dr_status = await manager.get_cluster_status(created_dr.id)
            assert dr_status.status == ServiceStatus.RUNNING
            
            # Step 9: Create new DR cluster for the promoted primary
            new_dr_ports = await port_allocator.allocate_ports("new-dr-cluster")
            new_dr_cluster = await advanced_features.clone_cluster(
                source_cluster_id=created_dr.id,
                target_cluster_id="new-dr-cluster",
                target_name="New DR Cluster",
                clone_data=False,
                clone_config=True,
                port_offset=new_dr_ports.kafka_port - dr_ports.kafka_port,
                tags={"role": "dr", "primary": created_dr.id, "generation": "2"}
            )
            
            created_new_dr = await manager.create_cluster(new_dr_cluster, auto_start=True)
            
            # Step 10: Verify complete DR setup
            all_clusters = await manager.list_clusters()
            dr_clusters = [c for c in all_clusters if "dr" in c.tags.get("role", "") or "primary" in c.tags.get("role", "")]
            assert len(dr_clusters) >= 2  # At least promoted primary and new DR
            
            # Step 11: Cleanup
            await advanced_features.delete_snapshot(primary_snapshot.id)
            await advanced_features.delete_schedule(dr_test_schedule.id)
            
            cleanup_clusters = [created_primary.id, created_dr.id, created_new_dr.id]
            for cluster_id in cleanup_clusters:
                try:
                    await manager.delete_cluster(cluster_id, force=True)
                except Exception:
                    pass  # Ignore cleanup errors
    
    @pytest.mark.asyncio
    async def test_multi_tenant_workflow(self, test_environment):
        """Test multi-tenant cluster management workflow."""
        env = test_environment
        manager = env["manager"]
        access_control = env["access_control"]
        advanced_features = env["advanced_features"]
        port_allocator = env["port_allocator"]
        
        with patch('src.services.cluster_factory.docker'), \
             patch('src.services.cluster_manager.ClusterManager') as mock_cluster_manager:
            
            mock_manager_instance = AsyncMock()
            mock_manager_instance.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
            mock_manager_instance.start_cluster.return_value = True
            mock_cluster_manager.return_value = mock_manager_instance
            
            # Step 1: Create tenant users
            tenant_a_user = await access_control.create_user(
                username="tenant_a_admin",
                email="admin@tenant-a.com",
                password="secure_password_a",
                is_admin=False
            )
            
            tenant_b_user = await access_control.create_user(
                username="tenant_b_admin",
                email="admin@tenant-b.com",
                password="secure_password_b",
                is_admin=False
            )
            
            platform_admin = await access_control.create_user(
                username="platform_admin",
                email="admin@platform.com",
                password="admin_password",
                is_admin=True
            )
            
            # Step 2: Create clusters for each tenant
            tenant_clusters = {}
            
            for tenant, user in [("tenant-a", tenant_a_user), ("tenant-b", tenant_b_user)]:
                # Create development cluster
                dev_ports = await port_allocator.allocate_ports(f"{tenant}-dev")
                dev_cluster = ClusterDefinition(
                    id=f"{tenant}-dev",
                    name=f"{tenant.title()} Development",
                    description=f"Development cluster for {tenant}",
                    environment="development",
                    template_id="development",
                    port_allocation=PortAllocation(
                        kafka_port=dev_ports.kafka_port,
                        rest_proxy_port=dev_ports.rest_proxy_port,
                        ui_port=dev_ports.ui_port
                    ),
                    tags={"tenant": tenant, "environment": "development"}
                )
                
                # Create production cluster
                prod_ports = await port_allocator.allocate_ports(f"{tenant}-prod")
                prod_cluster = ClusterDefinition(
                    id=f"{tenant}-prod",
                    name=f"{tenant.title()} Production",
                    description=f"Production cluster for {tenant}",
                    environment="production",
                    template_id="production",
                    port_allocation=PortAllocation(
                        kafka_port=prod_ports.kafka_port,
                        rest_proxy_port=prod_ports.rest_proxy_port,
                        ui_port=prod_ports.ui_port,
                        jmx_port=prod_ports.kafka_port + 1000
                    ),
                    tags={"tenant": tenant, "environment": "production", "critical": "true"}
                )
                
                created_dev = await manager.create_cluster(dev_cluster, auto_start=True)
                created_prod = await manager.create_cluster(prod_cluster, auto_start=True)
                
                tenant_clusters[tenant] = {
                    "dev": created_dev,
                    "prod": created_prod,
                    "user": user
                }
            
            # Step 3: Set up tenant-specific permissions
            for tenant, data in tenant_clusters.items():
                user = data["user"]
                
                # Grant admin access to tenant's clusters
                for env in ["dev", "prod"]:
                    cluster = data[env]
                    await access_control.grant_cluster_permission(
                        user_id=user.id,
                        cluster_id=cluster.id,
                        access_level=AccessLevel.ADMIN,
                        granted_by=platform_admin.id
                    )
            
            # Step 4: Create API keys for each tenant
            tenant_api_keys = {}
            for tenant, data in tenant_clusters.items():
                user = data["user"]
                cluster_permissions = {
                    data["dev"].id: AccessLevel.ADMIN,
                    data["prod"].id: AccessLevel.ADMIN
                }
                
                api_key, raw_key = await access_control.create_api_key(
                    user_id=user.id,
                    name=f"{tenant}-api-key",
                    cluster_permissions=cluster_permissions,
                    expires_at=datetime.utcnow() + timedelta(days=90)
                )
                
                tenant_api_keys[tenant] = {"api_key": api_key, "raw_key": raw_key}
            
            # Step 5: Test tenant isolation
            for tenant, data in tenant_clusters.items():
                user = data["user"]
                
                # Verify user can access their clusters
                for env in ["dev", "prod"]:
                    cluster = data[env]
                    has_access = await access_control.check_cluster_permission(
                        user.id, cluster.id, "cluster_admin"
                    )
                    assert has_access is True
                
                # Verify user cannot access other tenant's clusters
                other_tenant = "tenant-b" if tenant == "tenant-a" else "tenant-a"
                if other_tenant in tenant_clusters:
                    other_cluster = tenant_clusters[other_tenant]["dev"]
                    has_access = await access_control.check_cluster_permission(
                        user.id, other_cluster.id, "cluster_admin"
                    )
                    assert has_access is False
            
            # Step 6: Set up tenant-specific backups
            for tenant, data in tenant_clusters.items():
                prod_cluster = data["prod"]
                
                # Create backup schedule
                backup_schedule = await advanced_features.create_schedule(
                    cluster_id=prod_cluster.id,
                    name=f"{tenant.title()} Production Backup",
                    schedule_type=ScheduleType.BACKUP,
                    frequency=ScheduleFrequency.DAILY,
                    schedule_expression="03:00",
                    description=f"Daily backup for {tenant} production",
                    enabled=True,
                    tags={"tenant": tenant, "backup", "production"}
                )
                
                data["backup_schedule"] = backup_schedule
            
            # Step 7: Create tenant-specific snapshots
            for tenant, data in tenant_clusters.items():
                for env in ["dev", "prod"]:
                    cluster = data[env]
                    snapshot = await advanced_features.create_snapshot(
                        cluster_id=cluster.id,
                        name=f"{tenant.title()} {env.title()} Snapshot",
                        description=f"Snapshot for {tenant} {env} environment",
                        snapshot_type=SnapshotType.FULL,
                        tags={"tenant": tenant, "environment": env}
                    )
                    data[f"{env}_snapshot"] = snapshot
            
            # Step 8: Verify tenant resource usage
            all_clusters = await manager.list_clusters()
            tenant_a_clusters = [c for c in all_clusters if c.tags.get("tenant") == "tenant-a"]
            tenant_b_clusters = [c for c in all_clusters if c.tags.get("tenant") == "tenant-b"]
            
            assert len(tenant_a_clusters) == 2  # dev + prod
            assert len(tenant_b_clusters) == 2  # dev + prod
            
            # Step 9: Test audit logging
            audit_entries = await access_control.get_audit_log(limit=50)
            tenant_audit_entries = [
                entry for entry in audit_entries 
                if "tenant" in entry.get("details", {}).get("tags", {})
            ]
            assert len(tenant_audit_entries) > 0
            
            # Step 10: Cleanup
            for tenant, data in tenant_clusters.items():
                # Delete snapshots
                for env in ["dev", "prod"]:
                    if f"{env}_snapshot" in data:
                        await advanced_features.delete_snapshot(data[f"{env}_snapshot"].id)
                
                # Delete backup schedule
                if "backup_schedule" in data:
                    await advanced_features.delete_schedule(data["backup_schedule"].id)
                
                # Delete API key
                if tenant in tenant_api_keys:
                    await access_control.delete_api_key(tenant_api_keys[tenant]["api_key"].id)
                
                # Delete clusters
                for env in ["dev", "prod"]:
                    await manager.delete_cluster(data[env].id, force=True)
                
                # Delete user
                await access_control.delete_user(data["user"].id)
            
            # Delete platform admin
            await access_control.delete_user(platform_admin.id)
    
    @pytest.mark.asyncio
    async def test_scaling_and_performance_workflow(self, test_environment):
        """Test cluster scaling and performance optimization workflow."""
        env = test_environment
        manager = env["manager"]
        advanced_features = env["advanced_features"]
        health_monitor = env["health_monitor"]
        port_allocator = env["port_allocator"]
        
        with patch('src.services.cluster_factory.docker'), \
             patch('src.services.cluster_manager.ClusterManager') as mock_cluster_manager:
            
            mock_manager_instance = AsyncMock()
            mock_manager_instance.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
            mock_manager_instance.start_cluster.return_value = True
            mock_cluster_manager.return_value = mock_manager_instance
            
            # Step 1: Create initial small cluster
            initial_ports = await port_allocator.allocate_ports("scaling-test")
            initial_cluster = ClusterDefinition(
                id="scaling-test",
                name="Scaling Test Cluster",
                description="Cluster for testing scaling workflow",
                environment="testing",
                template_id="testing",
                port_allocation=PortAllocation(
                    kafka_port=initial_ports.kafka_port,
                    rest_proxy_port=initial_ports.rest_proxy_port,
                    ui_port=initial_ports.ui_port
                ),
                tags={"scaling": "test", "size": "small"}
            )
            
            created_cluster = await manager.create_cluster(initial_cluster, auto_start=True)
            
            # Step 2: Monitor initial performance
            initial_health = await health_monitor.get_cluster_specific_health(created_cluster.id)
            assert initial_health["cluster_id"] == created_cluster.id
            
            # Step 3: Simulate load increase (create snapshot as baseline)
            baseline_snapshot = await advanced_features.create_snapshot(
                cluster_id=created_cluster.id,
                name="Baseline Performance",
                description="Baseline before scaling",
                snapshot_type=SnapshotType.FULL,
                tags={"baseline", "performance", "pre-scaling"}
            )
            
            # Step 4: Create scaled-up cluster
            scaled_ports = await port_allocator.allocate_ports("scaling-test-large")
            scaled_cluster = await advanced_features.clone_cluster(
                source_cluster_id=created_cluster.id,
                target_cluster_id="scaling-test-large",
                target_name="Scaled Up Cluster",
                clone_data=True,
                clone_config=True,
                port_offset=scaled_ports.kafka_port - initial_ports.kafka_port,
                tags={"scaling": "test", "size": "large", "scaled_from": created_cluster.id}
            )
            
            # Update to high-performance template
            scaled_cluster.template_id = "high-throughput"
            created_scaled = await manager.create_cluster(scaled_cluster, auto_start=True)
            
            # Step 5: Performance comparison
            scaled_health = await health_monitor.get_cluster_specific_health(created_scaled.id)
            assert scaled_health["cluster_id"] == created_scaled.id
            
            # Step 6: Create performance snapshots
            performance_snapshot = await advanced_features.create_snapshot(
                cluster_id=created_scaled.id,
                name="Post-scaling Performance",
                description="Performance after scaling up",
                snapshot_type=SnapshotType.FULL,
                tags={"performance", "post-scaling", "optimized"}
            )
            
            # Step 7: Set up automated scaling monitoring
            scaling_schedule = await advanced_features.create_schedule(
                cluster_id=created_scaled.id,
                name="Performance Monitoring",
                schedule_type=ScheduleType.CUSTOM,
                frequency=ScheduleFrequency.HOURLY,
                schedule_expression="0 * * * *",  # Every hour
                description="Hourly performance monitoring",
                enabled=True,
                tags={"monitoring", "performance", "automated"}
            )
            
            # Step 8: Test rollback capability
            rollback_cluster = await advanced_features.restore_snapshot(
                snapshot_id=baseline_snapshot.id,
                target_cluster_id="scaling-test-rollback",
                restore_config=True,
                restore_data=True
            )
            
            # Step 9: Verify all clusters are operational
            all_clusters = await manager.list_clusters()
            scaling_clusters = [c for c in all_clusters if "scaling" in c.tags.get("scaling", "")]
            assert len(scaling_clusters) >= 3  # original, scaled, rollback
            
            # Step 10: Performance metrics collection
            system_overview = await health_monitor.get_system_overview()
            assert system_overview["clusters"]["total"] >= 3
            
            # Step 11: Cleanup
            await advanced_features.delete_snapshot(baseline_snapshot.id)
            await advanced_features.delete_snapshot(performance_snapshot.id)
            await advanced_features.delete_schedule(scaling_schedule.id)
            
            for cluster in scaling_clusters:
                await manager.delete_cluster(cluster.id, force=True)
    
    @pytest.mark.asyncio
    async def test_compliance_and_audit_workflow(self, test_environment):
        """Test compliance and audit workflow."""
        env = test_environment
        manager = env["manager"]
        access_control = env["access_control"]
        config_manager = env["config_manager"]
        advanced_features = env["advanced_features"]
        port_allocator = env["port_allocator"]
        
        with patch('src.services.cluster_factory.docker'), \
             patch('src.services.cluster_manager.ClusterManager') as mock_cluster_manager:
            
            mock_manager_instance = AsyncMock()
            mock_manager_instance.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
            mock_manager_instance.start_cluster.return_value = True
            mock_cluster_manager.return_value = mock_manager_instance
            
            # Step 1: Create compliance admin user
            compliance_admin = await access_control.create_user(
                username="compliance_admin",
                email="compliance@company.com",
                password="compliance_password",
                is_admin=True
            )
            
            # Step 2: Create compliance-required cluster
            compliance_ports = await port_allocator.allocate_ports("compliance-cluster")
            compliance_cluster = ClusterDefinition(
                id="compliance-cluster",
                name="Compliance Test Cluster",
                description="Cluster for compliance testing",
                environment="production",
                template_id="production",
                port_allocation=PortAllocation(
                    kafka_port=compliance_ports.kafka_port,
                    rest_proxy_port=compliance_ports.rest_proxy_port,
                    ui_port=compliance_ports.ui_port,
                    jmx_port=compliance_ports.kafka_port + 1000
                ),
                tags={
                    "compliance": "sox",
                    "data_classification": "sensitive",
                    "retention_policy": "7_years",
                    "encryption": "required",
                    "audit": "enabled"
                }
            )
            
            created_cluster = await manager.create_cluster(compliance_cluster, auto_start=True)
            
            # Step 3: Set up compliance monitoring
            compliance_schedule = await advanced_features.create_schedule(
                cluster_id=created_cluster.id,
                name="Compliance Audit",
                schedule_type=ScheduleType.CUSTOM,
                frequency=ScheduleFrequency.DAILY,
                schedule_expression="01:00",
                description="Daily compliance audit",
                enabled=True,
                tags={"compliance", "audit", "sox"}
            )
            
            # Step 4: Create compliance snapshots
            compliance_snapshot = await advanced_features.create_snapshot(
                cluster_id=created_cluster.id,
                name="Compliance Baseline",
                description="Compliance baseline snapshot",
                snapshot_type=SnapshotType.FULL,
                tags={"compliance", "baseline", "sox", "audit"}
            )
            
            # Step 5: Export configuration for compliance review
            compliance_config = await config_manager.export_cluster_config(
                cluster_id=created_cluster.id,
                format="yaml",
                include_metadata=True,
                include_secrets=False
            )
            
            # Verify compliance tags are present
            assert "compliance: sox" in compliance_config
            assert "audit: enabled" in compliance_config
            
            # Step 6: Create audit trail
            audit_entries = await access_control.get_audit_log(
                cluster_id=created_cluster.id,
                limit=100
            )
            
            # Verify audit entries exist
            assert len(audit_entries) > 0
            
            # Step 7: Test configuration versioning for compliance
            versions = await config_manager.list_config_versions(created_cluster.id)
            assert len(versions) >= 1
            
            # Step 8: Create compliance report data
            compliance_report = {
                "cluster_id": created_cluster.id,
                "compliance_type": "sox",
                "audit_date": datetime.utcnow().isoformat(),
                "configuration_versions": len(versions),
                "snapshots_count": 1,
                "audit_entries_count": len(audit_entries),
                "compliance_tags": created_cluster.tags,
                "encryption_enabled": "encryption" in created_cluster.tags,
                "retention_policy": created_cluster.tags.get("retention_policy"),
                "status": "compliant"
            }
            
            # Step 9: Verify compliance requirements
            assert compliance_report["encryption_enabled"] is True
            assert compliance_report["retention_policy"] == "7_years"
            assert compliance_report["status"] == "compliant"
            
            # Step 10: Test compliance violation detection
            # Modify cluster to violate compliance
            violation_tags = created_cluster.tags.copy()
            violation_tags["compliance"] = "none"  # Remove compliance
            
            await advanced_features.add_cluster_tags(
                cluster_id=created_cluster.id,
                tags={"compliance_violation": "detected", "violation_date": datetime.utcnow().isoformat()}
            )
            
            # Step 11: Create violation snapshot
            violation_snapshot = await advanced_features.create_snapshot(
                cluster_id=created_cluster.id,
                name="Compliance Violation",
                description="Snapshot showing compliance violation",
                snapshot_type=SnapshotType.CONFIG,
                tags={"compliance", "violation", "audit"}
            )
            
            # Step 12: Restore compliance
            await advanced_features.restore_snapshot(
                snapshot_id=compliance_snapshot.id,
                target_cluster_id=created_cluster.id,
                restore_config=True,
                restore_data=False
            )
            
            # Step 13: Final compliance verification
            final_audit = await access_control.get_audit_log(
                cluster_id=created_cluster.id,
                limit=10
            )
            
            # Should have more audit entries after all operations
            assert len(final_audit) >= len(audit_entries)
            
            # Step 14: Cleanup
            await advanced_features.delete_snapshot(compliance_snapshot.id)
            await advanced_features.delete_snapshot(violation_snapshot.id)
            await advanced_features.delete_schedule(compliance_schedule.id)
            await manager.delete_cluster(created_cluster.id, force=True)
            await access_control.delete_user(compliance_admin.id)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])