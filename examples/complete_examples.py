#!/usr/bin/env python3
"""
Multi-Cluster Kafka Manager - Complete Examples

This file contains comprehensive examples demonstrating all major features
of the Multi-Cluster Kafka Manager system.
"""

import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any

# Import all necessary modules
from src.services.multi_cluster_manager import MultiClusterManager
from src.services.advanced_cluster_features import AdvancedClusterFeatures
from src.services.cross_cluster_operations import CrossClusterOperations
from src.services.resource_manager import ResourceManager
from src.services.configuration_manager import ConfigurationManager
from src.monitoring.enhanced_health_monitor import EnhancedHealthMonitor
from src.security.access_control import AccessControl
from src.models.multi_cluster import (
    ClusterDefinition, PortAllocation, SnapshotType, 
    ScheduleType, ScheduleFrequency, AccessLevel
)
from src.models.resource_management import ResourceQuota, ResourceType, CleanupPolicy


class MultiClusterExamples:
    """Complete examples for Multi-Cluster Kafka Manager."""
    
    def __init__(self):
        self.manager = MultiClusterManager()
        self.advanced_features = AdvancedClusterFeatures()
        self.cross_cluster_ops = CrossClusterOperations()
        self.resource_manager = ResourceManager()
        self.config_manager = ConfigurationManager()
        self.monitor = EnhancedHealthMonitor()
        self.access_control = AccessControl()
    
    async def example_1_development_environment_setup(self):
        """
        Example 1: Set up a complete development environment with multiple clusters.
        """
        print("=== Example 1: Development Environment Setup ===")
        
        # Create development clusters for different services
        services = [
            ("user-service", 9092),
            ("order-service", 9093),
            ("notification-service", 9094),
            ("payment-service", 9095)
        ]
        
        clusters = []
        for service_name, kafka_port in services:
            cluster = ClusterDefinition(
                id=f"{service_name}-dev",
                name=f"{service_name.title()} Development",
                description=f"Development cluster for {service_name}",
                environment="development",
                template_id="development",
                port_allocation=PortAllocation(
                    kafka_port=kafka_port,
                    rest_proxy_port=kafka_port + 1000,
                    ui_port=kafka_port + 2000
                ),
                tags={
                    "environment": "development",
                    "service": service_name,
                    "team": "backend",
                    "auto-cleanup": "7days"
                }
            )
            
            result = await self.manager.create_cluster(cluster)
            clusters.append(result)
            print(f"✓ Created cluster: {result.id}")
        
        # Start all clusters
        for cluster in clusters:
            await self.manager.start_cluster(cluster.id)
            print(f"✓ Started cluster: {cluster.id}")
        
        # Set up development-specific configurations
        for cluster in clusters:
            # Configure for development (shorter retention, smaller segments)
            dev_config = {
                "kafka_config": {
                    "log.retention.hours": "24",  # 1 day retention
                    "log.segment.bytes": "104857600",  # 100MB segments
                    "log.cleanup.policy": "delete",
                    "auto.create.topics.enable": "true"
                }
            }
            await self.manager.update_cluster_config(cluster.id, dev_config)
            print(f"✓ Applied dev config to: {cluster.id}")
        
        print(f"✅ Development environment ready with {len(clusters)} clusters!")
        return clusters
    
    async def example_2_production_deployment_with_monitoring(self):
        """
        Example 2: Deploy production clusters with comprehensive monitoring and security.
        """
        print("\n=== Example 2: Production Deployment with Monitoring ===")
        
        # Create production cluster
        prod_cluster = ClusterDefinition(
            id="user-service-prod",
            name="User Service Production",
            description="Production cluster for user service",
            environment="production",
            template_id="production",
            port_allocation=PortAllocation(
                kafka_port=9092,
                rest_proxy_port=8082,
                ui_port=8080,
                jmx_port=9999
            ),
            tags={
                "environment": "production",
                "service": "user-service",
                "team": "backend",
                "critical": "true",
                "backup": "daily",
                "monitoring": "enabled",
                "compliance": "sox"
            }
        )
        
        # Create cluster
        cluster_result = await self.manager.create_cluster(prod_cluster)
        print(f"✓ Created production cluster: {cluster_result.id}")
        
        # Start cluster
        await self.manager.start_cluster(cluster_result.id)
        print("✓ Started production cluster")
        
        # Apply production configuration
        prod_config = {
            "kafka_config": {
                "log.retention.hours": "168",  # 7 days
                "log.segment.bytes": "1073741824",  # 1GB segments
                "log.cleanup.policy": "delete",
                "auto.create.topics.enable": "false",
                "min.insync.replicas": "2",
                "default.replication.factor": "3"
            }
        }
        await self.manager.update_cluster_config(cluster_result.id, prod_config)
        print("✓ Applied production configuration")
        
        # Set up daily backups
        backup_schedule = await self.advanced_features.create_schedule(
            cluster_id=cluster_result.id,
            name="Daily Production Backup",
            schedule_type=ScheduleType.BACKUP,
            frequency=ScheduleFrequency.DAILY,
            schedule_expression="02:00",
            description="Automated daily backup at 2 AM",
            enabled=True,
            tags={"backup", "automated", "production"}
        )
        print(f"✓ Created backup schedule: {backup_schedule.id}")
        
        # Create production users
        prod_operator = await self.access_control.create_user(
            username="prod-operator",
            email="prod-ops@company.com",
            password="secure_prod_password",
            is_admin=False
        )
        
        prod_admin = await self.access_control.create_user(
            username="prod-admin",
            email="prod-admin@company.com",
            password="admin_prod_password",
            is_admin=True
        )
        
        # Grant appropriate permissions
        await self.access_control.grant_cluster_permission(
            user_id=prod_operator.id,
            cluster_id=cluster_result.id,
            access_level=AccessLevel.READ,
            granted_by=prod_admin.id
        )
        
        await self.access_control.grant_cluster_permission(
            user_id=prod_admin.id,
            cluster_id=cluster_result.id,
            access_level=AccessLevel.ADMIN,
            granted_by=prod_admin.id
        )
        print("✓ Created users and set permissions")
        
        # Set up monitoring alerts
        def critical_alert_handler(alert):
            print(f"🚨 CRITICAL ALERT: {alert.name} - {alert.message}")
            # In real implementation, send to PagerDuty, Slack, etc.
        
        def warning_alert_handler(alert):
            print(f"⚠️  WARNING: {alert.name} - {alert.message}")
            # In real implementation, send to monitoring dashboard
        
        self.monitor.add_alert_handler(critical_alert_handler)
        self.monitor.add_alert_handler(warning_alert_handler)
        print("✓ Configured alert handlers")
        
        # Create initial snapshot
        snapshot = await self.advanced_features.create_snapshot(
            cluster_id=cluster_result.id,
            name="Initial Production Snapshot",
            description="Baseline snapshot after deployment",
            snapshot_type=SnapshotType.FULL,
            tags={"baseline", "deployment", "production"}
        )
        print(f"✓ Created initial snapshot: {snapshot.id}")
        
        # Create API key for monitoring systems
        monitoring_api_key, raw_key = await self.access_control.create_api_key(
            user_id=prod_operator.id,
            name="monitoring-system",
            cluster_permissions={
                cluster_result.id: AccessLevel.READ
            },
            expires_at=datetime.now() + timedelta(days=365)
        )
        print(f"✓ Created monitoring API key: {monitoring_api_key.id}")
        
        print("✅ Production environment deployed successfully!")
        return cluster_result, snapshot, monitoring_api_key
    
    async def example_3_disaster_recovery_setup(self):
        """
        Example 3: Set up disaster recovery with cross-cluster replication.
        """
        print("\n=== Example 3: Disaster Recovery Setup ===")
        
        # Assume primary cluster exists
        primary_cluster_id = "user-service-prod"
        
        # Create disaster recovery cluster
        dr_cluster = ClusterDefinition(
            id="user-service-dr",
            name="User Service Disaster Recovery",
            description="Disaster recovery cluster for user service",
            environment="production",
            template_id="production",
            port_allocation=PortAllocation(
                kafka_port=9096,
                rest_proxy_port=8086,
                ui_port=8084,
                jmx_port=9998
            ),
            tags={
                "environment": "production",
                "service": "user-service",
                "purpose": "disaster-recovery",
                "critical": "true",
                "backup": "continuous"
            }
        )
        
        # Create DR cluster
        dr_result = await self.manager.create_cluster(dr_cluster)
        await self.manager.start_cluster(dr_result.id)
        print(f"✓ Created DR cluster: {dr_result.id}")
        
        # Set up replication from primary to DR
        replication = await self.cross_cluster_ops.setup_replication(
            source_cluster_id=primary_cluster_id,
            target_cluster_ids=[dr_result.id],
            topics=["user-events", "user-updates", "user-deletions", "user-sessions"],
            replication_mode="async"  # Async for DR
        )
        print(f"✓ Set up replication: {replication.id}")
        
        # Schedule regular DR testing
        dr_test_schedule = await self.advanced_features.create_schedule(
            cluster_id=dr_result.id,
            name="DR Test",
            schedule_type=ScheduleType.CUSTOM,
            frequency=ScheduleFrequency.WEEKLY,
            schedule_expression="SUN 03:00",
            description="Weekly disaster recovery test",
            enabled=True,
            tags={"disaster-recovery", "testing", "automated"}
        )
        print(f"✓ Created DR test schedule: {dr_test_schedule.id}")
        
        # Create DR runbook snapshot
        dr_snapshot = await self.advanced_features.create_snapshot(
            cluster_id=dr_result.id,
            name="DR Baseline",
            description="Disaster recovery baseline configuration",
            snapshot_type=SnapshotType.CONFIG,
            tags={"disaster-recovery", "baseline", "runbook"}
        )
        print(f"✓ Created DR baseline snapshot: {dr_snapshot.id}")
        
        print("✅ Disaster recovery setup complete!")
        return dr_result, replication, dr_test_schedule
    
    async def example_4_cicd_pipeline(self):
        """
        Example 4: Automated CI/CD pipeline with multiple environments.
        """
        print("\n=== Example 4: Multi-Environment CI/CD Pipeline ===")
        
        # Development -> Staging -> Production pipeline
        environments = ["development", "staging", "production"]
        base_ports = {"development": 9100, "staging": 9103, "production": 9106}
        
        clusters = {}
        
        # Create clusters for each environment
        for env in environments:
            cluster = ClusterDefinition(
                id=f"payment-service-{env}",
                name=f"Payment Service {env.title()}",
                description=f"{env.title()} cluster for payment service",
                environment=env,
                template_id=env,
                port_allocation=PortAllocation(
                    kafka_port=base_ports[env],
                    rest_proxy_port=base_ports[env] + 1000,
                    ui_port=base_ports[env] + 2000
                ),
                tags={
                    "environment": env,
                    "service": "payment-service",
                    "pipeline": "cicd",
                    "team": "payments"
                }
            )
            
            result = await self.manager.create_cluster(cluster)
            await self.manager.start_cluster(result.id)
            clusters[env] = result
            print(f"✓ Created {env} cluster: {result.id}")
        
        # Simulate CI/CD pipeline stages
        async def deploy_to_environment(env: str, version: str):
            cluster_id = clusters[env].id
            
            # Create snapshot before deployment
            pre_deploy_snapshot = await self.advanced_features.create_snapshot(
                cluster_id=cluster_id,
                name=f"Pre-deploy {version}",
                description=f"Snapshot before deploying version {version}",
                snapshot_type=SnapshotType.FULL,
                tags={"pre-deploy", version, env}
            )
            print(f"  ✓ Created pre-deployment snapshot for {env}")
            
            # Simulate deployment (in real scenario, this would update application)
            await asyncio.sleep(1)
            
            # Add deployment tags
            await self.advanced_features.add_cluster_tags(
                cluster_id=cluster_id,
                tags={
                    "version": version,
                    "last-deployed": datetime.now().isoformat(),
                    "deployment-status": "success"
                }
            )
            print(f"  ✓ Deployed version {version} to {env}")
            
            # Create post-deployment snapshot
            post_deploy_snapshot = await self.advanced_features.create_snapshot(
                cluster_id=cluster_id,
                name=f"Post-deploy {version}",
                description=f"Snapshot after deploying version {version}",
                snapshot_type=SnapshotType.FULL,
                tags={"post-deploy", version, env}
            )
            print(f"  ✓ Created post-deployment snapshot for {env}")
            
            return pre_deploy_snapshot, post_deploy_snapshot
        
        # Run deployment pipeline
        version = "v2.1.0"
        
        # Deploy to development
        print(f"🚀 Deploying {version} to development...")
        await deploy_to_environment("development", version)
        
        # Run tests (simulated)
        print("🧪 Running development tests...")
        await asyncio.sleep(2)
        print("✅ Development tests passed!")
        
        # Deploy to staging
        print(f"🚀 Deploying {version} to staging...")
        await deploy_to_environment("staging", version)
        
        # Run integration tests (simulated)
        print("🧪 Running staging integration tests...")
        await asyncio.sleep(3)
        print("✅ Staging tests passed!")
        
        # Deploy to production
        print(f"🚀 Deploying {version} to production...")
        await deploy_to_environment("production", version)
        
        print(f"✅ Successfully deployed {version} through all environments!")
        return clusters
    
    async def example_5_resource_optimization_and_cleanup(self):
        """
        Example 5: Automated resource optimization and cleanup.
        """
        print("\n=== Example 5: Resource Optimization and Cleanup ===")
        
        # Get current resource usage
        system_usage = await self.resource_manager.get_system_resource_usage()
        print(f"📊 Current CPU usage: {system_usage['cpu'].current_usage}%")
        print(f"📊 Current memory usage: {system_usage['memory'].current_usage}%")
        print(f"📊 Current disk usage: {system_usage['disk'].current_usage}%")
        
        # Get scaling recommendations
        recommendations = await self.resource_manager.get_scaling_recommendations()
        
        if recommendations:
            print("\n💡 Scaling Recommendations:")
            for rec in recommendations:
                print(f"  - Cluster: {rec.cluster_id}")
                print(f"    Recommendation: {rec.recommendation.value}")
                print(f"    Reason: {rec.reason}")
                print(f"    Confidence: {rec.confidence:.2f}")
        else:
            print("✅ No scaling recommendations at this time")
        
        # Set up resource quotas
        quotas = [
            ResourceQuota(
                resource_type=ResourceType.CPU,
                soft_limit=70.0,
                hard_limit=85.0,
                unit="percent"
            ),
            ResourceQuota(
                resource_type=ResourceType.MEMORY,
                soft_limit=75.0,
                hard_limit=90.0,
                unit="percent"
            ),
            ResourceQuota(
                resource_type=ResourceType.DISK,
                soft_limit=80.0,
                hard_limit=95.0,
                unit="percent"
            )
        ]
        
        for quota in quotas:
            self.resource_manager.set_resource_quota(quota)
            print(f"✓ Set {quota.resource_type.value} quota: {quota.soft_limit}%/{quota.hard_limit}%")
        
        # Create cleanup policies
        cleanup_policies = [
            CleanupPolicy(
                name="Old Development Clusters",
                enabled=True,
                max_age_days=7,
                target_patterns=["*-dev", "*-test"],
                cluster_states=["stopped"]
            ),
            CleanupPolicy(
                name="Temporary Clusters",
                enabled=True,
                max_age_days=1,
                target_patterns=["temp-*", "tmp-*"],
                cluster_states=["stopped", "running"]
            ),
            CleanupPolicy(
                name="Old Snapshots",
                enabled=True,
                max_age_days=30,
                target_patterns=["*"],
                resource_types=["snapshots"]
            )
        ]
        
        for policy in cleanup_policies:
            self.resource_manager.add_cleanup_policy(policy)
            print(f"✓ Added cleanup policy: {policy.name}")
        
        # Run optimization
        optimization = await self.resource_manager.optimize_resource_allocation()
        
        print(f"\n🔧 Optimization Results:")
        print(f"  System optimizations: {len(optimization['system_optimization'])}")
        print(f"  Cluster optimizations: {len(optimization['cluster_optimizations'])}")
        print(f"  Potential savings: {optimization['potential_savings']}")
        
        # Run cleanup (dry run first)
        print("\n🧹 Running cleanup (dry run)...")
        dry_run_results = await self.resource_manager.run_cleanup(dry_run=True)
        
        print(f"  Would clean {len(dry_run_results['clusters_to_clean'])} clusters")
        print(f"  Would free {dry_run_results['space_to_free_bytes'] / 1_000_000_000:.1f} GB")
        
        # Confirm and run actual cleanup if there's something to clean
        if len(dry_run_results['clusters_to_clean']) > 0:
            print("\n🧹 Running actual cleanup...")
            cleanup_results = await self.resource_manager.run_cleanup(dry_run=False)
            
            print(f"✅ Cleaned {len(cleanup_results['clusters_cleaned'])} clusters")
            print(f"✅ Freed {cleanup_results['space_freed_bytes'] / 1_000_000_000:.1f} GB")
        else:
            print("✅ No cleanup needed at this time")
        
        print("✅ Resource optimization complete!")
    
    async def example_6_configuration_management(self):
        """
        Example 6: Advanced configuration management with versioning.
        """
        print("\n=== Example 6: Configuration Management ===")
        
        # Create a test cluster for configuration management
        config_cluster = ClusterDefinition(
            id="config-test-cluster",
            name="Configuration Test Cluster",
            description="Cluster for testing configuration management",
            environment="testing",
            template_id="testing",
            port_allocation=PortAllocation(
                kafka_port=9110,
                rest_proxy_port=8110,
                ui_port=8210
            ),
            tags={"purpose": "config-testing", "temporary": "true"}
        )
        
        cluster_result = await self.manager.create_cluster(config_cluster)
        await self.manager.start_cluster(cluster_result.id)
        print(f"✓ Created test cluster: {cluster_result.id}")
        
        # Export initial configuration
        initial_config = await self.config_manager.export_cluster_config(
            cluster_id=cluster_result.id,
            format="yaml",
            include_metadata=True,
            include_secrets=False
        )
        print("✓ Exported initial configuration")
        
        # Save configuration to file
        with open(f"config_backup_{cluster_result.id}_v1.yaml", "w") as f:
            f.write(initial_config)
        print("✓ Saved configuration to file")
        
        # Update cluster configuration
        updated_config = {
            "kafka_config": {
                "log.retention.hours": "48",
                "log.segment.bytes": "536870912",  # 512MB
                "num.partitions": "6"
            },
            "description": "Updated configuration for testing"
        }
        
        await self.manager.update_cluster_config(cluster_result.id, updated_config)
        print("✓ Updated cluster configuration")
        
        # Export updated configuration
        updated_config_export = await self.config_manager.export_cluster_config(
            cluster_id=cluster_result.id,
            format="yaml",
            include_metadata=True,
            include_secrets=False
        )
        
        # Save updated configuration
        with open(f"config_backup_{cluster_result.id}_v2.yaml", "w") as f:
            f.write(updated_config_export)
        print("✓ Exported and saved updated configuration")
        
        # List configuration versions
        versions = await self.config_manager.list_config_versions(cluster_result.id)
        print(f"✓ Found {len(versions)} configuration versions")
        
        # Compare versions if we have multiple
        if len(versions) >= 2:
            comparison = await self.config_manager.compare_versions(
                cluster_id=cluster_result.id,
                version_1=versions[0].version_id,
                version_2=versions[1].version_id
            )
            print(f"✓ Compared versions - found {len(comparison['differences'])} differences")
        
        # Demonstrate rollback (rollback to previous version)
        if len(versions) >= 2:
            rollback_result = await self.config_manager.rollback_to_version(
                cluster_id=cluster_result.id,
                version_id=versions[0].version_id
            )
            print(f"✓ Rolled back to version: {rollback_result.version_id}")
        
        # Clean up test cluster
        await self.manager.stop_cluster(cluster_result.id)
        await self.manager.delete_cluster(cluster_result.id)
        print("✓ Cleaned up test cluster")
        
        print("✅ Configuration management example complete!")
    
    async def example_7_comprehensive_monitoring(self):
        """
        Example 7: Comprehensive monitoring and alerting setup.
        """
        print("\n=== Example 7: Comprehensive Monitoring ===")
        
        # Get system overview
        overview = await self.monitor.get_system_overview()
        
        print(f"📊 System Overview:")
        print(f"  Total Clusters: {overview['clusters']['total']}")
        print(f"  Running Clusters: {overview['clusters']['by_status'].get('running', 0)}")
        print(f"  System CPU: {overview['system_resources']['cpu']['usage_percent']}%")
        print(f"  Active Alerts: {overview['alerts']['total_active']}")
        
        # Get health for all clusters
        all_clusters = await self.manager.list_clusters()
        
        for cluster in all_clusters:
            health = await self.monitor.get_cluster_specific_health(cluster.id)
            status = health['health']['overall_status']
            component_count = len(health['health']['components'])
            alert_count = len(health['alerts'])
            
            status_emoji = "✅" if status == "healthy" else "⚠️" if status == "warning" else "🚨"
            print(f"  {status_emoji} {cluster.id}: {status} ({component_count} components, {alert_count} alerts)")
        
        # Set up custom alert handlers
        alert_log = []
        
        def email_alert_handler(alert):
            alert_log.append(f"EMAIL: {alert.name} - {alert.message}")
            print(f"📧 Email alert: {alert.name}")
        
        def slack_alert_handler(alert):
            alert_log.append(f"SLACK: {alert.name} - {alert.message}")
            print(f"💬 Slack alert: {alert.name}")
        
        def pagerduty_alert_handler(alert):
            alert_log.append(f"PAGERDUTY: {alert.name} - {alert.message}")
            print(f"📟 PagerDuty alert: {alert.name}")
        
        # Add alert handlers
        self.monitor.add_alert_handler(email_alert_handler)
        self.monitor.add_alert_handler(slack_alert_handler)
        self.monitor.add_alert_handler(pagerduty_alert_handler)
        print("✓ Configured custom alert handlers")
        
        # Get metrics for analysis
        system_metrics = await self.monitor.get_system_metrics()
        print(f"✓ Collected system metrics: {len(system_metrics)} data points")
        
        # Simulate some monitoring scenarios
        print("\n🔍 Monitoring Scenarios:")
        
        # Check for resource pressure
        if overview['system_resources']['cpu']['usage_percent'] > 80:
            print("⚠️  High CPU usage detected")
        else:
            print("✅ CPU usage within normal range")
        
        if overview['system_resources']['memory']['usage_percent'] > 85:
            print("⚠️  High memory usage detected")
        else:
            print("✅ Memory usage within normal range")
        
        # Check cluster health trends
        unhealthy_clusters = [
            cluster for cluster in all_clusters 
            if (await self.monitor.get_cluster_specific_health(cluster.id))['health']['overall_status'] != 'healthy'
        ]
        
        if unhealthy_clusters:
            print(f"⚠️  {len(unhealthy_clusters)} clusters need attention")
            for cluster in unhealthy_clusters:
                print(f"    - {cluster.id}")
        else:
            print("✅ All clusters are healthy")
        
        print(f"✅ Monitoring setup complete! Alert log has {len(alert_log)} entries")
    
    async def run_all_examples(self):
        """Run all examples in sequence."""
        print("🚀 Starting Multi-Cluster Kafka Manager Examples")
        print("=" * 60)
        
        try:
            # Example 1: Development Environment
            await self.example_1_development_environment_setup()
            
            # Example 2: Production Deployment
            await self.example_2_production_deployment_with_monitoring()
            
            # Example 3: Disaster Recovery
            await self.example_3_disaster_recovery_setup()
            
            # Example 4: CI/CD Pipeline
            await self.example_4_cicd_pipeline()
            
            # Example 5: Resource Management
            await self.example_5_resource_optimization_and_cleanup()
            
            # Example 6: Configuration Management
            await self.example_6_configuration_management()
            
            # Example 7: Monitoring
            await self.example_7_comprehensive_monitoring()
            
            print("\n" + "=" * 60)
            print("🎉 All examples completed successfully!")
            print("=" * 60)
            
        except Exception as e:
            print(f"\n❌ Error running examples: {str(e)}")
            raise


async def main():
    """Main function to run examples."""
    examples = MultiClusterExamples()
    
    # You can run individual examples or all of them
    choice = input("Run all examples? (y/n): ").lower().strip()
    
    if choice == 'y':
        await examples.run_all_examples()
    else:
        print("\nAvailable examples:")
        print("1. Development Environment Setup")
        print("2. Production Deployment with Monitoring")
        print("3. Disaster Recovery Setup")
        print("4. CI/CD Pipeline")
        print("5. Resource Optimization and Cleanup")
        print("6. Configuration Management")
        print("7. Comprehensive Monitoring")
        
        example_choice = input("Choose example (1-7): ").strip()
        
        example_map = {
            "1": examples.example_1_development_environment_setup,
            "2": examples.example_2_production_deployment_with_monitoring,
            "3": examples.example_3_disaster_recovery_setup,
            "4": examples.example_4_cicd_pipeline,
            "5": examples.example_5_resource_optimization_and_cleanup,
            "6": examples.example_6_configuration_management,
            "7": examples.example_7_comprehensive_monitoring,
        }
        
        if example_choice in example_map:
            await example_map[example_choice]()
        else:
            print("Invalid choice. Please run the script again.")


if __name__ == "__main__":
    asyncio.run(main())