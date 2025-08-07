#!/usr/bin/env python3
"""
Multi-Cluster Kafka Manager - API Examples

This file contains comprehensive examples for using the REST API
of the Multi-Cluster Kafka Manager system.
"""

import requests
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional


class MultiClusterAPIExamples:
    """Complete API examples for Multi-Cluster Kafka Manager."""
    
    def __init__(self, base_url: str = "http://localhost:8000", api_key: Optional[str] = None):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        
        # Set up authentication if API key is provided
        if api_key:
            self.session.headers.update({"X-API-Key": api_key})
        
        # Set default headers
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json"
        })
    
    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Make HTTP request with error handling."""
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = self.session.request(method, url, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            print(f"❌ Request failed: {method} {url}")
            print(f"   Error: {str(e)}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"   Response: {e.response.text}")
            raise
    
    def example_1_basic_cluster_operations(self):
        """Example 1: Basic cluster CRUD operations."""
        print("=== Example 1: Basic Cluster Operations ===")
        
        # Create a new cluster
        cluster_data = {
            "id": "api-example-cluster",
            "name": "API Example Cluster",
            "description": "Cluster created via API examples",
            "environment": "development",
            "template_id": "development",
            "port_allocation": {
                "kafka_port": 9120,
                "rest_proxy_port": 8120,
                "ui_port": 8220
            },
            "tags": {
                "created_by": "api_example",
                "purpose": "demonstration",
                "team": "engineering"
            }
        }
        
        # Create cluster
        print("📝 Creating cluster...")
        response = self._make_request("POST", "/api/v1/clusters", json=cluster_data)
        created_cluster = response.json()
        cluster_id = created_cluster["id"]
        print(f"✅ Created cluster: {cluster_id}")
        
        # Get cluster details
        print("📋 Getting cluster details...")
        response = self._make_request("GET", f"/api/v1/clusters/{cluster_id}")
        cluster_details = response.json()
        print(f"✅ Retrieved cluster: {cluster_details['name']}")
        print(f"   Status: {cluster_details['status']}")
        print(f"   Environment: {cluster_details['environment']}")
        
        # List all clusters
        print("📋 Listing all clusters...")
        response = self._make_request("GET", "/api/v1/clusters")
        clusters = response.json()
        print(f"✅ Found {len(clusters)} clusters")
        
        # Start the cluster
        print("🚀 Starting cluster...")
        response = self._make_request("POST", f"/api/v1/clusters/{cluster_id}/start")
        start_result = response.json()
        print(f"✅ Start initiated: {start_result['message']}")
        
        # Wait for cluster to start
        print("⏳ Waiting for cluster to start...")
        for i in range(30):  # Wait up to 30 seconds
            response = self._make_request("GET", f"/api/v1/clusters/{cluster_id}/status")
            status = response.json()
            if status["status"] == "running":
                print("✅ Cluster is running!")
                break
            time.sleep(1)
        else:
            print("⚠️  Cluster took longer than expected to start")
        
        # Get cluster health
        print("🏥 Checking cluster health...")
        response = self._make_request("GET", f"/api/v1/clusters/{cluster_id}/health")
        health = response.json()
        print(f"✅ Health status: {health['overall_status']}")
        print(f"   Components: {len(health['components'])} checked")
        
        # Update cluster
        print("📝 Updating cluster...")
        update_data = {
            "description": "Updated via API examples",
            "tags": {
                "created_by": "api_example",
                "purpose": "demonstration",
                "team": "engineering",
                "updated": datetime.now().isoformat()
            }
        }
        response = self._make_request("PUT", f"/api/v1/clusters/{cluster_id}", json=update_data)
        updated_cluster = response.json()
        print(f"✅ Updated cluster: {updated_cluster['description']}")
        
        # Stop the cluster
        print("🛑 Stopping cluster...")
        response = self._make_request("POST", f"/api/v1/clusters/{cluster_id}/stop")
        stop_result = response.json()
        print(f"✅ Stop initiated: {stop_result['message']}")
        
        # Delete the cluster
        print("🗑️  Deleting cluster...")
        response = self._make_request("DELETE", f"/api/v1/clusters/{cluster_id}")
        delete_result = response.json()
        print(f"✅ Deleted cluster: {delete_result['message']}")
        
        print("✅ Basic cluster operations complete!")
        return cluster_id
    
    def example_2_advanced_cluster_features(self):
        """Example 2: Advanced cluster features (cloning, snapshots, scheduling)."""
        print("\n=== Example 2: Advanced Cluster Features ===")
        
        # First create a source cluster
        source_cluster_data = {
            "id": "source-cluster",
            "name": "Source Cluster",
            "description": "Source cluster for advanced operations",
            "environment": "development",
            "template_id": "development",
            "port_allocation": {
                "kafka_port": 9130,
                "rest_proxy_port": 8130,
                "ui_port": 8230
            },
            "tags": {"purpose": "source", "example": "advanced"}
        }
        
        print("📝 Creating source cluster...")
        response = self._make_request("POST", "/api/v1/clusters", json=source_cluster_data)
        source_cluster = response.json()
        source_id = source_cluster["id"]
        print(f"✅ Created source cluster: {source_id}")
        
        # Start source cluster
        self._make_request("POST", f"/api/v1/clusters/{source_id}/start")
        print("✅ Started source cluster")
        
        # Create a snapshot
        print("📸 Creating snapshot...")
        snapshot_data = {
            "cluster_id": source_id,
            "name": "API Example Snapshot",
            "description": "Snapshot created via API",
            "snapshot_type": "full",
            "tags": ["api-example", "demonstration"]
        }
        
        response = self._make_request("POST", "/api/v1/advanced/snapshots", json=snapshot_data)
        snapshot = response.json()
        snapshot_id = snapshot["id"]
        print(f"✅ Created snapshot: {snapshot_id}")
        
        # List snapshots
        print("📋 Listing snapshots...")
        response = self._make_request("GET", "/api/v1/advanced/snapshots")
        snapshots = response.json()
        print(f"✅ Found {len(snapshots)} snapshots")
        
        # Clone the cluster
        print("🔄 Cloning cluster...")
        clone_data = {
            "source_cluster_id": source_id,
            "target_cluster_id": "cloned-cluster",
            "target_name": "Cloned Cluster",
            "clone_data": True,
            "clone_config": True,
            "port_offset": 10,
            "tags": {"cloned_from": source_id, "purpose": "clone"}
        }
        
        response = self._make_request("POST", "/api/v1/advanced/clusters/clone", json=clone_data)
        cloned_cluster = response.json()
        clone_id = cloned_cluster["id"]
        print(f"✅ Cloned cluster: {clone_id}")
        
        # Create a schedule
        print("⏰ Creating schedule...")
        schedule_data = {
            "cluster_id": source_id,
            "name": "Daily Backup Schedule",
            "schedule_type": "backup",
            "frequency": "daily",
            "schedule_expression": "02:00",
            "description": "Daily backup at 2 AM",
            "enabled": True,
            "tags": ["backup", "automated"]
        }
        
        response = self._make_request("POST", "/api/v1/advanced/schedules", json=schedule_data)
        schedule = response.json()
        schedule_id = schedule["id"]
        print(f"✅ Created schedule: {schedule_id}")
        
        # List schedules
        print("📋 Listing schedules...")
        response = self._make_request("GET", "/api/v1/advanced/schedules")
        schedules = response.json()
        print(f"✅ Found {len(schedules)} schedules")
        
        # Add tags to cluster
        print("🏷️  Adding tags...")
        tag_data = {
            "cluster_id": source_id,
            "tags": {
                "api_example": "true",
                "last_updated": datetime.now().isoformat(),
                "feature_demo": "advanced"
            }
        }
        
        response = self._make_request("POST", "/api/v1/advanced/clusters/tags", json=tag_data)
        tag_result = response.json()
        print(f"✅ Added tags: {tag_result['message']}")
        
        # Search clusters by tags
        print("🔍 Searching clusters by tags...")
        search_data = {
            "tag_filters": {
                "api_example": "true",
                "purpose": "source"
            },
            "match_all": True
        }
        
        response = self._make_request("POST", "/api/v1/advanced/clusters/search", json=search_data)
        search_results = response.json()
        print(f"✅ Found {len(search_results)} matching clusters")
        
        # Clean up
        print("🧹 Cleaning up...")
        self._make_request("DELETE", f"/api/v1/advanced/schedules/{schedule_id}")
        self._make_request("DELETE", f"/api/v1/advanced/snapshots/{snapshot_id}")
        self._make_request("POST", f"/api/v1/clusters/{source_id}/stop")
        self._make_request("POST", f"/api/v1/clusters/{clone_id}/stop")
        self._make_request("DELETE", f"/api/v1/clusters/{source_id}")
        self._make_request("DELETE", f"/api/v1/clusters/{clone_id}")
        print("✅ Cleanup complete")
        
        print("✅ Advanced cluster features example complete!")
    
    def example_3_configuration_management(self):
        """Example 3: Configuration management operations."""
        print("\n=== Example 3: Configuration Management ===")
        
        # Create a test cluster
        cluster_data = {
            "id": "config-test-cluster",
            "name": "Config Test Cluster",
            "description": "Cluster for configuration testing",
            "environment": "testing",
            "template_id": "testing",
            "port_allocation": {
                "kafka_port": 9140,
                "rest_proxy_port": 8140,
                "ui_port": 8240
            }
        }
        
        print("📝 Creating test cluster...")
        response = self._make_request("POST", "/api/v1/clusters", json=cluster_data)
        cluster = response.json()
        cluster_id = cluster["id"]
        print(f"✅ Created cluster: {cluster_id}")
        
        # Export cluster configuration
        print("📤 Exporting configuration...")
        params = {
            "format": "yaml",
            "include_metadata": "true",
            "include_secrets": "false"
        }
        response = self._make_request("GET", f"/api/v1/config/export/{cluster_id}", params=params)
        config_yaml = response.text
        print(f"✅ Exported configuration ({len(config_yaml)} characters)")
        
        # Save configuration to file
        with open(f"exported_config_{cluster_id}.yaml", "w") as f:
            f.write(config_yaml)
        print("✅ Saved configuration to file")
        
        # Validate configuration
        print("✅ Validating configuration...")
        validate_data = {
            "config_content": config_yaml,
            "format": "yaml"
        }
        response = self._make_request("POST", "/api/v1/config/validate", json=validate_data)
        validation_result = response.json()
        print(f"✅ Validation result: {validation_result['valid']}")
        
        if not validation_result['valid']:
            print(f"   Errors: {validation_result['errors']}")
        
        # List configuration versions
        print("📋 Listing configuration versions...")
        response = self._make_request("GET", f"/api/v1/config/versions/{cluster_id}")
        versions = response.json()
        print(f"✅ Found {len(versions)} configuration versions")
        
        # Import configuration (modify and re-import)
        print("📥 Importing modified configuration...")
        
        # Modify the configuration slightly
        modified_config = config_yaml.replace(
            "description: Cluster for configuration testing",
            "description: Modified cluster for configuration testing"
        )
        
        import_data = {
            "config_content": modified_config,
            "format": "yaml",
            "validate_schema": True,
            "overwrite_existing": False
        }
        
        response = self._make_request("POST", "/api/v1/config/import", json=import_data)
        import_result = response.json()
        print(f"✅ Import result: {import_result['message']}")
        
        # List versions again to see the new one
        response = self._make_request("GET", f"/api/v1/config/versions/{cluster_id}")
        updated_versions = response.json()
        print(f"✅ Now have {len(updated_versions)} configuration versions")
        
        # Rollback to previous version if we have multiple
        if len(updated_versions) >= 2:
            print("🔄 Rolling back to previous version...")
            rollback_data = {
                "cluster_id": cluster_id,
                "version_id": updated_versions[1]["version_id"]  # Previous version
            }
            response = self._make_request("POST", "/api/v1/config/rollback", json=rollback_data)
            rollback_result = response.json()
            print(f"✅ Rollback result: {rollback_result['message']}")
        
        # Clean up
        print("🧹 Cleaning up...")
        self._make_request("DELETE", f"/api/v1/clusters/{cluster_id}")
        print("✅ Cleanup complete")
        
        print("✅ Configuration management example complete!")
    
    def example_4_monitoring_and_health(self):
        """Example 4: Monitoring and health check operations."""
        print("\n=== Example 4: Monitoring and Health ===")
        
        # Get system health overview
        print("🏥 Getting system health overview...")
        response = self._make_request("GET", "/api/v1/monitoring/health")
        system_health = response.json()
        print(f"✅ System status: {system_health['overall_status']}")
        print(f"   Total clusters: {system_health['clusters']['total']}")
        print(f"   Running clusters: {system_health['clusters']['by_status'].get('running', 0)}")
        print(f"   System CPU: {system_health['system_resources']['cpu']['usage_percent']}%")
        
        # Get system metrics
        print("📊 Getting system metrics...")
        response = self._make_request("GET", "/api/v1/monitoring/metrics")
        system_metrics = response.json()
        print(f"✅ Retrieved {len(system_metrics)} metric data points")
        
        # Create a test cluster for health monitoring
        cluster_data = {
            "id": "health-test-cluster",
            "name": "Health Test Cluster",
            "description": "Cluster for health monitoring testing",
            "environment": "testing",
            "template_id": "testing",
            "port_allocation": {
                "kafka_port": 9150,
                "rest_proxy_port": 8150,
                "ui_port": 8250
            }
        }
        
        print("📝 Creating test cluster for health monitoring...")
        response = self._make_request("POST", "/api/v1/clusters", json=cluster_data)
        cluster = response.json()
        cluster_id = cluster["id"]
        
        # Start the cluster
        self._make_request("POST", f"/api/v1/clusters/{cluster_id}/start")
        print(f"✅ Created and started cluster: {cluster_id}")
        
        # Wait a moment for cluster to initialize
        time.sleep(5)
        
        # Get cluster-specific health
        print("🏥 Getting cluster health...")
        response = self._make_request("GET", f"/api/v1/monitoring/clusters/{cluster_id}/health")
        cluster_health = response.json()
        print(f"✅ Cluster health: {cluster_health['health']['overall_status']}")
        print(f"   Components checked: {len(cluster_health['health']['components'])}")
        print(f"   Active alerts: {len(cluster_health['alerts'])}")
        
        # Get cluster metrics
        print("📊 Getting cluster metrics...")
        params = {
            "start_time": (datetime.now() - timedelta(minutes=5)).isoformat(),
            "end_time": datetime.now().isoformat()
        }
        response = self._make_request("GET", f"/api/v1/monitoring/clusters/{cluster_id}/metrics", params=params)
        cluster_metrics = response.json()
        print(f"✅ Retrieved cluster metrics: {len(cluster_metrics)} data points")
        
        # List active alerts
        print("🚨 Listing active alerts...")
        response = self._make_request("GET", "/api/v1/monitoring/alerts")
        alerts = response.json()
        print(f"✅ Found {len(alerts)} active alerts")
        
        for alert in alerts[:3]:  # Show first 3 alerts
            print(f"   - {alert['name']}: {alert['severity']} ({alert['component']})")
        
        # If there are alerts, acknowledge one
        if alerts:
            alert_id = alerts[0]["id"]
            print(f"✅ Acknowledging alert: {alert_id}")
            ack_data = {
                "acknowledged_by": "api_example_user",
                "acknowledgment_note": "Acknowledged via API example"
            }
            response = self._make_request("POST", f"/api/v1/monitoring/alerts/{alert_id}/acknowledge", json=ack_data)
            ack_result = response.json()
            print(f"✅ Acknowledgment result: {ack_result['message']}")
        
        # Clean up
        print("🧹 Cleaning up...")
        self._make_request("POST", f"/api/v1/clusters/{cluster_id}/stop")
        self._make_request("DELETE", f"/api/v1/clusters/{cluster_id}")
        print("✅ Cleanup complete")
        
        print("✅ Monitoring and health example complete!")
    
    def example_5_security_and_access_control(self):
        """Example 5: Security and access control operations."""
        print("\n=== Example 5: Security and Access Control ===")
        
        # Note: This example assumes you have admin privileges
        
        # Create a new user
        print("👤 Creating new user...")
        user_data = {
            "username": "api_test_user",
            "email": "api_test@example.com",
            "password": "secure_password_123",
            "is_admin": False
        }
        
        try:
            response = self._make_request("POST", "/api/v1/auth/users", json=user_data)
            user = response.json()
            user_id = user["id"]
            print(f"✅ Created user: {user['username']} (ID: {user_id})")
        except Exception as e:
            print(f"⚠️  User creation failed (may already exist): {str(e)}")
            # Try to get existing user
            response = self._make_request("GET", "/api/v1/auth/users")
            users = response.json()
            existing_user = next((u for u in users if u["username"] == "api_test_user"), None)
            if existing_user:
                user_id = existing_user["id"]
                print(f"✅ Using existing user: {existing_user['username']} (ID: {user_id})")
            else:
                print("❌ Could not create or find test user")
                return
        
        # List all users
        print("📋 Listing users...")
        response = self._make_request("GET", "/api/v1/auth/users")
        users = response.json()
        print(f"✅ Found {len(users)} users")
        
        # Create a test cluster for permission testing
        cluster_data = {
            "id": "permission-test-cluster",
            "name": "Permission Test Cluster",
            "description": "Cluster for permission testing",
            "environment": "testing",
            "template_id": "testing",
            "port_allocation": {
                "kafka_port": 9160,
                "rest_proxy_port": 8160,
                "ui_port": 8260
            }
        }
        
        print("📝 Creating test cluster...")
        response = self._make_request("POST", "/api/v1/clusters", json=cluster_data)
        cluster = response.json()
        cluster_id = cluster["id"]
        print(f"✅ Created cluster: {cluster_id}")
        
        # Grant cluster permission to user
        print("🔐 Granting cluster permission...")
        permission_data = {
            "user_id": user_id,
            "cluster_id": cluster_id,
            "access_level": "read",
            "granted_by": "admin"
        }
        
        response = self._make_request("POST", "/api/v1/auth/permissions", json=permission_data)
        permission = response.json()
        print(f"✅ Granted permission: {permission['access_level']} access to {cluster_id}")
        
        # List permissions
        print("📋 Listing permissions...")
        response = self._make_request("GET", "/api/v1/auth/permissions")
        permissions = response.json()
        print(f"✅ Found {len(permissions)} permissions")
        
        # Create API key for the user
        print("🔑 Creating API key...")
        api_key_data = {
            "user_id": user_id,
            "name": "api_example_key",
            "cluster_permissions": {
                cluster_id: "read"
            },
            "expires_at": (datetime.now() + timedelta(days=30)).isoformat()
        }
        
        response = self._make_request("POST", "/api/v1/auth/api-keys", json=api_key_data)
        api_key = response.json()
        api_key_id = api_key["id"]
        print(f"✅ Created API key: {api_key_id}")
        print(f"   Key: {api_key.get('key', 'hidden')}")
        
        # List API keys
        print("📋 Listing API keys...")
        response = self._make_request("GET", "/api/v1/auth/api-keys")
        api_keys = response.json()
        print(f"✅ Found {len(api_keys)} API keys")
        
        # Get audit log
        print("📜 Getting audit log...")
        params = {
            "user_id": user_id,
            "limit": 10
        }
        response = self._make_request("GET", "/api/v1/auth/audit", params=params)
        audit_entries = response.json()
        print(f"✅ Found {len(audit_entries)} audit entries")
        
        for entry in audit_entries[:3]:  # Show first 3 entries
            print(f"   - {entry['action']}: {entry['result']} at {entry['timestamp']}")
        
        # Clean up
        print("🧹 Cleaning up...")
        try:
            # Delete API key
            self._make_request("DELETE", f"/api/v1/auth/api-keys/{api_key_id}")
            
            # Delete cluster
            self._make_request("DELETE", f"/api/v1/clusters/{cluster_id}")
            
            # Delete user
            self._make_request("DELETE", f"/api/v1/auth/users/{user_id}")
            
            print("✅ Cleanup complete")
        except Exception as e:
            print(f"⚠️  Cleanup warning: {str(e)}")
        
        print("✅ Security and access control example complete!")
    
    def example_6_resource_management(self):
        """Example 6: Resource management operations."""
        print("\n=== Example 6: Resource Management ===")
        
        # Get system resource usage
        print("📊 Getting system resource usage...")
        response = self._make_request("GET", "/api/v1/resources/usage")
        system_usage = response.json()
        print(f"✅ System resource usage:")
        print(f"   CPU: {system_usage['cpu']['current_usage']}%")
        print(f"   Memory: {system_usage['memory']['current_usage']}%")
        print(f"   Disk: {system_usage['disk']['current_usage']}%")
        
        # Create a test cluster for resource monitoring
        cluster_data = {
            "id": "resource-test-cluster",
            "name": "Resource Test Cluster",
            "description": "Cluster for resource testing",
            "environment": "testing",
            "template_id": "testing",
            "port_allocation": {
                "kafka_port": 9170,
                "rest_proxy_port": 8170,
                "ui_port": 8270
            }
        }
        
        print("📝 Creating test cluster...")
        response = self._make_request("POST", "/api/v1/clusters", json=cluster_data)
        cluster = response.json()
        cluster_id = cluster["id"]
        
        # Start cluster
        self._make_request("POST", f"/api/v1/clusters/{cluster_id}/start")
        print(f"✅ Created and started cluster: {cluster_id}")
        
        # Get cluster resource usage
        print("📊 Getting cluster resource usage...")
        response = self._make_request("GET", f"/api/v1/resources/clusters/{cluster_id}/usage")
        cluster_usage = response.json()
        print(f"✅ Cluster resource usage:")
        print(f"   Disk: {cluster_usage['disk']['current_usage'] / 1_000_000:.1f} MB")
        print(f"   Ports: {cluster_usage['ports']['current_usage']}")
        
        # Set resource quota
        print("📏 Setting resource quota...")
        quota_data = {
            "resource_type": "cpu",
            "cluster_id": cluster_id,
            "soft_limit": 70.0,
            "hard_limit": 85.0,
            "unit": "percent"
        }
        
        response = self._make_request("POST", "/api/v1/resources/quotas", json=quota_data)
        quota = response.json()
        print(f"✅ Set CPU quota: {quota['soft_limit']}%/{quota['hard_limit']}%")
        
        # List resource quotas
        print("📋 Listing resource quotas...")
        response = self._make_request("GET", "/api/v1/resources/quotas")
        quotas = response.json()
        print(f"✅ Found {len(quotas)} resource quotas")
        
        # Get optimization recommendations
        print("💡 Getting optimization recommendations...")
        response = self._make_request("GET", "/api/v1/resources/optimization")
        recommendations = response.json()
        print(f"✅ Found {len(recommendations)} optimization recommendations")
        
        for rec in recommendations[:3]:  # Show first 3 recommendations
            print(f"   - {rec['cluster_id']}: {rec['recommendation']} (confidence: {rec['confidence']:.2f})")
        
        # Run resource cleanup (dry run)
        print("🧹 Running resource cleanup (dry run)...")
        cleanup_data = {
            "dry_run": True,
            "max_age_days": 7,
            "target_patterns": ["*-test", "*-temp"],
            "cluster_states": ["stopped"]
        }
        
        response = self._make_request("POST", "/api/v1/resources/cleanup", json=cleanup_data)
        cleanup_result = response.json()
        print(f"✅ Cleanup dry run results:")
        print(f"   Would clean: {len(cleanup_result['clusters_to_clean'])} clusters")
        print(f"   Would free: {cleanup_result['space_to_free_bytes'] / 1_000_000:.1f} MB")
        
        # Clean up
        print("🧹 Cleaning up...")
        self._make_request("POST", f"/api/v1/clusters/{cluster_id}/stop")
        self._make_request("DELETE", f"/api/v1/clusters/{cluster_id}")
        print("✅ Cleanup complete")
        
        print("✅ Resource management example complete!")
    
    def run_all_examples(self):
        """Run all API examples in sequence."""
        print("🚀 Starting Multi-Cluster Kafka Manager API Examples")
        print("=" * 60)
        
        try:
            # Example 1: Basic Operations
            self.example_1_basic_cluster_operations()
            
            # Example 2: Advanced Features
            self.example_2_advanced_cluster_features()
            
            # Example 3: Configuration Management
            self.example_3_configuration_management()
            
            # Example 4: Monitoring and Health
            self.example_4_monitoring_and_health()
            
            # Example 5: Security and Access Control
            self.example_5_security_and_access_control()
            
            # Example 6: Resource Management
            self.example_6_resource_management()
            
            print("\n" + "=" * 60)
            print("🎉 All API examples completed successfully!")
            print("=" * 60)
            
        except Exception as e:
            print(f"\n❌ Error running API examples: {str(e)}")
            raise


def main():
    """Main function to run API examples."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Multi-Cluster Kafka Manager API Examples")
    parser.add_argument("--base-url", default="http://localhost:8000", 
                       help="Base URL for the API (default: http://localhost:8000)")
    parser.add_argument("--api-key", help="API key for authentication")
    parser.add_argument("--example", type=int, choices=range(1, 7),
                       help="Run specific example (1-6)")
    
    args = parser.parse_args()
    
    # Create API examples instance
    api_examples = MultiClusterAPIExamples(base_url=args.base_url, api_key=args.api_key)
    
    if args.example:
        # Run specific example
        example_map = {
            1: api_examples.example_1_basic_cluster_operations,
            2: api_examples.example_2_advanced_cluster_features,
            3: api_examples.example_3_configuration_management,
            4: api_examples.example_4_monitoring_and_health,
            5: api_examples.example_5_security_and_access_control,
            6: api_examples.example_6_resource_management,
        }
        
        example_map[args.example]()
    else:
        # Run all examples
        api_examples.run_all_examples()


if __name__ == "__main__":
    main()