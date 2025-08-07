"""
Tests for configuration management service.

This module tests the configuration manager functionality including
import/export, validation, versioning, and rollback capabilities.
"""

import pytest
import tempfile
import yaml
import json
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock, AsyncMock, patch

from src.services.configuration_manager import ConfigurationManager
from src.models.multi_cluster import ClusterDefinition, KafkaConfig, PortAllocation
from src.exceptions import ConfigurationError


class TestConfigurationManager:
    """Test configuration manager functionality."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)
    
    @pytest.fixture
    def mock_storage(self):
        """Mock storage backend."""
        storage = AsyncMock()
        storage.load_cluster.return_value = None
        storage.save_cluster.return_value = True
        return storage
    
    @pytest.fixture
    def config_manager(self, mock_storage, temp_dir):
        """Create configuration manager with temp directory."""
        return ConfigurationManager(mock_storage, temp_dir)
    
    @pytest.fixture
    def sample_cluster(self):
        """Create sample cluster definition."""
        cluster = Mock(spec=ClusterDefinition)
        cluster.id = "test-cluster"
        cluster.name = "Test Cluster"
        cluster.description = "A test cluster"
        cluster.environment = "development"
        cluster.template_id = "development"
        cluster.tags = {"env": "test"}
        cluster.created_at = datetime.now()
        cluster.updated_at = datetime.now()
        cluster.created_by = "test-user"
        cluster.status = Mock()
        cluster.status.value = "stopped"
        cluster.last_started = None
        cluster.last_stopped = None
        
        # Mock configurations
        cluster.kafka_config = Mock(spec=KafkaConfig)
        cluster.kafka_config.heap_size = "1G"
        cluster.kafka_config.log_retention_hours = 168
        cluster.kafka_config.num_partitions = 3
        cluster.kafka_config.default_replication_factor = 1
        cluster.kafka_config.custom_properties = {"auto.create.topics.enable": "true"}
        
        cluster.rest_proxy_config = Mock()
        cluster.rest_proxy_config.heap_size = "512M"
        cluster.rest_proxy_config.consumer_request_timeout_ms = 30000
        
        cluster.ui_config = Mock()
        cluster.ui_config.heap_size = "512M"
        cluster.ui_config.readonly_mode = False
        
        cluster.port_allocation = Mock(spec=PortAllocation)
        cluster.port_allocation.kafka_port = 9092
        cluster.port_allocation.rest_proxy_port = 8082
        cluster.port_allocation.ui_port = 8080
        cluster.port_allocation.jmx_port = 9999
        
        cluster.retention_policy = None
        
        return cluster
    
    @pytest.fixture
    def sample_config_dict(self):
        """Create sample configuration dictionary."""
        return {
            "id": "test-cluster",
            "name": "Test Cluster",
            "description": "A test cluster",
            "environment": "development",
            "template_id": "development",
            "kafka_config": {
                "heap_size": "1G",
                "log_retention_hours": 168,
                "num_partitions": 3,
                "default_replication_factor": 1,
                "custom_properties": {
                    "auto.create.topics.enable": "true"
                }
            },
            "rest_proxy_config": {
                "heap_size": "512M",
                "consumer_request_timeout_ms": 30000
            },
            "ui_config": {
                "heap_size": "512M",
                "readonly_mode": False
            },
            "port_allocation": {
                "kafka_port": 9092,
                "rest_proxy_port": 8082,
                "ui_port": 8080,
                "jmx_port": 9999
            },
            "tags": {
                "env": "test"
            }
        }
    
    @pytest.mark.asyncio
    async def test_export_cluster_config_yaml(self, config_manager, mock_storage, sample_cluster):
        """Test exporting cluster configuration to YAML."""
        # Setup
        mock_storage.load_cluster.return_value = sample_cluster
        
        # Execute
        yaml_content = await config_manager.export_cluster_config("test-cluster", "yaml")
        
        # Verify
        assert yaml_content is not None
        assert "id: test-cluster" in yaml_content
        assert "name: Test Cluster" in yaml_content
        
        # Parse and verify structure
        parsed = yaml.safe_load(yaml_content)
        assert parsed["id"] == "test-cluster"
        assert parsed["name"] == "Test Cluster"
        assert "kafka_config" in parsed
        assert "port_allocation" in parsed
    
    @pytest.mark.asyncio
    async def test_export_cluster_config_json(self, config_manager, mock_storage, sample_cluster):
        """Test exporting cluster configuration to JSON."""
        # Setup
        mock_storage.load_cluster.return_value = sample_cluster
        
        # Execute
        json_content = await config_manager.export_cluster_config("test-cluster", "json")
        
        # Verify
        assert json_content is not None
        
        # Parse and verify structure
        parsed = json.loads(json_content)
        assert parsed["id"] == "test-cluster"
        assert parsed["name"] == "Test Cluster"
        assert "kafka_config" in parsed
        assert "port_allocation" in parsed
    
    @pytest.mark.asyncio
    async def test_export_cluster_config_not_found(self, config_manager, mock_storage):
        """Test exporting non-existent cluster configuration."""
        # Setup
        mock_storage.load_cluster.return_value = None
        
        # Execute & Verify
        with pytest.raises(ConfigurationError) as exc_info:
            await config_manager.export_cluster_config("nonexistent", "yaml")
        
        assert "not found" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_import_cluster_config_yaml(self, config_manager, mock_storage, sample_config_dict):
        """Test importing cluster configuration from YAML."""
        # Setup
        yaml_content = yaml.dump(sample_config_dict)
        
        # Execute
        cluster = await config_manager.import_cluster_config(yaml_content, "yaml")
        
        # Verify
        assert cluster.id == "test-cluster"
        assert cluster.name == "Test Cluster"
        assert cluster.environment == "development"
        mock_storage.save_cluster.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_import_cluster_config_json(self, config_manager, mock_storage, sample_config_dict):
        """Test importing cluster configuration from JSON."""
        # Setup
        json_content = json.dumps(sample_config_dict)
        
        # Execute
        cluster = await config_manager.import_cluster_config(json_content, "json")
        
        # Verify
        assert cluster.id == "test-cluster"
        assert cluster.name == "Test Cluster"
        mock_storage.save_cluster.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_import_cluster_config_auto_detect(self, config_manager, mock_storage, sample_config_dict):
        """Test importing cluster configuration with auto-detection."""
        # Test YAML auto-detection
        yaml_content = yaml.dump(sample_config_dict)
        cluster = await config_manager.import_cluster_config(yaml_content, "auto")
        assert cluster.id == "test-cluster"
        
        # Test JSON auto-detection
        json_content = json.dumps(sample_config_dict)
        cluster = await config_manager.import_cluster_config(json_content, "auto")
        assert cluster.id == "test-cluster"
    
    def test_validate_config_schema_valid(self, config_manager, sample_config_dict):
        """Test schema validation with valid configuration."""
        # Should not raise exception
        config_manager._validate_config_schema(sample_config_dict)
    
    def test_validate_config_schema_invalid(self, config_manager):
        """Test schema validation with invalid configuration."""
        invalid_config = {
            "id": "invalid-id!",  # Invalid characters
            "name": "",  # Empty name
            "environment": "invalid_env",  # Invalid environment
            "port_allocation": {
                "kafka_port": 99999999  # Invalid port
            }
        }
        
        with pytest.raises(ConfigurationError) as exc_info:
            config_manager._validate_config_schema(invalid_config)
        
        assert "Schema validation failed" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_create_config_version(self, config_manager, sample_config_dict):
        """Test creating configuration version."""
        # Execute
        version_id = await config_manager._create_config_version("test-cluster", sample_config_dict)
        
        # Verify
        assert version_id.startswith("test-cluster_")
        
        # Check version file exists
        version_file = config_manager.versions_dir / f"{version_id}.yaml"
        assert version_file.exists()
        
        # Verify version file content
        with open(version_file, 'r') as f:
            version_data = yaml.safe_load(f)
        
        assert "version_info" in version_data
        assert "configuration" in version_data
        assert version_data["version_info"]["cluster_id"] == "test-cluster"
    
    @pytest.mark.asyncio
    async def test_list_config_versions(self, config_manager, sample_config_dict):
        """Test listing configuration versions."""
        # Create some versions
        version1 = await config_manager._create_config_version("test-cluster", sample_config_dict)
        version2 = await config_manager._create_config_version("test-cluster", sample_config_dict)
        
        # Execute
        versions = await config_manager.list_config_versions("test-cluster")
        
        # Verify
        assert len(versions) == 2
        assert all("version_id" in v for v in versions)
        assert all("created_at" in v for v in versions)
        
        # Should be sorted by creation time (newest first)
        assert versions[0]["created_at"] >= versions[1]["created_at"]
    
    @pytest.mark.asyncio
    async def test_rollback_to_version(self, config_manager, mock_storage, sample_config_dict):
        """Test rolling back to a specific version."""
        # Create a version
        version_id = await config_manager._create_config_version("test-cluster", sample_config_dict)
        
        # Setup current cluster for backup
        current_cluster = Mock()
        current_cluster.id = "test-cluster"
        mock_storage.load_cluster.return_value = current_cluster
        
        # Execute
        cluster = await config_manager.rollback_to_version("test-cluster", version_id)
        
        # Verify
        assert cluster.id == "test-cluster"
        mock_storage.save_cluster.assert_called()
    
    @pytest.mark.asyncio
    async def test_rollback_to_nonexistent_version(self, config_manager):
        """Test rolling back to non-existent version."""
        with pytest.raises(ConfigurationError) as exc_info:
            await config_manager.rollback_to_version("test-cluster", "nonexistent_version")
        
        assert "not found" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_export_multiple_clusters(self, config_manager, mock_storage, sample_cluster):
        """Test exporting multiple clusters."""
        # Setup
        mock_storage.load_cluster.return_value = sample_cluster
        
        # Execute
        content = await config_manager.export_multiple_clusters(
            ["test-cluster", "test-cluster-2"], "yaml"
        )
        
        # Verify
        assert content is not None
        parsed = yaml.safe_load(content)
        assert "export_info" in parsed
        assert "clusters" in parsed
        assert parsed["export_info"]["cluster_count"] == 2
    
    @pytest.mark.asyncio
    async def test_import_multiple_clusters(self, config_manager, mock_storage, sample_config_dict):
        """Test importing multiple clusters."""
        # Setup multi-cluster config
        multi_config = {
            "export_info": {
                "exported_at": datetime.now().isoformat(),
                "cluster_count": 2
            },
            "clusters": {
                "cluster-1": sample_config_dict,
                "cluster-2": {**sample_config_dict, "id": "cluster-2", "name": "Cluster 2"}
            }
        }
        
        yaml_content = yaml.dump(multi_config)
        
        # Execute
        clusters = await config_manager.import_multiple_clusters(yaml_content, "yaml")
        
        # Verify
        assert len(clusters) == 2
        assert clusters[0].id in ["cluster-1", "cluster-2"]
        assert clusters[1].id in ["cluster-1", "cluster-2"]
        assert mock_storage.save_cluster.call_count == 2
    
    @pytest.mark.asyncio
    async def test_validate_config_file_valid(self, config_manager, temp_dir, sample_config_dict):
        """Test validating a valid configuration file."""
        # Create test file
        config_file = temp_dir / "test-config.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(sample_config_dict, f)
        
        # Execute
        result = await config_manager.validate_config_file(config_file)
        
        # Verify
        assert result["valid"] is True
        assert result["type"] == "single_cluster"
        assert result["cluster_id"] == "test-cluster"
    
    @pytest.mark.asyncio
    async def test_validate_config_file_invalid(self, config_manager, temp_dir):
        """Test validating an invalid configuration file."""
        # Create invalid test file
        invalid_config = {"id": "invalid!@#", "name": ""}
        config_file = temp_dir / "invalid-config.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(invalid_config, f)
        
        # Execute
        result = await config_manager.validate_config_file(config_file)
        
        # Verify
        assert result["valid"] is False
        assert "error" in result
    
    @pytest.mark.asyncio
    async def test_validate_config_file_not_found(self, config_manager, temp_dir):
        """Test validating non-existent configuration file."""
        # Execute
        result = await config_manager.validate_config_file(temp_dir / "nonexistent.yaml")
        
        # Verify
        assert result["valid"] is False
        assert "File not found" in result["error"]
    
    @pytest.mark.asyncio
    async def test_get_config_template(self, config_manager):
        """Test getting configuration templates."""
        # Test basic template
        basic_template = await config_manager.get_config_template("basic")
        assert basic_template["id"] == "my-cluster"
        assert basic_template["environment"] == "development"
        
        # Test development template
        dev_template = await config_manager.get_config_template("development")
        assert dev_template["id"] == "dev-cluster"
        assert dev_template["kafka_config"]["heap_size"] == "512M"
        
        # Test production template
        prod_template = await config_manager.get_config_template("production")
        assert prod_template["id"] == "prod-cluster"
        assert prod_template["kafka_config"]["heap_size"] == "4G"
        assert prod_template["ui_config"]["readonly_mode"] is True
        
        # Test unknown template (should return basic)
        unknown_template = await config_manager.get_config_template("unknown")
        assert unknown_template["id"] == "my-cluster"
    
    @pytest.mark.asyncio
    async def test_cleanup_old_versions(self, config_manager, sample_config_dict):
        """Test cleaning up old configuration versions."""
        # Create multiple versions
        for i in range(15):
            await config_manager._create_config_version("test-cluster", sample_config_dict)
        
        # Verify all versions exist
        versions_before = await config_manager.list_config_versions("test-cluster")
        assert len(versions_before) == 15
        
        # Execute cleanup (keep 10)
        deleted_count = await config_manager.cleanup_old_versions("test-cluster", keep_count=10)
        
        # Verify
        assert deleted_count == 5
        
        versions_after = await config_manager.list_config_versions("test-cluster")
        assert len(versions_after) == 10


class TestConfigurationManagerErrorHandling:
    """Test error handling in configuration manager."""
    
    @pytest.fixture
    def failing_storage(self):
        """Mock storage backend that fails."""
        storage = AsyncMock()
        storage.load_cluster.side_effect = Exception("Storage error")
        storage.save_cluster.side_effect = Exception("Storage error")
        return storage
    
    @pytest.fixture
    def config_manager_with_failing_storage(self, failing_storage):
        """Create configuration manager with failing storage."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield ConfigurationManager(failing_storage, Path(temp_dir))
    
    @pytest.mark.asyncio
    async def test_export_with_storage_error(self, config_manager_with_failing_storage):
        """Test export with storage error."""
        with pytest.raises(ConfigurationError) as exc_info:
            await config_manager_with_failing_storage.export_cluster_config("test", "yaml")
        
        assert "Export failed" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_import_with_storage_error(self, config_manager_with_failing_storage):
        """Test import with storage error."""
        config = {"id": "test", "name": "Test", "environment": "development", "port_allocation": {"kafka_port": 9092, "rest_proxy_port": 8082, "ui_port": 8080}}
        yaml_content = yaml.dump(config)
        
        with pytest.raises(ConfigurationError) as exc_info:
            await config_manager_with_failing_storage.import_cluster_config(yaml_content, "yaml")
        
        assert "Import failed" in str(exc_info.value)


class TestConfigurationManagerIntegration:
    """Integration tests for configuration manager."""
    
    @pytest.mark.asyncio
    async def test_full_export_import_cycle(self):
        """Test complete export-import cycle."""
        # This would be an integration test that tests the full flow
        # of exporting a cluster, modifying the config, and importing it back
        pass
    
    @pytest.mark.asyncio
    async def test_version_management_workflow(self):
        """Test complete version management workflow."""
        # This would test creating versions, listing them, and rolling back
        pass