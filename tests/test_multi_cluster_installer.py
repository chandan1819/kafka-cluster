"""
Tests for multi-cluster installation and setup system.
"""

import pytest
import asyncio
import tempfile
import json
import yaml
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from typing import Dict, List, Any

from src.setup.multi_cluster_installer import (
    MultiClusterInstaller,
    InstallationType,
    DeploymentScenario,
    InstallationConfig,
    InstallationResult
)
from src.exceptions import InstallationError, ValidationError


class TestMultiClusterInstaller:
    """Test multi-cluster installer functionality."""
    
    @pytest.fixture
    def temp_directory(self):
        """Create temporary directory for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)
    
    @pytest.fixture
    def installer(self, temp_directory):
        """Create installer instance."""
        return MultiClusterInstaller(base_directory=temp_directory)
    
    @pytest.fixture
    def sample_config(self, temp_directory):
        """Create sample installation configuration."""
        return InstallationConfig(
            installation_type=InstallationType.FRESH_INSTALL,
            deployment_scenario=DeploymentScenario.DEVELOPMENT,
            base_directory=temp_directory,
            data_directory=temp_directory / "data",
            config_directory=temp_directory / "config",
            enable_multi_cluster=True,
            default_cluster_count=1,
            port_range_start=9000,
            port_range_end=9999,
            max_memory_mb=2048,
            max_disk_gb=20,
            enable_resource_monitoring=True,
            enable_auto_cleanup=True,
            enable_authentication=False,
            enable_ssl=False,
            admin_username="admin",
            enable_backup=True,
            backup_retention_days=30
        )
    
    def test_installer_initialization(self, installer, temp_directory):
        """Test installer initialization."""
        assert installer.base_directory == temp_directory
        assert installer.current_config is None
        assert len(installer.installation_history) == 0
        assert len(installer.validation_results) == 0
    
    @pytest.mark.asyncio
    async def test_detect_existing_installation_none(self, installer):
        """Test detecting no existing installation."""
        existing = await installer._detect_existing_installation()
        assert existing is None
    
    @pytest.mark.asyncio
    async def test_detect_existing_single_cluster(self, installer, temp_directory):
        """Test detecting existing single-cluster installation."""
        # Create docker-compose.yml
        docker_compose = temp_directory / "docker-compose.yml"
        docker_compose.write_text("version: '3.8'\nservices:\n  kafka:\n    image: kafka")
        
        existing = await installer._detect_existing_installation()
        
        assert existing is not None
        assert existing["type"] == "single_cluster"
        assert existing["config_file"] == docker_compose
    
    @pytest.mark.asyncio
    async def test_detect_existing_multi_cluster(self, installer, temp_directory):
        """Test detecting existing multi-cluster installation."""
        # Create cluster registry directory
        cluster_registry = temp_directory / "cluster_registry"
        cluster_registry.mkdir()
        
        existing = await installer._detect_existing_installation()
        
        assert existing is not None
        assert existing["type"] == "multi_cluster"
        assert existing["registry_path"] == cluster_registry
    
    @pytest.mark.asyncio
    async def test_validate_system_requirements_success(self, installer):
        """Test successful system requirements validation."""
        with patch('subprocess.run') as mock_run, \
             patch('shutil.disk_usage') as mock_disk, \
             patch('sys.version_info', (3, 9, 0)):
            
            # Mock successful Docker and Docker Compose checks
            mock_run.side_effect = [
                Mock(returncode=0, stdout="Docker version 20.10.0"),  # docker --version
                Mock(returncode=0, stdout="docker-compose version 1.29.0")  # docker-compose --version
            ]
            
            # Mock sufficient disk space (10GB free)
            mock_disk.return_value = Mock(free=10 * 1024**3)
            
            validation = await installer._validate_system_requirements()
            
            assert validation["valid"] is True
            assert len(validation["errors"]) == 0
            assert "python" in validation["checks"]
            assert "docker" in validation["checks"]
            assert "docker_compose" in validation["checks"]
            assert "disk_space_gb" in validation["checks"]
    
    @pytest.mark.asyncio
    async def test_validate_system_requirements_failure(self, installer):
        """Test failed system requirements validation."""
        with patch('subprocess.run') as mock_run, \
             patch('shutil.disk_usage') as mock_disk, \
             patch('sys.version_info', (3, 7, 0)):  # Old Python version
            
            # Mock failed Docker check
            mock_run.side_effect = [
                Mock(returncode=1, stdout=""),  # docker --version fails
                Mock(returncode=0, stdout="docker-compose version 1.29.0")
            ]
            
            # Mock insufficient disk space (1GB free)
            mock_disk.return_value = Mock(free=1 * 1024**3)
            
            validation = await installer._validate_system_requirements()
            
            assert validation["valid"] is False
            assert len(validation["errors"]) >= 2  # Python version and Docker
            assert any("Python 3.8+" in error for error in validation["errors"])
            assert any("Docker" in error for error in validation["errors"])
    
    @pytest.mark.asyncio
    async def test_create_directory_structure(self, installer, sample_config):
        """Test creating directory structure."""
        await installer._create_directory_structure(sample_config)
        
        # Check main directories
        assert sample_config.base_directory.exists()
        assert sample_config.data_directory.exists()
        assert sample_config.config_directory.exists()
        
        # Check subdirectories
        assert (sample_config.data_directory / "clusters").exists()
        assert (sample_config.data_directory / "logs").exists()
        assert (sample_config.data_directory / "backups").exists()
        assert (sample_config.config_directory / "templates").exists()
        assert (sample_config.config_directory / "clusters").exists()
        assert (sample_config.base_directory / "cluster_registry").exists()
        
        # Check monitoring directory (enabled in sample config)
        assert (sample_config.data_directory / "monitoring").exists()
    
    @pytest.mark.asyncio
    async def test_create_builtin_templates(self, installer, sample_config, temp_directory):
        """Test creating built-in templates."""
        # Mock template manager
        template_manager = Mock()
        template_manager.create_template = AsyncMock()
        
        await installer._create_builtin_templates(template_manager, sample_config)
        
        # Should create 3 built-in templates
        assert template_manager.create_template.call_count == 3
        
        # Check template calls
        calls = template_manager.create_template.call_args_list
        template_ids = [call[0][0].id for call in calls]
        
        assert "development" in template_ids
        assert "testing" in template_ids
        assert "production" in template_ids
    
    @pytest.mark.asyncio
    async def test_create_default_clusters_development(self, installer, sample_config, temp_directory):
        """Test creating default clusters for development scenario."""
        # Mock dependencies
        cluster_registry = Mock()
        cluster_registry.register_cluster = AsyncMock()
        template_manager = Mock()
        
        sample_config.deployment_scenario = DeploymentScenario.DEVELOPMENT
        
        clusters = await installer._create_default_clusters(
            cluster_registry, template_manager, sample_config
        )
        
        assert len(clusters) == 1
        assert clusters[0] == "dev-cluster"
        cluster_registry.register_cluster.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_default_clusters_testing(self, installer, sample_config, temp_directory):
        """Test creating default clusters for testing scenario."""
        # Mock dependencies
        cluster_registry = Mock()
        cluster_registry.register_cluster = AsyncMock()
        template_manager = Mock()
        
        sample_config.deployment_scenario = DeploymentScenario.TESTING
        
        clusters = await installer._create_default_clusters(
            cluster_registry, template_manager, sample_config
        )
        
        assert len(clusters) == 2
        assert "dev-cluster" in clusters
        assert "test-cluster" in clusters
        assert cluster_registry.register_cluster.call_count == 2
    
    @pytest.mark.asyncio
    async def test_create_default_clusters_production(self, installer, sample_config, temp_directory):
        """Test creating default clusters for production scenario."""
        # Mock dependencies
        cluster_registry = Mock()
        cluster_registry.register_cluster = AsyncMock()
        template_manager = Mock()
        
        sample_config.deployment_scenario = DeploymentScenario.PRODUCTION
        
        clusters = await installer._create_default_clusters(
            cluster_registry, template_manager, sample_config
        )
        
        assert len(clusters) == 1
        assert clusters[0] == "prod-cluster"
        cluster_registry.register_cluster.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_default_clusters_demo(self, installer, sample_config, temp_directory):
        """Test creating default clusters for demo scenario."""
        # Mock dependencies
        cluster_registry = Mock()
        cluster_registry.register_cluster = AsyncMock()
        template_manager = Mock()
        
        sample_config.deployment_scenario = DeploymentScenario.DEMO
        
        clusters = await installer._create_default_clusters(
            cluster_registry, template_manager, sample_config
        )
        
        assert len(clusters) == 3
        assert "demo-ecommerce" in clusters
        assert "demo-iot" in clusters
        assert "demo-analytics" in clusters
        assert cluster_registry.register_cluster.call_count == 3
    
    @pytest.mark.asyncio
    async def test_generate_configuration_files(self, installer, sample_config, temp_directory):
        """Test generating configuration files."""
        clusters = ["dev-cluster", "test-cluster"]
        
        config_files = await installer._generate_configuration_files(sample_config, clusters)
        
        assert len(config_files) >= 2  # At least main config and env file
        
        # Check main configuration file
        main_config_file = sample_config.config_directory / "multi_cluster_config.yml"
        assert main_config_file in config_files
        assert main_config_file.exists()
        
        # Verify main config content
        with open(main_config_file, 'r') as f:
            config_data = yaml.safe_load(f)
        
        assert config_data["installation"]["type"] == "fresh_install"
        assert config_data["installation"]["scenario"] == "development"
        assert config_data["multi_cluster"]["enabled"] is True
        assert config_data["clusters"] == clusters
        
        # Check environment file
        env_file = sample_config.base_directory / ".env"
        assert env_file in config_files
        assert env_file.exists()
        
        # Verify env file content
        env_content = env_file.read_text()
        assert f"BASE_DIR={sample_config.base_directory}" in env_content
        assert "MULTI_CLUSTER_ENABLED=true" in env_content
    
    @pytest.mark.asyncio
    async def test_perform_fresh_install(self, installer, sample_config):
        """Test performing fresh installation."""
        with patch.object(installer, '_create_builtin_templates') as mock_templates, \
             patch.object(installer, '_create_default_clusters') as mock_clusters, \
             patch.object(installer, '_generate_configuration_files') as mock_configs, \
             patch.object(installer, '_setup_monitoring') as mock_monitoring, \
             patch.object(installer, '_setup_security') as mock_security:
            
            # Setup mocks
            mock_clusters.return_value = ["dev-cluster"]
            mock_configs.return_value = [Path("config.yml")]
            
            result = await installer._perform_fresh_install(sample_config)
            
            assert "storage_backend" in result["components"]
            assert "cluster_registry" in result["components"]
            assert "template_manager" in result["components"]
            assert "builtin_templates" in result["components"]
            assert result["clusters"] == ["dev-cluster"]
            assert len(result["config_files"]) == 1
            
            # Verify monitoring setup was called (enabled in sample config)
            mock_monitoring.assert_called_once()
            
            # Verify security setup was not called (disabled in sample config)
            mock_security.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_create_backup(self, installer, temp_directory):
        """Test creating backup."""
        # Create some files to backup
        (temp_directory / "test.yml").write_text("test: config")
        (temp_directory / "data").mkdir()
        (temp_directory / "data" / "test.log").write_text("test log")
        
        backup_path = await installer.create_backup()
        
        assert backup_path.exists()
        assert backup_path.is_dir()
        
        # Check backup manifest
        manifest_file = backup_path / "backup_manifest.json"
        assert manifest_file.exists()
        
        with open(manifest_file, 'r') as f:
            manifest = json.load(f)
        
        assert manifest["backup_type"] == "full"
        assert manifest["source_directory"] == str(temp_directory)
        assert "backup_created" in manifest
        
        # Check backed up files
        config_backup = backup_path / "config"
        assert config_backup.exists()
        assert (config_backup / "test.yml").exists()
        
        data_backup = backup_path / "data"
        assert data_backup.exists()
        assert (data_backup / "test.log").exists()
    
    @pytest.mark.asyncio
    async def test_restore_backup(self, installer, temp_directory):
        """Test restoring from backup."""
        # Create backup first
        (temp_directory / "original.yml").write_text("original: config")
        backup_path = await installer.create_backup()
        
        # Modify original files
        (temp_directory / "original.yml").write_text("modified: config")
        (temp_directory / "new.yml").write_text("new: config")
        
        # Restore backup
        success = await installer.restore_backup(backup_path)
        
        assert success is True
        
        # Check restored files
        assert (temp_directory / "original.yml").exists()
        restored_content = (temp_directory / "original.yml").read_text()
        assert "original: config" in restored_content
    
    @pytest.mark.asyncio
    async def test_restore_backup_invalid_path(self, installer):
        """Test restoring from invalid backup path."""
        invalid_path = Path("/nonexistent/backup")
        
        with pytest.raises(InstallationError, match="Backup path does not exist"):
            await installer.restore_backup(invalid_path)
    
    @pytest.mark.asyncio
    async def test_validate_installation_success(self, installer):
        """Test successful installation validation."""
        with patch.object(installer, '_validate_system_requirements') as mock_system, \
             patch.object(installer, '_validate_directory_structure') as mock_dirs, \
             patch.object(installer, '_validate_configuration_files') as mock_config, \
             patch.object(installer, '_validate_cluster_registry') as mock_registry, \
             patch.object(installer, '_validate_port_availability') as mock_ports, \
             patch.object(installer, '_generate_recommendations') as mock_recommendations:
            
            # Setup successful validation mocks
            mock_system.return_value = {"valid": True, "errors": [], "warnings": []}
            mock_dirs.return_value = {"valid": True, "errors": [], "warnings": []}
            mock_config.return_value = {"valid": True, "errors": [], "warnings": []}
            mock_registry.return_value = {"valid": True, "errors": [], "warnings": []}
            mock_ports.return_value = {"valid": True, "errors": [], "warnings": []}
            mock_recommendations.return_value = ["Consider enabling monitoring"]
            
            validation = await installer.validate_installation()
            
            assert validation["valid"] is True
            assert len(validation["errors"]) == 0
            assert len(validation["checks"]) == 5
            assert len(validation["recommendations"]) == 1
    
    @pytest.mark.asyncio
    async def test_validate_installation_failure(self, installer):
        """Test failed installation validation."""
        with patch.object(installer, '_validate_system_requirements') as mock_system, \
             patch.object(installer, '_validate_directory_structure') as mock_dirs, \
             patch.object(installer, '_validate_configuration_files') as mock_config, \
             patch.object(installer, '_validate_cluster_registry') as mock_registry, \
             patch.object(installer, '_validate_port_availability') as mock_ports, \
             patch.object(installer, '_generate_recommendations') as mock_recommendations:
            
            # Setup failed validation mocks
            mock_system.return_value = {"valid": False, "errors": ["Docker not found"], "warnings": []}
            mock_dirs.return_value = {"valid": False, "errors": ["Missing data directory"], "warnings": []}
            mock_config.return_value = {"valid": True, "errors": [], "warnings": []}
            mock_registry.return_value = {"valid": True, "errors": [], "warnings": []}
            mock_ports.return_value = {"valid": True, "errors": [], "warnings": []}
            mock_recommendations.return_value = ["Install Docker", "Create missing directories"]
            
            validation = await installer.validate_installation()
            
            assert validation["valid"] is False
            assert len(validation["errors"]) == 2
            assert "Docker not found" in validation["errors"]
            assert "Missing data directory" in validation["errors"]
            assert len(validation["recommendations"]) == 2
    
    @pytest.mark.asyncio
    async def test_migrate_from_single_cluster(self, installer, temp_directory):
        """Test migrating from single-cluster setup."""
        # Create single-cluster config file
        single_config = temp_directory / "docker-compose.yml"
        single_config.write_text("""
version: '3.8'
services:
  kafka:
    image: confluentinc/cp-kafka:latest
    ports:
      - "9092:9092"
""")
        
        with patch.object(installer, '_load_single_cluster_config') as mock_load, \
             patch.object(installer, '_migrate_single_cluster') as mock_migrate, \
             patch.object(installer, '_create_builtin_templates') as mock_templates, \
             patch.object(installer, '_generate_configuration_files') as mock_configs, \
             patch.object(installer, 'create_backup') as mock_backup:
            
            # Setup mocks
            mock_load.return_value = {"kafka_port": 9092}
            mock_migrate.return_value = "migrated-cluster"
            mock_configs.return_value = [Path("config.yml")]
            mock_backup.return_value = temp_directory / "backup"
            
            result = await installer.migrate_from_single_cluster(single_config)
            
            assert result.success is True
            assert result.installation_type == InstallationType.MIGRATION
            assert "migrated-cluster" in result.created_clusters
            assert len(result.warnings) > 0
            assert any("migrated" in warning.lower() for warning in result.warnings)
    
    @pytest.mark.asyncio
    async def test_repair_installation(self, installer):
        """Test repairing installation."""
        with patch.object(installer, 'validate_installation') as mock_validate, \
             patch.object(installer, '_create_directory_structure') as mock_dirs, \
             patch.object(installer, '_generate_configuration_files') as mock_configs, \
             patch.object(installer, '_repair_cluster_registry') as mock_registry, \
             patch.object(installer, '_fix_port_conflicts') as mock_ports:
            
            # Setup validation that shows issues
            mock_validate.return_value = {
                "valid": False,
                "checks": {
                    "directories": {"valid": False},
                    "configuration": {"valid": False},
                    "cluster_registry": {"valid": False},
                    "ports": {"valid": False}
                }
            }
            
            mock_configs.return_value = [Path("config.yml")]
            mock_ports.return_value = ["fixed-cluster"]
            
            result = await installer.repair_installation()
            
            assert result.success is True
            assert result.installation_type == InstallationType.REPAIR
            assert "directory_structure_repair" in result.installed_components
            assert "configuration_repair" in result.installed_components
            assert "cluster_registry_repair" in result.installed_components
            assert "port_conflict_repair" in result.installed_components
    
    @pytest.mark.asyncio
    async def test_repair_installation_no_issues(self, installer):
        """Test repairing installation when no issues found."""
        with patch.object(installer, 'validate_installation') as mock_validate:
            # Setup validation that shows no issues
            mock_validate.return_value = {"valid": True}
            
            result = await installer.repair_installation()
            
            assert result.success is True
            assert result.installation_type == InstallationType.REPAIR
            assert len(result.installed_components) == 0
            assert any("no repair was necessary" in warning.lower() for warning in result.warnings)
    
    @pytest.mark.asyncio
    async def test_install_fresh_install_success(self, installer, sample_config):
        """Test successful fresh installation."""
        with patch.object(installer, 'run_installation_wizard') as mock_wizard, \
             patch.object(installer, '_validate_system_requirements') as mock_validate, \
             patch.object(installer, '_create_directory_structure') as mock_dirs, \
             patch.object(installer, '_perform_fresh_install') as mock_install, \
             patch.object(installer, '_post_installation_setup') as mock_post, \
             patch.object(installer, '_generate_next_steps') as mock_steps, \
             patch.object(installer, '_save_installation_record') as mock_save, \
             patch.object(installer, '_display_installation_success') as mock_display:
            
            # Setup mocks
            mock_wizard.return_value = sample_config
            mock_validate.return_value = {"valid": True, "errors": []}
            mock_install.return_value = {
                "components": ["storage_backend", "cluster_registry"],
                "clusters": ["dev-cluster"],
                "config_files": [Path("config.yml")],
                "warnings": []
            }
            mock_post.return_value = {"warnings": []}
            mock_steps.return_value = ["Start the services", "Access web interface"]
            
            result = await installer.install(sample_config)
            
            assert result.success is True
            assert result.installation_type == InstallationType.FRESH_INSTALL
            assert result.deployment_scenario == DeploymentScenario.DEVELOPMENT
            assert "storage_backend" in result.installed_components
            assert "dev-cluster" in result.created_clusters
            assert len(result.next_steps) == 2
            
            # Verify methods were called
            mock_dirs.assert_called_once()
            mock_install.assert_called_once()
            mock_post.assert_called_once()
            mock_save.assert_called_once()
            mock_display.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_install_validation_failure(self, installer, sample_config):
        """Test installation with validation failure."""
        with patch.object(installer, 'run_installation_wizard') as mock_wizard, \
             patch.object(installer, '_validate_system_requirements') as mock_validate, \
             patch.object(installer, '_save_installation_record') as mock_save:
            
            # Setup mocks
            mock_wizard.return_value = sample_config
            mock_validate.return_value = {
                "valid": False,
                "errors": ["Docker not found", "Insufficient disk space"]
            }
            
            result = await installer.install(sample_config)
            
            assert result.success is False
            assert "Docker not found" in result.error_message or "Insufficient disk space" in result.error_message
            
            # Verify error was saved
            mock_save.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_save_installation_record(self, installer, temp_directory):
        """Test saving installation record."""
        result = InstallationResult(
            success=True,
            installation_type=InstallationType.FRESH_INSTALL,
            deployment_scenario=DeploymentScenario.DEVELOPMENT,
            installed_components=["storage_backend"],
            created_clusters=["dev-cluster"],
            configuration_files=[Path("config.yml")],
            start_time=datetime.now(),
            end_time=datetime.now(),
            warnings=["Test warning"],
            next_steps=["Test step"]
        )
        
        await installer._save_installation_record(result)
        
        # Check history file was created
        history_file = temp_directory / "installation_history.json"
        assert history_file.exists()
        
        # Verify content
        with open(history_file, 'r') as f:
            history = json.load(f)
        
        assert len(history) == 1
        assert history[0]["success"] is True
        assert history[0]["installation_type"] == "fresh_install"
        assert history[0]["deployment_scenario"] == "development"
        assert "dev-cluster" in history[0]["created_clusters"]
    
    def test_installation_config_creation(self, temp_directory):
        """Test installation configuration creation."""
        config = InstallationConfig(
            installation_type=InstallationType.FRESH_INSTALL,
            deployment_scenario=DeploymentScenario.TESTING,
            base_directory=temp_directory,
            data_directory=temp_directory / "data",
            config_directory=temp_directory / "config",
            enable_multi_cluster=True,
            default_cluster_count=2,
            max_memory_mb=4096,
            max_disk_gb=100,
            enable_authentication=True,
            admin_username="testadmin",
            admin_password="testpass"
        )
        
        assert config.installation_type == InstallationType.FRESH_INSTALL
        assert config.deployment_scenario == DeploymentScenario.TESTING
        assert config.enable_multi_cluster is True
        assert config.default_cluster_count == 2
        assert config.enable_authentication is True
        assert config.admin_username == "testadmin"
    
    def test_installation_result_creation(self):
        """Test installation result creation."""
        start_time = datetime.now()
        end_time = datetime.now()
        
        result = InstallationResult(
            success=True,
            installation_type=InstallationType.UPGRADE,
            deployment_scenario=DeploymentScenario.PRODUCTION,
            installed_components=["component1", "component2"],
            created_clusters=["prod-cluster"],
            configuration_files=[Path("config1.yml"), Path("config2.yml")],
            start_time=start_time,
            end_time=end_time,
            warnings=["Warning 1", "Warning 2"],
            next_steps=["Step 1", "Step 2"]
        )
        
        assert result.success is True
        assert result.installation_type == InstallationType.UPGRADE
        assert result.deployment_scenario == DeploymentScenario.PRODUCTION
        assert len(result.installed_components) == 2
        assert len(result.created_clusters) == 1
        assert len(result.configuration_files) == 2
        assert len(result.warnings) == 2
        assert len(result.next_steps) == 2
        assert result.error_message is None


class TestInstallationEnums:
    """Test installation enums."""
    
    def test_installation_type_enum(self):
        """Test InstallationType enum."""
        assert InstallationType.FRESH_INSTALL.value == "fresh_install"
        assert InstallationType.UPGRADE.value == "upgrade"
        assert InstallationType.MIGRATION.value == "migration"
        assert InstallationType.REPAIR.value == "repair"
    
    def test_deployment_scenario_enum(self):
        """Test DeploymentScenario enum."""
        assert DeploymentScenario.DEVELOPMENT.value == "development"
        assert DeploymentScenario.TESTING.value == "testing"
        assert DeploymentScenario.STAGING.value == "staging"
        assert DeploymentScenario.PRODUCTION.value == "production"
        assert DeploymentScenario.DEMO.value == "demo"
        assert DeploymentScenario.CUSTOM.value == "custom"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])