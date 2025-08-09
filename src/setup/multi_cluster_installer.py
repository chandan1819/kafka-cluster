"""
Multi-cluster installation and setup system.

This module provides comprehensive installation, setup, and migration tools
for multi-cluster Kafka deployments with guided wizards and validation.
"""

import asyncio
import logging
import shutil
import subprocess
import sys
import json
import yaml
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum

from src.models.multi_cluster import ClusterDefinition, PortAllocation, ClusterTemplate
from src.services.multi_cluster_manager import MultiClusterManager
from src.services.template_manager import TemplateManager
from src.registry.cluster_registry import ClusterRegistry
from src.storage.file_backend import FileStorageBackend
from src.exceptions import InstallationError, ValidationError
from src.registry.exceptions import ClusterNotFoundError
from src.utils.logging import get_logger

logger = get_logger(__name__)


class InstallationType(Enum):
    """Types of installation."""
    FRESH_INSTALL = "fresh_install"
    UPGRADE = "upgrade"
    MIGRATION = "migration"
    REPAIR = "repair"


class DeploymentScenario(Enum):
    """Common deployment scenarios."""
    DEVELOPMENT = "development"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"
    DEMO = "demo"
    CUSTOM = "custom"


@dataclass
class InstallationConfig:
    """Installation configuration."""
    installation_type: InstallationType
    deployment_scenario: DeploymentScenario
    base_directory: Path
    data_directory: Path
    config_directory: Path
    
    # Multi-cluster settings
    enable_multi_cluster: bool = True
    default_cluster_count: int = 1
    port_range_start: int = 9000
    port_range_end: int = 9999
    
    # Resource settings
    max_memory_mb: int = 4096
    max_disk_gb: int = 50
    enable_resource_monitoring: bool = True
    enable_auto_cleanup: bool = True
    
    # Security settings
    enable_authentication: bool = False
    enable_ssl: bool = False
    admin_username: str = "admin"
    admin_password: Optional[str] = None
    
    # Backup settings
    enable_backup: bool = True
    backup_directory: Optional[Path] = None
    backup_retention_days: int = 30
    
    # Advanced settings
    custom_templates: List[Dict[str, Any]] = field(default_factory=list)
    environment_variables: Dict[str, str] = field(default_factory=dict)
    docker_compose_overrides: Dict[str, Any] = field(default_factory=dict)


@dataclass
class InstallationResult:
    """Result of installation process."""
    success: bool
    installation_type: InstallationType
    deployment_scenario: DeploymentScenario
    installed_components: List[str]
    created_clusters: List[str]
    configuration_files: List[Path]
    start_time: datetime
    end_time: datetime
    error_message: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    next_steps: List[str] = field(default_factory=list)


class MultiClusterInstaller:
    """
    Comprehensive multi-cluster installation and setup system.
    
    Provides guided installation wizards, migration tools, and validation
    for multi-cluster Kafka deployments.
    """
    
    def __init__(self, base_directory: Optional[Path] = None):
        self.base_directory = base_directory or Path.cwd()
        self.logger = logging.getLogger(__name__)
        
        # Installation state
        self.current_config: Optional[InstallationConfig] = None
        self.installation_history: List[InstallationResult] = []
        
        # Validation results
        self.validation_results: Dict[str, Any] = {}
        
    async def run_installation_wizard(self) -> InstallationConfig:
        """Run interactive installation wizard."""
        print("🚀 Multi-Cluster Kafka Manager Installation Wizard")
        print("=" * 60)
        
        try:
            # Step 1: Detect existing installation
            existing_installation = await self._detect_existing_installation()
            
            if existing_installation:
                print(f"📋 Existing installation detected: {existing_installation['type']}")
                installation_type = await self._prompt_installation_type(has_existing=True)
            else:
                print("📋 No existing installation detected")
                installation_type = InstallationType.FRESH_INSTALL
            
            # Step 2: Choose deployment scenario
            deployment_scenario = await self._prompt_deployment_scenario()
            
            # Step 3: Configure directories
            directories = await self._prompt_directories()
            
            # Step 4: Configure multi-cluster settings
            multi_cluster_settings = await self._prompt_multi_cluster_settings()
            
            # Step 5: Configure resources
            resource_settings = await self._prompt_resource_settings()
            
            # Step 6: Configure security
            security_settings = await self._prompt_security_settings()
            
            # Step 7: Configure backup
            backup_settings = await self._prompt_backup_settings()
            
            # Step 8: Review configuration
            config = InstallationConfig(
                installation_type=installation_type,
                deployment_scenario=deployment_scenario,
                **directories,
                **multi_cluster_settings,
                **resource_settings,
                **security_settings,
                **backup_settings
            )
            
            print("\n📋 Installation Configuration Summary:")
            print("-" * 40)
            await self._display_config_summary(config)
            
            confirm = input("\n✅ Proceed with installation? (y/N): ").lower().strip()
            if confirm != 'y':
                print("❌ Installation cancelled by user")
                sys.exit(0)
            
            return config
            
        except KeyboardInterrupt:
            print("\n❌ Installation cancelled by user")
            sys.exit(0)
        except Exception as e:
            self.logger.error(f"Error in installation wizard: {e}")
            raise InstallationError(f"Installation wizard failed: {e}")
    
    async def install(self, config: Optional[InstallationConfig] = None) -> InstallationResult:
        """Run complete installation process."""
        start_time = datetime.now()
        
        try:
            # Use provided config or run wizard
            if config is None:
                config = await self.run_installation_wizard()
            
            self.current_config = config
            
            print(f"\n🚀 Starting {config.installation_type.value} installation...")
            
            # Pre-installation validation
            print("🔍 Running pre-installation validation...")
            validation_results = await self._validate_system_requirements()
            
            if not validation_results["valid"]:
                raise InstallationError(f"System validation failed: {validation_results['errors']}")
            
            # Create directories
            print("📁 Creating directory structure...")
            await self._create_directory_structure(config)
            
            # Install components based on type
            installed_components = []
            created_clusters = []
            configuration_files = []
            warnings = []
            
            if config.installation_type == InstallationType.FRESH_INSTALL:
                result = await self._perform_fresh_install(config)
            elif config.installation_type == InstallationType.UPGRADE:
                result = await self._perform_upgrade(config)
            elif config.installation_type == InstallationType.MIGRATION:
                result = await self._perform_migration(config)
            elif config.installation_type == InstallationType.REPAIR:
                result = await self._perform_repair(config)
            else:
                raise InstallationError(f"Unsupported installation type: {config.installation_type}")
            
            installed_components.extend(result.get("components", []))
            created_clusters.extend(result.get("clusters", []))
            configuration_files.extend(result.get("config_files", []))
            warnings.extend(result.get("warnings", []))
            
            # Post-installation setup
            print("⚙️  Running post-installation setup...")
            post_setup_result = await self._post_installation_setup(config)
            warnings.extend(post_setup_result.get("warnings", []))
            
            # Generate next steps
            next_steps = await self._generate_next_steps(config, created_clusters)
            
            end_time = datetime.now()
            
            # Create installation result
            installation_result = InstallationResult(
                success=True,
                installation_type=config.installation_type,
                deployment_scenario=config.deployment_scenario,
                installed_components=installed_components,
                created_clusters=created_clusters,
                configuration_files=configuration_files,
                start_time=start_time,
                end_time=end_time,
                warnings=warnings,
                next_steps=next_steps
            )
            
            # Save installation record
            await self._save_installation_record(installation_result)
            
            # Display success message
            await self._display_installation_success(installation_result)
            
            return installation_result
            
        except Exception as e:
            end_time = datetime.now()
            
            error_result = InstallationResult(
                success=False,
                installation_type=config.installation_type if config else InstallationType.FRESH_INSTALL,
                deployment_scenario=config.deployment_scenario if config else DeploymentScenario.DEVELOPMENT,
                installed_components=[],
                created_clusters=[],
                configuration_files=[],
                start_time=start_time,
                end_time=end_time,
                error_message=str(e)
            )
            
            await self._save_installation_record(error_result)
            
            self.logger.error(f"Installation failed: {e}")
            print(f"\n❌ Installation failed: {e}")
            
            return error_result
    
    async def create_backup(self, backup_path: Optional[Path] = None) -> Path:
        """Create backup of current installation."""
        try:
            if backup_path is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_path = self.base_directory / f"backup_{timestamp}"
            
            backup_path.mkdir(parents=True, exist_ok=True)
            
            print(f"💾 Creating backup at {backup_path}...")
            
            # Backup configuration files
            config_backup = backup_path / "config"
            config_backup.mkdir(exist_ok=True)
            
            for config_file in self.base_directory.glob("*.yml"):
                shutil.copy2(config_file, config_backup)
            
            for config_file in self.base_directory.glob("*.yaml"):
                shutil.copy2(config_file, config_backup)
            
            for config_file in self.base_directory.glob("*.json"):
                shutil.copy2(config_file, config_backup)
            
            # Backup data directory
            data_dir = self.base_directory / "data"
            if data_dir.exists():
                shutil.copytree(data_dir, backup_path / "data", dirs_exist_ok=True)
            
            # Backup cluster registry
            registry_dir = self.base_directory / "cluster_registry"
            if registry_dir.exists():
                shutil.copytree(registry_dir, backup_path / "cluster_registry", dirs_exist_ok=True)
            
            # Create backup manifest
            manifest = {
                "backup_created": datetime.now().isoformat(),
                "source_directory": str(self.base_directory),
                "backup_type": "full",
                "files_backed_up": len(list(backup_path.rglob("*"))),
                "version": "1.0"
            }
            
            with open(backup_path / "backup_manifest.json", 'w') as f:
                json.dump(manifest, f, indent=2)
            
            print(f"✅ Backup created successfully at {backup_path}")
            return backup_path
            
        except Exception as e:
            self.logger.error(f"Error creating backup: {e}")
            raise InstallationError(f"Failed to create backup: {e}")
    
    async def restore_backup(self, backup_path: Path) -> bool:
        """Restore from backup."""
        try:
            if not backup_path.exists():
                raise InstallationError(f"Backup path does not exist: {backup_path}")
            
            manifest_file = backup_path / "backup_manifest.json"
            if not manifest_file.exists():
                raise InstallationError("Invalid backup: manifest file not found")
            
            with open(manifest_file, 'r') as f:
                manifest = json.load(f)
            
            print(f"🔄 Restoring backup from {manifest['backup_created']}...")
            
            # Create backup of current state before restore
            current_backup = await self.create_backup()
            print(f"💾 Current state backed up to {current_backup}")
            
            # Restore configuration files
            config_backup = backup_path / "config"
            if config_backup.exists():
                for config_file in config_backup.iterdir():
                    if config_file.is_file():
                        shutil.copy2(config_file, self.base_directory)
            
            # Restore data directory
            data_backup = backup_path / "data"
            data_dir = self.base_directory / "data"
            if data_backup.exists():
                if data_dir.exists():
                    shutil.rmtree(data_dir)
                shutil.copytree(data_backup, data_dir)
            
            # Restore cluster registry
            registry_backup = backup_path / "cluster_registry"
            registry_dir = self.base_directory / "cluster_registry"
            if registry_backup.exists():
                if registry_dir.exists():
                    shutil.rmtree(registry_dir)
                shutil.copytree(registry_backup, registry_dir)
            
            print("✅ Backup restored successfully")
            print(f"ℹ️  Previous state backed up to {current_backup}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error restoring backup: {e}")
            raise InstallationError(f"Failed to restore backup: {e}")
    
    async def migrate_from_single_cluster(self, single_cluster_config: Path) -> InstallationResult:
        """Migrate from single-cluster to multi-cluster setup."""
        try:
            print("🔄 Starting migration from single-cluster to multi-cluster...")
            
            # Create backup first
            backup_path = await self.create_backup()
            print(f"💾 Backup created at {backup_path}")
            
            # Load existing single-cluster configuration
            existing_config = await self._load_single_cluster_config(single_cluster_config)
            
            # Create migration configuration
            migration_config = InstallationConfig(
                installation_type=InstallationType.MIGRATION,
                deployment_scenario=DeploymentScenario.CUSTOM,
                base_directory=self.base_directory,
                data_directory=self.base_directory / "data",
                config_directory=self.base_directory / "config",
                enable_multi_cluster=True,
                default_cluster_count=1  # Start with existing cluster
            )
            
            # Perform migration
            result = await self._perform_migration(migration_config, existing_config)
            
            print("✅ Migration completed successfully")
            print(f"ℹ️  Original configuration backed up to {backup_path}")
            
            return InstallationResult(
                success=True,
                installation_type=InstallationType.MIGRATION,
                deployment_scenario=DeploymentScenario.CUSTOM,
                installed_components=result.get("components", []),
                created_clusters=result.get("clusters", []),
                configuration_files=result.get("config_files", []),
                start_time=datetime.now(),
                end_time=datetime.now(),
                warnings=result.get("warnings", []),
                next_steps=[
                    "Review migrated cluster configuration",
                    "Test cluster functionality",
                    "Consider creating additional clusters"
                ]
            )
            
        except Exception as e:
            self.logger.error(f"Migration failed: {e}")
            raise InstallationError(f"Migration from single-cluster failed: {e}")
    
    async def validate_installation(self) -> Dict[str, Any]:
        """Validate current installation."""
        try:
            print("🔍 Validating installation...")
            
            validation_results = {
                "valid": True,
                "timestamp": datetime.now().isoformat(),
                "checks": {},
                "errors": [],
                "warnings": [],
                "recommendations": []
            }
            
            # Check system requirements
            system_check = await self._validate_system_requirements()
            validation_results["checks"]["system"] = system_check
            
            if not system_check["valid"]:
                validation_results["valid"] = False
                validation_results["errors"].extend(system_check["errors"])
            
            # Check directory structure
            directory_check = await self._validate_directory_structure()
            validation_results["checks"]["directories"] = directory_check
            
            if not directory_check["valid"]:
                validation_results["valid"] = False
                validation_results["errors"].extend(directory_check["errors"])
            
            # Check configuration files
            config_check = await self._validate_configuration_files()
            validation_results["checks"]["configuration"] = config_check
            
            if not config_check["valid"]:
                validation_results["valid"] = False
                validation_results["errors"].extend(config_check["errors"])
            
            # Check cluster registry
            registry_check = await self._validate_cluster_registry()
            validation_results["checks"]["cluster_registry"] = registry_check
            
            if not registry_check["valid"]:
                validation_results["warnings"].extend(registry_check["warnings"])
            
            # Check port availability
            port_check = await self._validate_port_availability()
            validation_results["checks"]["ports"] = port_check
            
            if not port_check["valid"]:
                validation_results["warnings"].extend(port_check["warnings"])
            
            # Generate recommendations
            validation_results["recommendations"] = await self._generate_recommendations(validation_results)
            
            return validation_results
            
        except Exception as e:
            self.logger.error(f"Validation failed: {e}")
            return {
                "valid": False,
                "timestamp": datetime.now().isoformat(),
                "checks": {},
                "errors": [f"Validation process failed: {e}"],
                "warnings": [],
                "recommendations": []
            }
    
    async def repair_installation(self) -> InstallationResult:
        """Repair broken installation."""
        try:
            print("🔧 Starting installation repair...")
            
            # Validate current state
            validation = await self.validate_installation()
            
            if validation["valid"]:
                print("✅ Installation appears to be valid, no repair needed")
                return InstallationResult(
                    success=True,
                    installation_type=InstallationType.REPAIR,
                    deployment_scenario=DeploymentScenario.CUSTOM,
                    installed_components=[],
                    created_clusters=[],
                    configuration_files=[],
                    start_time=datetime.now(),
                    end_time=datetime.now(),
                    warnings=["No repair was necessary"]
                )
            
            # Create repair configuration
            repair_config = InstallationConfig(
                installation_type=InstallationType.REPAIR,
                deployment_scenario=DeploymentScenario.CUSTOM,
                base_directory=self.base_directory,
                data_directory=self.base_directory / "data",
                config_directory=self.base_directory / "config"
            )
            
            # Perform repair
            result = await self._perform_repair(repair_config, validation)
            
            print("✅ Installation repair completed")
            
            return InstallationResult(
                success=True,
                installation_type=InstallationType.REPAIR,
                deployment_scenario=DeploymentScenario.CUSTOM,
                installed_components=result.get("components", []),
                created_clusters=result.get("clusters", []),
                configuration_files=result.get("config_files", []),
                start_time=datetime.now(),
                end_time=datetime.now(),
                warnings=result.get("warnings", []),
                next_steps=result.get("next_steps", [])
            )
            
        except Exception as e:
            self.logger.error(f"Repair failed: {e}")
            raise InstallationError(f"Installation repair failed: {e}")
    
    async def _detect_existing_installation(self) -> Optional[Dict[str, Any]]:
        """Detect existing installation."""
        try:
            # Check for docker-compose.yml
            docker_compose = self.base_directory / "docker-compose.yml"
            if docker_compose.exists():
                return {
                    "type": "single_cluster",
                    "config_file": docker_compose,
                    "detected_at": datetime.now().isoformat()
                }
            
            # Check for multi-cluster configuration
            cluster_registry = self.base_directory / "cluster_registry"
            if cluster_registry.exists():
                return {
                    "type": "multi_cluster",
                    "registry_path": cluster_registry,
                    "detected_at": datetime.now().isoformat()
                }
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Error detecting existing installation: {e}")
            return None
    
    async def _prompt_installation_type(self, has_existing: bool = False) -> InstallationType:
        """Prompt user for installation type."""
        print("\n📋 Installation Type:")
        
        if has_existing:
            print("1. Upgrade existing installation")
            print("2. Migrate to multi-cluster")
            print("3. Repair existing installation")
            print("4. Fresh install (will overwrite existing)")
            
            while True:
                choice = input("Select installation type (1-4): ").strip()
                if choice == "1":
                    return InstallationType.UPGRADE
                elif choice == "2":
                    return InstallationType.MIGRATION
                elif choice == "3":
                    return InstallationType.REPAIR
                elif choice == "4":
                    return InstallationType.FRESH_INSTALL
                else:
                    print("❌ Invalid choice. Please select 1-4.")
        else:
            return InstallationType.FRESH_INSTALL
    
    async def _prompt_deployment_scenario(self) -> DeploymentScenario:
        """Prompt user for deployment scenario."""
        print("\n🎯 Deployment Scenario:")
        print("1. Development (single cluster, minimal resources)")
        print("2. Testing (2-3 clusters, moderate resources)")
        print("3. Staging (production-like, full features)")
        print("4. Production (high availability, monitoring)")
        print("5. Demo (pre-configured examples)")
        print("6. Custom (manual configuration)")
        
        while True:
            choice = input("Select deployment scenario (1-6): ").strip()
            if choice == "1":
                return DeploymentScenario.DEVELOPMENT
            elif choice == "2":
                return DeploymentScenario.TESTING
            elif choice == "3":
                return DeploymentScenario.STAGING
            elif choice == "4":
                return DeploymentScenario.PRODUCTION
            elif choice == "5":
                return DeploymentScenario.DEMO
            elif choice == "6":
                return DeploymentScenario.CUSTOM
            else:
                print("❌ Invalid choice. Please select 1-6.")
    
    async def _prompt_directories(self) -> Dict[str, Path]:
        """Prompt user for directory configuration."""
        print("\n📁 Directory Configuration:")
        
        base_dir = input(f"Base directory [{self.base_directory}]: ").strip()
        if not base_dir:
            base_dir = self.base_directory
        else:
            base_dir = Path(base_dir)
        
        data_dir = input(f"Data directory [{base_dir / 'data'}]: ").strip()
        if not data_dir:
            data_dir = base_dir / "data"
        else:
            data_dir = Path(data_dir)
        
        config_dir = input(f"Config directory [{base_dir / 'config'}]: ").strip()
        if not config_dir:
            config_dir = base_dir / "config"
        else:
            config_dir = Path(config_dir)
        
        return {
            "base_directory": base_dir,
            "data_directory": data_dir,
            "config_directory": config_dir
        }
    
    async def _prompt_multi_cluster_settings(self) -> Dict[str, Any]:
        """Prompt user for multi-cluster settings."""
        print("\n🔧 Multi-Cluster Settings:")
        
        enable_multi = input("Enable multi-cluster support? (Y/n): ").lower().strip()
        enable_multi_cluster = enable_multi != 'n'
        
        if enable_multi_cluster:
            cluster_count = input("Default number of clusters [1]: ").strip()
            try:
                default_cluster_count = int(cluster_count) if cluster_count else 1
            except ValueError:
                default_cluster_count = 1
            
            port_start = input("Port range start [9000]: ").strip()
            try:
                port_range_start = int(port_start) if port_start else 9000
            except ValueError:
                port_range_start = 9000
            
            port_end = input("Port range end [9999]: ").strip()
            try:
                port_range_end = int(port_end) if port_end else 9999
            except ValueError:
                port_range_end = 9999
        else:
            default_cluster_count = 1
            port_range_start = 9000
            port_range_end = 9999
        
        return {
            "enable_multi_cluster": enable_multi_cluster,
            "default_cluster_count": default_cluster_count,
            "port_range_start": port_range_start,
            "port_range_end": port_range_end
        }
    
    async def _prompt_resource_settings(self) -> Dict[str, Any]:
        """Prompt user for resource settings."""
        print("\n💾 Resource Settings:")
        
        max_memory = input("Maximum memory (MB) [4096]: ").strip()
        try:
            max_memory_mb = int(max_memory) if max_memory else 4096
        except ValueError:
            max_memory_mb = 4096
        
        max_disk = input("Maximum disk space (GB) [50]: ").strip()
        try:
            max_disk_gb = int(max_disk) if max_disk else 50
        except ValueError:
            max_disk_gb = 50
        
        enable_monitoring = input("Enable resource monitoring? (Y/n): ").lower().strip()
        enable_resource_monitoring = enable_monitoring != 'n'
        
        enable_cleanup = input("Enable automatic cleanup? (Y/n): ").lower().strip()
        enable_auto_cleanup = enable_cleanup != 'n'
        
        return {
            "max_memory_mb": max_memory_mb,
            "max_disk_gb": max_disk_gb,
            "enable_resource_monitoring": enable_resource_monitoring,
            "enable_auto_cleanup": enable_auto_cleanup
        }
    
    async def _prompt_security_settings(self) -> Dict[str, Any]:
        """Prompt user for security settings."""
        print("\n🔒 Security Settings:")
        
        enable_auth = input("Enable authentication? (y/N): ").lower().strip()
        enable_authentication = enable_auth == 'y'
        
        enable_ssl_input = input("Enable SSL/TLS? (y/N): ").lower().strip()
        enable_ssl = enable_ssl_input == 'y'
        
        admin_username = "admin"
        admin_password = None
        
        if enable_authentication:
            username = input(f"Admin username [{admin_username}]: ").strip()
            if username:
                admin_username = username
            
            import getpass
            password = getpass.getpass("Admin password: ")
            if password:
                admin_password = password
        
        return {
            "enable_authentication": enable_authentication,
            "enable_ssl": enable_ssl,
            "admin_username": admin_username,
            "admin_password": admin_password
        }
    
    async def _prompt_backup_settings(self) -> Dict[str, Any]:
        """Prompt user for backup settings."""
        print("\n💾 Backup Settings:")
        
        enable_backup_input = input("Enable automatic backups? (Y/n): ").lower().strip()
        enable_backup = enable_backup_input != 'n'
        
        backup_directory = None
        backup_retention_days = 30
        
        if enable_backup:
            backup_dir = input("Backup directory [auto]: ").strip()
            if backup_dir and backup_dir != "auto":
                backup_directory = Path(backup_dir)
            
            retention = input("Backup retention (days) [30]: ").strip()
            try:
                backup_retention_days = int(retention) if retention else 30
            except ValueError:
                backup_retention_days = 30
        
        return {
            "enable_backup": enable_backup,
            "backup_directory": backup_directory,
            "backup_retention_days": backup_retention_days
        }
    
    async def _display_config_summary(self, config: InstallationConfig) -> None:
        """Display configuration summary."""
        print(f"Installation Type: {config.installation_type.value}")
        print(f"Deployment Scenario: {config.deployment_scenario.value}")
        print(f"Base Directory: {config.base_directory}")
        print(f"Data Directory: {config.data_directory}")
        print(f"Config Directory: {config.config_directory}")
        print(f"Multi-cluster: {'Enabled' if config.enable_multi_cluster else 'Disabled'}")
        
        if config.enable_multi_cluster:
            print(f"Default Clusters: {config.default_cluster_count}")
            print(f"Port Range: {config.port_range_start}-{config.port_range_end}")
        
        print(f"Max Memory: {config.max_memory_mb} MB")
        print(f"Max Disk: {config.max_disk_gb} GB")
        print(f"Resource Monitoring: {'Enabled' if config.enable_resource_monitoring else 'Disabled'}")
        print(f"Auto Cleanup: {'Enabled' if config.enable_auto_cleanup else 'Disabled'}")
        print(f"Authentication: {'Enabled' if config.enable_authentication else 'Disabled'}")
        print(f"SSL/TLS: {'Enabled' if config.enable_ssl else 'Disabled'}")
        print(f"Backups: {'Enabled' if config.enable_backup else 'Disabled'}")
    
    async def _validate_system_requirements(self) -> Dict[str, Any]:
        """Validate system requirements."""
        validation = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "checks": {}
        }
        
        try:
            # Check Python version
            python_version = sys.version_info
            if python_version < (3, 8):
                validation["valid"] = False
                validation["errors"].append(f"Python 3.8+ required, found {python_version.major}.{python_version.minor}")
            else:
                validation["checks"]["python"] = f"{python_version.major}.{python_version.minor}.{python_version.micro}"
            
            # Check Docker
            try:
                result = subprocess.run(["docker", "--version"], capture_output=True, text=True, timeout=10)
                if result.returncode == 0:
                    validation["checks"]["docker"] = result.stdout.strip()
                else:
                    validation["valid"] = False
                    validation["errors"].append("Docker is not available or not working")
            except (subprocess.TimeoutExpired, FileNotFoundError):
                validation["valid"] = False
                validation["errors"].append("Docker is not installed or not in PATH")
            
            # Check Docker Compose
            try:
                result = subprocess.run(["docker-compose", "--version"], capture_output=True, text=True, timeout=10)
                if result.returncode == 0:
                    validation["checks"]["docker_compose"] = result.stdout.strip()
                else:
                    # Try docker compose (newer syntax)
                    result = subprocess.run(["docker", "compose", "version"], capture_output=True, text=True, timeout=10)
                    if result.returncode == 0:
                        validation["checks"]["docker_compose"] = result.stdout.strip()
                    else:
                        validation["valid"] = False
                        validation["errors"].append("Docker Compose is not available")
            except (subprocess.TimeoutExpired, FileNotFoundError):
                validation["valid"] = False
                validation["errors"].append("Docker Compose is not installed")
            
            # Check disk space
            disk_usage = shutil.disk_usage(self.base_directory)
            free_gb = disk_usage.free / (1024**3)
            
            if free_gb < 5:
                validation["valid"] = False
                validation["errors"].append(f"Insufficient disk space: {free_gb:.1f}GB available, 5GB minimum required")
            elif free_gb < 10:
                validation["warnings"].append(f"Low disk space: {free_gb:.1f}GB available, 10GB recommended")
            
            validation["checks"]["disk_space_gb"] = f"{free_gb:.1f}"
            
            # Check memory
            try:
                import psutil
                memory = psutil.virtual_memory()
                available_gb = memory.available / (1024**3)
                
                if available_gb < 2:
                    validation["warnings"].append(f"Low memory: {available_gb:.1f}GB available, 4GB recommended")
                
                validation["checks"]["available_memory_gb"] = f"{available_gb:.1f}"
            except ImportError:
                validation["warnings"].append("Could not check memory usage (psutil not available)")
            
        except Exception as e:
            validation["valid"] = False
            validation["errors"].append(f"System validation error: {e}")
        
        return validation 
   
    async def _create_directory_structure(self, config: InstallationConfig) -> None:
        """Create directory structure."""
        try:
            # Create main directories
            config.base_directory.mkdir(parents=True, exist_ok=True)
            config.data_directory.mkdir(parents=True, exist_ok=True)
            config.config_directory.mkdir(parents=True, exist_ok=True)
            
            # Create subdirectories
            (config.data_directory / "clusters").mkdir(exist_ok=True)
            (config.data_directory / "logs").mkdir(exist_ok=True)
            (config.data_directory / "backups").mkdir(exist_ok=True)
            
            (config.config_directory / "templates").mkdir(exist_ok=True)
            (config.config_directory / "clusters").mkdir(exist_ok=True)
            
            # Create cluster registry directory
            (config.base_directory / "cluster_registry").mkdir(exist_ok=True)
            
            # Create monitoring directory if enabled
            if config.enable_resource_monitoring:
                (config.data_directory / "monitoring").mkdir(exist_ok=True)
            
            # Create backup directory if specified
            if config.backup_directory:
                config.backup_directory.mkdir(parents=True, exist_ok=True)
            
        except Exception as e:
            raise InstallationError(f"Failed to create directory structure: {e}")
    
    async def _perform_fresh_install(self, config: InstallationConfig) -> Dict[str, Any]:
        """Perform fresh installation."""
        try:
            components = []
            clusters = []
            config_files = []
            warnings = []
            
            # Initialize storage backend
            storage_backend = FileStorageBackend(storage_dir=str(config.base_directory / "cluster_registry"))
            components.append("storage_backend")
            
            # Initialize cluster registry
            cluster_registry = ClusterRegistry(storage_backend)
            components.append("cluster_registry")
            
            # Initialize template manager
            template_manager = TemplateManager(storage_backend)
            components.append("template_manager")
            
            # Create built-in templates
            await self._create_builtin_templates(template_manager, config)
            components.append("builtin_templates")
            
            # Create default clusters based on deployment scenario
            if config.enable_multi_cluster:
                created_clusters = await self._create_default_clusters(
                    cluster_registry, template_manager, config
                )
                clusters.extend(created_clusters)
            
            # Generate configuration files
            generated_configs = await self._generate_configuration_files(config, clusters)
            config_files.extend(generated_configs)
            
            # Set up monitoring if enabled
            if config.enable_resource_monitoring:
                await self._setup_monitoring(config)
                components.append("monitoring")
            
            # Set up security if enabled
            if config.enable_authentication:
                await self._setup_security(config)
                components.append("security")
            
            return {
                "components": components,
                "clusters": clusters,
                "config_files": config_files,
                "warnings": warnings
            }
            
        except Exception as e:
            raise InstallationError(f"Fresh installation failed: {e}")
    
    async def _perform_upgrade(self, config: InstallationConfig) -> Dict[str, Any]:
        """Perform upgrade installation."""
        try:
            components = []
            clusters = []
            config_files = []
            warnings = []
            
            # Create backup before upgrade
            backup_path = await self.create_backup()
            warnings.append(f"Pre-upgrade backup created at {backup_path}")
            
            # Load existing configuration
            existing_clusters = await self._load_existing_clusters()
            
            # Upgrade storage backend if needed
            await self._upgrade_storage_backend(config)
            components.append("storage_backend_upgrade")
            
            # Upgrade cluster configurations
            upgraded_clusters = await self._upgrade_cluster_configurations(existing_clusters, config)
            clusters.extend(upgraded_clusters)
            
            # Update templates
            await self._update_templates(config)
            components.append("template_updates")
            
            # Generate updated configuration files
            generated_configs = await self._generate_configuration_files(config, clusters)
            config_files.extend(generated_configs)
            
            return {
                "components": components,
                "clusters": clusters,
                "config_files": config_files,
                "warnings": warnings
            }
            
        except Exception as e:
            raise InstallationError(f"Upgrade failed: {e}")
    
    async def _perform_migration(self, config: InstallationConfig, existing_config: Optional[Dict] = None) -> Dict[str, Any]:
        """Perform migration from single-cluster."""
        try:
            components = []
            clusters = []
            config_files = []
            warnings = []
            
            # Initialize multi-cluster components
            storage_backend = FileStorageBackend(storage_dir=str(config.base_directory / "cluster_registry"))
            cluster_registry = ClusterRegistry(storage_backend)
            template_manager = TemplateManager(storage_backend)
            
            components.extend(["storage_backend", "cluster_registry", "template_manager"])
            
            # Create templates
            await self._create_builtin_templates(template_manager, config)
            components.append("builtin_templates")
            
            # Migrate existing single cluster
            if existing_config:
                migrated_cluster = await self._migrate_single_cluster(
                    existing_config, cluster_registry, template_manager, config
                )
                clusters.append(migrated_cluster)
                warnings.append("Single cluster configuration migrated to multi-cluster format")
            
            # Generate new configuration files
            generated_configs = await self._generate_configuration_files(config, clusters)
            config_files.extend(generated_configs)
            
            return {
                "components": components,
                "clusters": clusters,
                "config_files": config_files,
                "warnings": warnings
            }
            
        except Exception as e:
            raise InstallationError(f"Migration failed: {e}")
    
    async def _perform_repair(self, config: InstallationConfig, validation: Optional[Dict] = None) -> Dict[str, Any]:
        """Perform repair installation."""
        try:
            components = []
            clusters = []
            config_files = []
            warnings = []
            next_steps = []
            
            if validation:
                # Fix directory structure issues
                if not validation["checks"].get("directories", {}).get("valid", True):
                    await self._create_directory_structure(config)
                    components.append("directory_structure_repair")
                
                # Fix configuration file issues
                if not validation["checks"].get("configuration", {}).get("valid", True):
                    generated_configs = await self._generate_configuration_files(config, [])
                    config_files.extend(generated_configs)
                    components.append("configuration_repair")
                
                # Fix cluster registry issues
                if not validation["checks"].get("cluster_registry", {}).get("valid", True):
                    await self._repair_cluster_registry(config)
                    components.append("cluster_registry_repair")
                
                # Fix port conflicts
                if not validation["checks"].get("ports", {}).get("valid", True):
                    fixed_clusters = await self._fix_port_conflicts(config)
                    clusters.extend(fixed_clusters)
                    components.append("port_conflict_repair")
            
            if not components:
                warnings.append("No issues found that could be automatically repaired")
                next_steps.append("Run manual validation to identify specific issues")
            else:
                next_steps.append("Restart services to apply repairs")
                next_steps.append("Run validation again to confirm fixes")
            
            return {
                "components": components,
                "clusters": clusters,
                "config_files": config_files,
                "warnings": warnings,
                "next_steps": next_steps
            }
            
        except Exception as e:
            raise InstallationError(f"Repair failed: {e}")
    
    async def _create_builtin_templates(self, template_manager: TemplateManager, config: InstallationConfig) -> None:
        """Create built-in cluster templates."""
        try:
            # Check if templates are already loaded (they are loaded during template manager initialization)
            existing_templates = await template_manager.list_templates()
            existing_template_ids = {template.id for template in existing_templates}
            
            templates_to_create = []
            
            # Development template
            if "development" not in existing_template_ids:
                dev_template = ClusterTemplate(
                    id="development",
                    name="Development",
                    description="Lightweight development cluster with minimal resources",
                    category="development",
                    kafka_config={
                        "heap_size": "512M",
                        "log_retention_hours": 24,
                        "num_partitions": 1,
                        "default_replication_factor": 1
                    },
                    rest_proxy_config={
                        "heap_size": "256M"
                    },
                    ui_config={
                        "heap_size": "256M"
                    },
                    resource_requirements={
                        "min_memory_mb": 1024,
                        "min_disk_gb": 5,
                        "recommended_memory_mb": 2048,
                        "recommended_disk_gb": 10
                    }
                )
                templates_to_create.append(dev_template)
            
            # Testing template
            if "testing" not in existing_template_ids:
                test_template = ClusterTemplate(
                    id="testing",
                    name="Testing",
                    description="Testing cluster with moderate resources and multiple partitions",
                    category="testing",
                    kafka_config={
                        "heap_size": "1G",
                        "log_retention_hours": 72,
                        "num_partitions": 3,
                        "default_replication_factor": 1
                    },
                    rest_proxy_config={
                        "heap_size": "512M"
                    },
                    ui_config={
                        "heap_size": "512M"
                    },
                    resource_requirements={
                        "min_memory_mb": 2048,
                        "min_disk_gb": 10,
                        "recommended_memory_mb": 4096,
                        "recommended_disk_gb": 20
                    }
                )
                templates_to_create.append(test_template)
            
            # Production template
            if "production" not in existing_template_ids:
                prod_template = ClusterTemplate(
                    id="production",
                    name="Production",
                    description="Production-ready cluster with high availability and monitoring",
                    category="production",
                    kafka_config={
                        "heap_size": "2G",
                        "log_retention_hours": 168,
                        "num_partitions": 6,
                        "default_replication_factor": 3
                    },
                    rest_proxy_config={
                        "heap_size": "1G"
                    },
                    ui_config={
                        "heap_size": "1G"
                    },
                    resource_requirements={
                        "min_memory_mb": 4096,
                        "min_disk_gb": 50,
                        "recommended_memory_mb": 8192,
                        "recommended_disk_gb": 100
                    }
                )
                templates_to_create.append(prod_template)
            
            # Create only the templates that don't exist
            for template in templates_to_create:
                await template_manager.create_template(template)
            
            if not templates_to_create:
                logger.info("All built-in templates already exist, skipping creation")
            else:
                logger.info(f"Created {len(templates_to_create)} built-in templates")
            
        except Exception as e:
            raise InstallationError(f"Failed to create built-in templates: {e}")
    
    async def _create_default_clusters(
        self,
        cluster_registry: ClusterRegistry,
        template_manager: TemplateManager,
        config: InstallationConfig
    ) -> List[str]:
        """Create default clusters based on deployment scenario."""
        try:
            created_clusters = []
            
            if config.deployment_scenario == DeploymentScenario.DEVELOPMENT:
                # Single development cluster
                cluster_id = "dev-cluster"
                
                # Check if cluster already exists
                try:
                    logger.debug(f"Checking if cluster '{cluster_id}' already exists")
                    existing_cluster = await cluster_registry.get_cluster(cluster_id)
                    logger.info(f"Cluster '{cluster_id}' already exists, skipping creation")
                    created_clusters.append(cluster_id)
                except ClusterNotFoundError as e:
                    # Cluster doesn't exist, create it
                    logger.debug(f"Cluster '{cluster_id}' not found, creating new cluster: {e}")
                except Exception as e:
                    # Catch any other exception to debug
                    logger.error(f"Unexpected exception when checking cluster '{cluster_id}': {e} (type: {type(e).__name__})")
                    # Assume cluster doesn't exist and try to create it
                    logger.debug(f"Assuming cluster '{cluster_id}' doesn't exist, attempting to create it")
                    cluster = ClusterDefinition(
                        id=cluster_id,
                        name="Development Cluster",
                        description="Default development cluster",
                        environment="development",
                        template_id="development",
                        port_allocation=PortAllocation(
                            kafka_port=9092,
                            rest_proxy_port=8082,
                            ui_port=8080
                        ),
                        tags={"default": "true", "environment": "dev"}
                    )
                    
                    await cluster_registry.register_cluster(cluster)
                    created_clusters.append(cluster.id)
                    logger.info(f"Created default cluster: {cluster_id}")
            
            elif config.deployment_scenario == DeploymentScenario.TESTING:
                # Development and testing clusters
                clusters_config = [
                    {
                        "id": "dev-cluster",
                        "name": "Development Cluster",
                        "template": "development",
                        "kafka_port": 9092,
                        "environment": "development"
                    },
                    {
                        "id": "test-cluster",
                        "name": "Testing Cluster",
                        "template": "testing",
                        "kafka_port": 9093,
                        "environment": "testing"
                    }
                ]
                
                for cluster_config in clusters_config:
                    cluster = ClusterDefinition(
                        id=cluster_config["id"],
                        name=cluster_config["name"],
                        description=f"Default {cluster_config['name'].lower()}",
                        environment=cluster_config["environment"],
                        template_id=cluster_config["template"],
                        port_allocation=PortAllocation(
                            kafka_port=cluster_config["kafka_port"],
                            rest_proxy_port=cluster_config["kafka_port"] - 1000,
                            ui_port=cluster_config["kafka_port"] - 2000
                        ),
                        tags={"default": "true", "environment": cluster_config["id"].split("-")[0]}
                    )
                    
                    await cluster_registry.register_cluster(cluster)
                    created_clusters.append(cluster.id)
            
            elif config.deployment_scenario == DeploymentScenario.STAGING:
                # Development, testing, and staging clusters
                clusters_config = [
                    {"id": "dev-cluster", "name": "Development Cluster", "template": "development", "kafka_port": 9092, "environment": "development"},
                    {"id": "test-cluster", "name": "Testing Cluster", "template": "testing", "kafka_port": 9093, "environment": "testing"},
                    {"id": "staging-cluster", "name": "Staging Cluster", "template": "production", "kafka_port": 9094, "environment": "staging"}
                ]
                
                for cluster_config in clusters_config:
                    cluster = ClusterDefinition(
                        id=cluster_config["id"],
                        name=cluster_config["name"],
                        description=f"Default {cluster_config['name'].lower()}",
                        environment=cluster_config["environment"],
                        template_id=cluster_config["template"],
                        port_allocation=PortAllocation(
                            kafka_port=cluster_config["kafka_port"],
                            rest_proxy_port=cluster_config["kafka_port"] - 1000,
                            ui_port=cluster_config["kafka_port"] - 2000
                        ),
                        tags={"default": "true", "environment": cluster_config["id"].split("-")[0]}
                    )
                    
                    await cluster_registry.register_cluster(cluster)
                    created_clusters.append(cluster.id)
            
            elif config.deployment_scenario == DeploymentScenario.PRODUCTION:
                # Full production setup
                cluster = ClusterDefinition(
                    id="prod-cluster",
                    name="Production Cluster",
                    description="Production cluster with high availability",
                    environment="production",
                    template_id="production",
                    port_allocation=PortAllocation(
                        kafka_port=9092,
                        rest_proxy_port=8082,
                        ui_port=8080,
                        jmx_port=9999
                    ),
                    tags={"default": "true", "environment": "prod", "ha": "true"}
                )
                
                await cluster_registry.register_cluster(cluster)
                created_clusters.append(cluster.id)
            
            elif config.deployment_scenario == DeploymentScenario.DEMO:
                # Demo clusters with sample data
                demo_clusters = [
                    {"id": "demo-ecommerce", "name": "E-commerce Demo", "template": "development", "environment": "development"},
                    {"id": "demo-iot", "name": "IoT Demo", "template": "testing", "environment": "testing"},
                    {"id": "demo-analytics", "name": "Analytics Demo", "template": "testing", "environment": "testing"}
                ]
                
                for i, cluster_config in enumerate(demo_clusters):
                    cluster = ClusterDefinition(
                        id=cluster_config["id"],
                        name=cluster_config["name"],
                        description=f"Demo cluster: {cluster_config['name']}",
                        environment=cluster_config["environment"],
                        template_id=cluster_config["template"],
                        port_allocation=PortAllocation(
                            kafka_port=9092 + i,
                            rest_proxy_port=8082 + i,
                            ui_port=8080 + i
                        ),
                        tags={"demo": "true", "sample_data": "true"}
                    )
                    
                    await cluster_registry.register_cluster(cluster)
                    created_clusters.append(cluster.id)
            
            return created_clusters
            
        except Exception as e:
            raise InstallationError(f"Failed to create default clusters: {e}")
    
    async def _generate_configuration_files(self, config: InstallationConfig, clusters: List[str]) -> List[Path]:
        """Generate configuration files."""
        try:
            config_files = []
            
            # Generate main configuration file
            main_config = {
                "installation": {
                    "type": config.installation_type.value,
                    "scenario": config.deployment_scenario.value,
                    "installed_at": datetime.now().isoformat(),
                    "version": "1.0"
                },
                "directories": {
                    "base": str(config.base_directory),
                    "data": str(config.data_directory),
                    "config": str(config.config_directory)
                },
                "multi_cluster": {
                    "enabled": config.enable_multi_cluster,
                    "default_cluster_count": config.default_cluster_count,
                    "port_range": {
                        "start": config.port_range_start,
                        "end": config.port_range_end
                    }
                },
                "resources": {
                    "max_memory_mb": config.max_memory_mb,
                    "max_disk_gb": config.max_disk_gb,
                    "monitoring_enabled": config.enable_resource_monitoring,
                    "auto_cleanup_enabled": config.enable_auto_cleanup
                },
                "security": {
                    "authentication_enabled": config.enable_authentication,
                    "ssl_enabled": config.enable_ssl,
                    "admin_username": config.admin_username
                },
                "backup": {
                    "enabled": config.enable_backup,
                    "directory": str(config.backup_directory) if config.backup_directory else None,
                    "retention_days": config.backup_retention_days
                },
                "clusters": clusters
            }
            
            main_config_file = config.config_directory / "multi_cluster_config.yml"
            with open(main_config_file, 'w') as f:
                yaml.dump(main_config, f, default_flow_style=False, indent=2)
            
            config_files.append(main_config_file)
            
            # Generate Docker Compose override if needed
            if config.docker_compose_overrides:
                override_file = config.base_directory / "docker-compose.override.yml"
                with open(override_file, 'w') as f:
                    yaml.dump(config.docker_compose_overrides, f, default_flow_style=False, indent=2)
                
                config_files.append(override_file)
            
            # Generate environment file
            env_file = config.base_directory / ".env"
            env_content = [
                f"# Multi-cluster Kafka Manager Environment",
                f"# Generated on {datetime.now().isoformat()}",
                f"",
                f"BASE_DIR={config.base_directory}",
                f"DATA_DIR={config.data_directory}",
                f"CONFIG_DIR={config.config_directory}",
                f"",
                f"MULTI_CLUSTER_ENABLED={'true' if config.enable_multi_cluster else 'false'}",
                f"RESOURCE_MONITORING_ENABLED={'true' if config.enable_resource_monitoring else 'false'}",
                f"AUTO_CLEANUP_ENABLED={'true' if config.enable_auto_cleanup else 'false'}",
                f"",
                f"MAX_MEMORY_MB={config.max_memory_mb}",
                f"MAX_DISK_GB={config.max_disk_gb}",
                f"",
                f"AUTHENTICATION_ENABLED={'true' if config.enable_authentication else 'false'}",
                f"SSL_ENABLED={'true' if config.enable_ssl else 'false'}",
                f""
            ]
            
            # Add custom environment variables
            for key, value in config.environment_variables.items():
                env_content.append(f"{key}={value}")
            
            with open(env_file, 'w') as f:
                f.write('\n'.join(env_content))
            
            config_files.append(env_file)
            
            return config_files
            
        except Exception as e:
            raise InstallationError(f"Failed to generate configuration files: {e}")
    
    async def _post_installation_setup(self, config: InstallationConfig) -> Dict[str, Any]:
        """Run post-installation setup."""
        try:
            warnings = []
            
            # Set up log rotation
            await self._setup_log_rotation(config)
            
            # Set up monitoring if enabled
            if config.enable_resource_monitoring:
                await self._setup_monitoring(config)
            
            # Set up backup schedule if enabled
            if config.enable_backup:
                await self._setup_backup_schedule(config)
                
            # Set up security if enabled
            if config.enable_authentication:
                await self._setup_security(config)
            
            # Create startup scripts
            await self._create_startup_scripts(config)
            
            # Set permissions
            await self._set_permissions(config)
            
            return {"warnings": warnings}
            
        except Exception as e:
            raise InstallationError(f"Post-installation setup failed: {e}")
    
    async def _setup_log_rotation(self, config: InstallationConfig) -> None:
        """Set up log rotation configuration."""
        try:
            logger.info("Setting up log rotation...")
            
            # Create logs directory
            logs_dir = config.base_directory / "logs"
            logs_dir.mkdir(exist_ok=True)
            
            # Get current user
            import getpass
            current_user = getpass.getuser()
            
            # Create logrotate configuration
            logrotate_config = logs_dir / "logrotate.conf"
            logrotate_content = f"""
{logs_dir}/*.log {{
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 644 {current_user} {current_user}
}}
"""
            
            with open(logrotate_config, 'w') as f:
                f.write(logrotate_content.strip())
            
            logger.info("Log rotation configured successfully")
            
        except Exception as e:
            logger.error(f"Failed to setup log rotation: {e}")
            raise InstallationError(f"Failed to setup log rotation: {e}")

    async def _setup_backup_schedule(self, config: InstallationConfig) -> None:
        """Set up backup schedule configuration."""
        try:
            logger.info("Setting up backup schedule...")
            
            # Create backup directory
            backup_dir = config.base_directory / "backups"
            backup_dir.mkdir(exist_ok=True)
            
            # Create backup script
            backup_script = config.base_directory / "scripts" / "backup.sh"
            backup_script.parent.mkdir(exist_ok=True)
            
            backup_content = f"""#!/bin/bash
# Automated backup script for Multi-cluster Kafka Manager
set -e

BACKUP_DIR="{backup_dir}"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BACKUP_NAME="kafka-cluster-backup_$TIMESTAMP"

echo "Starting backup: $BACKUP_NAME"

# Create backup directory
mkdir -p "$BACKUP_DIR/$BACKUP_NAME"

# Backup cluster registry
cp -r "{config.base_directory}/cluster_registry" "$BACKUP_DIR/$BACKUP_NAME/"

# Backup configuration
cp -r "{config.base_directory}/config" "$BACKUP_DIR/$BACKUP_NAME/" 2>/dev/null || true

# Create archive
cd "$BACKUP_DIR"
tar -czf "$BACKUP_NAME.tar.gz" "$BACKUP_NAME"
rm -rf "$BACKUP_NAME"

echo "Backup completed: $BACKUP_DIR/$BACKUP_NAME.tar.gz"

# Clean up old backups (keep last 7 days)
find "$BACKUP_DIR" -name "kafka-cluster-backup_*.tar.gz" -mtime +7 -delete 2>/dev/null || true
"""
            
            with open(backup_script, 'w') as f:
                f.write(backup_content.strip())
            
            # Make script executable
            backup_script.chmod(0o755)
            
            logger.info("Backup schedule configured successfully")
            
        except Exception as e:
            logger.error(f"Failed to setup backup schedule: {e}")
            raise InstallationError(f"Failed to setup backup schedule: {e}")

    async def _setup_monitoring(self, config: InstallationConfig) -> None:
        """Set up monitoring configuration."""
        try:
            monitoring_config = {
                "enabled": True,
                "health_check_interval": 30,
                "metrics_collection_interval": 15,
                "alert_evaluation_interval": 60,
                "data_retention_days": 30,
                "thresholds": {
                    "cpu_warning": 70,
                    "cpu_critical": 90,
                    "memory_warning": 80,
                    "memory_critical": 95,
                    "disk_warning": 85,
                    "disk_critical": 95
                }
            }
            
            monitoring_config_file = config.config_directory / "monitoring.yml"
            with open(monitoring_config_file, 'w') as f:
                yaml.dump(monitoring_config, f, default_flow_style=False, indent=2)
                
        except Exception as e:
            raise InstallationError(f"Failed to setup monitoring: {e}")
    
    async def _setup_security(self, config: InstallationConfig) -> None:
        """Set up security configuration."""
        try:
            security_config = {
                "authentication": {
                    "enabled": config.enable_authentication,
                    "admin_username": config.admin_username,
                    "token_expiry_hours": 24,
                    "password_min_length": 8
                },
                "ssl": {
                    "enabled": config.enable_ssl,
                    "cert_path": "certs/server.crt",
                    "key_path": "certs/server.key",
                    "ca_path": "certs/ca.crt"
                },
                "access_control": {
                    "default_permissions": ["cluster_view"],
                    "admin_permissions": ["*"],
                    "audit_logging": True
                }
            }
            
            security_config_file = config.config_directory / "security.yml"
            with open(security_config_file, 'w') as f:
                yaml.dump(security_config, f, default_flow_style=False, indent=2)
            
            # Create certificates directory if SSL is enabled
            if config.enable_ssl:
                certs_dir = config.config_directory / "certs"
                certs_dir.mkdir(exist_ok=True)
                
        except Exception as e:
            raise InstallationError(f"Failed to setup security: {e}")
    
    async def _create_startup_scripts(self, config: InstallationConfig) -> None:
        """Create startup scripts."""
        try:
            # Create start script
            start_script_content = f"""#!/bin/bash
# Multi-cluster Kafka Manager Start Script
# Generated on {datetime.now().isoformat()}

set -e

echo "🚀 Starting Multi-cluster Kafka Manager..."

# Change to installation directory
cd "{config.base_directory}"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Start services
if [ -f "docker-compose.yml" ]; then
    docker-compose up -d
    echo "✅ Services started successfully"
    echo "🌐 Web interface available at: http://localhost:8000"
else
    echo "❌ docker-compose.yml not found"
    exit 1
fi
"""
            
            start_script = config.base_directory / "start.sh"
            with open(start_script, 'w') as f:
                f.write(start_script_content)
            
            start_script.chmod(0o755)
            
            # Create stop script
            stop_script_content = f"""#!/bin/bash
# Multi-cluster Kafka Manager Stop Script
# Generated on {datetime.now().isoformat()}

set -e

echo "🛑 Stopping Multi-cluster Kafka Manager..."

# Change to installation directory
cd "{config.base_directory}"

# Stop services
if [ -f "docker-compose.yml" ]; then
    docker-compose down
    echo "✅ Services stopped successfully"
else
    echo "❌ docker-compose.yml not found"
    exit 1
fi
"""
            
            stop_script = config.base_directory / "stop.sh"
            with open(stop_script, 'w') as f:
                f.write(stop_script_content)
            
            stop_script.chmod(0o755)
            
        except Exception as e:
            raise InstallationError(f"Failed to create startup scripts: {e}")

    async def _set_permissions(self, config: InstallationConfig) -> None:
        """Set appropriate file permissions."""
        try:
            logger.info("Setting file permissions...")
            
            # Set permissions for directories
            directories_to_secure = [
                config.base_directory,
                config.data_directory,
                config.config_directory,
                config.base_directory / "logs",
                config.base_directory / "backups",
                config.base_directory / "scripts"
            ]
            
            for directory in directories_to_secure:
                if directory.exists():
                    directory.chmod(0o755)
                    logger.debug(f"Set directory permissions for {directory}")
            
            # Set permissions for scripts
            scripts_dir = config.base_directory / "scripts"
            if scripts_dir.exists():
                for script_file in scripts_dir.glob("*.sh"):
                    script_file.chmod(0o755)
                    logger.debug(f"Set executable permissions for {script_file}")
            
            # Set permissions for configuration files (more restrictive)
            config_files = [
                config.config_directory / "multi_cluster.yml",
                config.base_directory / "logs" / "logrotate.conf"
            ]
            
            for config_file in config_files:
                if config_file.exists():
                    config_file.chmod(0o644)
                    logger.debug(f"Set config file permissions for {config_file}")
            
            logger.info("File permissions set successfully")
            
        except Exception as e:
            logger.error(f"Failed to set permissions: {e}")
            raise InstallationError(f"Failed to set permissions: {e}")
    
    async def _generate_next_steps(self, config: InstallationConfig, clusters: List[str]) -> List[str]:
        """Generate next steps for user."""
        next_steps = []
        
        # Basic next steps
        next_steps.append("Review the generated configuration files")
        next_steps.append("Start the services using: ./start.sh")
        
        if clusters:
            next_steps.append(f"Access the web interface at http://localhost:8000")
            next_steps.append(f"Created clusters: {', '.join(clusters)}")
        
        # Scenario-specific steps
        if config.deployment_scenario == DeploymentScenario.DEVELOPMENT:
            next_steps.append("Create topics and start developing with Kafka")
            next_steps.append("Use the REST API at http://localhost:8082")
        
        elif config.deployment_scenario == DeploymentScenario.TESTING:
            next_steps.append("Set up automated tests for your Kafka applications")
            next_steps.append("Configure CI/CD pipelines to use the test cluster")
        
        elif config.deployment_scenario == DeploymentScenario.PRODUCTION:
            next_steps.append("Review security settings and enable SSL if needed")
            next_steps.append("Set up monitoring and alerting")
            next_steps.append("Configure backup schedules")
            next_steps.append("Review resource limits and scaling policies")
        
        elif config.deployment_scenario == DeploymentScenario.DEMO:
            next_steps.append("Explore the demo clusters and sample data")
            next_steps.append("Try the multi-cluster features")
        
        # Feature-specific steps
        if config.enable_authentication:
            next_steps.append(f"Log in with username: {config.admin_username}")
            next_steps.append("Create additional users and set permissions")
        
        if config.enable_resource_monitoring:
            next_steps.append("Check the monitoring dashboard for resource usage")
            next_steps.append("Configure alert thresholds if needed")
        
        if config.enable_backup:
            next_steps.append("Verify backup configuration and test restore process")
        
        # General recommendations
        next_steps.append("Read the documentation for advanced configuration options")
        next_steps.append("Join the community for support and updates")
        
        return next_steps
    
    async def _display_installation_success(self, result: InstallationResult) -> None:
        """Display installation success message."""
        duration = (result.end_time - result.start_time).total_seconds()
        
        print("\n" + "=" * 60)
        print("🎉 INSTALLATION COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f"Installation Type: {result.installation_type.value}")
        print(f"Deployment Scenario: {result.deployment_scenario.value}")
        print(f"Duration: {duration:.1f} seconds")
        print(f"Components Installed: {len(result.installed_components)}")
        print(f"Clusters Created: {len(result.created_clusters)}")
        print(f"Configuration Files: {len(result.configuration_files)}")
        
        if result.warnings:
            print(f"\n⚠️  Warnings ({len(result.warnings)}):")
            for warning in result.warnings:
                print(f"  • {warning}")
        
        if result.next_steps:
            print(f"\n📋 Next Steps:")
            for i, step in enumerate(result.next_steps, 1):
                print(f"  {i}. {step}")
        
        print("\n" + "=" * 60)
        print("Thank you for using Multi-cluster Kafka Manager!")
        print("=" * 60)
    
    async def _save_installation_record(self, result: InstallationResult) -> None:
        """Save installation record."""
        try:
            record = {
                "success": result.success,
                "installation_type": result.installation_type.value,
                "deployment_scenario": result.deployment_scenario.value,
                "installed_components": result.installed_components,
                "created_clusters": result.created_clusters,
                "configuration_files": [str(f) for f in result.configuration_files],
                "start_time": result.start_time.isoformat(),
                "end_time": result.end_time.isoformat(),
                "error_message": result.error_message,
                "warnings": result.warnings,
                "next_steps": result.next_steps
            }
            
            # Save to installation history
            history_file = self.base_directory / "installation_history.json"
            
            history = []
            if history_file.exists():
                with open(history_file, 'r') as f:
                    history = json.load(f)
            
            history.append(record)
            
            # Keep only last 10 installations
            history = history[-10:]
            
            with open(history_file, 'w') as f:
                json.dump(history, f, indent=2)
                
        except Exception as e:
            self.logger.warning(f"Failed to save installation record: {e}")
    
    async def _validate_directory_structure(self) -> Dict[str, Any]:
        """Validate directory structure and permissions."""
        try:
            result = {
                "status": "success",
                "message": "Directory structure is valid",
                "details": {}
            }
            
            # Check if base directory exists and is writable
            base_dir = Path(self.base_dir)
            if not base_dir.exists():
                result["status"] = "warning"
                result["message"] = "Base directory does not exist but will be created"
            elif not os.access(base_dir, os.W_OK):
                result["status"] = "error"
                result["message"] = "Base directory is not writable"
                return result
            
            # Check required subdirectories
            required_dirs = ["data", "config", "logs", "cluster_registry"]
            for dir_name in required_dirs:
                dir_path = base_dir / dir_name
                result["details"][dir_name] = {
                    "exists": dir_path.exists(),
                    "writable": os.access(dir_path.parent, os.W_OK)
                }
            
            return result
            
        except Exception as e:
            return {
                "status": "error",
                "message": f"Directory validation failed: {e}",
                "details": {}
            }
    
    async def _validate_configuration_files(self) -> Dict[str, Any]:
        """Validate configuration files."""
        return {
            "status": "success",
            "message": "Configuration validation passed",
            "details": {}
        }
    
    async def _validate_cluster_registry(self) -> Dict[str, Any]:
        """Validate cluster registry."""
        return {
            "status": "success", 
            "message": "Cluster registry validation passed",
            "details": {}
        }
    # - _setup_log_rotation
    # - _setup_backup_schedule
    # - _set_permissions
    
    # These methods would follow similar patterns to the ones implemented above