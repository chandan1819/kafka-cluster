"""
Configuration management service for multi-cluster support.

This module provides functionality for importing, exporting, validating,
and versioning cluster configurations using YAML/JSON files.
"""

import logging
import json
import yaml
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from dataclasses import asdict
import jsonschema
from jsonschema import validate, ValidationError as JsonSchemaValidationError

from ..models.multi_cluster import ClusterDefinition, ClusterTemplate
from ..exceptions import ValidationError, ConfigurationError
from ..storage.base import StorageBackend

logger = logging.getLogger(__name__)


class ConfigurationManager:
    """
    Manages cluster configuration import/export, validation, and versioning.
    
    Supports YAML and JSON formats with schema validation and version control.
    """
    
    def __init__(self, storage_backend: StorageBackend, config_dir: Optional[Path] = None):
        """
        Initialize configuration manager.
        
        Args:
            storage_backend: Storage backend for persistence
            config_dir: Directory for configuration files
        """
        self.storage = storage_backend
        self.config_dir = config_dir or Path("cluster-configs")
        self.config_dir.mkdir(exist_ok=True)
        
        # Configuration versioning
        self.versions_dir = self.config_dir / "versions"
        self.versions_dir.mkdir(exist_ok=True)
        
        # Schema for validation
        self._cluster_schema = self._load_cluster_schema()
        
        logger.info(f"Configuration manager initialized with config dir: {self.config_dir}")
    
    def _load_cluster_schema(self) -> Dict[str, Any]:
        """Load JSON schema for cluster configuration validation."""
        return {
            "type": "object",
            "properties": {
                "id": {"type": "string", "pattern": "^[a-z0-9-]+$"},
                "name": {"type": "string", "minLength": 1},
                "description": {"type": ["string", "null"]},
                "environment": {
                    "type": "string",
                    "enum": ["development", "testing", "staging", "production"]
                },
                "template_id": {"type": ["string", "null"]},
                "kafka_config": {
                    "type": "object",
                    "properties": {
                        "heap_size": {"type": "string"},
                        "log_retention_hours": {"type": "integer", "minimum": 1},
                        "log_segment_bytes": {"type": "integer", "minimum": 1024},
                        "num_partitions": {"type": "integer", "minimum": 1},
                        "default_replication_factor": {"type": "integer", "minimum": 1},
                        "custom_properties": {"type": "object"}
                    }
                },
                "rest_proxy_config": {
                    "type": "object",
                    "properties": {
                        "heap_size": {"type": "string"},
                        "consumer_request_timeout_ms": {"type": "integer", "minimum": 1000},
                        "producer_request_timeout_ms": {"type": "integer", "minimum": 1000},
                        "custom_properties": {"type": "object"}
                    }
                },
                "ui_config": {
                    "type": "object",
                    "properties": {
                        "heap_size": {"type": "string"},
                        "readonly_mode": {"type": "boolean"},
                        "topic_creation_enabled": {"type": "boolean"},
                        "topic_deletion_enabled": {"type": "boolean"},
                        "clusters_config": {"type": "object"}
                    }
                },
                "port_allocation": {
                    "type": "object",
                    "properties": {
                        "kafka_port": {"type": "integer", "minimum": 1024, "maximum": 65535},
                        "rest_proxy_port": {"type": "integer", "minimum": 1024, "maximum": 65535},
                        "ui_port": {"type": "integer", "minimum": 1024, "maximum": 65535},
                        "jmx_port": {"type": ["integer", "null"], "minimum": 1024, "maximum": 65535}
                    },
                    "required": ["kafka_port", "rest_proxy_port", "ui_port"]
                },
                "tags": {"type": "object"},
                "retention_policy": {
                    "type": "object",
                    "properties": {
                        "default_retention_ms": {"type": "integer", "minimum": 1000},
                        "max_message_bytes": {"type": "integer", "minimum": 1024},
                        "cleanup_policy": {"type": "string", "enum": ["delete", "compact"]}
                    }
                }
            },
            "required": ["id", "name", "environment", "port_allocation"],
            "additionalProperties": False
        }
    
    async def export_cluster_config(
        self, 
        cluster_id: str, 
        format: str = "yaml",
        include_metadata: bool = True
    ) -> str:
        """
        Export cluster configuration to YAML or JSON format.
        
        Args:
            cluster_id: ID of cluster to export
            format: Export format ('yaml' or 'json')
            include_metadata: Whether to include metadata fields
            
        Returns:
            str: Serialized configuration
            
        Raises:
            ConfigurationError: If cluster not found or export fails
        """
        try:
            # Get cluster definition from storage
            cluster = await self.storage.load_cluster(cluster_id)
            if cluster is None:
                raise ConfigurationError(f"Cluster '{cluster_id}' not found")
            
            # Convert to dictionary
            config_dict = self._cluster_to_dict(cluster, include_metadata)
            
            # Serialize based on format
            if format.lower() == "yaml":
                return yaml.dump(config_dict, default_flow_style=False, sort_keys=False)
            elif format.lower() == "json":
                return json.dumps(config_dict, indent=2, default=str)
            else:
                raise ConfigurationError(f"Unsupported format: {format}")
                
        except Exception as e:
            logger.error(f"Failed to export cluster config for {cluster_id}: {e}")
            raise ConfigurationError(f"Export failed: {e}")
    
    async def import_cluster_config(
        self, 
        config_content: str, 
        format: str = "auto",
        validate_schema: bool = True,
        create_version: bool = True
    ) -> ClusterDefinition:
        """
        Import cluster configuration from YAML or JSON.
        
        Args:
            config_content: Configuration content as string
            format: Format ('yaml', 'json', or 'auto' for auto-detection)
            validate_schema: Whether to validate against schema
            create_version: Whether to create a version backup
            
        Returns:
            ClusterDefinition: Imported cluster definition
            
        Raises:
            ConfigurationError: If import or validation fails
        """
        try:
            # Parse configuration
            config_dict = self._parse_config_content(config_content, format)
            
            # Validate schema if requested
            if validate_schema:
                self._validate_config_schema(config_dict)
            
            # Convert to cluster definition
            cluster = self._dict_to_cluster(config_dict)
            
            # Create version backup if requested
            if create_version:
                await self._create_config_version(cluster.id, config_dict)
            
            # Save to storage
            await self.storage.save_cluster(cluster)
            
            logger.info(f"Successfully imported configuration for cluster {cluster.id}")
            return cluster
            
        except Exception as e:
            logger.error(f"Failed to import cluster config: {e}")
            raise ConfigurationError(f"Import failed: {e}")
    
    def _parse_config_content(self, content: str, format: str) -> Dict[str, Any]:
        """Parse configuration content based on format."""
        try:
            if format == "auto":
                # Try YAML first, then JSON
                try:
                    return yaml.safe_load(content)
                except yaml.YAMLError:
                    return json.loads(content)
            elif format.lower() == "yaml":
                return yaml.safe_load(content)
            elif format.lower() == "json":
                return json.loads(content)
            else:
                raise ConfigurationError(f"Unsupported format: {format}")
                
        except (yaml.YAMLError, json.JSONDecodeError) as e:
            raise ConfigurationError(f"Failed to parse {format} content: {e}")
    
    def _validate_config_schema(self, config_dict: Dict[str, Any]) -> None:
        """Validate configuration against JSON schema."""
        try:
            validate(instance=config_dict, schema=self._cluster_schema)
        except JsonSchemaValidationError as e:
            error_path = " -> ".join(str(p) for p in e.path) if e.path else "root"
            raise ConfigurationError(
                f"Schema validation failed at {error_path}: {e.message}"
            )
    
    def _cluster_to_dict(self, cluster: ClusterDefinition, include_metadata: bool) -> Dict[str, Any]:
        """Convert cluster definition to dictionary."""
        config_dict = {
            "id": cluster.id,
            "name": cluster.name,
            "description": cluster.description,
            "environment": cluster.environment,
            "template_id": cluster.template_id,
            "tags": cluster.tags
        }
        
        # Add configuration sections
        if cluster.kafka_config:
            try:
                config_dict["kafka_config"] = asdict(cluster.kafka_config)
            except TypeError:
                # Handle non-dataclass objects (e.g., mocks)
                config_dict["kafka_config"] = cluster.kafka_config.__dict__ if hasattr(cluster.kafka_config, '__dict__') else {}
        
        if cluster.rest_proxy_config:
            try:
                config_dict["rest_proxy_config"] = asdict(cluster.rest_proxy_config)
            except TypeError:
                config_dict["rest_proxy_config"] = cluster.rest_proxy_config.__dict__ if hasattr(cluster.rest_proxy_config, '__dict__') else {}
        
        if cluster.ui_config:
            try:
                config_dict["ui_config"] = asdict(cluster.ui_config)
            except TypeError:
                config_dict["ui_config"] = cluster.ui_config.__dict__ if hasattr(cluster.ui_config, '__dict__') else {}
        
        if cluster.port_allocation:
            try:
                config_dict["port_allocation"] = asdict(cluster.port_allocation)
            except TypeError:
                config_dict["port_allocation"] = cluster.port_allocation.__dict__ if hasattr(cluster.port_allocation, '__dict__') else {}
        
        if cluster.retention_policy:
            try:
                config_dict["retention_policy"] = asdict(cluster.retention_policy)
            except TypeError:
                config_dict["retention_policy"] = cluster.retention_policy.__dict__ if hasattr(cluster.retention_policy, '__dict__') else {}
        
        # Add metadata if requested
        if include_metadata:
            config_dict["metadata"] = {
                "created_at": cluster.created_at.isoformat(),
                "updated_at": cluster.updated_at.isoformat(),
                "created_by": cluster.created_by,
                "status": cluster.status.value if hasattr(cluster.status, 'value') else str(cluster.status),
                "last_started": cluster.last_started.isoformat() if cluster.last_started else None,
                "last_stopped": cluster.last_stopped.isoformat() if cluster.last_stopped else None
            }
        
        return config_dict
    
    def _dict_to_cluster(self, config_dict: Dict[str, Any]) -> ClusterDefinition:
        """Convert dictionary to cluster definition."""
        # This would need to properly reconstruct the ClusterDefinition
        # For now, return a basic implementation
        from ..models.multi_cluster import KafkaConfig, RestProxyConfig, UIConfig, PortAllocation
        
        # Extract basic fields
        cluster_id = config_dict["id"]
        name = config_dict["name"]
        description = config_dict.get("description")
        environment = config_dict["environment"]
        template_id = config_dict.get("template_id")
        tags = config_dict.get("tags", {})
        
        # Extract configurations
        kafka_config = None
        if "kafka_config" in config_dict:
            kafka_config = KafkaConfig(**config_dict["kafka_config"])
        
        rest_proxy_config = None
        if "rest_proxy_config" in config_dict:
            rest_proxy_config = RestProxyConfig(**config_dict["rest_proxy_config"])
        
        ui_config = None
        if "ui_config" in config_dict:
            ui_config = UIConfig(**config_dict["ui_config"])
        
        port_allocation = None
        if "port_allocation" in config_dict:
            port_allocation = PortAllocation(**config_dict["port_allocation"])
        
        # Create cluster definition
        cluster = ClusterDefinition(
            id=cluster_id,
            name=name,
            description=description,
            environment=environment,
            template_id=template_id,
            kafka_config=kafka_config,
            rest_proxy_config=rest_proxy_config,
            ui_config=ui_config,
            port_allocation=port_allocation,
            tags=tags,
            created_at=datetime.now(),
            updated_at=datetime.now(),
            created_by="config-import"
        )
        
        return cluster 
   
    async def _create_config_version(self, cluster_id: str, config_dict: Dict[str, Any]) -> str:
        """Create a versioned backup of configuration."""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            version_id = f"{cluster_id}_{timestamp}"
            
            version_file = self.versions_dir / f"{version_id}.yaml"
            
            # Add version metadata
            versioned_config = {
                "version_info": {
                    "version_id": version_id,
                    "cluster_id": cluster_id,
                    "created_at": datetime.now().isoformat(),
                    "created_by": "system"
                },
                "configuration": config_dict
            }
            
            # Save version file
            with open(version_file, 'w') as f:
                yaml.dump(versioned_config, f, default_flow_style=False)
            
            logger.info(f"Created configuration version {version_id} for cluster {cluster_id}")
            return version_id
            
        except Exception as e:
            logger.error(f"Failed to create config version for {cluster_id}: {e}")
            raise ConfigurationError(f"Version creation failed: {e}")
    
    async def list_config_versions(self, cluster_id: str) -> List[Dict[str, Any]]:
        """
        List all configuration versions for a cluster.
        
        Args:
            cluster_id: Cluster ID
            
        Returns:
            List of version information dictionaries
        """
        try:
            versions = []
            pattern = f"{cluster_id}_*.yaml"
            
            for version_file in self.versions_dir.glob(pattern):
                try:
                    with open(version_file, 'r') as f:
                        version_data = yaml.safe_load(f)
                    
                    version_info = version_data.get("version_info", {})
                    versions.append({
                        "version_id": version_info.get("version_id"),
                        "created_at": version_info.get("created_at"),
                        "created_by": version_info.get("created_by"),
                        "file_path": str(version_file)
                    })
                    
                except Exception as e:
                    logger.warning(f"Failed to read version file {version_file}: {e}")
                    continue
            
            # Sort by creation time (newest first)
            versions.sort(key=lambda x: x["created_at"], reverse=True)
            return versions
            
        except Exception as e:
            logger.error(f"Failed to list config versions for {cluster_id}: {e}")
            return []
    
    async def rollback_to_version(self, cluster_id: str, version_id: str) -> ClusterDefinition:
        """
        Rollback cluster configuration to a specific version.
        
        Args:
            cluster_id: Cluster ID
            version_id: Version ID to rollback to
            
        Returns:
            ClusterDefinition: Restored cluster definition
            
        Raises:
            ConfigurationError: If version not found or rollback fails
        """
        try:
            version_file = self.versions_dir / f"{version_id}.yaml"
            
            if not version_file.exists():
                raise ConfigurationError(f"Version {version_id} not found")
            
            # Load version data
            with open(version_file, 'r') as f:
                version_data = yaml.safe_load(f)
            
            config_dict = version_data.get("configuration")
            if not config_dict:
                raise ConfigurationError(f"Invalid version file: {version_id}")
            
            # Create backup of current config before rollback
            current_cluster = await self.storage.load_cluster(cluster_id)
            if current_cluster:
                current_dict = self._cluster_to_dict(current_cluster, include_metadata=False)
                await self._create_config_version(f"{cluster_id}_pre_rollback", current_dict)
            
            # Import the version configuration
            cluster = self._dict_to_cluster(config_dict)
            await self.storage.save_cluster(cluster)
            
            logger.info(f"Successfully rolled back cluster {cluster_id} to version {version_id}")
            return cluster
            
        except Exception as e:
            logger.error(f"Failed to rollback cluster {cluster_id} to version {version_id}: {e}")
            raise ConfigurationError(f"Rollback failed: {e}")
    
    async def export_multiple_clusters(
        self, 
        cluster_ids: List[str], 
        format: str = "yaml",
        output_file: Optional[Path] = None
    ) -> Union[str, Path]:
        """
        Export multiple cluster configurations to a single file.
        
        Args:
            cluster_ids: List of cluster IDs to export
            format: Export format ('yaml' or 'json')
            output_file: Optional output file path
            
        Returns:
            str or Path: Configuration content or file path
        """
        try:
            multi_config = {
                "export_info": {
                    "exported_at": datetime.now().isoformat(),
                    "cluster_count": len(cluster_ids),
                    "format": format
                },
                "clusters": {}
            }
            
            # Export each cluster
            for cluster_id in cluster_ids:
                try:
                    cluster = await self.storage.load_cluster(cluster_id)
                    if cluster:
                        multi_config["clusters"][cluster_id] = self._cluster_to_dict(
                            cluster, include_metadata=True
                        )
                    else:
                        logger.warning(f"Cluster {cluster_id} not found during export")
                        
                except Exception as e:
                    logger.error(f"Failed to export cluster {cluster_id}: {e}")
                    continue
            
            # Serialize configuration
            if format.lower() == "yaml":
                content = yaml.dump(multi_config, default_flow_style=False, sort_keys=False)
            elif format.lower() == "json":
                content = json.dumps(multi_config, indent=2, default=str)
            else:
                raise ConfigurationError(f"Unsupported format: {format}")
            
            # Save to file if specified
            if output_file:
                output_file = Path(output_file)
                output_file.parent.mkdir(parents=True, exist_ok=True)
                
                with open(output_file, 'w') as f:
                    f.write(content)
                
                logger.info(f"Exported {len(cluster_ids)} clusters to {output_file}")
                return output_file
            else:
                return content
                
        except Exception as e:
            logger.error(f"Failed to export multiple clusters: {e}")
            raise ConfigurationError(f"Multi-cluster export failed: {e}")
    
    async def import_multiple_clusters(
        self, 
        config_content: str, 
        format: str = "auto",
        validate_schema: bool = True,
        overwrite_existing: bool = False
    ) -> List[ClusterDefinition]:
        """
        Import multiple cluster configurations from a single file.
        
        Args:
            config_content: Configuration content
            format: Format ('yaml', 'json', or 'auto')
            validate_schema: Whether to validate schemas
            overwrite_existing: Whether to overwrite existing clusters
            
        Returns:
            List[ClusterDefinition]: Imported cluster definitions
        """
        try:
            # Parse multi-cluster configuration
            multi_config = self._parse_config_content(config_content, format)
            
            if "clusters" not in multi_config:
                raise ConfigurationError("Invalid multi-cluster configuration: missing 'clusters' section")
            
            imported_clusters = []
            
            # Import each cluster
            for cluster_id, config_dict in multi_config["clusters"].items():
                try:
                    # Check if cluster exists
                    existing_cluster = await self.storage.load_cluster(cluster_id)
                    if existing_cluster and not overwrite_existing:
                        logger.warning(f"Cluster {cluster_id} already exists, skipping")
                        continue
                    
                    # Validate schema if requested
                    if validate_schema:
                        self._validate_config_schema(config_dict)
                    
                    # Convert and save cluster
                    cluster = self._dict_to_cluster(config_dict)
                    await self.storage.save_cluster(cluster)
                    
                    # Create version backup
                    await self._create_config_version(cluster.id, config_dict)
                    
                    imported_clusters.append(cluster)
                    logger.info(f"Successfully imported cluster {cluster_id}")
                    
                except Exception as e:
                    logger.error(f"Failed to import cluster {cluster_id}: {e}")
                    continue
            
            logger.info(f"Successfully imported {len(imported_clusters)} clusters")
            return imported_clusters
            
        except Exception as e:
            logger.error(f"Failed to import multiple clusters: {e}")
            raise ConfigurationError(f"Multi-cluster import failed: {e}")
    
    async def validate_config_file(self, file_path: Path) -> Dict[str, Any]:
        """
        Validate a configuration file without importing it.
        
        Args:
            file_path: Path to configuration file
            
        Returns:
            Dict with validation results
        """
        try:
            if not file_path.exists():
                return {
                    "valid": False,
                    "error": f"File not found: {file_path}"
                }
            
            # Read file content
            with open(file_path, 'r') as f:
                content = f.read()
            
            # Determine format from extension
            format = "yaml" if file_path.suffix.lower() in ['.yml', '.yaml'] else "json"
            
            # Parse content
            try:
                config_dict = self._parse_config_content(content, format)
            except ConfigurationError as e:
                return {
                    "valid": False,
                    "error": f"Parse error: {e}"
                }
            
            # Check if it's multi-cluster or single cluster
            is_multi_cluster = "clusters" in config_dict
            
            if is_multi_cluster:
                # Validate each cluster in multi-cluster config
                validation_results = []
                for cluster_id, cluster_config in config_dict.get("clusters", {}).items():
                    try:
                        self._validate_config_schema(cluster_config)
                        validation_results.append({
                            "cluster_id": cluster_id,
                            "valid": True
                        })
                    except ConfigurationError as e:
                        validation_results.append({
                            "cluster_id": cluster_id,
                            "valid": False,
                            "error": str(e)
                        })
                
                return {
                    "valid": all(r["valid"] for r in validation_results),
                    "type": "multi_cluster",
                    "cluster_count": len(config_dict.get("clusters", {})),
                    "cluster_results": validation_results
                }
            else:
                # Validate single cluster config
                try:
                    self._validate_config_schema(config_dict)
                    return {
                        "valid": True,
                        "type": "single_cluster",
                        "cluster_id": config_dict.get("id")
                    }
                except ConfigurationError as e:
                    return {
                        "valid": False,
                        "type": "single_cluster",
                        "error": str(e)
                    }
                    
        except Exception as e:
            logger.error(f"Failed to validate config file {file_path}: {e}")
            return {
                "valid": False,
                "error": f"Validation failed: {e}"
            }
    
    async def get_config_template(self, template_type: str = "basic") -> Dict[str, Any]:
        """
        Get a configuration template for creating new cluster configs.
        
        Args:
            template_type: Type of template ('basic', 'development', 'production')
            
        Returns:
            Dict containing template configuration
        """
        templates = {
            "basic": {
                "id": "my-cluster",
                "name": "My Cluster",
                "description": "A basic Kafka cluster configuration",
                "environment": "development",
                "template_id": "development",
                "kafka_config": {
                    "heap_size": "1G",
                    "log_retention_hours": 168,
                    "num_partitions": 3,
                    "default_replication_factor": 1
                },
                "rest_proxy_config": {
                    "heap_size": "512M",
                    "consumer_request_timeout_ms": 30000,
                    "producer_request_timeout_ms": 30000
                },
                "ui_config": {
                    "heap_size": "512M",
                    "readonly_mode": False,
                    "topic_creation_enabled": True,
                    "topic_deletion_enabled": True
                },
                "port_allocation": {
                    "kafka_port": 9092,
                    "rest_proxy_port": 8082,
                    "ui_port": 8080,
                    "jmx_port": 9999
                },
                "tags": {
                    "environment": "development",
                    "purpose": "testing"
                }
            },
            "development": {
                "id": "dev-cluster",
                "name": "Development Cluster",
                "description": "Development environment cluster with relaxed settings",
                "environment": "development",
                "template_id": "development",
                "kafka_config": {
                    "heap_size": "512M",
                    "log_retention_hours": 24,
                    "num_partitions": 1,
                    "default_replication_factor": 1,
                    "custom_properties": {
                        "auto.create.topics.enable": "true",
                        "delete.topic.enable": "true"
                    }
                },
                "rest_proxy_config": {
                    "heap_size": "256M"
                },
                "ui_config": {
                    "heap_size": "256M",
                    "readonly_mode": False
                },
                "port_allocation": {
                    "kafka_port": 9092,
                    "rest_proxy_port": 8082,
                    "ui_port": 8080
                },
                "tags": {
                    "environment": "development",
                    "auto_cleanup": "true"
                }
            },
            "production": {
                "id": "prod-cluster",
                "name": "Production Cluster",
                "description": "Production environment cluster with secure settings",
                "environment": "production",
                "template_id": "production",
                "kafka_config": {
                    "heap_size": "4G",
                    "log_retention_hours": 168,
                    "num_partitions": 6,
                    "default_replication_factor": 3,
                    "custom_properties": {
                        "auto.create.topics.enable": "false",
                        "delete.topic.enable": "false",
                        "min.insync.replicas": "2"
                    }
                },
                "rest_proxy_config": {
                    "heap_size": "2G",
                    "consumer_request_timeout_ms": 60000,
                    "producer_request_timeout_ms": 60000
                },
                "ui_config": {
                    "heap_size": "1G",
                    "readonly_mode": True,
                    "topic_creation_enabled": False,
                    "topic_deletion_enabled": False
                },
                "port_allocation": {
                    "kafka_port": 9092,
                    "rest_proxy_port": 8082,
                    "ui_port": 8080,
                    "jmx_port": 9999
                },
                "tags": {
                    "environment": "production",
                    "backup_enabled": "true",
                    "monitoring": "enabled"
                }
            }
        }
        
        return templates.get(template_type, templates["basic"])
    
    async def cleanup_old_versions(self, cluster_id: str, keep_count: int = 10) -> int:
        """
        Clean up old configuration versions, keeping only the most recent ones.
        
        Args:
            cluster_id: Cluster ID
            keep_count: Number of versions to keep
            
        Returns:
            int: Number of versions deleted
        """
        try:
            versions = await self.list_config_versions(cluster_id)
            
            if len(versions) <= keep_count:
                return 0
            
            # Delete old versions
            versions_to_delete = versions[keep_count:]
            deleted_count = 0
            
            for version in versions_to_delete:
                try:
                    version_file = Path(version["file_path"])
                    if version_file.exists():
                        version_file.unlink()
                        deleted_count += 1
                        logger.debug(f"Deleted old version: {version['version_id']}")
                        
                except Exception as e:
                    logger.warning(f"Failed to delete version {version['version_id']}: {e}")
                    continue
            
            logger.info(f"Cleaned up {deleted_count} old versions for cluster {cluster_id}")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Failed to cleanup versions for {cluster_id}: {e}")
            return 0