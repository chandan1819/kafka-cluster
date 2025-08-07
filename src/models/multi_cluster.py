"""
Multi-cluster data models for managing multiple Kafka clusters.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field, validator
import re

from .base import ServiceStatus
from .cluster import ClusterStatus


class ClusterEnvironment(str, Enum):
    """Supported cluster environments."""
    DEVELOPMENT = "development"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"


class ClusterLifecycleStatus(str, Enum):
    """Lifecycle status of a cluster in the registry."""
    REGISTERED = "registered"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"
    DELETED = "deleted"


class OperationStatus(str, Enum):
    """Status of cross-cluster operations."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TemplateCategory(str, Enum):
    """Categories for cluster templates."""
    DEVELOPMENT = "development"
    TESTING = "testing"
    PRODUCTION = "production"
    HIGH_THROUGHPUT = "high-throughput"
    CUSTOM = "custom"


@dataclass
class PortAllocation:
    """Port allocation for cluster services."""
    kafka_port: int
    rest_proxy_port: int
    ui_port: int
    jmx_port: Optional[int] = None
    
    def __post_init__(self):
        """Validate port ranges."""
        ports = [self.kafka_port, self.rest_proxy_port, self.ui_port]
        if self.jmx_port:
            ports.append(self.jmx_port)
        
        # Check for duplicates
        if len(ports) != len(set(ports)):
            raise ValueError("Port allocation contains duplicate ports")
        
        # Check port ranges
        for port in ports:
            if not (1024 <= port <= 65535):
                raise ValueError(f"Port {port} is outside valid range (1024-65535)")
    
    def get_all_ports(self) -> List[int]:
        """Get all allocated ports as a list."""
        ports = [self.kafka_port, self.rest_proxy_port, self.ui_port]
        if self.jmx_port:
            ports.append(self.jmx_port)
        return ports
    
    def to_dict(self) -> Dict[str, int]:
        """Convert to dictionary representation."""
        result = {
            "kafka_port": self.kafka_port,
            "rest_proxy_port": self.rest_proxy_port,
            "ui_port": self.ui_port
        }
        if self.jmx_port:
            result["jmx_port"] = self.jmx_port
        return result


class KafkaConfig(BaseModel):
    """Kafka broker configuration."""
    heap_size: str = Field(default="1G", description="JVM heap size")
    log_retention_hours: int = Field(default=168, ge=1, description="Log retention in hours")
    log_segment_bytes: int = Field(default=1073741824, ge=1024, description="Log segment size in bytes")
    num_partitions: int = Field(default=3, ge=1, description="Default number of partitions")
    default_replication_factor: int = Field(default=1, ge=1, description="Default replication factor")
    
    # Performance tuning
    num_network_threads: int = Field(default=3, ge=1, le=32, description="Number of network threads")
    num_io_threads: int = Field(default=8, ge=1, le=64, description="Number of I/O threads")
    socket_send_buffer_bytes: int = Field(default=102400, ge=1024, description="Socket send buffer size")
    socket_receive_buffer_bytes: int = Field(default=102400, ge=1024, description="Socket receive buffer size")
    
    # Custom properties
    custom_properties: Dict[str, str] = Field(default_factory=dict, description="Custom Kafka properties")
    
    @validator('heap_size')
    def validate_heap_size(cls, v):
        """Validate heap size format."""
        if not re.match(r'^\d+[MG]$', v):
            raise ValueError('Heap size must be in format like "1G" or "512M"')
        return v


class RestProxyConfig(BaseModel):
    """Kafka REST Proxy configuration."""
    heap_size: str = Field(default="512M", description="JVM heap size")
    consumer_request_timeout_ms: int = Field(default=30000, ge=1000, description="Consumer request timeout")
    producer_request_timeout_ms: int = Field(default=30000, ge=1000, description="Producer request timeout")
    
    # Custom properties
    custom_properties: Dict[str, str] = Field(default_factory=dict, description="Custom REST Proxy properties")
    
    @validator('heap_size')
    def validate_heap_size(cls, v):
        """Validate heap size format."""
        if not re.match(r'^\d+[MG]$', v):
            raise ValueError('Heap size must be in format like "1G" or "512M"')
        return v


class UIConfig(BaseModel):
    """Kafka UI configuration."""
    heap_size: str = Field(default="512M", description="JVM heap size")
    
    # Feature flags
    readonly_mode: bool = Field(default=False, description="Enable read-only mode")
    topic_creation_enabled: bool = Field(default=True, description="Allow topic creation")
    topic_deletion_enabled: bool = Field(default=True, description="Allow topic deletion")
    
    # Custom configuration
    clusters_config: Dict[str, Any] = Field(default_factory=dict, description="UI cluster configuration")
    
    @validator('heap_size')
    def validate_heap_size(cls, v):
        """Validate heap size format."""
        if not re.match(r'^\d+[MG]$', v):
            raise ValueError('Heap size must be in format like "1G" or "512M"')
        return v


class RetentionPolicy(BaseModel):
    """Data retention policy for clusters."""
    log_retention_hours: int = Field(default=168, ge=1, description="Log retention in hours")
    log_retention_bytes: Optional[int] = Field(default=None, ge=1, description="Log retention in bytes")
    cleanup_policy: str = Field(default="delete", description="Cleanup policy (delete or compact)")
    
    @validator('cleanup_policy')
    def validate_cleanup_policy(cls, v):
        """Validate cleanup policy."""
        if v not in ["delete", "compact", "delete,compact"]:
            raise ValueError('Cleanup policy must be "delete", "compact", or "delete,compact"')
        return v


class ClusterDefinition(BaseModel):
    """Complete definition of a Kafka cluster."""
    id: str = Field(..., description="Unique cluster identifier")
    name: str = Field(..., description="Human-readable cluster name")
    description: Optional[str] = Field(None, description="Cluster description")
    template_id: Optional[str] = Field(None, description="Template used to create cluster")
    environment: ClusterEnvironment = Field(..., description="Cluster environment")
    
    # Resource configuration
    kafka_config: KafkaConfig = Field(default_factory=KafkaConfig, description="Kafka configuration")
    rest_proxy_config: RestProxyConfig = Field(default_factory=RestProxyConfig, description="REST Proxy configuration")
    ui_config: UIConfig = Field(default_factory=UIConfig, description="UI configuration")
    
    # Network configuration
    port_allocation: Optional[PortAllocation] = Field(None, description="Port allocation")
    network_name: Optional[str] = Field(None, description="Docker network name")
    
    # Storage configuration
    data_directory: Optional[str] = Field(None, description="Data directory path")
    retention_policy: RetentionPolicy = Field(default_factory=RetentionPolicy, description="Retention policy")
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Creation timestamp")
    updated_at: datetime = Field(default_factory=datetime.utcnow, description="Last update timestamp")
    created_by: str = Field(default="system", description="Creator identifier")
    tags: Dict[str, str] = Field(default_factory=dict, description="Cluster tags")
    
    # Status
    status: ClusterLifecycleStatus = Field(default=ClusterLifecycleStatus.REGISTERED, description="Current cluster lifecycle status")
    last_started: Optional[datetime] = Field(None, description="Last start timestamp")
    last_stopped: Optional[datetime] = Field(None, description="Last stop timestamp")
    
    @validator('id')
    def validate_id(cls, v):
        """Validate cluster ID format."""
        if not re.match(r'^[a-z0-9][a-z0-9-]*[a-z0-9]$', v):
            raise ValueError('Cluster ID must contain only lowercase letters, numbers, and hyphens, and cannot start or end with a hyphen')
        if len(v) < 3 or len(v) > 63:
            raise ValueError('Cluster ID must be between 3 and 63 characters long')
        return v
    
    @validator('name')
    def validate_name(cls, v):
        """Validate cluster name."""
        if len(v.strip()) < 1:
            raise ValueError('Cluster name cannot be empty')
        if len(v) > 100:
            raise ValueError('Cluster name cannot exceed 100 characters')
        return v.strip()
    
    def update_timestamp(self):
        """Update the updated_at timestamp."""
        self.updated_at = datetime.utcnow()


class ClusterTemplate(BaseModel):
    """Template for creating clusters with predefined configurations."""
    id: str = Field(..., description="Unique template identifier")
    name: str = Field(..., description="Template name")
    description: str = Field(..., description="Template description")
    category: TemplateCategory = Field(..., description="Template category")
    
    # Default configurations
    default_kafka_config: KafkaConfig = Field(default_factory=KafkaConfig, description="Default Kafka config")
    default_rest_proxy_config: RestProxyConfig = Field(default_factory=RestProxyConfig, description="Default REST Proxy config")
    default_ui_config: UIConfig = Field(default_factory=UIConfig, description="Default UI config")
    default_retention_policy: RetentionPolicy = Field(default_factory=RetentionPolicy, description="Default retention policy")
    
    # Resource requirements
    min_memory_mb: int = Field(default=2048, ge=512, description="Minimum memory requirement in MB")
    min_disk_gb: int = Field(default=5, ge=1, description="Minimum disk requirement in GB")
    recommended_memory_mb: int = Field(default=4096, ge=512, description="Recommended memory in MB")
    recommended_disk_gb: int = Field(default=20, ge=1, description="Recommended disk in GB")
    
    # Template metadata
    version: str = Field(default="1.0.0", description="Template version")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Creation timestamp")
    is_builtin: bool = Field(default=False, description="Whether this is a built-in template")
    tags: List[str] = Field(default_factory=list, description="Template tags")
    
    @validator('id')
    def validate_id(cls, v):
        """Validate template ID format."""
        if not re.match(r'^[a-z0-9][a-z0-9-]*[a-z0-9]$', v):
            raise ValueError('Template ID must contain only lowercase letters, numbers, and hyphens')
        return v
    
    @validator('version')
    def validate_version(cls, v):
        """Validate semantic version format."""
        if not re.match(r'^\d+\.\d+\.\d+$', v):
            raise ValueError('Version must be in semantic version format (e.g., 1.0.0)')
        return v
    
    def apply_to_definition(self, cluster_id: str, cluster_name: str, 
                           environment: ClusterEnvironment, 
                           overrides: Optional[Dict[str, Any]] = None) -> ClusterDefinition:
        """Apply template to create a cluster definition."""
        definition = ClusterDefinition(
            id=cluster_id,
            name=cluster_name,
            environment=environment,
            template_id=self.id,
            description=f"Cluster created from template: {self.name}",
            kafka_config=self.default_kafka_config.copy(deep=True),
            rest_proxy_config=self.default_rest_proxy_config.copy(deep=True),
            ui_config=self.default_ui_config.copy(deep=True),
            retention_policy=self.default_retention_policy.copy(deep=True),
            tags={"template": self.id, "category": self.category.value}
        )
        
        # Apply overrides if provided
        if overrides:
            for key, value in overrides.items():
                if key == "tags" and isinstance(value, dict):
                    # Merge tags instead of replacing them
                    definition.tags.update(value)
                elif hasattr(definition, key):
                    setattr(definition, key, value)
        
        return definition


class CrossClusterOperation(BaseModel):
    """Represents an operation across multiple clusters."""
    id: str = Field(..., description="Unique operation identifier")
    operation_type: str = Field(..., description="Type of operation (migrate, replicate, compare)")
    source_cluster_id: str = Field(..., description="Source cluster ID")
    target_cluster_ids: List[str] = Field(..., description="Target cluster IDs")
    
    # Operation parameters
    topics: Optional[List[str]] = Field(None, description="Topics to operate on (None = all)")
    include_data: bool = Field(default=True, description="Include message data")
    preserve_partitions: bool = Field(default=True, description="Preserve partition structure")
    preserve_timestamps: bool = Field(default=True, description="Preserve message timestamps")
    
    # Status tracking
    status: OperationStatus = Field(default=OperationStatus.PENDING, description="Operation status")
    progress_percent: float = Field(default=0.0, ge=0.0, le=100.0, description="Progress percentage")
    started_at: Optional[datetime] = Field(None, description="Start timestamp")
    completed_at: Optional[datetime] = Field(None, description="Completion timestamp")
    error_message: Optional[str] = Field(None, description="Error message if failed")
    
    # Results
    results: Dict[str, Any] = Field(default_factory=dict, description="Operation results")
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Creation timestamp")
    created_by: str = Field(default="system", description="Creator identifier")
    
    @validator('operation_type')
    def validate_operation_type(cls, v):
        """Validate operation type."""
        valid_types = ["migrate", "replicate", "compare", "sync"]
        if v not in valid_types:
            raise ValueError(f'Operation type must be one of: {", ".join(valid_types)}')
        return v
    
    def update_progress(self, progress: float, status: Optional[OperationStatus] = None):
        """Update operation progress."""
        self.progress_percent = max(0.0, min(100.0, progress))
        if status:
            self.status = status
        
        if status == OperationStatus.RUNNING and not self.started_at:
            self.started_at = datetime.utcnow()
        elif status in [OperationStatus.COMPLETED, OperationStatus.FAILED, OperationStatus.CANCELLED]:
            self.completed_at = datetime.utcnow()


class ClusterComparisonResult(BaseModel):
    """Result of comparing two clusters."""
    source_cluster: str = Field(..., description="Source cluster ID")
    target_cluster: str = Field(..., description="Target cluster ID")
    comparison_timestamp: datetime = Field(default_factory=datetime.utcnow, description="Comparison timestamp")
    
    # Configuration differences
    config_differences: Dict[str, Any] = Field(default_factory=dict, description="Configuration differences")
    
    # Topic differences
    topics_only_in_source: List[str] = Field(default_factory=list, description="Topics only in source")
    topics_only_in_target: List[str] = Field(default_factory=list, description="Topics only in target")
    topics_with_different_config: List[str] = Field(default_factory=list, description="Topics with different config")
    
    # Data differences (if requested)
    message_count_differences: Optional[Dict[str, Dict[str, int]]] = Field(None, description="Message count differences")
    
    # Summary
    summary: Dict[str, Any] = Field(default_factory=dict, description="Comparison summary")
    
    def is_identical(self) -> bool:
        """Check if clusters are identical."""
        return (
            not self.config_differences and
            not self.topics_only_in_source and
            not self.topics_only_in_target and
            not self.topics_with_different_config
        )


# Request/Response Models for API

class CreateClusterRequest(BaseModel):
    """Request model for creating a new cluster."""
    name: str = Field(..., description="Cluster name")
    description: Optional[str] = Field(None, description="Cluster description")
    template_id: Optional[str] = Field(None, description="Template to use")
    environment: ClusterEnvironment = Field(..., description="Environment type")
    
    # Optional configuration overrides
    kafka_config: Optional[KafkaConfig] = Field(None, description="Kafka configuration overrides")
    rest_proxy_config: Optional[RestProxyConfig] = Field(None, description="REST Proxy configuration overrides")
    ui_config: Optional[UIConfig] = Field(None, description="UI configuration overrides")
    retention_policy: Optional[RetentionPolicy] = Field(None, description="Retention policy overrides")
    
    # Metadata
    tags: Dict[str, str] = Field(default_factory=dict, description="Cluster tags")
    
    # Options
    auto_start: bool = Field(default=True, description="Auto-start cluster after creation")


class UpdateClusterRequest(BaseModel):
    """Request model for updating cluster configuration."""
    name: Optional[str] = Field(None, description="New cluster name")
    description: Optional[str] = Field(None, description="New cluster description")
    
    # Configuration updates
    kafka_config: Optional[KafkaConfig] = Field(None, description="Updated Kafka configuration")
    rest_proxy_config: Optional[RestProxyConfig] = Field(None, description="Updated REST Proxy configuration")
    ui_config: Optional[UIConfig] = Field(None, description="Updated UI configuration")
    retention_policy: Optional[RetentionPolicy] = Field(None, description="Updated retention policy")
    
    # Metadata updates
    tags: Optional[Dict[str, str]] = Field(None, description="Updated tags")


class ClusterSummary(BaseModel):
    """Summary information about a cluster."""
    id: str = Field(..., description="Cluster ID")
    name: str = Field(..., description="Cluster name")
    environment: ClusterEnvironment = Field(..., description="Environment type")
    status: ServiceStatus = Field(..., description="Current status")
    created_at: datetime = Field(..., description="Creation timestamp")
    last_started: Optional[datetime] = Field(None, description="Last start timestamp")
    endpoints: Dict[str, str] = Field(default_factory=dict, description="Service endpoints")
    tags: Dict[str, str] = Field(default_factory=dict, description="Cluster tags")


class MigrateDataRequest(BaseModel):
    """Request model for data migration between clusters."""
    source_cluster_id: str = Field(..., description="Source cluster ID")
    target_cluster_id: str = Field(..., description="Target cluster ID")
    topics: Optional[List[str]] = Field(None, description="Topics to migrate (None = all)")
    include_consumer_groups: bool = Field(default=False, description="Include consumer groups")
    preserve_timestamps: bool = Field(default=True, description="Preserve message timestamps")
    dry_run: bool = Field(default=False, description="Perform dry run without actual migration")


class CreateTemplateRequest(BaseModel):
    """Request model for creating a new template."""
    name: str = Field(..., description="Template name")
    description: str = Field(..., description="Template description")
    category: TemplateCategory = Field(..., description="Template category")
    
    # Configuration
    kafka_config: KafkaConfig = Field(default_factory=KafkaConfig, description="Default Kafka config")
    rest_proxy_config: RestProxyConfig = Field(default_factory=RestProxyConfig, description="Default REST Proxy config")
    ui_config: UIConfig = Field(default_factory=UIConfig, description="Default UI config")
    retention_policy: RetentionPolicy = Field(default_factory=RetentionPolicy, description="Default retention policy")
    
    # Resource requirements
    min_memory_mb: int = Field(default=2048, ge=512, description="Minimum memory requirement")
    min_disk_gb: int = Field(default=5, ge=1, description="Minimum disk requirement")
    recommended_memory_mb: int = Field(default=4096, ge=512, description="Recommended memory")
    recommended_disk_gb: int = Field(default=20, ge=1, description="Recommended disk")
    
    # Metadata
    tags: List[str] = Field(default_factory=list, description="Template tags")