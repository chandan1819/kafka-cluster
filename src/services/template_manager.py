"""
Template management service for cluster templates.
"""

import logging
from typing import List, Optional, Dict, Any
from datetime import datetime

from ..models.multi_cluster import (
    ClusterTemplate, 
    ClusterDefinition,
    TemplateCategory,
    KafkaConfig,
    RestProxyConfig,
    UIConfig,
    RetentionPolicy,
    ClusterEnvironment
)
from ..storage.base import StorageBackend
from ..exceptions import (
    LocalKafkaManagerError,
    ValidationError
)

logger = logging.getLogger(__name__)


class TemplateManagerError(LocalKafkaManagerError):
    """Base exception for template manager operations."""
    pass


class TemplateNotFoundError(TemplateManagerError):
    """Raised when template is not found."""
    
    def __init__(self, template_id: str):
        super().__init__(f"Template '{template_id}' not found")
        self.template_id = template_id


class TemplateAlreadyExistsError(TemplateManagerError):
    """Raised when trying to create a template with existing ID."""
    
    def __init__(self, template_id: str):
        super().__init__(f"Template '{template_id}' already exists")
        self.template_id = template_id


class TemplateValidationError(TemplateManagerError):
    """Raised when template validation fails."""
    pass


class TemplateManager:
    """Manages cluster templates and presets."""
    
    def __init__(self, storage_backend: StorageBackend):
        """Initialize template manager.
        
        Args:
            storage_backend: Storage backend for persisting templates
        """
        self.storage = storage_backend
        self._builtin_templates: Dict[str, ClusterTemplate] = {}
        self._initialized = False
        
    async def initialize(self) -> None:
        """Initialize the template manager and load built-in templates."""
        if self._initialized:
            return
            
        logger.info("Initializing template manager")
        
        # Initialize storage backend
        await self.storage.initialize()
        
        # Load built-in templates
        await self._load_builtin_templates()
        
        self._initialized = True
        logger.info("Template manager initialized successfully")
    
    async def _load_builtin_templates(self) -> None:
        """Load built-in templates into storage if they don't exist."""
        builtin_templates = self._create_builtin_templates()
        
        for template in builtin_templates:
            try:
                # Check if template already exists
                existing = await self.storage.load_template(template.id)
                if existing is None:
                    # Save built-in template
                    await self.storage.save_template(template)
                    logger.info(f"Loaded built-in template: {template.id}")
                else:
                    # Update built-in template if version is newer
                    if existing.version != template.version and template.is_builtin:
                        await self.storage.update_template(template)
                        logger.info(f"Updated built-in template: {template.id}")
                        
                self._builtin_templates[template.id] = template
                
            except Exception as e:
                logger.error(f"Failed to load built-in template {template.id}: {e}")
    
    def _create_builtin_templates(self) -> List[ClusterTemplate]:
        """Create built-in templates for common use cases."""
        templates = []
        
        # Development template - lightweight for local development
        dev_template = ClusterTemplate(
            id="development",
            name="Development",
            description="Lightweight template for local development with minimal resource usage",
            category=TemplateCategory.DEVELOPMENT,
            default_kafka_config=KafkaConfig(
                heap_size="512M",
                log_retention_hours=24,  # 1 day retention
                num_partitions=1,
                default_replication_factor=1,
                num_network_threads=2,
                num_io_threads=4
            ),
            default_rest_proxy_config=RestProxyConfig(
                heap_size="256M"
            ),
            default_ui_config=UIConfig(
                heap_size="256M",
                readonly_mode=False,
                topic_creation_enabled=True,
                topic_deletion_enabled=True
            ),
            default_retention_policy=RetentionPolicy(
                log_retention_hours=24,
                cleanup_policy="delete"
            ),
            min_memory_mb=1024,
            min_disk_gb=2,
            recommended_memory_mb=2048,
            recommended_disk_gb=5,
            version="1.0.0",
            is_builtin=True,
            tags=["development", "lightweight", "local"]
        )
        templates.append(dev_template)
        
        # Testing template - balanced for testing scenarios
        test_template = ClusterTemplate(
            id="testing",
            name="Testing",
            description="Balanced template for testing scenarios with moderate resource usage",
            category=TemplateCategory.TESTING,
            default_kafka_config=KafkaConfig(
                heap_size="1G",
                log_retention_hours=72,  # 3 days retention
                num_partitions=3,
                default_replication_factor=1,
                num_network_threads=3,
                num_io_threads=6
            ),
            default_rest_proxy_config=RestProxyConfig(
                heap_size="512M"
            ),
            default_ui_config=UIConfig(
                heap_size="512M",
                readonly_mode=False,
                topic_creation_enabled=True,
                topic_deletion_enabled=True
            ),
            default_retention_policy=RetentionPolicy(
                log_retention_hours=72,
                cleanup_policy="delete"
            ),
            min_memory_mb=2048,
            min_disk_gb=5,
            recommended_memory_mb=4096,
            recommended_disk_gb=10,
            version="1.0.0",
            is_builtin=True,
            tags=["testing", "balanced", "ci-cd"]
        )
        templates.append(test_template)
        
        # Production template - robust for production use
        prod_template = ClusterTemplate(
            id="production",
            name="Production",
            description="Robust template for production deployments with high reliability",
            category=TemplateCategory.PRODUCTION,
            default_kafka_config=KafkaConfig(
                heap_size="2G",
                log_retention_hours=168,  # 7 days retention
                num_partitions=6,
                default_replication_factor=3,
                num_network_threads=8,
                num_io_threads=16,
                socket_send_buffer_bytes=1048576,  # 1MB
                socket_receive_buffer_bytes=1048576  # 1MB
            ),
            default_rest_proxy_config=RestProxyConfig(
                heap_size="1G",
                consumer_request_timeout_ms=60000,
                producer_request_timeout_ms=60000
            ),
            default_ui_config=UIConfig(
                heap_size="1G",
                readonly_mode=False,
                topic_creation_enabled=False,  # Restrict in production
                topic_deletion_enabled=False   # Restrict in production
            ),
            default_retention_policy=RetentionPolicy(
                log_retention_hours=168,
                cleanup_policy="delete"
            ),
            min_memory_mb=8192,
            min_disk_gb=50,
            recommended_memory_mb=16384,
            recommended_disk_gb=100,
            version="1.0.0",
            is_builtin=True,
            tags=["production", "robust", "high-availability"]
        )
        templates.append(prod_template)
        
        # High-throughput template - optimized for high message volume
        high_throughput_template = ClusterTemplate(
            id="high-throughput",
            name="High Throughput",
            description="Optimized template for high message volume and throughput scenarios",
            category=TemplateCategory.HIGH_THROUGHPUT,
            default_kafka_config=KafkaConfig(
                heap_size="4G",
                log_retention_hours=48,  # 2 days retention for performance
                log_segment_bytes=2147483648,  # 2GB segments
                num_partitions=12,
                default_replication_factor=2,
                num_network_threads=16,
                num_io_threads=32,
                socket_send_buffer_bytes=2097152,  # 2MB
                socket_receive_buffer_bytes=2097152,  # 2MB
                custom_properties={
                    "batch.size": "65536",
                    "linger.ms": "10",
                    "compression.type": "lz4",
                    "acks": "1"
                }
            ),
            default_rest_proxy_config=RestProxyConfig(
                heap_size="2G",
                consumer_request_timeout_ms=10000,
                producer_request_timeout_ms=10000
            ),
            default_ui_config=UIConfig(
                heap_size="1G",
                readonly_mode=False,
                topic_creation_enabled=True,
                topic_deletion_enabled=True
            ),
            default_retention_policy=RetentionPolicy(
                log_retention_hours=48,
                cleanup_policy="delete"
            ),
            min_memory_mb=16384,
            min_disk_gb=100,
            recommended_memory_mb=32768,
            recommended_disk_gb=500,
            version="1.0.0",
            is_builtin=True,
            tags=["high-throughput", "performance", "streaming"]
        )
        templates.append(high_throughput_template)
        
        return templates
    
    async def create_template(self, template: ClusterTemplate) -> ClusterTemplate:
        """Create a new cluster template.
        
        Args:
            template: Template to create
            
        Returns:
            Created template
            
        Raises:
            TemplateAlreadyExistsError: If template already exists
            TemplateValidationError: If template validation fails
        """
        if not self._initialized:
            await self.initialize()
            
        logger.info(f"Creating template: {template.id}")
        
        # Validate template
        await self._validate_template(template)
        
        # Check if template already exists
        if await self.storage.template_exists(template.id):
            raise TemplateAlreadyExistsError(template.id)
        
        # Set creation timestamp and ensure it's not marked as built-in
        template.created_at = datetime.utcnow()
        template.is_builtin = False
        
        # Save template
        success = await self.storage.save_template(template)
        if not success:
            raise TemplateManagerError(f"Failed to save template: {template.id}")
        
        logger.info(f"Template created successfully: {template.id}")
        return template
    
    async def get_template(self, template_id: str) -> ClusterTemplate:
        """Get template by ID.
        
        Args:
            template_id: Unique template identifier
            
        Returns:
            Template if found
            
        Raises:
            TemplateNotFoundError: If template is not found
        """
        if not self._initialized:
            await self.initialize()
            
        template = await self.storage.load_template(template_id)
        if template is None:
            raise TemplateNotFoundError(template_id)
        
        return template
    
    async def update_template(self, template: ClusterTemplate) -> ClusterTemplate:
        """Update existing template.
        
        Args:
            template: Updated template
            
        Returns:
            Updated template
            
        Raises:
            TemplateNotFoundError: If template doesn't exist
            TemplateValidationError: If template validation fails
        """
        if not self._initialized:
            await self.initialize()
            
        logger.info(f"Updating template: {template.id}")
        
        # Check if template exists
        existing = await self.storage.load_template(template.id)
        if existing is None:
            raise TemplateNotFoundError(template.id)
        
        # Prevent updating built-in templates
        if existing.is_builtin:
            raise TemplateValidationError(f"Cannot modify built-in template: {template.id}")
        
        # Validate template
        await self._validate_template(template)
        
        # Preserve creation timestamp and built-in status
        template.created_at = existing.created_at
        template.is_builtin = existing.is_builtin
        
        # Update template
        success = await self.storage.update_template(template)
        if not success:
            raise TemplateManagerError(f"Failed to update template: {template.id}")
        
        logger.info(f"Template updated successfully: {template.id}")
        return template
    
    async def delete_template(self, template_id: str) -> bool:
        """Delete template.
        
        Args:
            template_id: Template ID to delete
            
        Returns:
            True if deletion was successful
            
        Raises:
            TemplateNotFoundError: If template doesn't exist
            TemplateValidationError: If trying to delete built-in template
        """
        if not self._initialized:
            await self.initialize()
            
        logger.info(f"Deleting template: {template_id}")
        
        # Check if template exists
        template = await self.storage.load_template(template_id)
        if template is None:
            raise TemplateNotFoundError(template_id)
        
        # Prevent deleting built-in templates
        if template.is_builtin:
            raise TemplateValidationError(f"Cannot delete built-in template: {template_id}")
        
        # Delete template
        success = await self.storage.delete_template(template_id)
        if not success:
            raise TemplateManagerError(f"Failed to delete template: {template_id}")
        
        logger.info(f"Template deleted successfully: {template_id}")
        return True
    
    async def list_templates(self, category: Optional[TemplateCategory] = None,
                           include_builtin: bool = True) -> List[ClusterTemplate]:
        """List all available templates.
        
        Args:
            category: Optional category filter
            include_builtin: Whether to include built-in templates
            
        Returns:
            List of templates
        """
        if not self._initialized:
            await self.initialize()
        
        filters = {}
        if category:
            filters["category"] = category.value
        
        templates = await self.storage.list_templates(filters)
        
        # Filter by built-in status if requested
        if not include_builtin:
            templates = [t for t in templates if not t.is_builtin]
        
        # Sort by category and name
        templates.sort(key=lambda t: (t.category.value, t.name))
        
        return templates
    
    async def get_builtin_templates(self) -> List[ClusterTemplate]:
        """Get all built-in templates.
        
        Returns:
            List of built-in templates
        """
        if not self._initialized:
            await self.initialize()
        
        return list(self._builtin_templates.values())
    
    async def apply_template(self, template_id: str, cluster_id: str, 
                           cluster_name: str, environment: ClusterEnvironment,
                           overrides: Optional[Dict[str, Any]] = None) -> ClusterDefinition:
        """Apply template to create cluster definition.
        
        Args:
            template_id: Template to apply
            cluster_id: ID for new cluster
            cluster_name: Name for new cluster
            environment: Environment for new cluster
            overrides: Optional configuration overrides
            
        Returns:
            Cluster definition created from template
            
        Raises:
            TemplateNotFoundError: If template doesn't exist
        """
        if not self._initialized:
            await self.initialize()
        
        logger.info(f"Applying template {template_id} to create cluster {cluster_id}")
        
        # Get template
        template = await self.get_template(template_id)
        
        # Apply template to create cluster definition
        cluster_definition = template.apply_to_definition(
            cluster_id=cluster_id,
            cluster_name=cluster_name,
            environment=environment,
            overrides=overrides
        )
        
        logger.info(f"Cluster definition created from template: {cluster_id}")
        return cluster_definition
    
    async def create_template_from_cluster(self, cluster: ClusterDefinition,
                                         template_id: str, template_name: str,
                                         template_description: str,
                                         category: TemplateCategory = TemplateCategory.CUSTOM) -> ClusterTemplate:
        """Create a template from existing cluster configuration.
        
        Args:
            cluster: Cluster to create template from
            template_id: ID for new template
            template_name: Name for new template
            template_description: Description for new template
            category: Template category
            
        Returns:
            Created template
            
        Raises:
            TemplateAlreadyExistsError: If template already exists
        """
        if not self._initialized:
            await self.initialize()
        
        logger.info(f"Creating template {template_id} from cluster {cluster.id}")
        
        # Create template from cluster configuration
        template = ClusterTemplate(
            id=template_id,
            name=template_name,
            description=template_description,
            category=category,
            default_kafka_config=cluster.kafka_config.copy(deep=True),
            default_rest_proxy_config=cluster.rest_proxy_config.copy(deep=True),
            default_ui_config=cluster.ui_config.copy(deep=True),
            default_retention_policy=cluster.retention_policy.copy(deep=True),
            # Set reasonable resource requirements based on cluster config
            min_memory_mb=self._estimate_memory_requirements(cluster.kafka_config),
            min_disk_gb=5,
            recommended_memory_mb=self._estimate_memory_requirements(cluster.kafka_config) * 2,
            recommended_disk_gb=20,
            version="1.0.0",
            is_builtin=False,
            tags=[f"from-cluster-{cluster.id}", cluster.environment.value]
        )
        
        # Create template
        return await self.create_template(template)
    
    def _estimate_memory_requirements(self, kafka_config: KafkaConfig) -> int:
        """Estimate memory requirements based on Kafka configuration.
        
        Args:
            kafka_config: Kafka configuration
            
        Returns:
            Estimated memory requirement in MB
        """
        # Parse heap size
        heap_size = kafka_config.heap_size
        if heap_size.endswith('G'):
            heap_mb = int(heap_size[:-1]) * 1024
        elif heap_size.endswith('M'):
            heap_mb = int(heap_size[:-1])
        else:
            heap_mb = 1024  # Default
        
        # Add overhead for system and other services
        total_mb = heap_mb + 1024  # Kafka heap + 1GB overhead
        
        return max(2048, total_mb)  # Minimum 2GB
    
    async def _validate_template(self, template: ClusterTemplate) -> None:
        """Validate template configuration.
        
        Args:
            template: Template to validate
            
        Raises:
            TemplateValidationError: If validation fails
        """
        # Basic validation is handled by Pydantic models
        # Additional business logic validation can be added here
        
        # Validate resource requirements
        if template.recommended_memory_mb < template.min_memory_mb:
            raise TemplateValidationError(
                "Recommended memory cannot be less than minimum memory"
            )
        
        if template.recommended_disk_gb < template.min_disk_gb:
            raise TemplateValidationError(
                "Recommended disk cannot be less than minimum disk"
            )
        
        # Validate Kafka configuration consistency
        kafka_config = template.default_kafka_config
        if kafka_config.default_replication_factor > kafka_config.num_partitions:
            logger.warning(
                f"Template {template.id}: Replication factor ({kafka_config.default_replication_factor}) "
                f"is greater than partitions ({kafka_config.num_partitions})"
            )
    
    async def get_template_usage_stats(self, template_id: str) -> Dict[str, Any]:
        """Get usage statistics for a template.
        
        Args:
            template_id: Template ID
            
        Returns:
            Usage statistics
            
        Raises:
            TemplateNotFoundError: If template doesn't exist
        """
        if not self._initialized:
            await self.initialize()
        
        # Check if template exists
        template = await self.get_template(template_id)
        
        # Get clusters using this template
        clusters = await self.storage.list_clusters({"template_id": template_id})
        
        stats = {
            "template_id": template_id,
            "template_name": template.name,
            "usage_count": len(clusters),
            "clusters_using_template": [c.id for c in clusters],
            "environments": list(set(c.environment.value for c in clusters)),
            "created_at": template.created_at,
            "is_builtin": template.is_builtin
        }
        
        return stats
    
    async def close(self) -> None:
        """Close template manager and cleanup resources."""
        if self._initialized:
            await self.storage.close()
            self._initialized = False
            logger.info("Template manager closed")