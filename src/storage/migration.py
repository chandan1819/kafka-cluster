"""
Storage migration utilities for upgrading between storage backends.
"""

import asyncio
from typing import List, Dict, Any, Optional
from ..models.multi_cluster import ClusterDefinition, ClusterTemplate
from ..exceptions import StorageBackendError
from ..utils.logging import get_logger
from .base import StorageBackend

logger = get_logger(__name__)


class StorageMigrator:
    """Utility for migrating data between storage backends."""
    
    def __init__(self, source_backend: StorageBackend, target_backend: StorageBackend):
        """Initialize storage migrator.
        
        Args:
            source_backend: Source storage backend to migrate from
            target_backend: Target storage backend to migrate to
        """
        self.source_backend = source_backend
        self.target_backend = target_backend
    
    async def migrate_all(self, dry_run: bool = False, batch_size: int = 10) -> Dict[str, Any]:
        """Migrate all data from source to target backend.
        
        Args:
            dry_run: If True, only simulate the migration without actually moving data
            batch_size: Number of items to process in each batch
            
        Returns:
            Migration results with statistics and any errors
        """
        logger.info(f"Starting {'dry run' if dry_run else 'migration'} from {type(self.source_backend).__name__} to {type(self.target_backend).__name__}")
        
        results = {
            "dry_run": dry_run,
            "source_backend": type(self.source_backend).__name__,
            "target_backend": type(self.target_backend).__name__,
            "clusters": {"total": 0, "migrated": 0, "errors": []},
            "templates": {"total": 0, "migrated": 0, "errors": []},
            "start_time": None,
            "end_time": None,
            "duration_seconds": 0
        }
        
        import time
        start_time = time.time()
        results["start_time"] = start_time
        
        try:
            # Initialize backends
            await self.source_backend.initialize()
            if not dry_run:
                await self.target_backend.initialize()
            
            # Migrate clusters
            cluster_results = await self._migrate_clusters(dry_run, batch_size)
            results["clusters"].update(cluster_results)
            
            # Migrate templates
            template_results = await self._migrate_templates(dry_run, batch_size)
            results["templates"].update(template_results)
            
            end_time = time.time()
            results["end_time"] = end_time
            results["duration_seconds"] = end_time - start_time
            
            logger.info(f"Migration completed: {results['clusters']['migrated']} clusters, {results['templates']['migrated']} templates")
            
            return results
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            raise StorageBackendError(
                "Migration failed",
                backend_type="migration",
                operation="migrate_all",
                cause=e
            )
        finally:
            # Cleanup
            try:
                await self.source_backend.close()
                if not dry_run:
                    await self.target_backend.close()
            except Exception as e:
                logger.warning(f"Error during cleanup: {e}")
    
    async def migrate_clusters_only(self, dry_run: bool = False, batch_size: int = 10) -> Dict[str, Any]:
        """Migrate only cluster definitions.
        
        Args:
            dry_run: If True, only simulate the migration
            batch_size: Number of clusters to process in each batch
            
        Returns:
            Migration results for clusters
        """
        logger.info(f"Starting cluster-only {'dry run' if dry_run else 'migration'}")
        
        try:
            await self.source_backend.initialize()
            if not dry_run:
                await self.target_backend.initialize()
            
            results = await self._migrate_clusters(dry_run, batch_size)
            
            logger.info(f"Cluster migration completed: {results['migrated']} clusters")
            return results
            
        finally:
            await self.source_backend.close()
            if not dry_run:
                await self.target_backend.close()
    
    async def migrate_templates_only(self, dry_run: bool = False, batch_size: int = 10) -> Dict[str, Any]:
        """Migrate only cluster templates.
        
        Args:
            dry_run: If True, only simulate the migration
            batch_size: Number of templates to process in each batch
            
        Returns:
            Migration results for templates
        """
        logger.info(f"Starting template-only {'dry run' if dry_run else 'migration'}")
        
        try:
            await self.source_backend.initialize()
            if not dry_run:
                await self.target_backend.initialize()
            
            results = await self._migrate_templates(dry_run, batch_size)
            
            logger.info(f"Template migration completed: {results['migrated']} templates")
            return results
            
        finally:
            await self.source_backend.close()
            if not dry_run:
                await self.target_backend.close()
    
    async def validate_migration(self) -> Dict[str, Any]:
        """Validate that migration was successful by comparing data.
        
        Returns:
            Validation results with any discrepancies found
        """
        logger.info("Starting migration validation")
        
        validation_results = {
            "clusters": {"source_count": 0, "target_count": 0, "missing": [], "mismatched": []},
            "templates": {"source_count": 0, "target_count": 0, "missing": [], "mismatched": []},
            "validation_passed": True
        }
        
        try:
            await self.source_backend.initialize()
            await self.target_backend.initialize()
            
            # Validate clusters
            await self._validate_clusters(validation_results)
            
            # Validate templates
            await self._validate_templates(validation_results)
            
            # Determine overall validation result
            validation_results["validation_passed"] = (
                len(validation_results["clusters"]["missing"]) == 0 and
                len(validation_results["clusters"]["mismatched"]) == 0 and
                len(validation_results["templates"]["missing"]) == 0 and
                len(validation_results["templates"]["mismatched"]) == 0
            )
            
            if validation_results["validation_passed"]:
                logger.info("Migration validation passed")
            else:
                logger.warning("Migration validation failed - discrepancies found")
            
            return validation_results
            
        finally:
            await self.source_backend.close()
            await self.target_backend.close()
    
    # Private helper methods
    
    async def _migrate_clusters(self, dry_run: bool, batch_size: int) -> Dict[str, Any]:
        """Migrate cluster definitions."""
        results = {"total": 0, "migrated": 0, "errors": []}
        
        try:
            # Get all clusters from source
            clusters = await self.source_backend.list_clusters()
            results["total"] = len(clusters)
            
            logger.info(f"Found {len(clusters)} clusters to migrate")
            
            # Process clusters in batches
            for i in range(0, len(clusters), batch_size):
                batch = clusters[i:i + batch_size]
                
                for cluster in batch:
                    try:
                        if not dry_run:
                            # Check if cluster already exists in target
                            if await self.target_backend.cluster_exists(cluster.id):
                                logger.warning(f"Cluster {cluster.id} already exists in target, skipping")
                                continue
                            
                            # Save cluster to target backend
                            await self.target_backend.save_cluster(cluster)
                        
                        results["migrated"] += 1
                        logger.debug(f"{'Would migrate' if dry_run else 'Migrated'} cluster: {cluster.id}")
                        
                    except Exception as e:
                        error_msg = f"Failed to migrate cluster {cluster.id}: {e}"
                        logger.error(error_msg)
                        results["errors"].append(error_msg)
                
                # Small delay between batches to avoid overwhelming the system
                if i + batch_size < len(clusters):
                    await asyncio.sleep(0.1)
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to migrate clusters: {e}")
            results["errors"].append(f"Cluster migration failed: {e}")
            return results
    
    async def _migrate_templates(self, dry_run: bool, batch_size: int) -> Dict[str, Any]:
        """Migrate cluster templates."""
        results = {"total": 0, "migrated": 0, "errors": []}
        
        try:
            # Get all templates from source
            templates = await self.source_backend.list_templates()
            results["total"] = len(templates)
            
            logger.info(f"Found {len(templates)} templates to migrate")
            
            # Process templates in batches
            for i in range(0, len(templates), batch_size):
                batch = templates[i:i + batch_size]
                
                for template in batch:
                    try:
                        if not dry_run:
                            # Check if template already exists in target
                            if await self.target_backend.template_exists(template.id):
                                logger.warning(f"Template {template.id} already exists in target, skipping")
                                continue
                            
                            # Save template to target backend
                            await self.target_backend.save_template(template)
                        
                        results["migrated"] += 1
                        logger.debug(f"{'Would migrate' if dry_run else 'Migrated'} template: {template.id}")
                        
                    except Exception as e:
                        error_msg = f"Failed to migrate template {template.id}: {e}"
                        logger.error(error_msg)
                        results["errors"].append(error_msg)
                
                # Small delay between batches
                if i + batch_size < len(templates):
                    await asyncio.sleep(0.1)
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to migrate templates: {e}")
            results["errors"].append(f"Template migration failed: {e}")
            return results
    
    async def _validate_clusters(self, validation_results: Dict[str, Any]) -> None:
        """Validate cluster migration."""
        try:
            source_clusters = await self.source_backend.list_clusters()
            target_clusters = await self.target_backend.list_clusters()
            
            validation_results["clusters"]["source_count"] = len(source_clusters)
            validation_results["clusters"]["target_count"] = len(target_clusters)
            
            # Create lookup maps
            source_map = {cluster.id: cluster for cluster in source_clusters}
            target_map = {cluster.id: cluster for cluster in target_clusters}
            
            # Check for missing clusters
            for cluster_id in source_map:
                if cluster_id not in target_map:
                    validation_results["clusters"]["missing"].append(cluster_id)
            
            # Check for mismatched clusters
            for cluster_id in source_map:
                if cluster_id in target_map:
                    source_cluster = source_map[cluster_id]
                    target_cluster = target_map[cluster_id]
                    
                    # Compare key fields (excluding timestamps which may differ)
                    if (source_cluster.name != target_cluster.name or
                        source_cluster.environment != target_cluster.environment or
                        source_cluster.kafka_config != target_cluster.kafka_config):
                        validation_results["clusters"]["mismatched"].append(cluster_id)
            
        except Exception as e:
            logger.error(f"Failed to validate clusters: {e}")
            validation_results["clusters"]["errors"] = [str(e)]
    
    async def _validate_templates(self, validation_results: Dict[str, Any]) -> None:
        """Validate template migration."""
        try:
            source_templates = await self.source_backend.list_templates()
            target_templates = await self.target_backend.list_templates()
            
            validation_results["templates"]["source_count"] = len(source_templates)
            validation_results["templates"]["target_count"] = len(target_templates)
            
            # Create lookup maps
            source_map = {template.id: template for template in source_templates}
            target_map = {template.id: template for template in target_templates}
            
            # Check for missing templates
            for template_id in source_map:
                if template_id not in target_map:
                    validation_results["templates"]["missing"].append(template_id)
            
            # Check for mismatched templates
            for template_id in source_map:
                if template_id in target_map:
                    source_template = source_map[template_id]
                    target_template = target_map[template_id]
                    
                    # Compare key fields
                    if (source_template.name != target_template.name or
                        source_template.category != target_template.category or
                        source_template.version != target_template.version):
                        validation_results["templates"]["mismatched"].append(template_id)
            
        except Exception as e:
            logger.error(f"Failed to validate templates: {e}")
            validation_results["templates"]["errors"] = [str(e)]


async def migrate_storage(source_backend: StorageBackend, target_backend: StorageBackend, 
                         dry_run: bool = False, batch_size: int = 10) -> Dict[str, Any]:
    """Convenience function for migrating storage backends.
    
    Args:
        source_backend: Source storage backend
        target_backend: Target storage backend
        dry_run: If True, only simulate the migration
        batch_size: Number of items to process in each batch
        
    Returns:
        Migration results
    """
    migrator = StorageMigrator(source_backend, target_backend)
    return await migrator.migrate_all(dry_run=dry_run, batch_size=batch_size)


async def validate_storage_migration(source_backend: StorageBackend, target_backend: StorageBackend) -> Dict[str, Any]:
    """Convenience function for validating storage migration.
    
    Args:
        source_backend: Source storage backend
        target_backend: Target storage backend
        
    Returns:
        Validation results
    """
    migrator = StorageMigrator(source_backend, target_backend)
    return await migrator.validate_migration()