"""
File-based storage backend using JSON files.
"""

import json
import asyncio
import aiofiles
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any
from ..models.multi_cluster import ClusterDefinition, ClusterTemplate
from ..exceptions import (
    StorageBackendError,
    ClusterNotFoundError,
    ClusterAlreadyExistsError,
    TemplateNotFoundError,
    TemplateAlreadyExistsError
)
from ..utils.logging import get_logger
from .base import StorageBackend

logger = get_logger(__name__)


class FileStorageBackend(StorageBackend):
    """File-based storage backend using JSON files."""
    
    def __init__(self, storage_dir: str = "data/multi-cluster"):
        """Initialize file storage backend.
        
        Args:
            storage_dir: Directory to store data files
        """
        self.storage_dir = Path(storage_dir)
        self.clusters_dir = self.storage_dir / "clusters"
        self.templates_dir = self.storage_dir / "templates"
        self.backup_dir = self.storage_dir / "backups"
        
        # File locks for concurrent access
        self._cluster_locks: Dict[str, asyncio.Lock] = {}
        self._template_locks: Dict[str, asyncio.Lock] = {}
        self._global_lock = asyncio.Lock()
    
    async def initialize(self) -> bool:
        """Initialize the storage backend."""
        try:
            # Create directories if they don't exist
            self.storage_dir.mkdir(parents=True, exist_ok=True)
            self.clusters_dir.mkdir(parents=True, exist_ok=True)
            self.templates_dir.mkdir(parents=True, exist_ok=True)
            self.backup_dir.mkdir(parents=True, exist_ok=True)
            
            # Create index files if they don't exist
            await self._ensure_index_files()
            
            logger.info(f"File storage backend initialized at {self.storage_dir}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize file storage backend: {e}")
            raise StorageBackendError(
                "Failed to initialize file storage backend",
                backend_type="file",
                operation="initialize",
                cause=e
            )
    
    async def close(self) -> None:
        """Close the storage backend and cleanup resources."""
        # Clear locks
        self._cluster_locks.clear()
        self._template_locks.clear()
        logger.info("File storage backend closed")
    
    # Cluster operations
    
    async def save_cluster(self, cluster: ClusterDefinition) -> bool:
        """Save cluster definition."""
        try:
            async with self._get_cluster_lock(cluster.id):
                cluster_file = self.clusters_dir / f"{cluster.id}.json"
                
                # Check if cluster already exists for create operations
                if cluster_file.exists():
                    raise ClusterAlreadyExistsError(cluster.id)
                
                # Update timestamp
                cluster.update_timestamp()
                
                # Save cluster data
                cluster_data = cluster.dict()
                await self._write_json_file(cluster_file, cluster_data)
                
                # Update index
                await self._update_cluster_index(cluster.id, "create")
                
                logger.info(f"Saved cluster definition: {cluster.id}")
                return True
                
        except (ClusterAlreadyExistsError, StorageBackendError):
            raise
        except Exception as e:
            logger.error(f"Failed to save cluster {cluster.id}: {e}")
            raise StorageBackendError(
                f"Failed to save cluster {cluster.id}",
                backend_type="file",
                operation="save_cluster",
                cause=e
            )
    
    async def load_cluster(self, cluster_id: str) -> Optional[ClusterDefinition]:
        """Load cluster definition by ID."""
        try:
            async with self._get_cluster_lock(cluster_id):
                cluster_file = self.clusters_dir / f"{cluster_id}.json"
                
                if not cluster_file.exists():
                    return None
                
                cluster_data = await self._read_json_file(cluster_file)
                cluster = ClusterDefinition(**cluster_data)
                
                logger.debug(f"Loaded cluster definition: {cluster_id}")
                return cluster
                
        except Exception as e:
            logger.error(f"Failed to load cluster {cluster_id}: {e}")
            raise StorageBackendError(
                f"Failed to load cluster {cluster_id}",
                backend_type="file",
                operation="load_cluster",
                cause=e
            )
    
    async def update_cluster(self, cluster: ClusterDefinition) -> bool:
        """Update existing cluster definition."""
        try:
            async with self._get_cluster_lock(cluster.id):
                cluster_file = self.clusters_dir / f"{cluster.id}.json"
                
                if not cluster_file.exists():
                    raise ClusterNotFoundError(cluster.id)
                
                # Update timestamp
                cluster.update_timestamp()
                
                # Save updated cluster data
                cluster_data = cluster.dict()
                await self._write_json_file(cluster_file, cluster_data)
                
                # Update index
                await self._update_cluster_index(cluster.id, "update")
                
                logger.info(f"Updated cluster definition: {cluster.id}")
                return True
                
        except (ClusterNotFoundError, StorageBackendError):
            raise
        except Exception as e:
            logger.error(f"Failed to update cluster {cluster.id}: {e}")
            raise StorageBackendError(
                f"Failed to update cluster {cluster.id}",
                backend_type="file",
                operation="update_cluster",
                cause=e
            )
    
    async def delete_cluster(self, cluster_id: str) -> bool:
        """Delete cluster definition."""
        try:
            async with self._get_cluster_lock(cluster_id):
                cluster_file = self.clusters_dir / f"{cluster_id}.json"
                
                if not cluster_file.exists():
                    raise ClusterNotFoundError(cluster_id)
                
                # Delete cluster file
                cluster_file.unlink()
                
                # Update index
                await self._update_cluster_index(cluster_id, "delete")
                
                logger.info(f"Deleted cluster definition: {cluster_id}")
                return True
                
        except (ClusterNotFoundError, StorageBackendError):
            raise
        except Exception as e:
            logger.error(f"Failed to delete cluster {cluster_id}: {e}")
            raise StorageBackendError(
                f"Failed to delete cluster {cluster_id}",
                backend_type="file",
                operation="delete_cluster",
                cause=e
            )
    
    async def list_clusters(self, filters: Optional[Dict[str, Any]] = None) -> List[ClusterDefinition]:
        """List all cluster definitions with optional filtering."""
        try:
            clusters = []
            
            # Get all cluster files
            cluster_files = list(self.clusters_dir.glob("*.json"))
            
            for cluster_file in cluster_files:
                try:
                    cluster_data = await self._read_json_file(cluster_file)
                    cluster = ClusterDefinition(**cluster_data)
                    
                    # Apply filters if provided
                    if filters and not self._matches_filters(cluster.dict(), filters):
                        continue
                    
                    clusters.append(cluster)
                    
                except Exception as e:
                    logger.warning(f"Failed to load cluster from {cluster_file}: {e}")
                    continue
            
            logger.debug(f"Listed {len(clusters)} clusters")
            return clusters
            
        except Exception as e:
            logger.error(f"Failed to list clusters: {e}")
            raise StorageBackendError(
                "Failed to list clusters",
                backend_type="file",
                operation="list_clusters",
                cause=e
            )
    
    async def cluster_exists(self, cluster_id: str) -> bool:
        """Check if cluster exists."""
        try:
            cluster_file = self.clusters_dir / f"{cluster_id}.json"
            return cluster_file.exists()
        except Exception as e:
            logger.error(f"Failed to check cluster existence {cluster_id}: {e}")
            raise StorageBackendError(
                f"Failed to check cluster existence {cluster_id}",
                backend_type="file",
                operation="cluster_exists",
                cause=e
            )
    
    # Template operations
    
    async def save_template(self, template: ClusterTemplate) -> bool:
        """Save cluster template."""
        try:
            async with self._get_template_lock(template.id):
                template_file = self.templates_dir / f"{template.id}.json"
                
                # Check if template already exists for create operations
                if template_file.exists():
                    raise TemplateAlreadyExistsError(template.id)
                
                # Save template data
                template_data = template.dict()
                await self._write_json_file(template_file, template_data)
                
                # Update index
                await self._update_template_index(template.id, "create")
                
                logger.info(f"Saved template definition: {template.id}")
                return True
                
        except (TemplateAlreadyExistsError, StorageBackendError):
            raise
        except Exception as e:
            logger.error(f"Failed to save template {template.id}: {e}")
            raise StorageBackendError(
                f"Failed to save template {template.id}",
                backend_type="file",
                operation="save_template",
                cause=e
            )
    
    async def load_template(self, template_id: str) -> Optional[ClusterTemplate]:
        """Load cluster template by ID."""
        try:
            async with self._get_template_lock(template_id):
                template_file = self.templates_dir / f"{template_id}.json"
                
                if not template_file.exists():
                    return None
                
                template_data = await self._read_json_file(template_file)
                template = ClusterTemplate(**template_data)
                
                logger.debug(f"Loaded template definition: {template_id}")
                return template
                
        except Exception as e:
            logger.error(f"Failed to load template {template_id}: {e}")
            raise StorageBackendError(
                f"Failed to load template {template_id}",
                backend_type="file",
                operation="load_template",
                cause=e
            )
    
    async def update_template(self, template: ClusterTemplate) -> bool:
        """Update existing cluster template."""
        try:
            async with self._get_template_lock(template.id):
                template_file = self.templates_dir / f"{template.id}.json"
                
                if not template_file.exists():
                    raise TemplateNotFoundError(template.id)
                
                # Save updated template data
                template_data = template.dict()
                await self._write_json_file(template_file, template_data)
                
                # Update index
                await self._update_template_index(template.id, "update")
                
                logger.info(f"Updated template definition: {template.id}")
                return True
                
        except (TemplateNotFoundError, StorageBackendError):
            raise
        except Exception as e:
            logger.error(f"Failed to update template {template.id}: {e}")
            raise StorageBackendError(
                f"Failed to update template {template.id}",
                backend_type="file",
                operation="update_template",
                cause=e
            )
    
    async def delete_template(self, template_id: str) -> bool:
        """Delete cluster template."""
        try:
            async with self._get_template_lock(template_id):
                template_file = self.templates_dir / f"{template_id}.json"
                
                if not template_file.exists():
                    raise TemplateNotFoundError(template_id)
                
                # Delete template file
                template_file.unlink()
                
                # Update index
                await self._update_template_index(template_id, "delete")
                
                logger.info(f"Deleted template definition: {template_id}")
                return True
                
        except (TemplateNotFoundError, StorageBackendError):
            raise
        except Exception as e:
            logger.error(f"Failed to delete template {template_id}: {e}")
            raise StorageBackendError(
                f"Failed to delete template {template_id}",
                backend_type="file",
                operation="delete_template",
                cause=e
            )
    
    async def list_templates(self, filters: Optional[Dict[str, Any]] = None) -> List[ClusterTemplate]:
        """List all cluster templates with optional filtering."""
        try:
            templates = []
            
            # Get all template files
            template_files = list(self.templates_dir.glob("*.json"))
            
            for template_file in template_files:
                try:
                    template_data = await self._read_json_file(template_file)
                    template = ClusterTemplate(**template_data)
                    
                    # Apply filters if provided
                    if filters and not self._matches_filters(template.dict(), filters):
                        continue
                    
                    templates.append(template)
                    
                except Exception as e:
                    logger.warning(f"Failed to load template from {template_file}: {e}")
                    continue
            
            logger.debug(f"Listed {len(templates)} templates")
            return templates
            
        except Exception as e:
            logger.error(f"Failed to list templates: {e}")
            raise StorageBackendError(
                "Failed to list templates",
                backend_type="file",
                operation="list_templates",
                cause=e
            )
    
    async def template_exists(self, template_id: str) -> bool:
        """Check if template exists."""
        try:
            template_file = self.templates_dir / f"{template_id}.json"
            return template_file.exists()
        except Exception as e:
            logger.error(f"Failed to check template existence {template_id}: {e}")
            raise StorageBackendError(
                f"Failed to check template existence {template_id}",
                backend_type="file",
                operation="template_exists",
                cause=e
            )
    
    # Utility methods
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on storage backend."""
        try:
            # Check if directories exist and are writable
            directories_ok = all([
                self.storage_dir.exists() and self.storage_dir.is_dir(),
                self.clusters_dir.exists() and self.clusters_dir.is_dir(),
                self.templates_dir.exists() and self.templates_dir.is_dir(),
                self.backup_dir.exists() and self.backup_dir.is_dir()
            ])
            
            # Count files
            cluster_count = len(list(self.clusters_dir.glob("*.json")))
            template_count = len(list(self.templates_dir.glob("*.json")))
            
            # Check disk space
            storage_stats = shutil.disk_usage(self.storage_dir)
            free_space_mb = storage_stats.free // (1024 * 1024)
            
            return {
                "status": "healthy" if directories_ok else "unhealthy",
                "backend_type": "file",
                "storage_dir": str(self.storage_dir),
                "directories_ok": directories_ok,
                "cluster_count": cluster_count,
                "template_count": template_count,
                "free_space_mb": free_space_mb,
                "last_check": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                "status": "unhealthy",
                "backend_type": "file",
                "error": str(e),
                "last_check": datetime.utcnow().isoformat()
            }
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get storage backend statistics."""
        try:
            # File counts
            cluster_files = list(self.clusters_dir.glob("*.json"))
            template_files = list(self.templates_dir.glob("*.json"))
            
            # Calculate total size
            total_size = sum(f.stat().st_size for f in cluster_files + template_files)
            
            # Disk usage
            storage_stats = shutil.disk_usage(self.storage_dir)
            
            return {
                "backend_type": "file",
                "storage_dir": str(self.storage_dir),
                "cluster_count": len(cluster_files),
                "template_count": len(template_files),
                "total_size_bytes": total_size,
                "total_size_mb": total_size // (1024 * 1024),
                "disk_total_gb": storage_stats.total // (1024 * 1024 * 1024),
                "disk_used_gb": storage_stats.used // (1024 * 1024 * 1024),
                "disk_free_gb": storage_stats.free // (1024 * 1024 * 1024),
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to get storage stats: {e}")
            raise StorageBackendError(
                "Failed to get storage statistics",
                backend_type="file",
                operation="get_stats",
                cause=e
            )
    
    async def backup(self, backup_path: str) -> bool:
        """Create backup of storage data."""
        try:
            backup_file = Path(backup_path)
            backup_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Create tar archive of storage directory
            shutil.make_archive(
                str(backup_file.with_suffix("")),
                "gztar",
                self.storage_dir
            )
            
            logger.info(f"Created backup at {backup_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create backup: {e}")
            raise StorageBackendError(
                "Failed to create backup",
                backend_type="file",
                operation="backup",
                cause=e
            )
    
    async def restore(self, backup_path: str) -> bool:
        """Restore storage data from backup."""
        try:
            backup_file = Path(backup_path)
            
            if not backup_file.exists():
                raise StorageBackendError(
                    f"Backup file not found: {backup_path}",
                    backend_type="file",
                    operation="restore"
                )
            
            # Create temporary directory for extraction
            temp_dir = self.storage_dir.parent / f"restore_temp_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
            temp_dir.mkdir(parents=True, exist_ok=True)
            
            try:
                # Extract backup
                shutil.unpack_archive(str(backup_file), str(temp_dir))
                
                # Replace current storage with backup
                if self.storage_dir.exists():
                    shutil.rmtree(self.storage_dir)
                
                # Move extracted data to storage directory
                extracted_dir = temp_dir / self.storage_dir.name
                if extracted_dir.exists():
                    shutil.move(str(extracted_dir), str(self.storage_dir))
                else:
                    # If backup was created differently, move the temp dir contents
                    shutil.move(str(temp_dir), str(self.storage_dir))
                
                logger.info(f"Restored backup from {backup_path}")
                return True
                
            finally:
                # Cleanup temp directory
                if temp_dir.exists():
                    shutil.rmtree(temp_dir)
            
        except Exception as e:
            logger.error(f"Failed to restore backup: {e}")
            raise StorageBackendError(
                "Failed to restore backup",
                backend_type="file",
                operation="restore",
                cause=e
            )
    
    # Private helper methods
    
    def _get_cluster_lock(self, cluster_id: str) -> asyncio.Lock:
        """Get or create lock for cluster operations."""
        if cluster_id not in self._cluster_locks:
            self._cluster_locks[cluster_id] = asyncio.Lock()
        return self._cluster_locks[cluster_id]
    
    def _get_template_lock(self, template_id: str) -> asyncio.Lock:
        """Get or create lock for template operations."""
        if template_id not in self._template_locks:
            self._template_locks[template_id] = asyncio.Lock()
        return self._template_locks[template_id]
    
    async def _read_json_file(self, file_path: Path) -> Dict[str, Any]:
        """Read JSON data from file."""
        async with aiofiles.open(file_path, 'r') as f:
            content = await f.read()
            return json.loads(content)
    
    async def _write_json_file(self, file_path: Path, data: Dict[str, Any]) -> None:
        """Write JSON data to file."""
        async with aiofiles.open(file_path, 'w') as f:
            content = json.dumps(data, indent=2, default=str)
            await f.write(content)
    
    async def _ensure_index_files(self) -> None:
        """Ensure index files exist."""
        cluster_index = self.clusters_dir / "index.json"
        template_index = self.templates_dir / "index.json"
        
        if not cluster_index.exists():
            await self._write_json_file(cluster_index, {"clusters": [], "last_updated": datetime.utcnow().isoformat()})
        
        if not template_index.exists():
            await self._write_json_file(template_index, {"templates": [], "last_updated": datetime.utcnow().isoformat()})
    
    async def _update_cluster_index(self, cluster_id: str, operation: str) -> None:
        """Update cluster index file."""
        try:
            index_file = self.clusters_dir / "index.json"
            index_data = await self._read_json_file(index_file)
            
            clusters = index_data.get("clusters", [])
            
            if operation == "create":
                if cluster_id not in clusters:
                    clusters.append(cluster_id)
            elif operation == "delete":
                if cluster_id in clusters:
                    clusters.remove(cluster_id)
            
            index_data["clusters"] = clusters
            index_data["last_updated"] = datetime.utcnow().isoformat()
            
            await self._write_json_file(index_file, index_data)
            
        except Exception as e:
            logger.warning(f"Failed to update cluster index: {e}")
    
    async def _update_template_index(self, template_id: str, operation: str) -> None:
        """Update template index file."""
        try:
            index_file = self.templates_dir / "index.json"
            index_data = await self._read_json_file(index_file)
            
            templates = index_data.get("templates", [])
            
            if operation == "create":
                if template_id not in templates:
                    templates.append(template_id)
            elif operation == "delete":
                if template_id in templates:
                    templates.remove(template_id)
            
            index_data["templates"] = templates
            index_data["last_updated"] = datetime.utcnow().isoformat()
            
            await self._write_json_file(index_file, index_data)
            
        except Exception as e:
            logger.warning(f"Failed to update template index: {e}")
    
    def _matches_filters(self, data: Dict[str, Any], filters: Dict[str, Any]) -> bool:
        """Check if data matches the provided filters."""
        for key, value in filters.items():
            if key not in data:
                return False
            
            data_value = data[key]
            
            # Handle different filter types
            if isinstance(value, list):
                # Value must be in the list
                if data_value not in value:
                    return False
            elif isinstance(value, dict):
                # Handle nested filters (not implemented for now)
                continue
            else:
                # Exact match
                if data_value != value:
                    return False
        
        return True