"""
Database storage backend using SQLite.
"""

import json
import sqlite3
import aiosqlite
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


class DatabaseStorageBackend(StorageBackend):
    """Database storage backend using SQLite."""
    
    def __init__(self, database_path: str = "data/multi-cluster/storage.db"):
        """Initialize database storage backend.
        
        Args:
            database_path: Path to SQLite database file
        """
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection: Optional[aiosqlite.Connection] = None
    
    async def initialize(self) -> bool:
        """Initialize the storage backend."""
        try:
            # Connect to database
            self._connection = await aiosqlite.connect(str(self.database_path))
            
            # Enable foreign keys
            await self._connection.execute("PRAGMA foreign_keys = ON")
            
            # Create tables
            await self._create_tables()
            
            logger.info(f"Database storage backend initialized at {self.database_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize database storage backend: {e}")
            raise StorageBackendError(
                "Failed to initialize database storage backend",
                backend_type="database",
                operation="initialize",
                cause=e
            )
    
    async def close(self) -> None:
        """Close the storage backend and cleanup resources."""
        if self._connection:
            await self._connection.close()
            self._connection = None
        logger.info("Database storage backend closed")
    
    # Cluster operations
    
    async def save_cluster(self, cluster: ClusterDefinition) -> bool:
        """Save cluster definition."""
        try:
            # Check if cluster already exists
            if await self.cluster_exists(cluster.id):
                raise ClusterAlreadyExistsError(cluster.id)
            
            # Update timestamp
            cluster.update_timestamp()
            
            # Insert cluster
            cluster_data = cluster.dict()
            await self._connection.execute(
                """
                INSERT INTO clusters (
                    id, name, description, template_id, environment,
                    kafka_config, rest_proxy_config, ui_config, retention_policy,
                    port_allocation, network_name, data_directory,
                    created_at, updated_at, created_by, tags, status,
                    last_started, last_stopped
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    cluster.id,
                    cluster.name,
                    cluster.description,
                    cluster.template_id,
                    cluster.environment.value,
                    json.dumps(cluster.kafka_config.dict()),
                    json.dumps(cluster.rest_proxy_config.dict()),
                    json.dumps(cluster.ui_config.dict()),
                    json.dumps(cluster.retention_policy.dict()),
                    json.dumps(cluster.port_allocation.to_dict()) if cluster.port_allocation else None,
                    cluster.network_name,
                    cluster.data_directory,
                    cluster.created_at.isoformat(),
                    cluster.updated_at.isoformat(),
                    cluster.created_by,
                    json.dumps(cluster.tags),
                    cluster.status.value,
                    cluster.last_started.isoformat() if cluster.last_started else None,
                    cluster.last_stopped.isoformat() if cluster.last_stopped else None
                )
            )
            
            await self._connection.commit()
            
            logger.info(f"Saved cluster definition: {cluster.id}")
            return True
            
        except (ClusterAlreadyExistsError, StorageBackendError):
            raise
        except Exception as e:
            logger.error(f"Failed to save cluster {cluster.id}: {e}")
            raise StorageBackendError(
                f"Failed to save cluster {cluster.id}",
                backend_type="database",
                operation="save_cluster",
                cause=e
            )
    
    async def load_cluster(self, cluster_id: str) -> Optional[ClusterDefinition]:
        """Load cluster definition by ID."""
        try:
            cursor = await self._connection.execute(
                "SELECT * FROM clusters WHERE id = ?",
                (cluster_id,)
            )
            row = await cursor.fetchone()
            
            if not row:
                return None
            
            cluster = self._row_to_cluster(row)
            
            logger.debug(f"Loaded cluster definition: {cluster_id}")
            return cluster
            
        except Exception as e:
            logger.error(f"Failed to load cluster {cluster_id}: {e}")
            raise StorageBackendError(
                f"Failed to load cluster {cluster_id}",
                backend_type="database",
                operation="load_cluster",
                cause=e
            )
    
    async def update_cluster(self, cluster: ClusterDefinition) -> bool:
        """Update existing cluster definition."""
        try:
            # Check if cluster exists
            if not await self.cluster_exists(cluster.id):
                raise ClusterNotFoundError(cluster.id)
            
            # Update timestamp
            cluster.update_timestamp()
            
            # Update cluster
            cluster_data = cluster.dict()
            await self._connection.execute(
                """
                UPDATE clusters SET
                    name = ?, description = ?, template_id = ?, environment = ?,
                    kafka_config = ?, rest_proxy_config = ?, ui_config = ?, retention_policy = ?,
                    port_allocation = ?, network_name = ?, data_directory = ?,
                    updated_at = ?, created_by = ?, tags = ?, status = ?,
                    last_started = ?, last_stopped = ?
                WHERE id = ?
                """,
                (
                    cluster.name,
                    cluster.description,
                    cluster.template_id,
                    cluster.environment.value,
                    json.dumps(cluster.kafka_config.dict()),
                    json.dumps(cluster.rest_proxy_config.dict()),
                    json.dumps(cluster.ui_config.dict()),
                    json.dumps(cluster.retention_policy.dict()),
                    json.dumps(cluster.port_allocation.to_dict()) if cluster.port_allocation else None,
                    cluster.network_name,
                    cluster.data_directory,
                    cluster.updated_at.isoformat(),
                    cluster.created_by,
                    json.dumps(cluster.tags),
                    cluster.status.value,
                    cluster.last_started.isoformat() if cluster.last_started else None,
                    cluster.last_stopped.isoformat() if cluster.last_stopped else None,
                    cluster.id
                )
            )
            
            await self._connection.commit()
            
            logger.info(f"Updated cluster definition: {cluster.id}")
            return True
            
        except (ClusterNotFoundError, StorageBackendError):
            raise
        except Exception as e:
            logger.error(f"Failed to update cluster {cluster.id}: {e}")
            raise StorageBackendError(
                f"Failed to update cluster {cluster.id}",
                backend_type="database",
                operation="update_cluster",
                cause=e
            )
    
    async def delete_cluster(self, cluster_id: str) -> bool:
        """Delete cluster definition."""
        try:
            # Check if cluster exists
            if not await self.cluster_exists(cluster_id):
                raise ClusterNotFoundError(cluster_id)
            
            # Delete cluster
            await self._connection.execute(
                "DELETE FROM clusters WHERE id = ?",
                (cluster_id,)
            )
            
            await self._connection.commit()
            
            logger.info(f"Deleted cluster definition: {cluster_id}")
            return True
            
        except (ClusterNotFoundError, StorageBackendError):
            raise
        except Exception as e:
            logger.error(f"Failed to delete cluster {cluster_id}: {e}")
            raise StorageBackendError(
                f"Failed to delete cluster {cluster_id}",
                backend_type="database",
                operation="delete_cluster",
                cause=e
            )
    
    async def list_clusters(self, filters: Optional[Dict[str, Any]] = None) -> List[ClusterDefinition]:
        """List all cluster definitions with optional filtering."""
        try:
            query = "SELECT * FROM clusters"
            params = []
            
            # Apply filters
            if filters:
                conditions = []
                for key, value in filters.items():
                    if key in ["id", "name", "environment", "status", "template_id", "created_by"]:
                        conditions.append(f"{key} = ?")
                        params.append(value)
                
                if conditions:
                    query += " WHERE " + " AND ".join(conditions)
            
            query += " ORDER BY created_at DESC"
            
            cursor = await self._connection.execute(query, params)
            rows = await cursor.fetchall()
            
            clusters = []
            for row in rows:
                try:
                    cluster = self._row_to_cluster(row)
                    clusters.append(cluster)
                except Exception as e:
                    logger.warning(f"Failed to parse cluster row: {e}")
                    continue
            
            logger.debug(f"Listed {len(clusters)} clusters")
            return clusters
            
        except Exception as e:
            logger.error(f"Failed to list clusters: {e}")
            raise StorageBackendError(
                "Failed to list clusters",
                backend_type="database",
                operation="list_clusters",
                cause=e
            )
    
    async def cluster_exists(self, cluster_id: str) -> bool:
        """Check if cluster exists."""
        try:
            cursor = await self._connection.execute(
                "SELECT 1 FROM clusters WHERE id = ?",
                (cluster_id,)
            )
            row = await cursor.fetchone()
            return row is not None
        except Exception as e:
            logger.error(f"Failed to check cluster existence {cluster_id}: {e}")
            raise StorageBackendError(
                f"Failed to check cluster existence {cluster_id}",
                backend_type="database",
                operation="cluster_exists",
                cause=e
            )
    
    # Template operations
    
    async def save_template(self, template: ClusterTemplate) -> bool:
        """Save cluster template."""
        try:
            # Check if template already exists
            if await self.template_exists(template.id):
                raise TemplateAlreadyExistsError(template.id)
            
            # Insert template
            await self._connection.execute(
                """
                INSERT INTO templates (
                    id, name, description, category, version,
                    default_kafka_config, default_rest_proxy_config, 
                    default_ui_config, default_retention_policy,
                    min_memory_mb, min_disk_gb, recommended_memory_mb, recommended_disk_gb,
                    created_at, is_builtin, tags
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    template.id,
                    template.name,
                    template.description,
                    template.category.value,
                    template.version,
                    json.dumps(template.default_kafka_config.dict()),
                    json.dumps(template.default_rest_proxy_config.dict()),
                    json.dumps(template.default_ui_config.dict()),
                    json.dumps(template.default_retention_policy.dict()),
                    template.min_memory_mb,
                    template.min_disk_gb,
                    template.recommended_memory_mb,
                    template.recommended_disk_gb,
                    template.created_at.isoformat(),
                    template.is_builtin,
                    json.dumps(template.tags)
                )
            )
            
            await self._connection.commit()
            
            logger.info(f"Saved template definition: {template.id}")
            return True
            
        except (TemplateAlreadyExistsError, StorageBackendError):
            raise
        except Exception as e:
            logger.error(f"Failed to save template {template.id}: {e}")
            raise StorageBackendError(
                f"Failed to save template {template.id}",
                backend_type="database",
                operation="save_template",
                cause=e
            )
    
    async def load_template(self, template_id: str) -> Optional[ClusterTemplate]:
        """Load cluster template by ID."""
        try:
            cursor = await self._connection.execute(
                "SELECT * FROM templates WHERE id = ?",
                (template_id,)
            )
            row = await cursor.fetchone()
            
            if not row:
                return None
            
            template = self._row_to_template(row)
            
            logger.debug(f"Loaded template definition: {template_id}")
            return template
            
        except Exception as e:
            logger.error(f"Failed to load template {template_id}: {e}")
            raise StorageBackendError(
                f"Failed to load template {template_id}",
                backend_type="database",
                operation="load_template",
                cause=e
            )
    
    async def update_template(self, template: ClusterTemplate) -> bool:
        """Update existing cluster template."""
        try:
            # Check if template exists
            if not await self.template_exists(template.id):
                raise TemplateNotFoundError(template.id)
            
            # Update template
            await self._connection.execute(
                """
                UPDATE templates SET
                    name = ?, description = ?, category = ?, version = ?,
                    default_kafka_config = ?, default_rest_proxy_config = ?, 
                    default_ui_config = ?, default_retention_policy = ?,
                    min_memory_mb = ?, min_disk_gb = ?, recommended_memory_mb = ?, recommended_disk_gb = ?,
                    is_builtin = ?, tags = ?
                WHERE id = ?
                """,
                (
                    template.name,
                    template.description,
                    template.category.value,
                    template.version,
                    json.dumps(template.default_kafka_config.dict()),
                    json.dumps(template.default_rest_proxy_config.dict()),
                    json.dumps(template.default_ui_config.dict()),
                    json.dumps(template.default_retention_policy.dict()),
                    template.min_memory_mb,
                    template.min_disk_gb,
                    template.recommended_memory_mb,
                    template.recommended_disk_gb,
                    template.is_builtin,
                    json.dumps(template.tags),
                    template.id
                )
            )
            
            await self._connection.commit()
            
            logger.info(f"Updated template definition: {template.id}")
            return True
            
        except (TemplateNotFoundError, StorageBackendError):
            raise
        except Exception as e:
            logger.error(f"Failed to update template {template.id}: {e}")
            raise StorageBackendError(
                f"Failed to update template {template.id}",
                backend_type="database",
                operation="update_template",
                cause=e
            )
    
    async def delete_template(self, template_id: str) -> bool:
        """Delete cluster template."""
        try:
            # Check if template exists
            if not await self.template_exists(template_id):
                raise TemplateNotFoundError(template_id)
            
            # Delete template
            await self._connection.execute(
                "DELETE FROM templates WHERE id = ?",
                (template_id,)
            )
            
            await self._connection.commit()
            
            logger.info(f"Deleted template definition: {template_id}")
            return True
            
        except (TemplateNotFoundError, StorageBackendError):
            raise
        except Exception as e:
            logger.error(f"Failed to delete template {template_id}: {e}")
            raise StorageBackendError(
                f"Failed to delete template {template_id}",
                backend_type="database",
                operation="delete_template",
                cause=e
            )
    
    async def list_templates(self, filters: Optional[Dict[str, Any]] = None) -> List[ClusterTemplate]:
        """List all cluster templates with optional filtering."""
        try:
            query = "SELECT * FROM templates"
            params = []
            
            # Apply filters
            if filters:
                conditions = []
                for key, value in filters.items():
                    if key in ["id", "name", "category", "version", "is_builtin"]:
                        conditions.append(f"{key} = ?")
                        params.append(value)
                
                if conditions:
                    query += " WHERE " + " AND ".join(conditions)
            
            query += " ORDER BY created_at DESC"
            
            cursor = await self._connection.execute(query, params)
            rows = await cursor.fetchall()
            
            templates = []
            for row in rows:
                try:
                    template = self._row_to_template(row)
                    templates.append(template)
                except Exception as e:
                    logger.warning(f"Failed to parse template row: {e}")
                    continue
            
            logger.debug(f"Listed {len(templates)} templates")
            return templates
            
        except Exception as e:
            logger.error(f"Failed to list templates: {e}")
            raise StorageBackendError(
                "Failed to list templates",
                backend_type="database",
                operation="list_templates",
                cause=e
            )
    
    async def template_exists(self, template_id: str) -> bool:
        """Check if template exists."""
        try:
            cursor = await self._connection.execute(
                "SELECT 1 FROM templates WHERE id = ?",
                (template_id,)
            )
            row = await cursor.fetchone()
            return row is not None
        except Exception as e:
            logger.error(f"Failed to check template existence {template_id}: {e}")
            raise StorageBackendError(
                f"Failed to check template existence {template_id}",
                backend_type="database",
                operation="template_exists",
                cause=e
            )
    
    # Utility methods
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on storage backend."""
        try:
            # Test database connection
            cursor = await self._connection.execute("SELECT 1")
            await cursor.fetchone()
            
            # Count records
            cursor = await self._connection.execute("SELECT COUNT(*) FROM clusters")
            cluster_count = (await cursor.fetchone())[0]
            
            cursor = await self._connection.execute("SELECT COUNT(*) FROM templates")
            template_count = (await cursor.fetchone())[0]
            
            # Get database size
            cursor = await self._connection.execute("PRAGMA page_count")
            page_count = (await cursor.fetchone())[0]
            cursor = await self._connection.execute("PRAGMA page_size")
            page_size = (await cursor.fetchone())[0]
            db_size_bytes = page_count * page_size
            
            return {
                "status": "healthy",
                "backend_type": "database",
                "database_path": str(self.database_path),
                "cluster_count": cluster_count,
                "template_count": template_count,
                "database_size_bytes": db_size_bytes,
                "database_size_mb": db_size_bytes // (1024 * 1024),
                "last_check": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                "status": "unhealthy",
                "backend_type": "database",
                "error": str(e),
                "last_check": datetime.utcnow().isoformat()
            }
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get storage backend statistics."""
        try:
            # Record counts
            cursor = await self._connection.execute("SELECT COUNT(*) FROM clusters")
            cluster_count = (await cursor.fetchone())[0]
            
            cursor = await self._connection.execute("SELECT COUNT(*) FROM templates")
            template_count = (await cursor.fetchone())[0]
            
            # Database size
            cursor = await self._connection.execute("PRAGMA page_count")
            page_count = (await cursor.fetchone())[0]
            cursor = await self._connection.execute("PRAGMA page_size")
            page_size = (await cursor.fetchone())[0]
            db_size_bytes = page_count * page_size
            
            return {
                "backend_type": "database",
                "database_path": str(self.database_path),
                "cluster_count": cluster_count,
                "template_count": template_count,
                "database_size_bytes": db_size_bytes,
                "database_size_mb": db_size_bytes // (1024 * 1024),
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to get storage stats: {e}")
            raise StorageBackendError(
                "Failed to get storage statistics",
                backend_type="database",
                operation="get_stats",
                cause=e
            )
    
    async def backup(self, backup_path: str) -> bool:
        """Create backup of storage data."""
        try:
            backup_file = Path(backup_path)
            backup_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Create backup using SQLite backup API
            backup_conn = await aiosqlite.connect(str(backup_file))
            try:
                await self._connection.backup(backup_conn)
                logger.info(f"Created database backup at {backup_path}")
                return True
            finally:
                await backup_conn.close()
            
        except Exception as e:
            logger.error(f"Failed to create backup: {e}")
            raise StorageBackendError(
                "Failed to create backup",
                backend_type="database",
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
                    backend_type="database",
                    operation="restore"
                )
            
            # Close current connection
            await self.close()
            
            # Replace database file with backup
            if self.database_path.exists():
                self.database_path.unlink()
            
            backup_file.rename(self.database_path)
            
            # Reconnect
            await self.initialize()
            
            logger.info(f"Restored database backup from {backup_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to restore backup: {e}")
            raise StorageBackendError(
                "Failed to restore backup",
                backend_type="database",
                operation="restore",
                cause=e
            )
    
    # Private helper methods
    
    async def _create_tables(self) -> None:
        """Create database tables."""
        # Clusters table
        await self._connection.execute("""
            CREATE TABLE IF NOT EXISTS clusters (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                template_id TEXT,
                environment TEXT NOT NULL,
                kafka_config TEXT NOT NULL,
                rest_proxy_config TEXT NOT NULL,
                ui_config TEXT NOT NULL,
                retention_policy TEXT NOT NULL,
                port_allocation TEXT,
                network_name TEXT,
                data_directory TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                created_by TEXT NOT NULL,
                tags TEXT NOT NULL,
                status TEXT NOT NULL,
                last_started TEXT,
                last_stopped TEXT
            )
        """)
        
        # Templates table
        await self._connection.execute("""
            CREATE TABLE IF NOT EXISTS templates (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT NOT NULL,
                category TEXT NOT NULL,
                version TEXT NOT NULL,
                default_kafka_config TEXT NOT NULL,
                default_rest_proxy_config TEXT NOT NULL,
                default_ui_config TEXT NOT NULL,
                default_retention_policy TEXT NOT NULL,
                min_memory_mb INTEGER NOT NULL,
                min_disk_gb INTEGER NOT NULL,
                recommended_memory_mb INTEGER NOT NULL,
                recommended_disk_gb INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                is_builtin BOOLEAN NOT NULL,
                tags TEXT NOT NULL
            )
        """)
        
        # Create indexes
        await self._connection.execute("CREATE INDEX IF NOT EXISTS idx_clusters_environment ON clusters(environment)")
        await self._connection.execute("CREATE INDEX IF NOT EXISTS idx_clusters_status ON clusters(status)")
        await self._connection.execute("CREATE INDEX IF NOT EXISTS idx_clusters_template_id ON clusters(template_id)")
        await self._connection.execute("CREATE INDEX IF NOT EXISTS idx_templates_category ON templates(category)")
        await self._connection.execute("CREATE INDEX IF NOT EXISTS idx_templates_builtin ON templates(is_builtin)")
        
        await self._connection.commit()
    
    def _row_to_cluster(self, row) -> ClusterDefinition:
        """Convert database row to ClusterDefinition."""
        from ..models.multi_cluster import (
            ClusterEnvironment, KafkaConfig, RestProxyConfig, 
            UIConfig, RetentionPolicy, PortAllocation
        )
        from ..models.base import ServiceStatus
        
        # Parse JSON fields
        kafka_config = KafkaConfig(**json.loads(row[5]))
        rest_proxy_config = RestProxyConfig(**json.loads(row[6]))
        ui_config = UIConfig(**json.loads(row[7]))
        retention_policy = RetentionPolicy(**json.loads(row[8]))
        
        port_allocation = None
        if row[9]:
            port_data = json.loads(row[9])
            port_allocation = PortAllocation(**port_data)
        
        tags = json.loads(row[15])
        
        return ClusterDefinition(
            id=row[0],
            name=row[1],
            description=row[2],
            template_id=row[3],
            environment=ClusterEnvironment(row[4]),
            kafka_config=kafka_config,
            rest_proxy_config=rest_proxy_config,
            ui_config=ui_config,
            retention_policy=retention_policy,
            port_allocation=port_allocation,
            network_name=row[10],
            data_directory=row[11],
            created_at=datetime.fromisoformat(row[12]),
            updated_at=datetime.fromisoformat(row[13]),
            created_by=row[14],
            tags=tags,
            status=ServiceStatus(row[16]),
            last_started=datetime.fromisoformat(row[17]) if row[17] else None,
            last_stopped=datetime.fromisoformat(row[18]) if row[18] else None
        )
    
    def _row_to_template(self, row) -> ClusterTemplate:
        """Convert database row to ClusterTemplate."""
        from ..models.multi_cluster import (
            TemplateCategory, KafkaConfig, RestProxyConfig, 
            UIConfig, RetentionPolicy
        )
        
        # Parse JSON fields
        kafka_config = KafkaConfig(**json.loads(row[5]))
        rest_proxy_config = RestProxyConfig(**json.loads(row[6]))
        ui_config = UIConfig(**json.loads(row[7]))
        retention_policy = RetentionPolicy(**json.loads(row[8]))
        tags = json.loads(row[15])
        
        return ClusterTemplate(
            id=row[0],
            name=row[1],
            description=row[2],
            category=TemplateCategory(row[3]),
            version=row[4],
            default_kafka_config=kafka_config,
            default_rest_proxy_config=rest_proxy_config,
            default_ui_config=ui_config,
            default_retention_policy=retention_policy,
            min_memory_mb=row[9],
            min_disk_gb=row[10],
            recommended_memory_mb=row[11],
            recommended_disk_gb=row[12],
            created_at=datetime.fromisoformat(row[13]),
            is_builtin=bool(row[14]),
            tags=tags
        )