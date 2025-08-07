"""
Advanced cluster features for multi-cluster Kafka deployments.

This module provides advanced cluster management capabilities including
cluster cloning, snapshots, scheduling, and enhanced tagging systems.
"""

import asyncio
import logging
import json
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
import uuid

from src.models.multi_cluster import ClusterDefinition, PortAllocation
from src.services.multi_cluster_manager import MultiClusterManager
from src.registry.cluster_registry import ClusterRegistry
from src.exceptions import ClusterError, ValidationError


class SnapshotType(Enum):
    """Types of cluster snapshots."""
    FULL = "full"
    CONFIG_ONLY = "config_only"
    DATA_ONLY = "data_only"
    INCREMENTAL = "incremental"


class ScheduleType(Enum):
    """Types of cluster schedules."""
    START = "start"
    STOP = "stop"
    RESTART = "restart"
    BACKUP = "backup"
    SNAPSHOT = "snapshot"
    CLEANUP = "cleanup"


class ScheduleFrequency(Enum):
    """Schedule frequency options."""
    ONCE = "once"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    HOURLY = "hourly"
    CUSTOM_CRON = "custom_cron"


@dataclass
class ClusterSnapshot:
    """Represents a cluster snapshot."""
    id: str
    cluster_id: str
    name: str
    description: Optional[str]
    snapshot_type: SnapshotType
    created_at: datetime
    size_bytes: int
    file_path: Path
    metadata: Dict[str, Any] = field(default_factory=dict)
    tags: Set[str] = field(default_factory=set)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert snapshot to dictionary."""
        return {
            "id": self.id,
            "cluster_id": self.cluster_id,
            "name": self.name,
            "description": self.description,
            "snapshot_type": self.snapshot_type.value,
            "created_at": self.created_at.isoformat(),
            "size_bytes": self.size_bytes,
            "file_path": str(self.file_path),
            "metadata": self.metadata,
            "tags": list(self.tags)
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ClusterSnapshot':
        """Create snapshot from dictionary."""
        return cls(
            id=data["id"],
            cluster_id=data["cluster_id"],
            name=data["name"],
            description=data.get("description"),
            snapshot_type=SnapshotType(data["snapshot_type"]),
            created_at=datetime.fromisoformat(data["created_at"]),
            size_bytes=data["size_bytes"],
            file_path=Path(data["file_path"]),
            metadata=data.get("metadata", {}),
            tags=set(data.get("tags", []))
        )
@
dataclass
class ClusterSchedule:
    """Represents a cluster schedule."""
    id: str
    cluster_id: str
    name: str
    description: Optional[str]
    schedule_type: ScheduleType
    frequency: ScheduleFrequency
    schedule_expression: str  # Cron expression or simple schedule
    enabled: bool
    created_at: datetime
    last_executed: Optional[datetime] = None
    next_execution: Optional[datetime] = None
    execution_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    tags: Set[str] = field(default_factory=set)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert schedule to dictionary."""
        return {
            "id": self.id,
            "cluster_id": self.cluster_id,
            "name": self.name,
            "description": self.description,
            "schedule_type": self.schedule_type.value,
            "frequency": self.frequency.value,
            "schedule_expression": self.schedule_expression,
            "enabled": self.enabled,
            "created_at": self.created_at.isoformat(),
            "last_executed": self.last_executed.isoformat() if self.last_executed else None,
            "next_execution": self.next_execution.isoformat() if self.next_execution else None,
            "execution_count": self.execution_count,
            "metadata": self.metadata,
            "tags": list(self.tags)
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ClusterSchedule':
        """Create schedule from dictionary."""
        return cls(
            id=data["id"],
            cluster_id=data["cluster_id"],
            name=data["name"],
            description=data.get("description"),
            schedule_type=ScheduleType(data["schedule_type"]),
            frequency=ScheduleFrequency(data["frequency"]),
            schedule_expression=data["schedule_expression"],
            enabled=data["enabled"],
            created_at=datetime.fromisoformat(data["created_at"]),
            last_executed=datetime.fromisoformat(data["last_executed"]) if data.get("last_executed") else None,
            next_execution=datetime.fromisoformat(data["next_execution"]) if data.get("next_execution") else None,
            execution_count=data.get("execution_count", 0),
            metadata=data.get("metadata", {}),
            tags=set(data.get("tags", []))
        )


@dataclass
class ClusterTag:
    """Represents a cluster tag with metadata."""
    key: str
    value: str
    description: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __str__(self) -> str:
        return f"{self.key}:{self.value}"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert tag to dictionary."""
        return {
            "key": self.key,
            "value": self.value,
            "description": self.description,
            "created_at": self.created_at.isoformat(),
            "metadata": self.metadata
        }


class AdvancedClusterFeatures:
    """
    Advanced cluster features manager.
    
    Provides cluster cloning, snapshots, scheduling, and enhanced tagging
    capabilities for multi-cluster Kafka deployments.
    """
    
    def __init__(
        self,
        multi_cluster_manager: MultiClusterManager,
        cluster_registry: ClusterRegistry,
        data_directory: Optional[Path] = None
    ):
        self.multi_cluster_manager = multi_cluster_manager
        self.cluster_registry = cluster_registry
        self.data_directory = data_directory or Path("advanced_cluster_data")
        
        # Ensure data directories exist
        self.snapshots_dir = self.data_directory / "snapshots"
        self.schedules_dir = self.data_directory / "schedules"
        self.tags_dir = self.data_directory / "tags"
        
        for directory in [self.data_directory, self.snapshots_dir, self.schedules_dir, self.tags_dir]:
            directory.mkdir(parents=True, exist_ok=True)
        
        # Internal storage
        self._snapshots: Dict[str, ClusterSnapshot] = {}
        self._schedules: Dict[str, ClusterSchedule] = {}
        self._cluster_tags: Dict[str, Dict[str, ClusterTag]] = {}  # cluster_id -> tag_key -> tag
        
        # Scheduling
        self._scheduler_active = False
        self._scheduler_task: Optional[asyncio.Task] = None
        
        self.logger = logging.getLogger(__name__)
        
        # Load existing data
        asyncio.create_task(self._load_data())
    
    async def clone_cluster(
        self,
        source_cluster_id: str,
        target_cluster_id: str,
        target_name: Optional[str] = None,
        clone_data: bool = True,
        clone_config: bool = True,
        port_offset: int = 100,
        tags: Optional[Dict[str, str]] = None
    ) -> ClusterDefinition:
        """Clone an existing cluster to create a new one."""
        try:
            self.logger.info(f"Cloning cluster {source_cluster_id} to {target_cluster_id}")
            
            # Get source cluster
            source_cluster = await self.cluster_registry.get_cluster(source_cluster_id)
            if not source_cluster:
                raise ClusterError(f"Source cluster {source_cluster_id} not found")
            
            # Check if target cluster already exists
            existing_target = await self.cluster_registry.get_cluster(target_cluster_id)
            if existing_target:
                raise ClusterError(f"Target cluster {target_cluster_id} already exists")
            
            # Create new cluster definition based on source
            target_cluster = ClusterDefinition(
                id=target_cluster_id,
                name=target_name or f"{source_cluster.name} (Clone)",
                description=f"Clone of {source_cluster.name}",
                environment=source_cluster.environment,
                template_id=source_cluster.template_id,
                port_allocation=PortAllocation(
                    kafka_port=source_cluster.port_allocation.kafka_port + port_offset,
                    rest_proxy_port=source_cluster.port_allocation.rest_proxy_port + port_offset,
                    ui_port=source_cluster.port_allocation.ui_port + port_offset,
                    jmx_port=source_cluster.port_allocation.jmx_port + port_offset if source_cluster.port_allocation.jmx_port else None
                ),
                tags=dict(source_cluster.tags) if source_cluster.tags else {}
            )
            
            # Add clone-specific tags
            if not target_cluster.tags:
                target_cluster.tags = {}
            
            target_cluster.tags.update({
                "cloned_from": source_cluster_id,
                "clone_created": datetime.now().isoformat(),
                "clone_type": "full" if clone_data and clone_config else "config_only" if clone_config else "data_only"
            })
            
            # Add custom tags if provided
            if tags:
                target_cluster.tags.update(tags)
            
            # Register the new cluster
            await self.cluster_registry.register_cluster(target_cluster)
            
            # Clone configuration if requested
            if clone_config:
                await self._clone_cluster_config(source_cluster_id, target_cluster_id)
            
            # Clone data if requested
            if clone_data:
                await self._clone_cluster_data(source_cluster_id, target_cluster_id)
            
            # Create the cluster in the multi-cluster manager
            created_cluster = await self.multi_cluster_manager.create_cluster(target_cluster)
            
            self.logger.info(f"Successfully cloned cluster {source_cluster_id} to {target_cluster_id}")
            
            return created_cluster
            
        except Exception as e:
            self.logger.error(f"Error cloning cluster {source_cluster_id}: {e}")
            raise ClusterError(f"Failed to clone cluster: {e}")
    
    async def create_snapshot(
        self,
        cluster_id: str,
        name: str,
        description: Optional[str] = None,
        snapshot_type: SnapshotType = SnapshotType.FULL,
        tags: Optional[Set[str]] = None
    ) -> ClusterSnapshot:
        """Create a snapshot of a cluster."""
        try:
            self.logger.info(f"Creating {snapshot_type.value} snapshot of cluster {cluster_id}")
            
            # Verify cluster exists
            cluster = await self.cluster_registry.get_cluster(cluster_id)
            if not cluster:
                raise ClusterError(f"Cluster {cluster_id} not found")
            
            # Generate snapshot ID and file path
            snapshot_id = str(uuid.uuid4())
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            snapshot_filename = f"{cluster_id}_{snapshot_type.value}_{timestamp}.tar.gz"
            snapshot_path = self.snapshots_dir / snapshot_filename
            
            # Create snapshot based on type
            size_bytes = 0
            metadata = {}
            
            if snapshot_type == SnapshotType.FULL:
                size_bytes, metadata = await self._create_full_snapshot(cluster_id, snapshot_path)
            elif snapshot_type == SnapshotType.CONFIG_ONLY:
                size_bytes, metadata = await self._create_config_snapshot(cluster_id, snapshot_path)
            elif snapshot_type == SnapshotType.DATA_ONLY:
                size_bytes, metadata = await self._create_data_snapshot(cluster_id, snapshot_path)
            elif snapshot_type == SnapshotType.INCREMENTAL:
                size_bytes, metadata = await self._create_incremental_snapshot(cluster_id, snapshot_path)
            
            # Create snapshot object
            snapshot = ClusterSnapshot(
                id=snapshot_id,
                cluster_id=cluster_id,
                name=name,
                description=description,
                snapshot_type=snapshot_type,
                created_at=datetime.now(),
                size_bytes=size_bytes,
                file_path=snapshot_path,
                metadata=metadata,
                tags=tags or set()
            )
            
            # Store snapshot
            self._snapshots[snapshot_id] = snapshot
            await self._save_snapshots()
            
            self.logger.info(f"Created snapshot {snapshot_id} for cluster {cluster_id}")
            
            return snapshot
            
        except Exception as e:
            self.logger.error(f"Error creating snapshot for cluster {cluster_id}: {e}")
            raise ClusterError(f"Failed to create snapshot: {e}")
    
    async def restore_snapshot(
        self,
        snapshot_id: str,
        target_cluster_id: Optional[str] = None,
        restore_config: bool = True,
        restore_data: bool = True
    ) -> bool:
        """Restore a cluster from a snapshot."""
        try:
            # Get snapshot
            snapshot = self._snapshots.get(snapshot_id)
            if not snapshot:
                raise ClusterError(f"Snapshot {snapshot_id} not found")
            
            # Determine target cluster
            cluster_id = target_cluster_id or snapshot.cluster_id
            
            self.logger.info(f"Restoring snapshot {snapshot_id} to cluster {cluster_id}")
            
            # Verify target cluster exists
            cluster = await self.cluster_registry.get_cluster(cluster_id)
            if not cluster:
                raise ClusterError(f"Target cluster {cluster_id} not found")
            
            # Stop cluster if running
            cluster_status = await self.multi_cluster_manager.get_cluster_status(cluster_id)
            was_running = cluster_status is not None
            
            if was_running:
                await self.multi_cluster_manager.stop_cluster(cluster_id)
            
            # Restore based on snapshot type and options
            if snapshot.snapshot_type == SnapshotType.FULL:
                await self._restore_full_snapshot(snapshot, cluster_id, restore_config, restore_data)
            elif snapshot.snapshot_type == SnapshotType.CONFIG_ONLY:
                if restore_config:
                    await self._restore_config_snapshot(snapshot, cluster_id)
            elif snapshot.snapshot_type == SnapshotType.DATA_ONLY:
                if restore_data:
                    await self._restore_data_snapshot(snapshot, cluster_id)
            
            # Restart cluster if it was running
            if was_running:
                await self.multi_cluster_manager.start_cluster(cluster_id)
            
            self.logger.info(f"Successfully restored snapshot {snapshot_id} to cluster {cluster_id}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error restoring snapshot {snapshot_id}: {e}")
            raise ClusterError(f"Failed to restore snapshot: {e}")
    
    async def create_schedule(
        self,
        cluster_id: str,
        name: str,
        schedule_type: ScheduleType,
        frequency: ScheduleFrequency,
        schedule_expression: str,
        description: Optional[str] = None,
        enabled: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
        tags: Optional[Set[str]] = None
    ) -> ClusterSchedule:
        """Create a new cluster schedule."""
        try:
            # Verify cluster exists
            cluster = await self.cluster_registry.get_cluster(cluster_id)
            if not cluster:
                raise ClusterError(f"Cluster {cluster_id} not found")
            
            # Generate schedule ID
            schedule_id = str(uuid.uuid4())
            
            # Calculate next execution time
            next_execution = self._calculate_next_execution(frequency, schedule_expression)
            
            # Create schedule object
            schedule = ClusterSchedule(
                id=schedule_id,
                cluster_id=cluster_id,
                name=name,
                description=description,
                schedule_type=schedule_type,
                frequency=frequency,
                schedule_expression=schedule_expression,
                enabled=enabled,
                created_at=datetime.now(),
                next_execution=next_execution,
                metadata=metadata or {},
                tags=tags or set()
            )
            
            # Store schedule
            self._schedules[schedule_id] = schedule
            await self._save_schedules()
            
            # Start scheduler if not already running
            if not self._scheduler_active:
                await self.start_scheduler()
            
            self.logger.info(f"Created schedule {schedule_id} for cluster {cluster_id}")
            
            return schedule
            
        except Exception as e:
            self.logger.error(f"Error creating schedule for cluster {cluster_id}: {e}")
            raise ClusterError(f"Failed to create schedule: {e}")
    
    async def add_cluster_tags(
        self,
        cluster_id: str,
        tags: Dict[str, str],
        descriptions: Optional[Dict[str, str]] = None
    ) -> Dict[str, ClusterTag]:
        """Add tags to a cluster."""
        try:
            # Verify cluster exists
            cluster = await self.cluster_registry.get_cluster(cluster_id)
            if not cluster:
                raise ClusterError(f"Cluster {cluster_id} not found")
            
            # Initialize cluster tags if not exists
            if cluster_id not in self._cluster_tags:
                self._cluster_tags[cluster_id] = {}
            
            # Add new tags
            added_tags = {}
            for key, value in tags.items():
                tag = ClusterTag(
                    key=key,
                    value=value,
                    description=descriptions.get(key) if descriptions else None
                )
                
                self._cluster_tags[cluster_id][key] = tag
                added_tags[key] = tag
            
            # Update cluster definition tags
            if not cluster.tags:
                cluster.tags = {}
            cluster.tags.update(tags)
            
            # Update cluster in registry
            await self.cluster_registry.update_cluster(cluster)
            
            # Save tags
            await self._save_cluster_tags()
            
            self.logger.info(f"Added {len(tags)} tags to cluster {cluster_id}")
            
            return added_tags
            
        except Exception as e:
            self.logger.error(f"Error adding tags to cluster {cluster_id}: {e}")
            raise ClusterError(f"Failed to add cluster tags: {e}")
    
    async def search_clusters_by_tags(
        self,
        tag_filters: Dict[str, str],
        match_all: bool = True
    ) -> List[ClusterDefinition]:
        """Search clusters by tags."""
        try:
            matching_clusters = []
            all_clusters = await self.multi_cluster_manager.list_clusters()
            
            for cluster in all_clusters:
                if not cluster.tags:
                    continue
                
                matches = []
                for key, value in tag_filters.items():
                    if key in cluster.tags:
                        if value == "*" or cluster.tags[key] == value:
                            matches.append(True)
                        else:
                            matches.append(False)
                    else:
                        matches.append(False)
                
                # Check if cluster matches criteria
                if match_all:
                    if all(matches):
                        matching_clusters.append(cluster)
                else:
                    if any(matches):
                        matching_clusters.append(cluster)
            
            return matching_clusters
            
        except Exception as e:
            self.logger.error(f"Error searching clusters by tags: {e}")
            raise ClusterError(f"Failed to search clusters by tags: {e}")
    
    async def start_scheduler(self) -> None:
        """Start the cluster scheduler."""
        if self._scheduler_active:
            self.logger.warning("Scheduler is already running")
            return
        
        self._scheduler_active = True
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())
        self.logger.info("Cluster scheduler started")
    
    async def stop_scheduler(self) -> None:
        """Stop the cluster scheduler."""
        if not self._scheduler_active:
            return
        
        self._scheduler_active = False
        
        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass
        
        self.logger.info("Cluster scheduler stopped")
    
    # Getter methods for accessing stored data
    def get_snapshots(self, cluster_id: Optional[str] = None) -> List[ClusterSnapshot]:
        """Get snapshots, optionally filtered by cluster."""
        snapshots = list(self._snapshots.values())
        if cluster_id:
            snapshots = [s for s in snapshots if s.cluster_id == cluster_id]
        return sorted(snapshots, key=lambda s: s.created_at, reverse=True)
    
    def get_schedules(self, cluster_id: Optional[str] = None) -> List[ClusterSchedule]:
        """Get schedules, optionally filtered by cluster."""
        schedules = list(self._schedules.values())
        if cluster_id:
            schedules = [s for s in schedules if s.cluster_id == cluster_id]
        return sorted(schedules, key=lambda s: s.created_at, reverse=True)
    
    def get_cluster_tags(self, cluster_id: str) -> Dict[str, ClusterTag]:
        """Get tags for a specific cluster."""
        return self._cluster_tags.get(cluster_id, {})
    
    async def delete_snapshot(self, snapshot_id: str) -> bool:
        """Delete a snapshot."""
        try:
            snapshot = self._snapshots.get(snapshot_id)
            if not snapshot:
                return False
            
            # Delete snapshot file
            if snapshot.file_path.exists():
                snapshot.file_path.unlink()
            
            # Remove from storage
            del self._snapshots[snapshot_id]
            await self._save_snapshots()
            
            self.logger.info(f"Deleted snapshot {snapshot_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error deleting snapshot {snapshot_id}: {e}")
            return False
    
    async def delete_schedule(self, schedule_id: str) -> bool:
        """Delete a schedule."""
        try:
            if schedule_id not in self._schedules:
                return False
            
            del self._schedules[schedule_id]
            await self._save_schedules()
            
            self.logger.info(f"Deleted schedule {schedule_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error deleting schedule {schedule_id}: {e}")
            return False 
   
    # Private helper methods
    async def _clone_cluster_config(self, source_cluster_id: str, target_cluster_id: str) -> None:
        """Clone cluster configuration files."""
        try:
            source_config_dir = Path(f"config/clusters/{source_cluster_id}")
            target_config_dir = Path(f"config/clusters/{target_cluster_id}")
            
            if source_config_dir.exists():
                target_config_dir.parent.mkdir(parents=True, exist_ok=True)
                shutil.copytree(source_config_dir, target_config_dir, dirs_exist_ok=True)
                
                self.logger.debug(f"Cloned configuration from {source_cluster_id} to {target_cluster_id}")
            
        except Exception as e:
            self.logger.warning(f"Error cloning configuration: {e}")
    
    async def _clone_cluster_data(self, source_cluster_id: str, target_cluster_id: str) -> None:
        """Clone cluster data files."""
        try:
            source_data_dir = Path(f"data/clusters/{source_cluster_id}")
            target_data_dir = Path(f"data/clusters/{target_cluster_id}")
            
            if source_data_dir.exists():
                target_data_dir.parent.mkdir(parents=True, exist_ok=True)
                shutil.copytree(source_data_dir, target_data_dir, dirs_exist_ok=True)
                
                self.logger.debug(f"Cloned data from {source_cluster_id} to {target_cluster_id}")
            
        except Exception as e:
            self.logger.warning(f"Error cloning data: {e}")
    
    async def _create_full_snapshot(self, cluster_id: str, snapshot_path: Path) -> Tuple[int, Dict[str, Any]]:
        """Create a full cluster snapshot."""
        import tarfile
        
        try:
            metadata = {"type": "full", "includes": ["config", "data", "logs"]}
            
            with tarfile.open(snapshot_path, "w:gz") as tar:
                # Add configuration
                config_dir = Path(f"config/clusters/{cluster_id}")
                if config_dir.exists():
                    tar.add(config_dir, arcname="config")
                
                # Add data
                data_dir = Path(f"data/clusters/{cluster_id}")
                if data_dir.exists():
                    tar.add(data_dir, arcname="data")
                
                # Add logs
                logs_dir = Path(f"data/logs/{cluster_id}")
                if logs_dir.exists():
                    tar.add(logs_dir, arcname="logs")
            
            size_bytes = snapshot_path.stat().st_size
            metadata["files_included"] = self._count_files_in_tar(snapshot_path)
            
            return size_bytes, metadata
            
        except Exception as e:
            if snapshot_path.exists():
                snapshot_path.unlink()
            raise ClusterError(f"Failed to create full snapshot: {e}")
    
    async def _create_config_snapshot(self, cluster_id: str, snapshot_path: Path) -> Tuple[int, Dict[str, Any]]:
        """Create a configuration-only snapshot."""
        import tarfile
        
        try:
            metadata = {"type": "config_only", "includes": ["config"]}
            
            with tarfile.open(snapshot_path, "w:gz") as tar:
                config_dir = Path(f"config/clusters/{cluster_id}")
                if config_dir.exists():
                    tar.add(config_dir, arcname="config")
            
            size_bytes = snapshot_path.stat().st_size
            metadata["files_included"] = self._count_files_in_tar(snapshot_path)
            
            return size_bytes, metadata
            
        except Exception as e:
            if snapshot_path.exists():
                snapshot_path.unlink()
            raise ClusterError(f"Failed to create config snapshot: {e}")
    
    async def _create_data_snapshot(self, cluster_id: str, snapshot_path: Path) -> Tuple[int, Dict[str, Any]]:
        """Create a data-only snapshot."""
        import tarfile
        
        try:
            metadata = {"type": "data_only", "includes": ["data"]}
            
            with tarfile.open(snapshot_path, "w:gz") as tar:
                data_dir = Path(f"data/clusters/{cluster_id}")
                if data_dir.exists():
                    tar.add(data_dir, arcname="data")
            
            size_bytes = snapshot_path.stat().st_size
            metadata["files_included"] = self._count_files_in_tar(snapshot_path)
            
            return size_bytes, metadata
            
        except Exception as e:
            if snapshot_path.exists():
                snapshot_path.unlink()
            raise ClusterError(f"Failed to create data snapshot: {e}")
    
    async def _create_incremental_snapshot(self, cluster_id: str, snapshot_path: Path) -> Tuple[int, Dict[str, Any]]:
        """Create an incremental snapshot."""
        # For now, create a full snapshot
        # In a real implementation, this would compare with the last snapshot
        return await self._create_full_snapshot(cluster_id, snapshot_path)
    
    async def _restore_full_snapshot(
        self,
        snapshot: ClusterSnapshot,
        cluster_id: str,
        restore_config: bool,
        restore_data: bool
    ) -> None:
        """Restore a full snapshot."""
        import tarfile
        
        try:
            with tarfile.open(snapshot.file_path, "r:gz") as tar:
                if restore_config:
                    # Extract config
                    config_members = [m for m in tar.getmembers() if m.name.startswith("config/")]
                    if config_members:
                        config_dir = Path(f"config/clusters/{cluster_id}")
                        config_dir.parent.mkdir(parents=True, exist_ok=True)
                        
                        for member in config_members:
                            member.name = member.name.replace("config/", "")
                            tar.extract(member, config_dir)
                
                if restore_data:
                    # Extract data
                    data_members = [m for m in tar.getmembers() if m.name.startswith("data/")]
                    if data_members:
                        data_dir = Path(f"data/clusters/{cluster_id}")
                        data_dir.parent.mkdir(parents=True, exist_ok=True)
                        
                        for member in data_members:
                            member.name = member.name.replace("data/", "")
                            tar.extract(member, data_dir)
            
        except Exception as e:
            raise ClusterError(f"Failed to restore full snapshot: {e}")
    
    async def _restore_config_snapshot(self, snapshot: ClusterSnapshot, cluster_id: str) -> None:
        """Restore a config-only snapshot."""
        import tarfile
        
        try:
            with tarfile.open(snapshot.file_path, "r:gz") as tar:
                config_dir = Path(f"config/clusters/{cluster_id}")
                config_dir.parent.mkdir(parents=True, exist_ok=True)
                tar.extractall(config_dir)
            
        except Exception as e:
            raise ClusterError(f"Failed to restore config snapshot: {e}")
    
    async def _restore_data_snapshot(self, snapshot: ClusterSnapshot, cluster_id: str) -> None:
        """Restore a data-only snapshot."""
        import tarfile
        
        try:
            with tarfile.open(snapshot.file_path, "r:gz") as tar:
                data_dir = Path(f"data/clusters/{cluster_id}")
                data_dir.parent.mkdir(parents=True, exist_ok=True)
                tar.extractall(data_dir)
            
        except Exception as e:
            raise ClusterError(f"Failed to restore data snapshot: {e}")
    
    def _count_files_in_tar(self, tar_path: Path) -> int:
        """Count files in a tar archive."""
        import tarfile
        
        try:
            with tarfile.open(tar_path, "r:gz") as tar:
                return len([m for m in tar.getmembers() if m.isfile()])
        except Exception:
            return 0
    
    def _calculate_next_execution(self, frequency: ScheduleFrequency, expression: str) -> Optional[datetime]:
        """Calculate next execution time for a schedule."""
        try:
            now = datetime.now()
            
            if frequency == ScheduleFrequency.ONCE:
                # Parse datetime string
                return datetime.fromisoformat(expression)
            elif frequency == ScheduleFrequency.HOURLY:
                return now + timedelta(hours=1)
            elif frequency == ScheduleFrequency.DAILY:
                # Parse time (HH:MM format)
                hour, minute = map(int, expression.split(":"))
                next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                if next_run <= now:
                    next_run += timedelta(days=1)
                return next_run
            elif frequency == ScheduleFrequency.WEEKLY:
                # Parse day and time (e.g., "MON 09:00")
                day_name, time_str = expression.split()
                hour, minute = map(int, time_str.split(":"))
                
                days = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]
                target_weekday = days.index(day_name)
                
                next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                days_ahead = target_weekday - now.weekday()
                if days_ahead <= 0:  # Target day already happened this week
                    days_ahead += 7
                
                return next_run + timedelta(days=days_ahead)
            elif frequency == ScheduleFrequency.MONTHLY:
                # Parse day and time (e.g., "15 09:00")
                day, time_str = expression.split()
                day = int(day)
                hour, minute = map(int, time_str.split(":"))
                
                next_run = now.replace(day=day, hour=hour, minute=minute, second=0, microsecond=0)
                if next_run <= now:
                    # Move to next month
                    if now.month == 12:
                        next_run = next_run.replace(year=now.year + 1, month=1)
                    else:
                        next_run = next_run.replace(month=now.month + 1)
                
                return next_run
            elif frequency == ScheduleFrequency.CUSTOM_CRON:
                # For now, return None (would need cron parser)
                return None
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error calculating next execution: {e}")
            return None
    
    async def _scheduler_loop(self) -> None:
        """Main scheduler loop."""
        while self._scheduler_active:
            try:
                now = datetime.now()
                
                # Check all schedules
                for schedule in list(self._schedules.values()):
                    if not schedule.enabled:
                        continue
                    
                    if schedule.next_execution and schedule.next_execution <= now:
                        await self._execute_schedule(schedule)
                
                # Sleep for 60 seconds before next check
                await asyncio.sleep(60)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in scheduler loop: {e}")
                await asyncio.sleep(60)
    
    async def _execute_schedule(self, schedule: ClusterSchedule) -> None:
        """Execute a scheduled task."""
        try:
            self.logger.info(f"Executing schedule {schedule.id}: {schedule.name}")
            
            if schedule.schedule_type == ScheduleType.START:
                await self.multi_cluster_manager.start_cluster(schedule.cluster_id)
            elif schedule.schedule_type == ScheduleType.STOP:
                await self.multi_cluster_manager.stop_cluster(schedule.cluster_id)
            elif schedule.schedule_type == ScheduleType.RESTART:
                await self.multi_cluster_manager.stop_cluster(schedule.cluster_id)
                await asyncio.sleep(5)  # Wait a bit
                await self.multi_cluster_manager.start_cluster(schedule.cluster_id)
            elif schedule.schedule_type == ScheduleType.BACKUP:
                # Create a snapshot as backup
                await self.create_snapshot(
                    schedule.cluster_id,
                    f"Scheduled backup - {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                    "Automatically created by scheduler",
                    SnapshotType.FULL,
                    {"scheduled", "backup"}
                )
            elif schedule.schedule_type == ScheduleType.SNAPSHOT:
                # Create a snapshot
                await self.create_snapshot(
                    schedule.cluster_id,
                    f"Scheduled snapshot - {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                    "Automatically created by scheduler",
                    SnapshotType.FULL,
                    {"scheduled", "snapshot"}
                )
            
            # Update schedule execution info
            schedule.last_executed = datetime.now()
            schedule.execution_count += 1
            schedule.next_execution = self._calculate_next_execution(
                schedule.frequency, schedule.schedule_expression
            )
            
            await self._save_schedules()
            
        except Exception as e:
            self.logger.error(f"Error executing schedule {schedule.id}: {e}")
    
    async def _load_data(self) -> None:
        """Load snapshots, schedules, and tags from disk."""
        try:
            # Load snapshots
            snapshots_file = self.snapshots_dir / "snapshots.json"
            if snapshots_file.exists():
                with open(snapshots_file, 'r') as f:
                    snapshots_data = json.load(f)
                    for snapshot_data in snapshots_data:
                        snapshot = ClusterSnapshot.from_dict(snapshot_data)
                        self._snapshots[snapshot.id] = snapshot
            
            # Load schedules
            schedules_file = self.schedules_dir / "schedules.json"
            if schedules_file.exists():
                with open(schedules_file, 'r') as f:
                    schedules_data = json.load(f)
                    for schedule_data in schedules_data:
                        schedule = ClusterSchedule.from_dict(schedule_data)
                        self._schedules[schedule.id] = schedule
            
            # Load cluster tags
            tags_file = self.tags_dir / "cluster_tags.json"
            if tags_file.exists():
                with open(tags_file, 'r') as f:
                    tags_data = json.load(f)
                    for cluster_id, cluster_tags in tags_data.items():
                        self._cluster_tags[cluster_id] = {}
                        for tag_key, tag_data in cluster_tags.items():
                            tag = ClusterTag(
                                key=tag_data["key"],
                                value=tag_data["value"],
                                description=tag_data.get("description"),
                                created_at=datetime.fromisoformat(tag_data["created_at"]),
                                metadata=tag_data.get("metadata", {})
                            )
                            self._cluster_tags[cluster_id][tag_key] = tag
            
            self.logger.info(f"Loaded {len(self._snapshots)} snapshots, {len(self._schedules)} schedules, and tags for {len(self._cluster_tags)} clusters")
            
        except Exception as e:
            self.logger.error(f"Error loading data: {e}")
    
    async def _save_snapshots(self) -> None:
        """Save snapshots to disk."""
        try:
            snapshots_file = self.snapshots_dir / "snapshots.json"
            snapshots_data = [snapshot.to_dict() for snapshot in self._snapshots.values()]
            
            with open(snapshots_file, 'w') as f:
                json.dump(snapshots_data, f, indent=2)
            
        except Exception as e:
            self.logger.error(f"Error saving snapshots: {e}")
    
    async def _save_schedules(self) -> None:
        """Save schedules to disk."""
        try:
            schedules_file = self.schedules_dir / "schedules.json"
            schedules_data = [schedule.to_dict() for schedule in self._schedules.values()]
            
            with open(schedules_file, 'w') as f:
                json.dump(schedules_data, f, indent=2)
            
        except Exception as e:
            self.logger.error(f"Error saving schedules: {e}")
    
    async def _save_cluster_tags(self) -> None:
        """Save cluster tags to disk."""
        try:
            tags_file = self.tags_dir / "cluster_tags.json"
            tags_data = {}
            
            for cluster_id, cluster_tags in self._cluster_tags.items():
                tags_data[cluster_id] = {}
                for tag_key, tag in cluster_tags.items():
                    tags_data[cluster_id][tag_key] = tag.to_dict()
            
            with open(tags_file, 'w') as f:
                json.dump(tags_data, f, indent=2)
            
        except Exception as e:
            self.logger.error(f"Error saving cluster tags: {e}")