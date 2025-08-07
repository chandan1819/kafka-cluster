"""
Cross-cluster operations for data migration, replication, and comparison.
"""

import asyncio
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set, Tuple
from dataclasses import dataclass
from enum import Enum

from ..models.multi_cluster import (
    CrossClusterOperation,
    OperationStatus,
    ClusterComparisonResult
)
from ..models.topic import TopicInfo, TopicConfig
from ..models.message import Message, ProduceRequest, ConsumeRequest
from ..services.multi_cluster_service_catalog import MultiClusterServiceCatalog
from ..services.topic_manager import TopicManager
from ..services.message_manager import MessageManager
from ..exceptions import (
    CrossClusterOperationError,
    ClusterNotFoundError,
    TopicNotFoundError,
    ValidationError
)
from ..utils.logging import get_logger
from ..utils.metrics import counter, timer, gauge

logger = get_logger(__name__)


class ReplicationMode(str, Enum):
    """Replication modes for cross-cluster operations."""
    MIRROR = "mirror"  # One-way replication
    SYNC = "sync"      # Bidirectional synchronization
    BACKUP = "backup"  # One-time backup copy


@dataclass
class OperationProgress:
    """Progress tracking for cross-cluster operations."""
    operation_id: str
    total_items: int
    processed_items: int
    failed_items: int
    current_phase: str
    estimated_completion: Optional[datetime] = None
    
    @property
    def progress_percent(self) -> float:
        """Calculate progress percentage."""
        if self.total_items == 0:
            return 100.0
        return min(100.0, (self.processed_items / self.total_items) * 100.0)
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        if self.processed_items == 0:
            return 100.0
        return ((self.processed_items - self.failed_items) / self.processed_items) * 100.0


class CrossClusterOperations:
    """Manages cross-cluster operations including migration, replication, and comparison."""
    
    def __init__(self, service_catalog: MultiClusterServiceCatalog):
        """Initialize cross-cluster operations manager.
        
        Args:
            service_catalog: Multi-cluster service catalog for cluster access
        """
        self.service_catalog = service_catalog
        
        # Active operations tracking
        self._active_operations: Dict[str, CrossClusterOperation] = {}
        self._operation_progress: Dict[str, OperationProgress] = {}
        self._operation_tasks: Dict[str, asyncio.Task] = {}
        
        # Configuration
        self.max_concurrent_operations = 5
        self.operation_timeout = timedelta(hours=2)
        self.batch_size = 100
        
        logger.info("Cross-cluster operations manager initialized")
    
    async def migrate_topics(self, 
                           source_cluster_id: str,
                           target_cluster_id: str,
                           topics: Optional[List[str]] = None,
                           include_data: bool = True,
                           preserve_partitions: bool = True,
                           preserve_timestamps: bool = True,
                           dry_run: bool = False) -> str:
        """Migrate topics from source to target cluster.
        
        Args:
            source_cluster_id: Source cluster ID
            target_cluster_id: Target cluster ID
            topics: List of topics to migrate (None = all topics)
            include_data: Whether to migrate message data
            preserve_partitions: Whether to preserve partition structure
            preserve_timestamps: Whether to preserve message timestamps
            dry_run: Whether to perform a dry run without actual migration
            
        Returns:
            Operation ID for tracking progress
            
        Raises:
            CrossClusterOperationError: If migration setup fails
        """
        operation_id = str(uuid.uuid4())
        
        logger.info(f"Starting topic migration: {source_cluster_id} -> {target_cluster_id} (operation: {operation_id})")
        
        try:
            # Validate clusters exist
            await self._validate_clusters([source_cluster_id, target_cluster_id])
            
            # Create operation record
            operation = CrossClusterOperation(
                id=operation_id,
                operation_type="migrate",
                source_cluster_id=source_cluster_id,
                target_cluster_ids=[target_cluster_id],
                topics=topics,
                include_data=include_data,
                preserve_partitions=preserve_partitions,
                preserve_timestamps=preserve_timestamps,
                status=OperationStatus.PENDING
            )
            
            self._active_operations[operation_id] = operation
            
            # Start migration task
            task = asyncio.create_task(
                self._execute_migration(operation, dry_run)
            )
            self._operation_tasks[operation_id] = task
            
            logger.info(f"Migration operation {operation_id} started")
            counter('cross_cluster.migration.started', 1, tags={
                'source': source_cluster_id,
                'target': target_cluster_id
            })
            
            return operation_id
            
        except Exception as e:
            logger.error(f"Failed to start migration operation: {e}")
            raise CrossClusterOperationError(
                f"Failed to start migration: {e}",
                operation_id=operation_id,
                operation_type="migrate",
                cause=e
            )
    
    async def replicate_topics(self,
                             source_cluster_id: str,
                             target_cluster_ids: List[str],
                             topics: Optional[List[str]] = None,
                             mode: ReplicationMode = ReplicationMode.MIRROR,
                             continuous: bool = False) -> str:
        """Set up topic replication between clusters.
        
        Args:
            source_cluster_id: Source cluster ID
            target_cluster_ids: List of target cluster IDs
            topics: List of topics to replicate (None = all topics)
            mode: Replication mode
            continuous: Whether to set up continuous replication
            
        Returns:
            Operation ID for tracking progress
            
        Raises:
            CrossClusterOperationError: If replication setup fails
        """
        operation_id = str(uuid.uuid4())
        
        logger.info(f"Starting topic replication: {source_cluster_id} -> {target_cluster_ids} (operation: {operation_id})")
        
        try:
            # Validate clusters exist
            await self._validate_clusters([source_cluster_id] + target_cluster_ids)
            
            # Create operation record
            operation = CrossClusterOperation(
                id=operation_id,
                operation_type="replicate",
                source_cluster_id=source_cluster_id,
                target_cluster_ids=target_cluster_ids,
                topics=topics,
                include_data=True,
                preserve_partitions=True,
                preserve_timestamps=True,
                status=OperationStatus.PENDING,
                results={"mode": mode.value, "continuous": continuous}
            )
            
            self._active_operations[operation_id] = operation
            
            # Start replication task
            task = asyncio.create_task(
                self._execute_replication(operation, mode, continuous)
            )
            self._operation_tasks[operation_id] = task
            
            logger.info(f"Replication operation {operation_id} started")
            counter('cross_cluster.replication.started', 1, tags={
                'source': source_cluster_id,
                'mode': mode.value
            })
            
            return operation_id
            
        except Exception as e:
            logger.error(f"Failed to start replication operation: {e}")
            raise CrossClusterOperationError(
                f"Failed to start replication: {e}",
                operation_id=operation_id,
                operation_type="replicate",
                cause=e
            )
    
    async def compare_clusters(self,
                             source_cluster_id: str,
                             target_cluster_id: str,
                             include_data: bool = False,
                             topics: Optional[List[str]] = None) -> ClusterComparisonResult:
        """Compare configuration and data between two clusters.
        
        Args:
            source_cluster_id: Source cluster ID
            target_cluster_id: Target cluster ID
            include_data: Whether to compare message data
            topics: List of topics to compare (None = all topics)
            
        Returns:
            ClusterComparisonResult with comparison details
            
        Raises:
            CrossClusterOperationError: If comparison fails
        """
        logger.info(f"Comparing clusters: {source_cluster_id} vs {target_cluster_id}")
        
        try:
            # Validate clusters exist
            await self._validate_clusters([source_cluster_id, target_cluster_id])
            
            # Get topic managers for both clusters
            source_topic_manager = await self.service_catalog.create_cluster_topic_manager(source_cluster_id)
            target_topic_manager = await self.service_catalog.create_cluster_topic_manager(target_cluster_id)
            
            try:
                # Get topics from both clusters
                source_topics = await source_topic_manager.list_topics()
                target_topics = await target_topic_manager.list_topics()
                
                # Filter topics if specified
                if topics:
                    source_topics = [t for t in source_topics if t.name in topics]
                    target_topics = [t for t in target_topics if t.name in topics]
                
                # Create topic name sets
                source_topic_names = {t.name for t in source_topics}
                target_topic_names = {t.name for t in target_topics}
                
                # Find differences
                topics_only_in_source = list(source_topic_names - target_topic_names)
                topics_only_in_target = list(target_topic_names - source_topic_names)
                common_topics = source_topic_names & target_topic_names
                
                # Compare configurations for common topics
                topics_with_different_config = []
                config_differences = {}
                
                for topic_name in common_topics:
                    source_topic = next(t for t in source_topics if t.name == topic_name)
                    target_topic = next(t for t in target_topics if t.name == topic_name)
                    
                    differences = self._compare_topic_configs(source_topic, target_topic)
                    if differences:
                        topics_with_different_config.append(topic_name)
                        config_differences[topic_name] = differences
                
                # Compare message data if requested
                message_count_differences = None
                if include_data:
                    message_count_differences = await self._compare_message_counts(
                        source_cluster_id, target_cluster_id, list(common_topics)
                    )
                
                # Create comparison result
                result = ClusterComparisonResult(
                    source_cluster=source_cluster_id,
                    target_cluster=target_cluster_id,
                    comparison_timestamp=datetime.utcnow(),
                    config_differences=config_differences,
                    topics_only_in_source=topics_only_in_source,
                    topics_only_in_target=topics_only_in_target,
                    topics_with_different_config=topics_with_different_config,
                    message_count_differences=message_count_differences,
                    summary={
                        "total_topics_source": len(source_topics),
                        "total_topics_target": len(target_topics),
                        "common_topics": len(common_topics),
                        "config_differences": len(topics_with_different_config),
                        "identical": len(topics_with_different_config) == 0 and 
                                   len(topics_only_in_source) == 0 and 
                                   len(topics_only_in_target) == 0
                    }
                )
                
                logger.info(f"Cluster comparison completed: {source_cluster_id} vs {target_cluster_id}")
                counter('cross_cluster.comparison.completed', 1, tags={
                    'source': source_cluster_id,
                    'target': target_cluster_id
                })
                
                return result
                
            finally:
                # Cleanup topic managers
                source_topic_manager.close()
                target_topic_manager.close()
                
        except Exception as e:
            logger.error(f"Failed to compare clusters {source_cluster_id} vs {target_cluster_id}: {e}")
            raise CrossClusterOperationError(
                f"Failed to compare clusters: {e}",
                operation_type="compare",
                cause=e
            )
    
    async def get_operation_status(self, operation_id: str) -> CrossClusterOperation:
        """Get status of a cross-cluster operation.
        
        Args:
            operation_id: Operation ID
            
        Returns:
            CrossClusterOperation with current status
            
        Raises:
            CrossClusterOperationError: If operation not found
        """
        if operation_id not in self._active_operations:
            raise CrossClusterOperationError(
                f"Operation {operation_id} not found",
                operation_id=operation_id
            )
        
        operation = self._active_operations[operation_id]
        
        # Update progress if available
        if operation_id in self._operation_progress:
            progress = self._operation_progress[operation_id]
            operation.progress_percent = progress.progress_percent
            operation.results.update({
                "processed_items": progress.processed_items,
                "failed_items": progress.failed_items,
                "current_phase": progress.current_phase,
                "success_rate": progress.success_rate
            })
        
        return operation
    
    async def cancel_operation(self, operation_id: str) -> bool:
        """Cancel a running cross-cluster operation.
        
        Args:
            operation_id: Operation ID
            
        Returns:
            True if operation was cancelled
            
        Raises:
            CrossClusterOperationError: If operation not found or cannot be cancelled
        """
        if operation_id not in self._active_operations:
            raise CrossClusterOperationError(
                f"Operation {operation_id} not found",
                operation_id=operation_id
            )
        
        operation = self._active_operations[operation_id]
        
        if operation.status not in [OperationStatus.PENDING, OperationStatus.RUNNING]:
            raise CrossClusterOperationError(
                f"Operation {operation_id} cannot be cancelled (status: {operation.status})",
                operation_id=operation_id
            )
        
        # Cancel the task
        if operation_id in self._operation_tasks:
            task = self._operation_tasks[operation_id]
            task.cancel()
            
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        # Update operation status
        operation.status = OperationStatus.CANCELLED
        operation.completed_at = datetime.utcnow()
        
        logger.info(f"Operation {operation_id} cancelled")
        counter('cross_cluster.operation.cancelled', 1, tags={
            'operation_type': operation.operation_type
        })
        
        return True
    
    async def list_operations(self, 
                            status_filter: Optional[OperationStatus] = None,
                            operation_type_filter: Optional[str] = None) -> List[CrossClusterOperation]:
        """List cross-cluster operations with optional filtering.
        
        Args:
            status_filter: Filter by operation status
            operation_type_filter: Filter by operation type
            
        Returns:
            List of CrossClusterOperation objects
        """
        operations = []
        
        for operation in self._active_operations.values():
            # Apply status filter
            if status_filter and operation.status != status_filter:
                continue
            
            # Apply operation type filter
            if operation_type_filter and operation.operation_type != operation_type_filter:
                continue
            
            operations.append(operation)
        
        return operations
    
    async def cleanup_completed_operations(self, max_age_hours: int = 24) -> int:
        """Clean up completed operations older than specified age.
        
        Args:
            max_age_hours: Maximum age in hours for completed operations
            
        Returns:
            Number of operations cleaned up
        """
        cutoff_time = datetime.utcnow() - timedelta(hours=max_age_hours)
        operations_to_remove = []
        
        for operation_id, operation in self._active_operations.items():
            if (operation.status in [OperationStatus.COMPLETED, OperationStatus.FAILED, OperationStatus.CANCELLED] and
                operation.completed_at and operation.completed_at < cutoff_time):
                operations_to_remove.append(operation_id)
        
        # Remove old operations
        for operation_id in operations_to_remove:
            del self._active_operations[operation_id]
            
            if operation_id in self._operation_progress:
                del self._operation_progress[operation_id]
            
            if operation_id in self._operation_tasks:
                del self._operation_tasks[operation_id]
        
        logger.info(f"Cleaned up {len(operations_to_remove)} completed operations")
        return len(operations_to_remove)
    
    # Private helper methods
    
    async def _validate_clusters(self, cluster_ids: List[str]) -> None:
        """Validate that all specified clusters exist and are accessible."""
        for cluster_id in cluster_ids:
            try:
                # Check if cluster exists in registry
                cluster_status = await self.service_catalog.multi_cluster_manager.get_cluster_status(cluster_id)
                logger.debug(f"Cluster {cluster_id} status: {cluster_status}")
            except Exception as e:
                raise ClusterNotFoundError(cluster_id, details={"error": str(e)})
    
    async def _execute_migration(self, operation: CrossClusterOperation, dry_run: bool) -> None:
        """Execute topic migration operation."""
        operation_id = operation.id
        
        try:
            operation.status = OperationStatus.RUNNING
            operation.started_at = datetime.utcnow()
            
            logger.info(f"Executing migration operation {operation_id} (dry_run: {dry_run})")
            
            # Get topic managers
            source_topic_manager = await self.service_catalog.create_cluster_topic_manager(operation.source_cluster_id)
            target_topic_manager = await self.service_catalog.create_cluster_topic_manager(operation.target_cluster_ids[0])
            
            try:
                # Phase 1: Get source topics
                self._update_progress(operation_id, 0, 0, 0, "Discovering topics")
                
                source_topics = await source_topic_manager.list_topics()
                
                # Filter topics if specified
                if operation.topics:
                    source_topics = [t for t in source_topics if t.name in operation.topics]
                
                total_topics = len(source_topics)
                self._update_progress(operation_id, total_topics, 0, 0, "Migrating topic configurations")
                
                # Phase 2: Migrate topic configurations
                migrated_topics = []
                failed_topics = []
                
                for i, topic in enumerate(source_topics):
                    try:
                        if not dry_run:
                            # Create topic configuration
                            topic_config = TopicConfig(
                                name=topic.name,
                                partitions=topic.partitions if operation.preserve_partitions else 1,
                                replication_factor=topic.replication_factor,
                                config=topic.config or {}
                            )
                            
                            # Create topic in target cluster
                            await target_topic_manager.create_topic(topic_config)
                        
                        migrated_topics.append(topic.name)
                        self._update_progress(operation_id, total_topics, i + 1, len(failed_topics), "Migrating topic configurations")
                        
                        logger.debug(f"Migrated topic configuration: {topic.name}")
                        
                    except Exception as e:
                        logger.warning(f"Failed to migrate topic {topic.name}: {e}")
                        failed_topics.append(topic.name)
                        self._update_progress(operation_id, total_topics, i + 1, len(failed_topics), "Migrating topic configurations")
                
                # Phase 3: Migrate data if requested
                if operation.include_data and not dry_run:
                    self._update_progress(operation_id, total_topics, total_topics, len(failed_topics), "Migrating message data")
                    
                    # This would be implemented with actual message migration
                    # For now, we'll simulate the process
                    await asyncio.sleep(1)  # Simulate data migration time
                
                # Complete operation
                operation.status = OperationStatus.COMPLETED
                operation.completed_at = datetime.utcnow()
                operation.progress_percent = 100.0
                operation.results.update({
                    "migrated_topics": migrated_topics,
                    "failed_topics": failed_topics,
                    "total_topics": total_topics,
                    "dry_run": dry_run
                })
                
                logger.info(f"Migration operation {operation_id} completed successfully")
                counter('cross_cluster.migration.completed', 1, tags={
                    'source': operation.source_cluster_id,
                    'target': operation.target_cluster_ids[0],
                    'success': 'true'
                })
                
            finally:
                # Cleanup topic managers
                source_topic_manager.close()
                target_topic_manager.close()
                
        except asyncio.CancelledError:
            operation.status = OperationStatus.CANCELLED
            operation.completed_at = datetime.utcnow()
            logger.info(f"Migration operation {operation_id} was cancelled")
            raise
        except Exception as e:
            operation.status = OperationStatus.FAILED
            operation.completed_at = datetime.utcnow()
            operation.error_message = str(e)
            
            logger.error(f"Migration operation {operation_id} failed: {e}")
            counter('cross_cluster.migration.completed', 1, tags={
                'source': operation.source_cluster_id,
                'target': operation.target_cluster_ids[0],
                'success': 'false'
            })
    
    async def _execute_replication(self, operation: CrossClusterOperation, 
                                 mode: ReplicationMode, continuous: bool) -> None:
        """Execute topic replication operation."""
        operation_id = operation.id
        
        try:
            operation.status = OperationStatus.RUNNING
            operation.started_at = datetime.utcnow()
            
            logger.info(f"Executing replication operation {operation_id} (mode: {mode}, continuous: {continuous})")
            
            # For now, implement a basic replication simulation
            # In a real implementation, this would set up continuous replication
            
            self._update_progress(operation_id, 1, 0, 0, "Setting up replication")
            
            # Simulate replication setup
            await asyncio.sleep(2)
            
            self._update_progress(operation_id, 1, 1, 0, "Replication active")
            
            if continuous:
                # For continuous replication, the operation would remain running
                # For this simulation, we'll mark it as completed
                pass
            
            operation.status = OperationStatus.COMPLETED
            operation.completed_at = datetime.utcnow()
            operation.progress_percent = 100.0
            operation.results.update({
                "replication_mode": mode.value,
                "continuous": continuous,
                "target_clusters": operation.target_cluster_ids
            })
            
            logger.info(f"Replication operation {operation_id} completed successfully")
            counter('cross_cluster.replication.completed', 1, tags={
                'source': operation.source_cluster_id,
                'mode': mode.value,
                'success': 'true'
            })
            
        except asyncio.CancelledError:
            operation.status = OperationStatus.CANCELLED
            operation.completed_at = datetime.utcnow()
            logger.info(f"Replication operation {operation_id} was cancelled")
            raise
        except Exception as e:
            operation.status = OperationStatus.FAILED
            operation.completed_at = datetime.utcnow()
            operation.error_message = str(e)
            
            logger.error(f"Replication operation {operation_id} failed: {e}")
            counter('cross_cluster.replication.completed', 1, tags={
                'source': operation.source_cluster_id,
                'mode': mode.value,
                'success': 'false'
            })
    
    def _update_progress(self, operation_id: str, total: int, processed: int, 
                        failed: int, phase: str) -> None:
        """Update operation progress."""
        progress = OperationProgress(
            operation_id=operation_id,
            total_items=total,
            processed_items=processed,
            failed_items=failed,
            current_phase=phase
        )
        
        # Estimate completion time
        if processed > 0 and total > processed:
            elapsed = datetime.utcnow() - self._active_operations[operation_id].started_at
            rate = processed / elapsed.total_seconds()
            remaining_items = total - processed
            remaining_seconds = remaining_items / rate if rate > 0 else 0
            progress.estimated_completion = datetime.utcnow() + timedelta(seconds=remaining_seconds)
        
        self._operation_progress[operation_id] = progress
        
        # Update operation progress
        if operation_id in self._active_operations:
            self._active_operations[operation_id].progress_percent = progress.progress_percent
    
    def _compare_topic_configs(self, source_topic: TopicInfo, target_topic: TopicInfo) -> Dict[str, Any]:
        """Compare configurations between two topics."""
        differences = {}
        
        # Compare basic properties
        if source_topic.partitions != target_topic.partitions:
            differences["partitions"] = {
                "source": source_topic.partitions,
                "target": target_topic.partitions
            }
        
        if source_topic.replication_factor != target_topic.replication_factor:
            differences["replication_factor"] = {
                "source": source_topic.replication_factor,
                "target": target_topic.replication_factor
            }
        
        # Compare configurations
        source_config = source_topic.config or {}
        target_config = target_topic.config or {}
        
        all_config_keys = set(source_config.keys()) | set(target_config.keys())
        
        for key in all_config_keys:
            source_value = source_config.get(key)
            target_value = target_config.get(key)
            
            if source_value != target_value:
                differences[f"config.{key}"] = {
                    "source": source_value,
                    "target": target_value
                }
        
        return differences
    
    async def _compare_message_counts(self, source_cluster_id: str, target_cluster_id: str, 
                                    topics: List[str]) -> Dict[str, Dict[str, int]]:
        """Compare message counts between clusters for specified topics."""
        message_counts = {}
        
        try:
            # Get topic managers
            source_topic_manager = await self.service_catalog.create_cluster_topic_manager(source_cluster_id)
            target_topic_manager = await self.service_catalog.create_cluster_topic_manager(target_cluster_id)
            
            try:
                for topic_name in topics:
                    try:
                        # Get metadata for both topics
                        source_metadata = await source_topic_manager.get_topic_metadata(topic_name)
                        target_metadata = await target_topic_manager.get_topic_metadata(topic_name)
                        
                        message_counts[topic_name] = {
                            "source": source_metadata.message_count or 0,
                            "target": target_metadata.message_count or 0
                        }
                        
                    except Exception as e:
                        logger.warning(f"Failed to get message count for topic {topic_name}: {e}")
                        message_counts[topic_name] = {
                            "source": -1,  # Indicates error
                            "target": -1
                        }
                
                return message_counts
                
            finally:
                source_topic_manager.close()
                target_topic_manager.close()
                
        except Exception as e:
            logger.error(f"Failed to compare message counts: {e}")
            return {}