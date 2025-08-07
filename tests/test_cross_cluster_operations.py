"""
Tests for cross-cluster operations functionality.
"""

import pytest
import asyncio
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch
from uuid import uuid4

from src.services.cross_cluster_operations import (
    CrossClusterOperations,
    ReplicationMode,
    OperationProgress
)
from src.services.multi_cluster_service_catalog import MultiClusterServiceCatalog
from src.services.topic_manager import TopicManager
from src.models.multi_cluster import (
    CrossClusterOperation,
    OperationStatus,
    ClusterComparisonResult
)
from src.models.topic import TopicInfo, TopicConfig
from src.models.base import ServiceStatus
from src.exceptions import (
    CrossClusterOperationError,
    ClusterNotFoundError
)


@pytest.fixture
def mock_service_catalog():
    """Mock multi-cluster service catalog."""
    catalog = AsyncMock(spec=MultiClusterServiceCatalog)
    catalog.multi_cluster_manager = AsyncMock()
    catalog.multi_cluster_manager.get_cluster_status = AsyncMock(return_value=ServiceStatus.RUNNING)
    catalog.create_cluster_topic_manager = AsyncMock()
    catalog.create_cluster_message_manager = AsyncMock()
    return catalog


@pytest.fixture
def mock_topic_manager():
    """Mock topic manager."""
    manager = AsyncMock(spec=TopicManager)
    manager.close = Mock()
    manager.list_topics = AsyncMock(return_value=[])
    manager.create_topic = AsyncMock(return_value=True)
    manager.get_topic_metadata = AsyncMock()
    return manager


@pytest.fixture
def sample_topics():
    """Sample topics for testing."""
    return [
        TopicInfo(
            name="topic1",
            partitions=3,
            replication_factor=1,
            config={"retention.ms": "86400000"}
        ),
        TopicInfo(
            name="topic2",
            partitions=1,
            replication_factor=1,
            config={"cleanup.policy": "compact"}
        )
    ]


@pytest.fixture
async def cross_cluster_ops(mock_service_catalog):
    """Cross-cluster operations instance for testing."""
    return CrossClusterOperations(mock_service_catalog)


class TestCrossClusterOperationsInit:
    """Test CrossClusterOperations initialization."""
    
    def test_init(self, mock_service_catalog):
        """Test CrossClusterOperations initialization."""
        ops = CrossClusterOperations(mock_service_catalog)
        
        assert ops.service_catalog == mock_service_catalog
        assert ops.max_concurrent_operations == 5
        assert ops.batch_size == 100
        assert len(ops._active_operations) == 0
        assert len(ops._operation_progress) == 0
        assert len(ops._operation_tasks) == 0


class TestOperationProgress:
    """Test OperationProgress functionality."""
    
    def test_progress_percent_calculation(self):
        """Test progress percentage calculation."""
        progress = OperationProgress(
            operation_id="test-op",
            total_items=100,
            processed_items=25,
            failed_items=5,
            current_phase="processing"
        )
        
        assert progress.progress_percent == 25.0
    
    def test_progress_percent_zero_total(self):
        """Test progress percentage with zero total items."""
        progress = OperationProgress(
            operation_id="test-op",
            total_items=0,
            processed_items=0,
            failed_items=0,
            current_phase="complete"
        )
        
        assert progress.progress_percent == 100.0
    
    def test_success_rate_calculation(self):
        """Test success rate calculation."""
        progress = OperationProgress(
            operation_id="test-op",
            total_items=100,
            processed_items=20,
            failed_items=5,
            current_phase="processing"
        )
        
        assert progress.success_rate == 75.0  # (20-5)/20 * 100
    
    def test_success_rate_zero_processed(self):
        """Test success rate with zero processed items."""
        progress = OperationProgress(
            operation_id="test-op",
            total_items=100,
            processed_items=0,
            failed_items=0,
            current_phase="starting"
        )
        
        assert progress.success_rate == 100.0


class TestTopicMigration:
    """Test topic migration functionality."""
    
    async def test_migrate_topics_success(self, cross_cluster_ops, mock_topic_manager, sample_topics):
        """Test successful topic migration."""
        # Setup mocks
        cross_cluster_ops.service_catalog.create_cluster_topic_manager.return_value = mock_topic_manager
        mock_topic_manager.list_topics.return_value = sample_topics
        
        # Start migration
        operation_id = await cross_cluster_ops.migrate_topics(
            source_cluster_id="source-cluster",
            target_cluster_id="target-cluster",
            topics=["topic1"],
            dry_run=False
        )
        
        assert operation_id is not None
        assert operation_id in cross_cluster_ops._active_operations
        
        # Wait for operation to complete
        await asyncio.sleep(0.1)  # Allow task to start
        
        operation = cross_cluster_ops._active_operations[operation_id]
        assert operation.operation_type == "migrate"
        assert operation.source_cluster_id == "source-cluster"
        assert operation.target_cluster_ids == ["target-cluster"]
        assert operation.topics == ["topic1"]
    
    async def test_migrate_topics_dry_run(self, cross_cluster_ops, mock_topic_manager, sample_topics):
        """Test topic migration dry run."""
        # Setup mocks
        cross_cluster_ops.service_catalog.create_cluster_topic_manager.return_value = mock_topic_manager
        mock_topic_manager.list_topics.return_value = sample_topics
        
        # Start dry run migration
        operation_id = await cross_cluster_ops.migrate_topics(
            source_cluster_id="source-cluster",
            target_cluster_id="target-cluster",
            dry_run=True
        )
        
        assert operation_id is not None
        
        # Wait for operation to complete
        await asyncio.sleep(0.1)
        
        # Verify no actual topic creation occurred
        mock_topic_manager.create_topic.assert_not_called()
    
    async def test_migrate_topics_invalid_cluster(self, cross_cluster_ops):
        """Test migration with invalid cluster."""
        # Setup mock to raise exception for invalid cluster
        cross_cluster_ops.service_catalog.multi_cluster_manager.get_cluster_status.side_effect = Exception("Cluster not found")
        
        with pytest.raises(CrossClusterOperationError):
            await cross_cluster_ops.migrate_topics(
                source_cluster_id="invalid-cluster",
                target_cluster_id="target-cluster"
            )
    
    async def test_migrate_topics_with_specific_topics(self, cross_cluster_ops, mock_topic_manager, sample_topics):
        """Test migration with specific topic list."""
        # Setup mocks
        cross_cluster_ops.service_catalog.create_cluster_topic_manager.return_value = mock_topic_manager
        mock_topic_manager.list_topics.return_value = sample_topics
        
        # Start migration with specific topics
        operation_id = await cross_cluster_ops.migrate_topics(
            source_cluster_id="source-cluster",
            target_cluster_id="target-cluster",
            topics=["topic1", "topic2"],
            include_data=False,
            preserve_partitions=False
        )
        
        assert operation_id is not None
        
        operation = cross_cluster_ops._active_operations[operation_id]
        assert operation.topics == ["topic1", "topic2"]
        assert operation.include_data is False
        assert operation.preserve_partitions is False


class TestTopicReplication:
    """Test topic replication functionality."""
    
    async def test_replicate_topics_mirror_mode(self, cross_cluster_ops):
        """Test topic replication in mirror mode."""
        operation_id = await cross_cluster_ops.replicate_topics(
            source_cluster_id="source-cluster",
            target_cluster_ids=["target-cluster-1", "target-cluster-2"],
            topics=["topic1"],
            mode=ReplicationMode.MIRROR,
            continuous=False
        )
        
        assert operation_id is not None
        
        operation = cross_cluster_ops._active_operations[operation_id]
        assert operation.operation_type == "replicate"
        assert operation.source_cluster_id == "source-cluster"
        assert operation.target_cluster_ids == ["target-cluster-1", "target-cluster-2"]
        assert operation.results["mode"] == "mirror"
        assert operation.results["continuous"] is False
    
    async def test_replicate_topics_sync_mode(self, cross_cluster_ops):
        """Test topic replication in sync mode."""
        operation_id = await cross_cluster_ops.replicate_topics(
            source_cluster_id="source-cluster",
            target_cluster_ids=["target-cluster"],
            mode=ReplicationMode.SYNC,
            continuous=True
        )
        
        assert operation_id is not None
        
        operation = cross_cluster_ops._active_operations[operation_id]
        assert operation.results["mode"] == "sync"
        assert operation.results["continuous"] is True
    
    async def test_replicate_topics_backup_mode(self, cross_cluster_ops):
        """Test topic replication in backup mode."""
        operation_id = await cross_cluster_ops.replicate_topics(
            source_cluster_id="source-cluster",
            target_cluster_ids=["backup-cluster"],
            mode=ReplicationMode.BACKUP
        )
        
        assert operation_id is not None
        
        operation = cross_cluster_ops._active_operations[operation_id]
        assert operation.results["mode"] == "backup"


class TestClusterComparison:
    """Test cluster comparison functionality."""
    
    async def test_compare_clusters_identical(self, cross_cluster_ops, mock_topic_manager):
        """Test comparison of identical clusters."""
        # Setup identical topics
        identical_topics = [
            TopicInfo(name="topic1", partitions=3, replication_factor=1, config={"retention.ms": "86400000"})
        ]
        
        cross_cluster_ops.service_catalog.create_cluster_topic_manager.return_value = mock_topic_manager
        mock_topic_manager.list_topics.return_value = identical_topics
        
        result = await cross_cluster_ops.compare_clusters(
            source_cluster_id="cluster1",
            target_cluster_id="cluster2"
        )
        
        assert isinstance(result, ClusterComparisonResult)
        assert result.source_cluster == "cluster1"
        assert result.target_cluster == "cluster2"
        assert len(result.topics_only_in_source) == 0
        assert len(result.topics_only_in_target) == 0
        assert len(result.topics_with_different_config) == 0
        assert result.summary["identical"] is True
    
    async def test_compare_clusters_different_topics(self, cross_cluster_ops, mock_topic_manager):
        """Test comparison of clusters with different topics."""
        # Setup different topics for each cluster
        source_topics = [
            TopicInfo(name="topic1", partitions=3, replication_factor=1),
            TopicInfo(name="topic2", partitions=1, replication_factor=1)
        ]
        target_topics = [
            TopicInfo(name="topic1", partitions=3, replication_factor=1),
            TopicInfo(name="topic3", partitions=2, replication_factor=1)
        ]
        
        def mock_create_topic_manager(cluster_id):
            manager = AsyncMock(spec=TopicManager)
            manager.close = Mock()
            if cluster_id == "cluster1":
                manager.list_topics.return_value = source_topics
            else:
                manager.list_topics.return_value = target_topics
            return manager
        
        cross_cluster_ops.service_catalog.create_cluster_topic_manager.side_effect = mock_create_topic_manager
        
        result = await cross_cluster_ops.compare_clusters(
            source_cluster_id="cluster1",
            target_cluster_id="cluster2"
        )
        
        assert "topic2" in result.topics_only_in_source
        assert "topic3" in result.topics_only_in_target
        assert result.summary["identical"] is False
    
    async def test_compare_clusters_different_configs(self, cross_cluster_ops, mock_topic_manager):
        """Test comparison of clusters with different topic configurations."""
        # Setup topics with different configurations
        source_topics = [
            TopicInfo(name="topic1", partitions=3, replication_factor=1, config={"retention.ms": "86400000"})
        ]
        target_topics = [
            TopicInfo(name="topic1", partitions=3, replication_factor=1, config={"retention.ms": "172800000"})
        ]
        
        def mock_create_topic_manager(cluster_id):
            manager = AsyncMock(spec=TopicManager)
            manager.close = Mock()
            if cluster_id == "cluster1":
                manager.list_topics.return_value = source_topics
            else:
                manager.list_topics.return_value = target_topics
            return manager
        
        cross_cluster_ops.service_catalog.create_cluster_topic_manager.side_effect = mock_create_topic_manager
        
        result = await cross_cluster_ops.compare_clusters(
            source_cluster_id="cluster1",
            target_cluster_id="cluster2"
        )
        
        assert "topic1" in result.topics_with_different_config
        assert "topic1" in result.config_differences
        assert "config.retention.ms" in result.config_differences["topic1"]
        assert result.summary["identical"] is False
    
    async def test_compare_clusters_with_data(self, cross_cluster_ops, mock_topic_manager):
        """Test cluster comparison including message data."""
        # Setup topics
        topics = [TopicInfo(name="topic1", partitions=3, replication_factor=1)]
        
        # Setup topic metadata with message counts
        source_metadata = Mock()
        source_metadata.message_count = 100
        target_metadata = Mock()
        target_metadata.message_count = 95
        
        def mock_create_topic_manager(cluster_id):
            manager = AsyncMock(spec=TopicManager)
            manager.close = Mock()
            manager.list_topics.return_value = topics
            if cluster_id == "cluster1":
                manager.get_topic_metadata.return_value = source_metadata
            else:
                manager.get_topic_metadata.return_value = target_metadata
            return manager
        
        cross_cluster_ops.service_catalog.create_cluster_topic_manager.side_effect = mock_create_topic_manager
        
        result = await cross_cluster_ops.compare_clusters(
            source_cluster_id="cluster1",
            target_cluster_id="cluster2",
            include_data=True
        )
        
        assert result.message_count_differences is not None
        assert "topic1" in result.message_count_differences
        assert result.message_count_differences["topic1"]["source"] == 100
        assert result.message_count_differences["topic1"]["target"] == 95


class TestOperationManagement:
    """Test operation management functionality."""
    
    async def test_get_operation_status(self, cross_cluster_ops):
        """Test getting operation status."""
        # Create a test operation
        operation = CrossClusterOperation(
            id="test-op",
            operation_type="migrate",
            source_cluster_id="source",
            target_cluster_ids=["target"],
            status=OperationStatus.RUNNING,
            progress_percent=50.0
        )
        
        cross_cluster_ops._active_operations["test-op"] = operation
        
        # Add progress information
        progress = OperationProgress(
            operation_id="test-op",
            total_items=100,
            processed_items=50,
            failed_items=5,
            current_phase="migrating"
        )
        cross_cluster_ops._operation_progress["test-op"] = progress
        
        # Get status
        status = await cross_cluster_ops.get_operation_status("test-op")
        
        assert status.id == "test-op"
        assert status.progress_percent == 50.0
        assert status.results["processed_items"] == 50
        assert status.results["failed_items"] == 5
        assert status.results["current_phase"] == "migrating"
    
    async def test_get_operation_status_not_found(self, cross_cluster_ops):
        """Test getting status for non-existent operation."""
        with pytest.raises(CrossClusterOperationError) as exc_info:
            await cross_cluster_ops.get_operation_status("non-existent")
        
        assert "not found" in str(exc_info.value)
    
    async def test_cancel_operation(self, cross_cluster_ops):
        """Test cancelling an operation."""
        # Create a running operation
        operation = CrossClusterOperation(
            id="test-op",
            operation_type="migrate",
            source_cluster_id="source",
            target_cluster_ids=["target"],
            status=OperationStatus.RUNNING
        )
        
        cross_cluster_ops._active_operations["test-op"] = operation
        
        # Create a mock task
        mock_task = AsyncMock()
        mock_task.cancel = Mock()
        cross_cluster_ops._operation_tasks["test-op"] = mock_task
        
        # Cancel operation
        result = await cross_cluster_ops.cancel_operation("test-op")
        
        assert result is True
        assert operation.status == OperationStatus.CANCELLED
        assert operation.completed_at is not None
        mock_task.cancel.assert_called_once()
    
    async def test_cancel_operation_not_cancellable(self, cross_cluster_ops):
        """Test cancelling an operation that cannot be cancelled."""
        # Create a completed operation
        operation = CrossClusterOperation(
            id="test-op",
            operation_type="migrate",
            source_cluster_id="source",
            target_cluster_ids=["target"],
            status=OperationStatus.COMPLETED
        )
        
        cross_cluster_ops._active_operations["test-op"] = operation
        
        with pytest.raises(CrossClusterOperationError) as exc_info:
            await cross_cluster_ops.cancel_operation("test-op")
        
        assert "cannot be cancelled" in str(exc_info.value)
    
    async def test_list_operations(self, cross_cluster_ops):
        """Test listing operations with filters."""
        # Create test operations
        operations = [
            CrossClusterOperation(
                id="op1",
                operation_type="migrate",
                source_cluster_id="source",
                target_cluster_ids=["target"],
                status=OperationStatus.RUNNING
            ),
            CrossClusterOperation(
                id="op2",
                operation_type="replicate",
                source_cluster_id="source",
                target_cluster_ids=["target"],
                status=OperationStatus.COMPLETED
            ),
            CrossClusterOperation(
                id="op3",
                operation_type="migrate",
                source_cluster_id="source",
                target_cluster_ids=["target"],
                status=OperationStatus.FAILED
            )
        ]
        
        for op in operations:
            cross_cluster_ops._active_operations[op.id] = op
        
        # Test listing all operations
        all_ops = await cross_cluster_ops.list_operations()
        assert len(all_ops) == 3
        
        # Test filtering by status
        running_ops = await cross_cluster_ops.list_operations(status_filter=OperationStatus.RUNNING)
        assert len(running_ops) == 1
        assert running_ops[0].id == "op1"
        
        # Test filtering by operation type
        migrate_ops = await cross_cluster_ops.list_operations(operation_type_filter="migrate")
        assert len(migrate_ops) == 2
        assert all(op.operation_type == "migrate" for op in migrate_ops)
    
    async def test_cleanup_completed_operations(self, cross_cluster_ops):
        """Test cleanup of old completed operations."""
        # Create operations with different completion times
        old_time = datetime.utcnow() - timedelta(hours=25)  # Older than 24 hours
        recent_time = datetime.utcnow() - timedelta(hours=1)  # Recent
        
        operations = [
            CrossClusterOperation(
                id="old-op",
                operation_type="migrate",
                source_cluster_id="source",
                target_cluster_ids=["target"],
                status=OperationStatus.COMPLETED,
                completed_at=old_time
            ),
            CrossClusterOperation(
                id="recent-op",
                operation_type="migrate",
                source_cluster_id="source",
                target_cluster_ids=["target"],
                status=OperationStatus.COMPLETED,
                completed_at=recent_time
            ),
            CrossClusterOperation(
                id="running-op",
                operation_type="migrate",
                source_cluster_id="source",
                target_cluster_ids=["target"],
                status=OperationStatus.RUNNING
            )
        ]
        
        for op in operations:
            cross_cluster_ops._active_operations[op.id] = op
        
        # Cleanup operations older than 24 hours
        cleaned_count = await cross_cluster_ops.cleanup_completed_operations(max_age_hours=24)
        
        assert cleaned_count == 1
        assert "old-op" not in cross_cluster_ops._active_operations
        assert "recent-op" in cross_cluster_ops._active_operations
        assert "running-op" in cross_cluster_ops._active_operations


class TestPrivateHelpers:
    """Test private helper methods."""
    
    async def test_validate_clusters_success(self, cross_cluster_ops):
        """Test successful cluster validation."""
        # Mock successful cluster status checks
        cross_cluster_ops.service_catalog.multi_cluster_manager.get_cluster_status.return_value = ServiceStatus.RUNNING
        
        # Should not raise any exception
        await cross_cluster_ops._validate_clusters(["cluster1", "cluster2"])
        
        # Verify all clusters were checked
        assert cross_cluster_ops.service_catalog.multi_cluster_manager.get_cluster_status.call_count == 2
    
    async def test_validate_clusters_failure(self, cross_cluster_ops):
        """Test cluster validation with invalid cluster."""
        # Mock cluster status check to raise exception
        cross_cluster_ops.service_catalog.multi_cluster_manager.get_cluster_status.side_effect = Exception("Cluster not found")
        
        with pytest.raises(ClusterNotFoundError):
            await cross_cluster_ops._validate_clusters(["invalid-cluster"])
    
    def test_compare_topic_configs_identical(self, cross_cluster_ops):
        """Test comparing identical topic configurations."""
        topic1 = TopicInfo(
            name="test-topic",
            partitions=3,
            replication_factor=1,
            config={"retention.ms": "86400000"}
        )
        topic2 = TopicInfo(
            name="test-topic",
            partitions=3,
            replication_factor=1,
            config={"retention.ms": "86400000"}
        )
        
        differences = cross_cluster_ops._compare_topic_configs(topic1, topic2)
        assert len(differences) == 0
    
    def test_compare_topic_configs_different(self, cross_cluster_ops):
        """Test comparing different topic configurations."""
        topic1 = TopicInfo(
            name="test-topic",
            partitions=3,
            replication_factor=1,
            config={"retention.ms": "86400000", "cleanup.policy": "delete"}
        )
        topic2 = TopicInfo(
            name="test-topic",
            partitions=5,
            replication_factor=2,
            config={"retention.ms": "172800000", "compression.type": "gzip"}
        )
        
        differences = cross_cluster_ops._compare_topic_configs(topic1, topic2)
        
        assert "partitions" in differences
        assert differences["partitions"]["source"] == 3
        assert differences["partitions"]["target"] == 5
        
        assert "replication_factor" in differences
        assert differences["replication_factor"]["source"] == 1
        assert differences["replication_factor"]["target"] == 2
        
        assert "config.retention.ms" in differences
        assert differences["config.retention.ms"]["source"] == "86400000"
        assert differences["config.retention.ms"]["target"] == "172800000"
        
        assert "config.cleanup.policy" in differences
        assert differences["config.cleanup.policy"]["source"] == "delete"
        assert differences["config.cleanup.policy"]["target"] is None
        
        assert "config.compression.type" in differences
        assert differences["config.compression.type"]["source"] is None
        assert differences["config.compression.type"]["target"] == "gzip"
    
    def test_update_progress(self, cross_cluster_ops):
        """Test progress update functionality."""
        operation_id = "test-op"
        
        # Create a test operation
        operation = CrossClusterOperation(
            id=operation_id,
            operation_type="migrate",
            source_cluster_id="source",
            target_cluster_ids=["target"],
            status=OperationStatus.RUNNING,
            started_at=datetime.utcnow()
        )
        cross_cluster_ops._active_operations[operation_id] = operation
        
        # Update progress
        cross_cluster_ops._update_progress(operation_id, 100, 25, 5, "processing")
        
        # Verify progress was updated
        assert operation_id in cross_cluster_ops._operation_progress
        progress = cross_cluster_ops._operation_progress[operation_id]
        
        assert progress.total_items == 100
        assert progress.processed_items == 25
        assert progress.failed_items == 5
        assert progress.current_phase == "processing"
        assert progress.progress_percent == 25.0
        assert progress.success_rate == 80.0  # (25-5)/25 * 100
        
        # Verify operation progress was updated
        assert operation.progress_percent == 25.0


@pytest.mark.integration
class TestCrossClusterIntegration:
    """Integration tests for cross-cluster operations."""
    
    async def test_full_migration_workflow(self, cross_cluster_ops, mock_topic_manager, sample_topics):
        """Test complete migration workflow."""
        # Setup mocks
        cross_cluster_ops.service_catalog.create_cluster_topic_manager.return_value = mock_topic_manager
        mock_topic_manager.list_topics.return_value = sample_topics
        
        # Start migration
        operation_id = await cross_cluster_ops.migrate_topics(
            source_cluster_id="source-cluster",
            target_cluster_id="target-cluster",
            topics=["topic1", "topic2"],
            include_data=True,
            preserve_partitions=True
        )
        
        # Wait for operation to complete
        max_wait = 5  # seconds
        wait_time = 0
        while wait_time < max_wait:
            try:
                operation = await cross_cluster_ops.get_operation_status(operation_id)
                if operation.status in [OperationStatus.COMPLETED, OperationStatus.FAILED]:
                    break
            except:
                pass
            await asyncio.sleep(0.1)
            wait_time += 0.1
        
        # Verify operation completed
        operation = await cross_cluster_ops.get_operation_status(operation_id)
        assert operation.status == OperationStatus.COMPLETED
        assert operation.progress_percent == 100.0
        assert "migrated_topics" in operation.results
        
        # Verify topic managers were called correctly
        assert mock_topic_manager.list_topics.call_count >= 1
        assert mock_topic_manager.create_topic.call_count >= 1
        mock_topic_manager.close.assert_called()
    
    async def test_operation_cancellation_workflow(self, cross_cluster_ops):
        """Test operation cancellation workflow."""
        # Start a replication operation
        operation_id = await cross_cluster_ops.replicate_topics(
            source_cluster_id="source-cluster",
            target_cluster_ids=["target-cluster"],
            continuous=True
        )
        
        # Wait a bit for operation to start
        await asyncio.sleep(0.1)
        
        # Cancel the operation
        cancelled = await cross_cluster_ops.cancel_operation(operation_id)
        assert cancelled is True
        
        # Verify operation status
        operation = await cross_cluster_ops.get_operation_status(operation_id)
        assert operation.status == OperationStatus.CANCELLED
        assert operation.completed_at is not None
    
    async def test_multiple_concurrent_operations(self, cross_cluster_ops, mock_topic_manager, sample_topics):
        """Test multiple concurrent operations."""
        # Setup mocks
        cross_cluster_ops.service_catalog.create_cluster_topic_manager.return_value = mock_topic_manager
        mock_topic_manager.list_topics.return_value = sample_topics
        
        # Start multiple operations
        operation_ids = []
        for i in range(3):
            op_id = await cross_cluster_ops.migrate_topics(
                source_cluster_id=f"source-{i}",
                target_cluster_id=f"target-{i}",
                dry_run=True
            )
            operation_ids.append(op_id)
        
        # Verify all operations were created
        assert len(operation_ids) == 3
        assert len(cross_cluster_ops._active_operations) == 3
        
        # List all operations
        all_operations = await cross_cluster_ops.list_operations()
        assert len(all_operations) == 3
        
        # Wait for operations to complete
        await asyncio.sleep(0.2)
        
        # Verify operations completed
        for op_id in operation_ids:
            operation = await cross_cluster_ops.get_operation_status(op_id)
            assert operation.status in [OperationStatus.COMPLETED, OperationStatus.RUNNING]