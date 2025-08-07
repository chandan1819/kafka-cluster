"""
Multi-cluster API routes for the Local Kafka Manager.
"""

import logging
from datetime import datetime
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Query, Path, Body, status, BackgroundTasks
from fastapi.responses import JSONResponse

from ..models.base import ErrorResponse
from ..models.multi_cluster import (
    CreateClusterRequest,
    UpdateClusterRequest,
    ClusterSummary,
    ClusterDefinition,
    ClusterTemplate,
    CreateTemplateRequest,
    MigrateDataRequest,
    CrossClusterOperation,
    OperationStatus,
    ClusterComparisonResult
)
from ..models.topic import TopicCreateRequest, TopicInfo
from ..models.message import ProduceRequest, ProduceResult, ConsumeRequest, ConsumeResponse
from ..services.multi_cluster_manager import MultiClusterManager
from ..services.multi_cluster_service_catalog import MultiClusterServiceCatalog
from ..services.template_manager import TemplateManager
from ..services.cross_cluster_operations import CrossClusterOperations, ReplicationMode
from ..exceptions import (
    LocalKafkaManagerError,
    ClusterNotFoundError,
    ClusterAlreadyExistsError,
    CrossClusterOperationError,
    TemplateNotFoundError,
    ValidationError
)

logger = logging.getLogger(__name__)

# Create multi-cluster API router
router = APIRouter(prefix="/multi-cluster", tags=["Multi-Cluster Management"])

# Global instances (will be initialized in main.py)
multi_cluster_manager: Optional[MultiClusterManager] = None
multi_cluster_catalog: Optional[MultiClusterServiceCatalog] = None
template_manager: Optional[TemplateManager] = None
cross_cluster_ops: Optional[CrossClusterOperations] = None


def init_multi_cluster_services(
    manager: MultiClusterManager,
    catalog: MultiClusterServiceCatalog,
    templates: TemplateManager,
    operations: CrossClusterOperations
):
    """Initialize multi-cluster service instances."""
    global multi_cluster_manager, multi_cluster_catalog, template_manager, cross_cluster_ops
    multi_cluster_manager = manager
    multi_cluster_catalog = catalog
    template_manager = templates
    cross_cluster_ops = operations

# Cluster Registry Management Endpoints

@router.get("/clusters", response_model=List[ClusterSummary])
async def list_clusters(
    status_filter: Optional[str] = Query(None, description="Filter by cluster status"),
    environment_filter: Optional[str] = Query(None, description="Filter by environment"),
    tag_key: Optional[str] = Query(None, description="Filter by tag key"),
    tag_value: Optional[str] = Query(None, description="Filter by tag value")
) -> List[ClusterSummary]:
    """List all registered clusters with optional filtering."""
    try:
        # Build tag filter
        tag_filter = None
        if tag_key:
            tag_filter = {tag_key: tag_value} if tag_value else {tag_key: None}
        
        clusters = await multi_cluster_manager.list_clusters(
            status_filter=status_filter,
            environment_filter=environment_filter,
            tag_filter=tag_filter
        )
        return clusters
    except Exception as e:
        logger.error(f"Failed to list clusters: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list clusters: {str(e)}"
        )


@router.post("/clusters", response_model=ClusterDefinition, status_code=status.HTTP_201_CREATED)
async def create_cluster(request: CreateClusterRequest) -> ClusterDefinition:
    """Create a new cluster."""
    try:
        # Generate cluster ID from name if not provided
        cluster_id = request.name.lower().replace(" ", "-").replace("_", "-")
        
        # Create cluster definition
        cluster_definition = ClusterDefinition(
            id=cluster_id,
            name=request.name,
            description=request.description,
            template_id=request.template_id,
            environment=request.environment,
            tags=request.tags
        )
        
        # Apply configuration overrides
        if request.kafka_config:
            cluster_definition.kafka_config = request.kafka_config
        if request.rest_proxy_config:
            cluster_definition.rest_proxy_config = request.rest_proxy_config
        if request.ui_config:
            cluster_definition.ui_config = request.ui_config
        if request.retention_policy:
            cluster_definition.retention_policy = request.retention_policy
        
        # Create cluster
        created_cluster = await multi_cluster_manager.create_cluster(
            cluster_definition,
            auto_start=request.auto_start
        )
        
        return created_cluster
        
    except ClusterAlreadyExistsError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Cluster already exists: {str(e)}"
        )
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid cluster configuration: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Failed to create cluster: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create cluster: {str(e)}"
        )


@router.get("/clusters/{cluster_id}", response_model=ClusterDefinition)
async def get_cluster(cluster_id: str = Path(..., description="Cluster ID")) -> ClusterDefinition:
    """Get details of a specific cluster."""
    try:
        cluster = await multi_cluster_manager.registry.get_cluster(cluster_id)
        return cluster
    except ClusterNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cluster not found: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Failed to get cluster {cluster_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get cluster: {str(e)}"
        )


@router.put("/clusters/{cluster_id}", response_model=ClusterDefinition)
async def update_cluster(
    cluster_id: str = Path(..., description="Cluster ID"),
    request: UpdateClusterRequest = Body(...)
) -> ClusterDefinition:
    """Update cluster configuration."""
    try:
        # Build updates dictionary
        updates = {}
        if request.name is not None:
            updates["name"] = request.name
        if request.description is not None:
            updates["description"] = request.description
        if request.kafka_config is not None:
            updates["kafka_config"] = request.kafka_config
        if request.rest_proxy_config is not None:
            updates["rest_proxy_config"] = request.rest_proxy_config
        if request.ui_config is not None:
            updates["ui_config"] = request.ui_config
        if request.retention_policy is not None:
            updates["retention_policy"] = request.retention_policy
        if request.tags is not None:
            updates["tags"] = request.tags
        
        updated_cluster = await multi_cluster_manager.registry.update_cluster(cluster_id, updates)
        return updated_cluster
        
    except ClusterNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cluster not found: {str(e)}"
        )
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid update data: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Failed to update cluster {cluster_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update cluster: {str(e)}"
        )


@router.delete("/clusters/{cluster_id}")
async def delete_cluster(
    cluster_id: str = Path(..., description="Cluster ID"),
    force: bool = Query(False, description="Force deletion even if running"),
    cleanup_data: bool = Query(True, description="Remove data directories")
) -> Dict[str, Any]:
    """Delete a cluster."""
    try:
        success = await multi_cluster_manager.delete_cluster(
            cluster_id,
            force=force,
            cleanup_data=cleanup_data
        )
        
        return {
            "success": success,
            "message": f"Cluster {cluster_id} deleted successfully"
        }
        
    except ClusterNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cluster not found: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Failed to delete cluster {cluster_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete cluster: {str(e)}"
        )
# Cluster Lifecycle Management Endpoints

@router.post("/clusters/{cluster_id}/start")
async def start_cluster(
    cluster_id: str = Path(..., description="Cluster ID"),
    force: bool = Query(False, description="Force start even if already running"),
    timeout: int = Query(60, description="Startup timeout in seconds")
) -> Dict[str, Any]:
    """Start a specific cluster."""
    try:
        final_status = await multi_cluster_manager.start_cluster(
            cluster_id,
            force=force,
            timeout=timeout
        )
        
        return {
            "cluster_id": cluster_id,
            "status": final_status.value,
            "message": f"Cluster {cluster_id} started successfully"
        }
        
    except ClusterNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cluster not found: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Failed to start cluster {cluster_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start cluster: {str(e)}"
        )


@router.post("/clusters/{cluster_id}/stop")
async def stop_cluster(
    cluster_id: str = Path(..., description="Cluster ID"),
    force: bool = Query(False, description="Force stop containers"),
    cleanup: bool = Query(False, description="Remove containers and volumes"),
    timeout: int = Query(30, description="Stop timeout in seconds")
) -> Dict[str, Any]:
    """Stop a specific cluster."""
    try:
        success = await multi_cluster_manager.stop_cluster(
            cluster_id,
            force=force,
            cleanup=cleanup,
            timeout=timeout
        )
        
        return {
            "cluster_id": cluster_id,
            "success": success,
            "message": f"Cluster {cluster_id} stopped successfully"
        }
        
    except ClusterNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cluster not found: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Failed to stop cluster {cluster_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to stop cluster: {str(e)}"
        )


@router.get("/clusters/{cluster_id}/status")
async def get_cluster_status(
    cluster_id: str = Path(..., description="Cluster ID")
) -> Dict[str, Any]:
    """Get current status of a specific cluster."""
    try:
        cluster_status = await multi_cluster_manager.get_cluster_status(cluster_id)
        
        return {
            "cluster_id": cluster_id,
            "status": cluster_status.value,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except ClusterNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cluster not found: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Failed to get cluster status for {cluster_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get cluster status: {str(e)}"
        )


@router.get("/clusters/{cluster_id}/health")
async def get_cluster_health(
    cluster_id: str = Path(..., description="Cluster ID")
) -> Dict[str, Any]:
    """Get detailed health information for a specific cluster."""
    try:
        health_info = await multi_cluster_manager.get_cluster_health(cluster_id)
        return health_info
        
    except ClusterNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cluster not found: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Failed to get cluster health for {cluster_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get cluster health: {str(e)}"
        )


# Cluster-Specific Topic Management Endpoints

@router.get("/clusters/{cluster_id}/topics", response_model=List[Dict[str, Any]])
async def list_cluster_topics(
    cluster_id: str = Path(..., description="Cluster ID"),
    include_internal: bool = Query(False, description="Include internal topics")
) -> List[Dict[str, Any]]:
    """List topics in a specific cluster."""
    try:
        topics = await multi_cluster_catalog.get_cluster_topics(cluster_id)
        
        # Filter internal topics if requested
        if not include_internal:
            topics = [t for t in topics if not t["name"].startswith("__")]
        
        return topics
        
    except ClusterNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cluster not found: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Failed to list topics for cluster {cluster_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list topics: {str(e)}"
        )


@router.post("/clusters/{cluster_id}/topics", status_code=status.HTTP_201_CREATED)
async def create_cluster_topic(
    cluster_id: str = Path(..., description="Cluster ID"),
    request: TopicCreateRequest = Body(...)
) -> Dict[str, Any]:
    """Create a topic in a specific cluster."""
    try:
        # Get cluster-specific topic manager
        topic_manager = await multi_cluster_catalog.create_cluster_topic_manager(cluster_id)
        
        try:
            # Create topic
            success = await topic_manager.create_topic(request.to_topic_config())
            
            return {
                "cluster_id": cluster_id,
                "topic_name": request.name,
                "success": success,
                "message": f"Topic {request.name} created successfully in cluster {cluster_id}"
            }
            
        finally:
            topic_manager.close()
            
    except ClusterNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cluster not found: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Failed to create topic in cluster {cluster_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create topic: {str(e)}"
        )


@router.delete("/clusters/{cluster_id}/topics/{topic_name}")
async def delete_cluster_topic(
    cluster_id: str = Path(..., description="Cluster ID"),
    topic_name: str = Path(..., description="Topic name"),
    force: bool = Query(False, description="Force delete even if topic has data")
) -> Dict[str, Any]:
    """Delete a topic from a specific cluster."""
    try:
        # Get cluster-specific topic manager
        topic_manager = await multi_cluster_catalog.create_cluster_topic_manager(cluster_id)
        
        try:
            # Delete topic
            success = await topic_manager.delete_topic(topic_name, force=force)
            
            return {
                "cluster_id": cluster_id,
                "topic_name": topic_name,
                "success": success,
                "message": f"Topic {topic_name} deleted successfully from cluster {cluster_id}"
            }
            
        finally:
            topic_manager.close()
            
    except ClusterNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cluster not found: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Failed to delete topic from cluster {cluster_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete topic: {str(e)}"
        )


# Cluster-Specific Message Operations Endpoints

@router.post("/clusters/{cluster_id}/produce", response_model=ProduceResult)
async def produce_message_to_cluster(
    cluster_id: str = Path(..., description="Cluster ID"),
    request: ProduceRequest = Body(...)
) -> ProduceResult:
    """Produce a message to a topic in a specific cluster."""
    try:
        # Get cluster-specific message manager
        message_manager = await multi_cluster_catalog.create_cluster_message_manager(cluster_id)
        
        try:
            # Produce message
            result = await message_manager.produce_message(request)
            return result
            
        finally:
            await message_manager.close()
            
    except ClusterNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cluster not found: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Failed to produce message to cluster {cluster_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to produce message: {str(e)}"
        )


@router.get("/clusters/{cluster_id}/consume", response_model=ConsumeResponse)
async def consume_messages_from_cluster(
    cluster_id: str = Path(..., description="Cluster ID"),
    topic: str = Query(..., description="Topic name"),
    consumer_group: str = Query(..., description="Consumer group ID"),
    max_messages: int = Query(10, description="Maximum number of messages to consume"),
    timeout_ms: int = Query(5000, description="Consumer timeout in milliseconds"),
    from_beginning: bool = Query(False, description="Start consuming from beginning")
) -> ConsumeResponse:
    """Consume messages from a topic in a specific cluster."""
    try:
        # Get cluster-specific message manager
        message_manager = await multi_cluster_catalog.create_cluster_message_manager(cluster_id)
        
        try:
            # Create consume request
            consume_request = ConsumeRequest(
                topic=topic,
                consumer_group=consumer_group,
                max_messages=max_messages,
                timeout_ms=timeout_ms,
                from_beginning=from_beginning
            )
            
            # Consume messages
            response = await message_manager.consume_messages(consume_request)
            return response
            
        finally:
            await message_manager.close()
            
    except ClusterNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cluster not found: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Failed to consume messages from cluster {cluster_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to consume messages: {str(e)}"
        )


# Template Management Endpoints

@router.get("/templates", response_model=List[ClusterTemplate])
async def list_templates(
    category: Optional[str] = Query(None, description="Filter by template category")
) -> List[ClusterTemplate]:
    """List all available cluster templates."""
    try:
        templates = await template_manager.list_templates(category_filter=category)
        return templates
    except Exception as e:
        logger.error(f"Failed to list templates: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list templates: {str(e)}"
        )


@router.post("/templates", response_model=ClusterTemplate, status_code=status.HTTP_201_CREATED)
async def create_template(request: CreateTemplateRequest) -> ClusterTemplate:
    """Create a new cluster template."""
    try:
        template = await template_manager.create_template(request)
        return template
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid template data: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Failed to create template: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create template: {str(e)}"
        )


@router.get("/templates/{template_id}", response_model=ClusterTemplate)
async def get_template(
    template_id: str = Path(..., description="Template ID")
) -> ClusterTemplate:
    """Get details of a specific template."""
    try:
        template = await template_manager.get_template(template_id)
        return template
    except TemplateNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Template not found: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Failed to get template {template_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get template: {str(e)}"
        )


@router.delete("/templates/{template_id}")
async def delete_template(
    template_id: str = Path(..., description="Template ID")
) -> Dict[str, Any]:
    """Delete a cluster template."""
    try:
        success = await template_manager.delete_template(template_id)
        return {
            "success": success,
            "message": f"Template {template_id} deleted successfully"
        }
    except TemplateNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Template not found: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Failed to delete template {template_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete template: {str(e)}"
        )


# Cross-Cluster Operations Endpoints

@router.post("/operations/migrate")
async def start_migration(
    request: MigrateDataRequest = Body(...)
) -> Dict[str, Any]:
    """Start a data migration operation between clusters."""
    try:
        operation_id = await cross_cluster_ops.migrate_topics(
            source_cluster_id=request.source_cluster_id,
            target_cluster_id=request.target_cluster_id,
            topics=request.topics,
            include_data=not request.dry_run,  # Don't include data in dry run
            preserve_timestamps=request.preserve_timestamps,
            dry_run=request.dry_run
        )
        
        return {
            "operation_id": operation_id,
            "operation_type": "migrate",
            "status": "started",
            "message": f"Migration operation started with ID: {operation_id}"
        }
        
    except ClusterNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cluster not found: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Failed to start migration: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start migration: {str(e)}"
        )


@router.post("/operations/replicate")
async def start_replication(
    source_cluster_id: str = Body(..., description="Source cluster ID"),
    target_cluster_ids: List[str] = Body(..., description="Target cluster IDs"),
    topics: Optional[List[str]] = Body(None, description="Topics to replicate"),
    mode: str = Body("mirror", description="Replication mode (mirror, sync, backup)"),
    continuous: bool = Body(False, description="Continuous replication")
) -> Dict[str, Any]:
    """Start a topic replication operation."""
    try:
        # Validate replication mode
        try:
            replication_mode = ReplicationMode(mode)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid replication mode: {mode}. Must be one of: mirror, sync, backup"
            )
        
        operation_id = await cross_cluster_ops.replicate_topics(
            source_cluster_id=source_cluster_id,
            target_cluster_ids=target_cluster_ids,
            topics=topics,
            mode=replication_mode,
            continuous=continuous
        )
        
        return {
            "operation_id": operation_id,
            "operation_type": "replicate",
            "status": "started",
            "message": f"Replication operation started with ID: {operation_id}"
        }
        
    except ClusterNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cluster not found: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Failed to start replication: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start replication: {str(e)}"
        )


@router.post("/operations/compare", response_model=ClusterComparisonResult)
async def compare_clusters(
    source_cluster_id: str = Body(..., description="Source cluster ID"),
    target_cluster_id: str = Body(..., description="Target cluster ID"),
    include_data: bool = Body(False, description="Include message data comparison"),
    topics: Optional[List[str]] = Body(None, description="Topics to compare")
) -> ClusterComparisonResult:
    """Compare configuration and data between two clusters."""
    try:
        result = await cross_cluster_ops.compare_clusters(
            source_cluster_id=source_cluster_id,
            target_cluster_id=target_cluster_id,
            include_data=include_data,
            topics=topics
        )
        
        return result
        
    except ClusterNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cluster not found: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Failed to compare clusters: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to compare clusters: {str(e)}"
        )


@router.get("/operations", response_model=List[CrossClusterOperation])
async def list_operations(
    status_filter: Optional[str] = Query(None, description="Filter by operation status"),
    operation_type_filter: Optional[str] = Query(None, description="Filter by operation type")
) -> List[CrossClusterOperation]:
    """List cross-cluster operations with optional filtering."""
    try:
        # Convert status filter
        status_enum = None
        if status_filter:
            try:
                status_enum = OperationStatus(status_filter)
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid status filter: {status_filter}"
                )
        
        operations = await cross_cluster_ops.list_operations(
            status_filter=status_enum,
            operation_type_filter=operation_type_filter
        )
        
        return operations
        
    except Exception as e:
        logger.error(f"Failed to list operations: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list operations: {str(e)}"
        )


@router.get("/operations/{operation_id}", response_model=CrossClusterOperation)
async def get_operation_status(
    operation_id: str = Path(..., description="Operation ID")
) -> CrossClusterOperation:
    """Get status and progress of a specific operation."""
    try:
        operation = await cross_cluster_ops.get_operation_status(operation_id)
        return operation
        
    except CrossClusterOperationError as e:
        if "not found" in str(e):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Operation not found: {str(e)}"
            )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get operation status: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Failed to get operation status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get operation status: {str(e)}"
        )


@router.post("/operations/{operation_id}/cancel")
async def cancel_operation(
    operation_id: str = Path(..., description="Operation ID")
) -> Dict[str, Any]:
    """Cancel a running cross-cluster operation."""
    try:
        success = await cross_cluster_ops.cancel_operation(operation_id)
        
        return {
            "operation_id": operation_id,
            "success": success,
            "message": f"Operation {operation_id} cancelled successfully"
        }
        
    except CrossClusterOperationError as e:
        if "not found" in str(e):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Operation not found: {str(e)}"
            )
        elif "cannot be cancelled" in str(e):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Operation cannot be cancelled: {str(e)}"
            )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to cancel operation: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Failed to cancel operation: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to cancel operation: {str(e)}"
        )


# Multi-Cluster Batch Operations

@router.post("/operations/start-multiple")
async def start_multiple_clusters(
    cluster_ids: List[str] = Body(..., description="List of cluster IDs to start"),
    max_concurrent: int = Body(3, description="Maximum concurrent operations"),
    timeout: int = Body(60, description="Startup timeout per cluster in seconds")
) -> Dict[str, Any]:
    """Start multiple clusters concurrently."""
    try:
        results = await multi_cluster_manager.start_multiple_clusters(
            cluster_ids=cluster_ids,
            max_concurrent=max_concurrent,
            timeout=timeout
        )
        
        # Count successes and failures
        successful = sum(1 for status in results.values() if status.value == "running")
        failed = len(results) - successful
        
        return {
            "total_clusters": len(cluster_ids),
            "successful": successful,
            "failed": failed,
            "results": {cid: status.value for cid, status in results.items()},
            "message": f"Started {successful}/{len(cluster_ids)} clusters successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to start multiple clusters: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start multiple clusters: {str(e)}"
        )


@router.post("/operations/stop-multiple")
async def stop_multiple_clusters(
    cluster_ids: List[str] = Body(..., description="List of cluster IDs to stop"),
    max_concurrent: int = Body(5, description="Maximum concurrent operations"),
    force: bool = Body(False, description="Force stop containers"),
    timeout: int = Body(30, description="Stop timeout per cluster in seconds")
) -> Dict[str, Any]:
    """Stop multiple clusters concurrently."""
    try:
        results = await multi_cluster_manager.stop_multiple_clusters(
            cluster_ids=cluster_ids,
            max_concurrent=max_concurrent,
            force=force,
            timeout=timeout
        )
        
        # Count successes and failures
        successful = sum(1 for success in results.values() if success)
        failed = len(results) - successful
        
        return {
            "total_clusters": len(cluster_ids),
            "successful": successful,
            "failed": failed,
            "results": results,
            "message": f"Stopped {successful}/{len(cluster_ids)} clusters successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to stop multiple clusters: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to stop multiple clusters: {str(e)}"
        )


# Service Catalog and Health Endpoints

@router.get("/catalog")
async def get_multi_cluster_catalog(
    force_refresh: bool = Query(False, description="Force refresh of cached data")
) -> Dict[str, Any]:
    """Get aggregated catalog from all clusters."""
    try:
        catalog = await multi_cluster_catalog.get_aggregated_catalog(force_refresh=force_refresh)
        return catalog
    except Exception as e:
        logger.error(f"Failed to get multi-cluster catalog: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get catalog: {str(e)}"
        )


@router.get("/health")
async def get_multi_cluster_health() -> Dict[str, Any]:
    """Get health status of all clusters."""
    try:
        # Get all cluster status
        all_status = await multi_cluster_manager.get_all_cluster_status()
        
        # Get manager stats
        manager_stats = await multi_cluster_manager.get_manager_stats()
        
        # Calculate health summary
        total_clusters = len(all_status)
        running_clusters = sum(1 for status in all_status.values() if status.value == "running")
        error_clusters = sum(1 for status in all_status.values() if status.value == "error")
        
        overall_health = "healthy"
        if error_clusters > 0:
            overall_health = "degraded" if running_clusters > 0 else "unhealthy"
        elif total_clusters == 0:
            overall_health = "no_clusters"
        
        return {
            "overall_health": overall_health,
            "total_clusters": total_clusters,
            "running_clusters": running_clusters,
            "error_clusters": error_clusters,
            "cluster_status": {cid: status.value for cid, status in all_status.items()},
            "manager_stats": manager_stats,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get multi-cluster health: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get health status: {str(e)}"
        )


@router.get("/stats")
async def get_multi_cluster_stats() -> Dict[str, Any]:
    """Get multi-cluster manager statistics."""
    try:
        stats = await multi_cluster_manager.get_manager_stats()
        return stats
    except Exception as e:
        logger.error(f"Failed to get multi-cluster stats: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get statistics: {str(e)}"
        )