"""
Web interface API routes for multi-cluster support.

This module provides API endpoints specifically designed for web interface consumption,
including cluster dashboards, aggregated views, and management interfaces.
"""

import logging
from datetime import datetime
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Query, Path, Body, status
from fastapi.responses import JSONResponse

from ..models.base import ErrorResponse
from ..models.multi_cluster import (
    ClusterSummary,
    ClusterDefinition,
    ClusterTemplate,
    CreateClusterRequest,
    CrossClusterOperation
)
from ..services.multi_cluster_manager import MultiClusterManager
from ..services.multi_cluster_service_catalog import MultiClusterServiceCatalog
from ..services.template_manager import TemplateManager
from ..services.cross_cluster_operations import CrossClusterOperations
from ..exceptions import (
    ClusterNotFoundError,
    TemplateNotFoundError,
    ValidationError
)

logger = logging.getLogger(__name__)

# Create web interface API router
router = APIRouter(prefix="/web", tags=["Web Interface"])

# Global instances (will be initialized in main.py)
multi_cluster_manager: Optional[MultiClusterManager] = None
multi_cluster_catalog: Optional[MultiClusterServiceCatalog] = None
template_manager: Optional[TemplateManager] = None
cross_cluster_ops: Optional[CrossClusterOperations] = None


def init_web_interface_services(
    manager: MultiClusterManager,
    catalog: MultiClusterServiceCatalog,
    templates: TemplateManager,
    operations: CrossClusterOperations
):
    """Initialize web interface service instances."""
    global multi_cluster_manager, multi_cluster_catalog, template_manager, cross_cluster_ops
    multi_cluster_manager = manager
    multi_cluster_catalog = catalog
    template_manager = templates
    cross_cluster_ops = operations


# Dashboard and Overview Endpoints

@router.get("/dashboard", response_model=Dict[str, Any])
async def get_dashboard_data() -> Dict[str, Any]:
    """
    Get comprehensive dashboard data for the web interface.
    
    Returns aggregated information about all clusters, their status,
    key metrics, and recent activities.
    """
    try:
        # Get all clusters
        clusters = await multi_cluster_manager.list_clusters()
        
        # Get cluster status summary
        status_counts = {}
        total_topics = 0
        total_messages = 0
        cluster_details = []
        
        for cluster in clusters:
            # Count status
            status = cluster.status.value if hasattr(cluster.status, 'value') else str(cluster.status)
            status_counts[status] = status_counts.get(status, 0) + 1
            
            # Get cluster details
            try:
                # Get topics count for this cluster
                topics = await multi_cluster_catalog.get_cluster_topics(cluster.id)
                topic_count = len(topics)
                total_topics += topic_count
                
                # Get cluster health
                health = await multi_cluster_manager.get_cluster_health(cluster.id)
                
                cluster_details.append({
                    "id": cluster.id,
                    "name": cluster.name,
                    "environment": cluster.environment,
                    "status": status,
                    "topic_count": topic_count,
                    "endpoints": cluster.endpoints,
                    "health": health.get("overall_health", "unknown"),
                    "created_at": cluster.created_at.isoformat(),
                    "last_started": cluster.last_started.isoformat() if cluster.last_started else None,
                    "tags": cluster.tags
                })
                
            except Exception as e:
                logger.warning(f"Failed to get details for cluster {cluster.id}: {e}")
                cluster_details.append({
                    "id": cluster.id,
                    "name": cluster.name,
                    "environment": cluster.environment,
                    "status": "error",
                    "topic_count": 0,
                    "endpoints": {},
                    "health": "error",
                    "created_at": cluster.created_at.isoformat(),
                    "last_started": cluster.last_started.isoformat() if cluster.last_started else None,
                    "tags": cluster.tags,
                    "error": str(e)
                })
        
        # Get recent operations
        try:
            recent_operations = await cross_cluster_ops.list_operations()
            recent_operations = recent_operations[:10]  # Last 10 operations
        except Exception as e:
            logger.warning(f"Failed to get recent operations: {e}")
            recent_operations = []
        
        # Get available templates
        try:
            templates = await template_manager.list_templates()
            template_count = len(templates)
        except Exception as e:
            logger.warning(f"Failed to get templates: {e}")
            template_count = 0
        
        return {
            "summary": {
                "total_clusters": len(clusters),
                "total_topics": total_topics,
                "total_templates": template_count,
                "status_distribution": status_counts
            },
            "clusters": cluster_details,
            "recent_operations": [
                {
                    "id": op.id,
                    "type": op.operation_type,
                    "status": op.status.value if hasattr(op.status, 'value') else str(op.status),
                    "progress": op.progress_percent,
                    "started_at": op.started_at.isoformat(),
                    "completed_at": op.completed_at.isoformat() if op.completed_at else None,
                    "source_cluster": op.source_cluster_id,
                    "target_clusters": op.target_cluster_ids
                }
                for op in recent_operations
            ],
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get dashboard data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get dashboard data: {str(e)}"
        )


@router.get("/clusters/overview", response_model=List[Dict[str, Any]])
async def get_clusters_overview() -> List[Dict[str, Any]]:
    """
    Get overview of all clusters with key metrics for the cluster selector.
    
    Returns simplified cluster information optimized for UI display.
    """
    try:
        clusters = await multi_cluster_manager.list_clusters()
        overview = []
        
        for cluster in clusters:
            try:
                # Get basic cluster info
                status = cluster.status.value if hasattr(cluster.status, 'value') else str(cluster.status)
                
                # Get topic count
                topics = await multi_cluster_catalog.get_cluster_topics(cluster.id)
                topic_count = len(topics)
                
                overview.append({
                    "id": cluster.id,
                    "name": cluster.name,
                    "environment": cluster.environment,
                    "status": status,
                    "topic_count": topic_count,
                    "endpoints": cluster.endpoints,
                    "tags": cluster.tags,
                    "created_at": cluster.created_at.isoformat()
                })
                
            except Exception as e:
                logger.warning(f"Failed to get overview for cluster {cluster.id}: {e}")
                overview.append({
                    "id": cluster.id,
                    "name": cluster.name,
                    "environment": cluster.environment,
                    "status": "error",
                    "topic_count": 0,
                    "endpoints": {},
                    "tags": cluster.tags,
                    "created_at": cluster.created_at.isoformat(),
                    "error": str(e)
                })
        
        return overview
        
    except Exception as e:
        logger.error(f"Failed to get clusters overview: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get clusters overview: {str(e)}"
        )


# Cluster Management Endpoints for Web Interface

@router.get("/clusters/{cluster_id}/details", response_model=Dict[str, Any])
async def get_cluster_details(
    cluster_id: str = Path(..., description="Cluster ID")
) -> Dict[str, Any]:
    """
    Get detailed information about a specific cluster for the web interface.
    
    Returns comprehensive cluster information including topics, health,
    configuration, and recent activities.
    """
    try:
        # Get cluster definition
        cluster = await multi_cluster_manager.registry.get_cluster(cluster_id)
        
        # Get cluster health
        health = await multi_cluster_manager.get_cluster_health(cluster_id)
        
        # Get topics
        topics = await multi_cluster_catalog.get_cluster_topics(cluster_id)
        
        # Get cluster status
        current_status = await multi_cluster_manager.get_cluster_status(cluster_id)
        
        return {
            "cluster": {
                "id": cluster.id,
                "name": cluster.name,
                "description": cluster.description,
                "environment": cluster.environment,
                "template_id": cluster.template_id,
                "status": current_status.value if hasattr(current_status, 'value') else str(current_status),
                "created_at": cluster.created_at.isoformat(),
                "updated_at": cluster.updated_at.isoformat(),
                "last_started": cluster.last_started.isoformat() if cluster.last_started else None,
                "last_stopped": cluster.last_stopped.isoformat() if cluster.last_stopped else None,
                "tags": cluster.tags,
                "endpoints": cluster.endpoints
            },
            "configuration": {
                "kafka_config": cluster.kafka_config.__dict__ if cluster.kafka_config else {},
                "rest_proxy_config": cluster.rest_proxy_config.__dict__ if cluster.rest_proxy_config else {},
                "ui_config": cluster.ui_config.__dict__ if cluster.ui_config else {},
                "port_allocation": {
                    "kafka_port": cluster.port_allocation.kafka_port,
                    "rest_proxy_port": cluster.port_allocation.rest_proxy_port,
                    "ui_port": cluster.port_allocation.ui_port,
                    "jmx_port": cluster.port_allocation.jmx_port
                } if cluster.port_allocation else {}
            },
            "health": health,
            "topics": {
                "count": len(topics),
                "list": topics[:20]  # First 20 topics for preview
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except ClusterNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cluster not found: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Failed to get cluster details for {cluster_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get cluster details: {str(e)}"
        )


@router.post("/clusters/{cluster_id}/actions/{action}")
async def perform_cluster_action(
    cluster_id: str = Path(..., description="Cluster ID"),
    action: str = Path(..., description="Action to perform (start, stop, restart)"),
    force: bool = Query(False, description="Force the action"),
    timeout: int = Query(60, description="Action timeout in seconds")
) -> Dict[str, Any]:
    """
    Perform cluster lifecycle actions from the web interface.
    
    Supports start, stop, and restart actions with optional force and timeout parameters.
    """
    try:
        if action not in ["start", "stop", "restart"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid action: {action}. Must be one of: start, stop, restart"
            )
        
        result = {"cluster_id": cluster_id, "action": action}
        
        if action == "start":
            final_status = await multi_cluster_manager.start_cluster(
                cluster_id, force=force, timeout=timeout
            )
            result["status"] = final_status.value if hasattr(final_status, 'value') else str(final_status)
            result["message"] = f"Cluster {cluster_id} started successfully"
            
        elif action == "stop":
            success = await multi_cluster_manager.stop_cluster(
                cluster_id, force=force, timeout=timeout
            )
            result["success"] = success
            result["message"] = f"Cluster {cluster_id} stopped successfully"
            
        elif action == "restart":
            # Stop then start
            await multi_cluster_manager.stop_cluster(cluster_id, force=force, timeout=timeout)
            final_status = await multi_cluster_manager.start_cluster(
                cluster_id, force=True, timeout=timeout
            )
            result["status"] = final_status.value if hasattr(final_status, 'value') else str(final_status)
            result["message"] = f"Cluster {cluster_id} restarted successfully"
        
        result["timestamp"] = datetime.utcnow().isoformat()
        return result
        
    except ClusterNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cluster not found: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Failed to perform action {action} on cluster {cluster_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to perform action: {str(e)}"
        )


# Template Management for Web Interface

@router.get("/templates/for-creation", response_model=List[Dict[str, Any]])
async def get_templates_for_creation() -> List[Dict[str, Any]]:
    """
    Get templates formatted for cluster creation forms in the web interface.
    
    Returns template information optimized for UI display and form population.
    """
    try:
        templates = await template_manager.list_templates()
        
        formatted_templates = []
        for template in templates:
            formatted_templates.append({
                "id": template.id,
                "name": template.name,
                "description": template.description,
                "category": template.category,
                "is_builtin": template.is_builtin,
                "tags": template.tags,
                "resource_requirements": {
                    "min_memory_mb": template.min_memory_mb,
                    "min_disk_gb": template.min_disk_gb,
                    "recommended_memory_mb": template.recommended_memory_mb,
                    "recommended_disk_gb": template.recommended_disk_gb
                },
                "default_config": {
                    "kafka": template.default_kafka_config.__dict__ if template.default_kafka_config else {},
                    "rest_proxy": template.default_rest_proxy_config.__dict__ if template.default_rest_proxy_config else {},
                    "ui": template.default_ui_config.__dict__ if template.default_ui_config else {}
                }
            })
        
        return formatted_templates
        
    except Exception as e:
        logger.error(f"Failed to get templates for creation: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get templates: {str(e)}"
        )


@router.post("/clusters/create-from-template", response_model=Dict[str, Any])
async def create_cluster_from_template(
    request: CreateClusterRequest = Body(...)
) -> Dict[str, Any]:
    """
    Create a new cluster from a template via the web interface.
    
    Provides enhanced response format suitable for web interface feedback.
    """
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
        
        # Get cluster status
        current_status = await multi_cluster_manager.get_cluster_status(cluster_id)
        
        return {
            "success": True,
            "cluster": {
                "id": created_cluster.id,
                "name": created_cluster.name,
                "environment": created_cluster.environment,
                "status": current_status.value if hasattr(current_status, 'value') else str(current_status),
                "endpoints": created_cluster.endpoints,
                "created_at": created_cluster.created_at.isoformat()
            },
            "message": f"Cluster '{request.name}' created successfully",
            "next_steps": [
                "Cluster is being initialized",
                "Check the dashboard for status updates",
                f"Access Kafka UI at {created_cluster.endpoints.get('ui', 'N/A')}" if created_cluster.endpoints else "Endpoints will be available once cluster is running"
            ],
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid cluster configuration: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Failed to create cluster from template: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create cluster: {str(e)}"
        )


# Cross-Cluster Operations for Web Interface

@router.get("/operations/dashboard", response_model=Dict[str, Any])
async def get_operations_dashboard() -> Dict[str, Any]:
    """
    Get cross-cluster operations dashboard data for the web interface.
    
    Returns information about running, completed, and failed operations.
    """
    try:
        operations = await cross_cluster_ops.list_operations()
        
        # Categorize operations
        running_ops = [op for op in operations if op.status.value == "running"]
        completed_ops = [op for op in operations if op.status.value == "completed"]
        failed_ops = [op for op in operations if op.status.value == "failed"]
        
        # Format operations for UI
        def format_operation(op):
            return {
                "id": op.id,
                "type": op.operation_type,
                "status": op.status.value if hasattr(op.status, 'value') else str(op.status),
                "progress": op.progress_percent,
                "source_cluster": op.source_cluster_id,
                "target_clusters": op.target_cluster_ids,
                "started_at": op.started_at.isoformat(),
                "completed_at": op.completed_at.isoformat() if op.completed_at else None,
                "error_message": op.error_message,
                "topics": op.topics
            }
        
        return {
            "summary": {
                "total_operations": len(operations),
                "running": len(running_ops),
                "completed": len(completed_ops),
                "failed": len(failed_ops)
            },
            "running_operations": [format_operation(op) for op in running_ops],
            "recent_completed": [format_operation(op) for op in completed_ops[:10]],
            "recent_failed": [format_operation(op) for op in failed_ops[:10]],
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get operations dashboard: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get operations dashboard: {str(e)}"
        )


# Health and Status Endpoints for Web Interface

@router.get("/system/health", response_model=Dict[str, Any])
async def get_system_health() -> Dict[str, Any]:
    """
    Get comprehensive system health information for the web interface.
    
    Returns health status of all clusters and system components.
    """
    try:
        clusters = await multi_cluster_manager.list_clusters()
        cluster_health = {}
        
        overall_status = "healthy"
        unhealthy_count = 0
        
        for cluster in clusters:
            try:
                health = await multi_cluster_manager.get_cluster_health(cluster.id)
                cluster_health[cluster.id] = {
                    "name": cluster.name,
                    "status": health.get("overall_health", "unknown"),
                    "details": health
                }
                
                if health.get("overall_health") in ["unhealthy", "error"]:
                    unhealthy_count += 1
                    
            except Exception as e:
                cluster_health[cluster.id] = {
                    "name": cluster.name,
                    "status": "error",
                    "error": str(e)
                }
                unhealthy_count += 1
        
        # Determine overall system status
        if unhealthy_count == 0:
            overall_status = "healthy"
        elif unhealthy_count < len(clusters):
            overall_status = "degraded"
        else:
            overall_status = "unhealthy"
        
        return {
            "overall_status": overall_status,
            "total_clusters": len(clusters),
            "healthy_clusters": len(clusters) - unhealthy_count,
            "unhealthy_clusters": unhealthy_count,
            "cluster_health": cluster_health,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get system health: {e}")
        return {
            "overall_status": "error",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }


# Configuration and Settings Endpoints

@router.get("/settings/cluster-environments", response_model=List[str])
async def get_cluster_environments() -> List[str]:
    """
    Get list of available cluster environments for the web interface.
    
    Returns predefined environment options for cluster creation forms.
    """
    return ["development", "testing", "staging", "production"]


@router.get("/settings/port-ranges", response_model=Dict[str, Any])
async def get_port_ranges() -> Dict[str, Any]:
    """
    Get information about port allocation ranges for the web interface.
    
    Returns port range information and current allocations.
    """
    try:
        # This would typically come from configuration
        # For now, return default ranges
        return {
            "kafka_port_range": {"start": 9092, "end": 9199},
            "rest_proxy_port_range": {"start": 8082, "end": 8199},
            "ui_port_range": {"start": 8080, "end": 8179},
            "jmx_port_range": {"start": 9999, "end": 10099},
            "allocated_ports": [],  # Would get from port allocator
            "available_ports": 100,  # Would calculate from allocator
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get port ranges: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get port ranges: {str(e)}"
        )