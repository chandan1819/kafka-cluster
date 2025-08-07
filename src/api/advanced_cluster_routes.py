"""
API routes for advanced cluster features.

This module provides REST API endpoints for cluster cloning, snapshots,
scheduling, and enhanced tagging capabilities.
"""

from fastapi import APIRouter, HTTPException, Depends, Query, Body
from fastapi.responses import JSONResponse
from typing import Dict, List, Optional, Set, Any
from datetime import datetime
from pydantic import BaseModel, Field

from src.services.advanced_cluster_features import (
    AdvancedClusterFeatures,
    SnapshotType,
    ScheduleType,
    ScheduleFrequency
)
from src.exceptions import ClusterError, ValidationError


# Request/Response Models
class CloneClusterRequest(BaseModel):
    """Request model for cluster cloning."""
    source_cluster_id: str = Field(..., description="ID of the source cluster to clone")
    target_cluster_id: str = Field(..., description="ID for the new cloned cluster")
    target_name: Optional[str] = Field(None, description="Name for the cloned cluster")
    clone_data: bool = Field(True, description="Whether to clone cluster data")
    clone_config: bool = Field(True, description="Whether to clone cluster configuration")
    port_offset: int = Field(100, description="Port offset for the cloned cluster")
    tags: Optional[Dict[str, str]] = Field(None, description="Additional tags for the cloned cluster")


class CreateSnapshotRequest(BaseModel):
    """Request model for creating snapshots."""
    cluster_id: str = Field(..., description="ID of the cluster to snapshot")
    name: str = Field(..., description="Name for the snapshot")
    description: Optional[str] = Field(None, description="Description of the snapshot")
    snapshot_type: SnapshotType = Field(SnapshotType.FULL, description="Type of snapshot to create")
    tags: Optional[Set[str]] = Field(None, description="Tags for the snapshot")


class RestoreSnapshotRequest(BaseModel):
    """Request model for restoring snapshots."""
    snapshot_id: str = Field(..., description="ID of the snapshot to restore")
    target_cluster_id: Optional[str] = Field(None, description="Target cluster ID (defaults to original)")
    restore_config: bool = Field(True, description="Whether to restore configuration")
    restore_data: bool = Field(True, description="Whether to restore data")


class CreateScheduleRequest(BaseModel):
    """Request model for creating schedules."""
    cluster_id: str = Field(..., description="ID of the cluster to schedule")
    name: str = Field(..., description="Name for the schedule")
    description: Optional[str] = Field(None, description="Description of the schedule")
    schedule_type: ScheduleType = Field(..., description="Type of scheduled action")
    frequency: ScheduleFrequency = Field(..., description="Frequency of the schedule")
    schedule_expression: str = Field(..., description="Schedule expression (time, cron, etc.)")
    enabled: bool = Field(True, description="Whether the schedule is enabled")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    tags: Optional[Set[str]] = Field(None, description="Tags for the schedule")


class AddTagsRequest(BaseModel):
    """Request model for adding cluster tags."""
    cluster_id: str = Field(..., description="ID of the cluster")
    tags: Dict[str, str] = Field(..., description="Tags to add to the cluster")
    descriptions: Optional[Dict[str, str]] = Field(None, description="Descriptions for the tags")


class SearchClustersRequest(BaseModel):
    """Request model for searching clusters by tags."""
    tag_filters: Dict[str, str] = Field(..., description="Tag filters for search")
    match_all: bool = Field(True, description="Whether all filters must match (AND) or any (OR)")


class SnapshotResponse(BaseModel):
    """Response model for snapshots."""
    id: str
    cluster_id: str
    name: str
    description: Optional[str]
    snapshot_type: str
    created_at: str
    size_bytes: int
    file_path: str
    metadata: Dict[str, Any]
    tags: List[str]


class ScheduleResponse(BaseModel):
    """Response model for schedules."""
    id: str
    cluster_id: str
    name: str
    description: Optional[str]
    schedule_type: str
    frequency: str
    schedule_expression: str
    enabled: bool
    created_at: str
    last_executed: Optional[str]
    next_execution: Optional[str]
    execution_count: int
    metadata: Dict[str, Any]
    tags: List[str]


class TagResponse(BaseModel):
    """Response model for tags."""
    key: str
    value: str
    description: Optional[str]
    created_at: str
    metadata: Dict[str, Any]


# Create router
router = APIRouter(prefix="/api/v1/advanced", tags=["Advanced Cluster Features"])


# Dependency to get advanced features service
async def get_advanced_features() -> AdvancedClusterFeatures:
    """Get advanced cluster features service."""
    # This would be injected in a real application
    # For now, we'll assume it's available globally
    from src.main import app
    return app.state.advanced_features


@router.post("/clusters/clone", response_model=Dict[str, Any])
async def clone_cluster(
    request: CloneClusterRequest,
    advanced_features: AdvancedClusterFeatures = Depends(get_advanced_features)
):
    """Clone an existing cluster to create a new one."""
    try:
        cloned_cluster = await advanced_features.clone_cluster(
            source_cluster_id=request.source_cluster_id,
            target_cluster_id=request.target_cluster_id,
            target_name=request.target_name,
            clone_data=request.clone_data,
            clone_config=request.clone_config,
            port_offset=request.port_offset,
            tags=request.tags
        )
        
        return {
            "success": True,
            "message": f"Successfully cloned cluster {request.source_cluster_id} to {request.target_cluster_id}",
            "cluster": {
                "id": cloned_cluster.id,
                "name": cloned_cluster.name,
                "description": cloned_cluster.description,
                "environment": cloned_cluster.environment,
                "port_allocation": {
                    "kafka_port": cloned_cluster.port_allocation.kafka_port,
                    "rest_proxy_port": cloned_cluster.port_allocation.rest_proxy_port,
                    "ui_port": cloned_cluster.port_allocation.ui_port,
                    "jmx_port": cloned_cluster.port_allocation.jmx_port
                },
                "tags": cloned_cluster.tags
            }
        }
        
    except ClusterError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.post("/snapshots", response_model=SnapshotResponse)
async def create_snapshot(
    request: CreateSnapshotRequest,
    advanced_features: AdvancedClusterFeatures = Depends(get_advanced_features)
):
    """Create a snapshot of a cluster."""
    try:
        snapshot = await advanced_features.create_snapshot(
            cluster_id=request.cluster_id,
            name=request.name,
            description=request.description,
            snapshot_type=request.snapshot_type,
            tags=request.tags
        )
        
        return SnapshotResponse(
            id=snapshot.id,
            cluster_id=snapshot.cluster_id,
            name=snapshot.name,
            description=snapshot.description,
            snapshot_type=snapshot.snapshot_type.value,
            created_at=snapshot.created_at.isoformat(),
            size_bytes=snapshot.size_bytes,
            file_path=str(snapshot.file_path),
            metadata=snapshot.metadata,
            tags=list(snapshot.tags)
        )
        
    except ClusterError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/snapshots", response_model=List[SnapshotResponse])
async def list_snapshots(
    cluster_id: Optional[str] = Query(None, description="Filter by cluster ID"),
    advanced_features: AdvancedClusterFeatures = Depends(get_advanced_features)
):
    """List snapshots, optionally filtered by cluster."""
    try:
        snapshots = advanced_features.get_snapshots(cluster_id)
        
        return [
            SnapshotResponse(
                id=snapshot.id,
                cluster_id=snapshot.cluster_id,
                name=snapshot.name,
                description=snapshot.description,
                snapshot_type=snapshot.snapshot_type.value,
                created_at=snapshot.created_at.isoformat(),
                size_bytes=snapshot.size_bytes,
                file_path=str(snapshot.file_path),
                metadata=snapshot.metadata,
                tags=list(snapshot.tags)
            )
            for snapshot in snapshots
        ]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.post("/snapshots/restore", response_model=Dict[str, Any])
async def restore_snapshot(
    request: RestoreSnapshotRequest,
    advanced_features: AdvancedClusterFeatures = Depends(get_advanced_features)
):
    """Restore a cluster from a snapshot."""
    try:
        success = await advanced_features.restore_snapshot(
            snapshot_id=request.snapshot_id,
            target_cluster_id=request.target_cluster_id,
            restore_config=request.restore_config,
            restore_data=request.restore_data
        )
        
        return {
            "success": success,
            "message": f"Successfully restored snapshot {request.snapshot_id}"
        }
        
    except ClusterError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.delete("/snapshots/{snapshot_id}", response_model=Dict[str, Any])
async def delete_snapshot(
    snapshot_id: str,
    advanced_features: AdvancedClusterFeatures = Depends(get_advanced_features)
):
    """Delete a snapshot."""
    try:
        success = await advanced_features.delete_snapshot(snapshot_id)
        
        if not success:
            raise HTTPException(status_code=404, detail=f"Snapshot {snapshot_id} not found")
        
        return {
            "success": True,
            "message": f"Successfully deleted snapshot {snapshot_id}"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.post("/schedules", response_model=ScheduleResponse)
async def create_schedule(
    request: CreateScheduleRequest,
    advanced_features: AdvancedClusterFeatures = Depends(get_advanced_features)
):
    """Create a cluster schedule."""
    try:
        schedule = await advanced_features.create_schedule(
            cluster_id=request.cluster_id,
            name=request.name,
            schedule_type=request.schedule_type,
            frequency=request.frequency,
            schedule_expression=request.schedule_expression,
            description=request.description,
            enabled=request.enabled,
            metadata=request.metadata,
            tags=request.tags
        )
        
        return ScheduleResponse(
            id=schedule.id,
            cluster_id=schedule.cluster_id,
            name=schedule.name,
            description=schedule.description,
            schedule_type=schedule.schedule_type.value,
            frequency=schedule.frequency.value,
            schedule_expression=schedule.schedule_expression,
            enabled=schedule.enabled,
            created_at=schedule.created_at.isoformat(),
            last_executed=schedule.last_executed.isoformat() if schedule.last_executed else None,
            next_execution=schedule.next_execution.isoformat() if schedule.next_execution else None,
            execution_count=schedule.execution_count,
            metadata=schedule.metadata,
            tags=list(schedule.tags)
        )
        
    except ClusterError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/schedules", response_model=List[ScheduleResponse])
async def list_schedules(
    cluster_id: Optional[str] = Query(None, description="Filter by cluster ID"),
    advanced_features: AdvancedClusterFeatures = Depends(get_advanced_features)
):
    """List schedules, optionally filtered by cluster."""
    try:
        schedules = advanced_features.get_schedules(cluster_id)
        
        return [
            ScheduleResponse(
                id=schedule.id,
                cluster_id=schedule.cluster_id,
                name=schedule.name,
                description=schedule.description,
                schedule_type=schedule.schedule_type.value,
                frequency=schedule.frequency.value,
                schedule_expression=schedule.schedule_expression,
                enabled=schedule.enabled,
                created_at=schedule.created_at.isoformat(),
                last_executed=schedule.last_executed.isoformat() if schedule.last_executed else None,
                next_execution=schedule.next_execution.isoformat() if schedule.next_execution else None,
                execution_count=schedule.execution_count,
                metadata=schedule.metadata,
                tags=list(schedule.tags)
            )
            for schedule in schedules
        ]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.delete("/schedules/{schedule_id}", response_model=Dict[str, Any])
async def delete_schedule(
    schedule_id: str,
    advanced_features: AdvancedClusterFeatures = Depends(get_advanced_features)
):
    """Delete a schedule."""
    try:
        success = await advanced_features.delete_schedule(schedule_id)
        
        if not success:
            raise HTTPException(status_code=404, detail=f"Schedule {schedule_id} not found")
        
        return {
            "success": True,
            "message": f"Successfully deleted schedule {schedule_id}"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.post("/clusters/tags", response_model=Dict[str, TagResponse])
async def add_cluster_tags(
    request: AddTagsRequest,
    advanced_features: AdvancedClusterFeatures = Depends(get_advanced_features)
):
    """Add tags to a cluster."""
    try:
        added_tags = await advanced_features.add_cluster_tags(
            cluster_id=request.cluster_id,
            tags=request.tags,
            descriptions=request.descriptions
        )
        
        return {
            tag_key: TagResponse(
                key=tag.key,
                value=tag.value,
                description=tag.description,
                created_at=tag.created_at.isoformat(),
                metadata=tag.metadata
            )
            for tag_key, tag in added_tags.items()
        }
        
    except ClusterError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/clusters/{cluster_id}/tags", response_model=Dict[str, TagResponse])
async def get_cluster_tags(
    cluster_id: str,
    advanced_features: AdvancedClusterFeatures = Depends(get_advanced_features)
):
    """Get tags for a specific cluster."""
    try:
        cluster_tags = advanced_features.get_cluster_tags(cluster_id)
        
        return {
            tag_key: TagResponse(
                key=tag.key,
                value=tag.value,
                description=tag.description,
                created_at=tag.created_at.isoformat(),
                metadata=tag.metadata
            )
            for tag_key, tag in cluster_tags.items()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.post("/clusters/search", response_model=List[Dict[str, Any]])
async def search_clusters_by_tags(
    request: SearchClustersRequest,
    advanced_features: AdvancedClusterFeatures = Depends(get_advanced_features)
):
    """Search clusters by tags."""
    try:
        matching_clusters = await advanced_features.search_clusters_by_tags(
            tag_filters=request.tag_filters,
            match_all=request.match_all
        )
        
        return [
            {
                "id": cluster.id,
                "name": cluster.name,
                "description": cluster.description,
                "environment": cluster.environment,
                "template_id": cluster.template_id,
                "port_allocation": {
                    "kafka_port": cluster.port_allocation.kafka_port,
                    "rest_proxy_port": cluster.port_allocation.rest_proxy_port,
                    "ui_port": cluster.port_allocation.ui_port,
                    "jmx_port": cluster.port_allocation.jmx_port
                },
                "tags": cluster.tags,
                "created_at": cluster.created_at.isoformat() if cluster.created_at else None
            }
            for cluster in matching_clusters
        ]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.post("/scheduler/start", response_model=Dict[str, Any])
async def start_scheduler(
    advanced_features: AdvancedClusterFeatures = Depends(get_advanced_features)
):
    """Start the cluster scheduler."""
    try:
        await advanced_features.start_scheduler()
        
        return {
            "success": True,
            "message": "Scheduler started successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.post("/scheduler/stop", response_model=Dict[str, Any])
async def stop_scheduler(
    advanced_features: AdvancedClusterFeatures = Depends(get_advanced_features)
):
    """Stop the cluster scheduler."""
    try:
        await advanced_features.stop_scheduler()
        
        return {
            "success": True,
            "message": "Scheduler stopped successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/scheduler/status", response_model=Dict[str, Any])
async def get_scheduler_status(
    advanced_features: AdvancedClusterFeatures = Depends(get_advanced_features)
):
    """Get scheduler status."""
    try:
        return {
            "active": advanced_features._scheduler_active,
            "schedules_count": len(advanced_features._schedules),
            "enabled_schedules": len([s for s in advanced_features._schedules.values() if s.enabled])
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")