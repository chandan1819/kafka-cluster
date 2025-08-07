"""
Configuration management API routes.

This module provides API endpoints for cluster configuration import/export,
validation, versioning, and rollback functionality.
"""

import logging
from datetime import datetime
from typing import List, Optional, Dict, Any
from pathlib import Path
from fastapi import APIRouter, HTTPException, Query, Path as PathParam, Body, UploadFile, File, status
from fastapi.responses import JSONResponse, FileResponse

from ..models.base import ErrorResponse
from ..models.multi_cluster import ClusterDefinition
from ..services.configuration_manager import ConfigurationManager
from ..storage.file_backend import FileStorageBackend
from ..exceptions import ConfigurationError

logger = logging.getLogger(__name__)

# Create configuration API router
router = APIRouter(prefix="/config", tags=["Configuration Management"])

# Global configuration manager instance
config_manager: Optional[ConfigurationManager] = None


def init_configuration_services(storage_backend):
    """Initialize configuration management services."""
    global config_manager
    config_manager = ConfigurationManager(storage_backend)


# Configuration Export Endpoints

@router.get("/clusters/{cluster_id}/export", response_model=Dict[str, Any])
async def export_cluster_config(
    cluster_id: str = PathParam(..., description="Cluster ID to export"),
    format: str = Query("yaml", description="Export format (yaml or json)"),
    include_metadata: bool = Query(True, description="Include metadata in export")
) -> Dict[str, Any]:
    """
    Export cluster configuration to YAML or JSON format.
    
    Returns the cluster configuration as a string that can be saved to a file
    or used for importing into another system.
    """
    try:
        config_content = await config_manager.export_cluster_config(
            cluster_id, format, include_metadata
        )
        
        return {
            "cluster_id": cluster_id,
            "format": format,
            "content": config_content,
            "exported_at": datetime.utcnow().isoformat(),
            "include_metadata": include_metadata
        }
        
    except ConfigurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Failed to export cluster config for {cluster_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Export failed: {str(e)}"
        )


@router.get("/clusters/export-multiple", response_model=Dict[str, Any])
async def export_multiple_clusters(
    cluster_ids: str = Query(..., description="Comma-separated cluster IDs"),
    format: str = Query("yaml", description="Export format (yaml or json)")
) -> Dict[str, Any]:
    """
    Export multiple cluster configurations to a single file.
    
    Useful for backing up multiple clusters or migrating entire environments.
    """
    try:
        cluster_id_list = [cid.strip() for cid in cluster_ids.split(",")]
        
        config_content = await config_manager.export_multiple_clusters(
            cluster_id_list, format
        )
        
        return {
            "cluster_ids": cluster_id_list,
            "cluster_count": len(cluster_id_list),
            "format": format,
            "content": config_content,
            "exported_at": datetime.utcnow().isoformat()
        }
        
    except ConfigurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Failed to export multiple clusters: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Multi-cluster export failed: {str(e)}"
        )


# Configuration Import Endpoints

@router.post("/clusters/import", response_model=Dict[str, Any])
async def import_cluster_config(
    config_content: str = Body(..., description="Configuration content (YAML or JSON)"),
    format: str = Body("auto", description="Format (yaml, json, or auto)"),
    validate_schema: bool = Body(True, description="Validate configuration schema"),
    create_version: bool = Body(True, description="Create version backup")
) -> Dict[str, Any]:
    """
    Import cluster configuration from YAML or JSON content.
    
    Creates a new cluster or updates an existing one based on the configuration.
    Optionally validates the schema and creates a version backup.
    """
    try:
        cluster = await config_manager.import_cluster_config(
            config_content, format, validate_schema, create_version
        )
        
        return {
            "success": True,
            "cluster_id": cluster.id,
            "cluster_name": cluster.name,
            "environment": cluster.environment,
            "imported_at": datetime.utcnow().isoformat(),
            "schema_validated": validate_schema,
            "version_created": create_version,
            "message": f"Successfully imported cluster '{cluster.name}'"
        }
        
    except ConfigurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Failed to import cluster config: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Import failed: {str(e)}"
        )


@router.post("/clusters/import-multiple", response_model=Dict[str, Any])
async def import_multiple_clusters(
    config_content: str = Body(..., description="Multi-cluster configuration content"),
    format: str = Body("auto", description="Format (yaml, json, or auto)"),
    validate_schema: bool = Body(True, description="Validate configuration schemas"),
    overwrite_existing: bool = Body(False, description="Overwrite existing clusters")
) -> Dict[str, Any]:
    """
    Import multiple cluster configurations from a single file.
    
    Useful for restoring backups or setting up entire environments.
    """
    try:
        clusters = await config_manager.import_multiple_clusters(
            config_content, format, validate_schema, overwrite_existing
        )
        
        return {
            "success": True,
            "imported_count": len(clusters),
            "cluster_ids": [c.id for c in clusters],
            "imported_at": datetime.utcnow().isoformat(),
            "schema_validated": validate_schema,
            "overwrite_existing": overwrite_existing,
            "message": f"Successfully imported {len(clusters)} clusters"
        }
        
    except ConfigurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Failed to import multiple clusters: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Multi-cluster import failed: {str(e)}"
        )


@router.post("/clusters/import-file", response_model=Dict[str, Any])
async def import_config_file(
    file: UploadFile = File(..., description="Configuration file (YAML or JSON)"),
    validate_schema: bool = Query(True, description="Validate configuration schema"),
    overwrite_existing: bool = Query(False, description="Overwrite existing clusters")
) -> Dict[str, Any]:
    """
    Import cluster configuration from uploaded file.
    
    Supports both single-cluster and multi-cluster configuration files.
    """
    try:
        # Read file content
        content = await file.read()
        config_content = content.decode('utf-8')
        
        # Determine format from file extension
        file_format = "yaml" if file.filename.lower().endswith(('.yml', '.yaml')) else "json"
        
        # Try single cluster import first
        try:
            cluster = await config_manager.import_cluster_config(
                config_content, file_format, validate_schema, True
            )
            
            return {
                "success": True,
                "type": "single_cluster",
                "cluster_id": cluster.id,
                "cluster_name": cluster.name,
                "imported_at": datetime.utcnow().isoformat(),
                "message": f"Successfully imported cluster '{cluster.name}' from file"
            }
            
        except ConfigurationError:
            # Try multi-cluster import
            clusters = await config_manager.import_multiple_clusters(
                config_content, file_format, validate_schema, overwrite_existing
            )
            
            return {
                "success": True,
                "type": "multi_cluster",
                "imported_count": len(clusters),
                "cluster_ids": [c.id for c in clusters],
                "imported_at": datetime.utcnow().isoformat(),
                "message": f"Successfully imported {len(clusters)} clusters from file"
            }
            
    except ConfigurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Failed to import config file: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"File import failed: {str(e)}"
        )


# Configuration Validation Endpoints

@router.post("/validate", response_model=Dict[str, Any])
async def validate_config_content(
    config_content: str = Body(..., description="Configuration content to validate"),
    format: str = Body("auto", description="Format (yaml, json, or auto)")
) -> Dict[str, Any]:
    """
    Validate cluster configuration content without importing it.
    
    Useful for checking configuration files before importing them.
    """
    try:
        # Parse and validate content
        config_dict = config_manager._parse_config_content(config_content, format)
        
        # Check if it's multi-cluster or single cluster
        is_multi_cluster = "clusters" in config_dict
        
        if is_multi_cluster:
            # Validate each cluster in multi-cluster config
            validation_results = []
            for cluster_id, cluster_config in config_dict.get("clusters", {}).items():
                try:
                    config_manager._validate_config_schema(cluster_config)
                    validation_results.append({
                        "cluster_id": cluster_id,
                        "valid": True
                    })
                except ConfigurationError as e:
                    validation_results.append({
                        "cluster_id": cluster_id,
                        "valid": False,
                        "error": str(e)
                    })
            
            return {
                "valid": all(r["valid"] for r in validation_results),
                "type": "multi_cluster",
                "cluster_count": len(config_dict.get("clusters", {})),
                "cluster_results": validation_results,
                "validated_at": datetime.utcnow().isoformat()
            }
        else:
            # Validate single cluster config
            try:
                config_manager._validate_config_schema(config_dict)
                return {
                    "valid": True,
                    "type": "single_cluster",
                    "cluster_id": config_dict.get("id"),
                    "validated_at": datetime.utcnow().isoformat()
                }
            except ConfigurationError as e:
                return {
                    "valid": False,
                    "type": "single_cluster",
                    "error": str(e),
                    "validated_at": datetime.utcnow().isoformat()
                }
                
    except Exception as e:
        logger.error(f"Failed to validate config content: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Validation failed: {str(e)}"
        )


# Configuration Versioning Endpoints

@router.get("/clusters/{cluster_id}/versions", response_model=List[Dict[str, Any]])
async def list_config_versions(
    cluster_id: str = PathParam(..., description="Cluster ID")
) -> List[Dict[str, Any]]:
    """
    List all configuration versions for a cluster.
    
    Returns version history with creation timestamps and version IDs.
    """
    try:
        versions = await config_manager.list_config_versions(cluster_id)
        return versions
        
    except Exception as e:
        logger.error(f"Failed to list config versions for {cluster_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list versions: {str(e)}"
        )


@router.post("/clusters/{cluster_id}/rollback", response_model=Dict[str, Any])
async def rollback_to_version(
    cluster_id: str = PathParam(..., description="Cluster ID"),
    version_id: str = Body(..., description="Version ID to rollback to")
) -> Dict[str, Any]:
    """
    Rollback cluster configuration to a specific version.
    
    Creates a backup of the current configuration before rolling back.
    """
    try:
        cluster = await config_manager.rollback_to_version(cluster_id, version_id)
        
        return {
            "success": True,
            "cluster_id": cluster.id,
            "cluster_name": cluster.name,
            "rolled_back_to": version_id,
            "rolled_back_at": datetime.utcnow().isoformat(),
            "message": f"Successfully rolled back cluster '{cluster.name}' to version {version_id}"
        }
        
    except ConfigurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Failed to rollback cluster {cluster_id} to version {version_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Rollback failed: {str(e)}"
        )


@router.delete("/clusters/{cluster_id}/versions/cleanup", response_model=Dict[str, Any])
async def cleanup_old_versions(
    cluster_id: str = PathParam(..., description="Cluster ID"),
    keep_count: int = Query(10, description="Number of versions to keep")
) -> Dict[str, Any]:
    """
    Clean up old configuration versions for a cluster.
    
    Keeps only the most recent versions and deletes older ones.
    """
    try:
        deleted_count = await config_manager.cleanup_old_versions(cluster_id, keep_count)
        
        return {
            "success": True,
            "cluster_id": cluster_id,
            "deleted_count": deleted_count,
            "kept_count": keep_count,
            "cleaned_up_at": datetime.utcnow().isoformat(),
            "message": f"Cleaned up {deleted_count} old versions for cluster '{cluster_id}'"
        }
        
    except Exception as e:
        logger.error(f"Failed to cleanup versions for {cluster_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Cleanup failed: {str(e)}"
        )


# Configuration Templates Endpoints

@router.get("/templates", response_model=Dict[str, Any])
async def list_config_templates() -> Dict[str, Any]:
    """
    List available configuration templates.
    
    Returns predefined templates that can be used as starting points for new clusters.
    """
    try:
        templates = {
            "basic": await config_manager.get_config_template("basic"),
            "development": await config_manager.get_config_template("development"),
            "production": await config_manager.get_config_template("production")
        }
        
        return {
            "templates": templates,
            "template_count": len(templates),
            "retrieved_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to list config templates: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list templates: {str(e)}"
        )


@router.get("/templates/{template_type}", response_model=Dict[str, Any])
async def get_config_template(
    template_type: str = PathParam(..., description="Template type (basic, development, production)")
) -> Dict[str, Any]:
    """
    Get a specific configuration template.
    
    Returns a template configuration that can be customized and used to create new clusters.
    """
    try:
        template = await config_manager.get_config_template(template_type)
        
        return {
            "template_type": template_type,
            "template": template,
            "retrieved_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get config template {template_type}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get template: {str(e)}"
        )


# Configuration Schema Endpoints

@router.get("/schema", response_model=Dict[str, Any])
async def get_config_schema() -> Dict[str, Any]:
    """
    Get the JSON schema for cluster configuration validation.
    
    Useful for client-side validation and documentation.
    """
    try:
        schema = config_manager._cluster_schema
        
        return {
            "schema": schema,
            "schema_version": "1.0",
            "retrieved_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get config schema: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get schema: {str(e)}"
        )