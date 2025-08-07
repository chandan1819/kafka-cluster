"""
Authentication and authorization API routes.

This module provides API endpoints for user authentication, authorization,
API key management, and security administration.
"""

import logging
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Depends, Body, Query, Path as PathParam, status
from pydantic import BaseModel

from ..security.access_control import (
    AccessControlManager, User, Role, ClusterPermission, APIKey, AuditLogEntry,
    AccessLevel, Permission
)
from ..security.auth_middleware import (
    get_current_user, get_current_active_user, require_admin,
    access_control_manager
)
from ..exceptions import ValidationError

logger = logging.getLogger(__name__)

# Create authentication API router
router = APIRouter(prefix="/auth", tags=["Authentication & Authorization"])


# Request/Response Models

class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    user_info: Dict[str, Any]


class RegisterRequest(BaseModel):
    username: str
    email: str
    password: str


class UserResponse(BaseModel):
    id: str
    username: str
    email: str
    is_active: bool
    is_admin: bool
    created_at: datetime
    last_login: Optional[datetime]


class GrantPermissionRequest(BaseModel):
    user_id: str
    cluster_id: str
    access_level: AccessLevel
    expires_at: Optional[datetime] = None


class CreateAPIKeyRequest(BaseModel):
    name: str
    cluster_permissions: Dict[str, AccessLevel]
    expires_at: Optional[datetime] = None


class APIKeyResponse(BaseModel):
    id: str
    name: str
    cluster_permissions: Dict[str, AccessLevel]
    created_at: datetime
    last_used: Optional[datetime]
    expires_at: Optional[datetime]


class AuditLogResponse(BaseModel):
    id: str
    user_id: str
    cluster_id: Optional[str]
    action: str
    resource: str
    result: str
    ip_address: Optional[str]
    user_agent: Optional[str]
    details: Dict[str, Any]
    timestamp: datetime


# Authentication Endpoints

@router.post("/login", response_model=LoginResponse)
async def login(request: LoginRequest) -> LoginResponse:
    """
    Authenticate user and return JWT token.
    
    Provides JWT token for API access with user information.
    """
    try:
        token = await access_control_manager.authenticate_user(
            request.username, request.password
        )
        
        if not token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid username or password",
                headers={"WWW-Authenticate": "Bearer"}
            )
        
        # Get user info
        user = None
        for u in access_control_manager.users.values():
            if u.username == request.username:
                user = u
                break
        
        return LoginResponse(
            access_token=token,
            expires_in=access_control_manager.token_expiry_hours * 3600,
            user_info={
                "id": user.id,
                "username": user.username,
                "email": user.email,
                "is_admin": user.is_admin
            }
        )
        
    except Exception as e:
        logger.error(f"Login failed for {request.username}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Authentication service error"
        )


@router.post("/register", response_model=UserResponse)
async def register(
    request: RegisterRequest,
    current_user: Dict[str, Any] = Depends(require_admin)
) -> UserResponse:
    """
    Register a new user (admin only).
    
    Creates a new user account with specified credentials.
    """
    try:
        user = await access_control_manager.create_user(
            username=request.username,
            email=request.email,
            password=request.password,
            is_admin=False
        )
        
        return UserResponse(
            id=user.id,
            username=user.username,
            email=user.email,
            is_active=user.is_active,
            is_admin=user.is_admin,
            created_at=user.created_at,
            last_login=user.last_login
        )
        
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"User registration failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Registration failed"
        )


@router.get("/me", response_model=Dict[str, Any])
async def get_current_user_info(
    current_user: Dict[str, Any] = Depends(get_current_active_user)
) -> Dict[str, Any]:
    """
    Get current user information and permissions.
    
    Returns detailed information about the authenticated user including
    cluster permissions and API keys.
    """
    try:
        user_id = current_user["user_id"]
        permissions_summary = await access_control_manager.get_user_permissions_summary(user_id)
        
        return {
            "user_info": current_user,
            "permissions": permissions_summary,
            "accessible_clusters": await access_control_manager.get_user_clusters(user_id)
        }
        
    except Exception as e:
        logger.error(f"Failed to get user info: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve user information"
        )


# User Management Endpoints

@router.get("/users", response_model=List[UserResponse])
async def list_users(
    current_user: Dict[str, Any] = Depends(require_admin)
) -> List[UserResponse]:
    """
    List all users (admin only).
    
    Returns list of all registered users with their basic information.
    """
    try:
        users = []
        for user in access_control_manager.users.values():
            users.append(UserResponse(
                id=user.id,
                username=user.username,
                email=user.email,
                is_active=user.is_active,
                is_admin=user.is_admin,
                created_at=user.created_at,
                last_login=user.last_login
            ))
        
        return users
        
    except Exception as e:
        logger.error(f"Failed to list users: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve users"
        )


@router.get("/users/{user_id}", response_model=Dict[str, Any])
async def get_user_details(
    user_id: str = PathParam(..., description="User ID"),
    current_user: Dict[str, Any] = Depends(require_admin)
) -> Dict[str, Any]:
    """
    Get detailed user information (admin only).
    
    Returns comprehensive user information including permissions and API keys.
    """
    try:
        if user_id not in access_control_manager.users:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User {user_id} not found"
            )
        
        user = access_control_manager.users[user_id]
        permissions_summary = await access_control_manager.get_user_permissions_summary(user_id)
        
        return {
            "user": UserResponse(
                id=user.id,
                username=user.username,
                email=user.email,
                is_active=user.is_active,
                is_admin=user.is_admin,
                created_at=user.created_at,
                last_login=user.last_login
            ),
            "permissions": permissions_summary
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get user details for {user_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve user details"
        )


# Permission Management Endpoints

@router.post("/permissions/grant", response_model=Dict[str, Any])
async def grant_cluster_permission(
    request: GrantPermissionRequest,
    current_user: Dict[str, Any] = Depends(require_admin)
) -> Dict[str, Any]:
    """
    Grant cluster permission to user (admin only).
    
    Assigns specific access level to user for a cluster.
    """
    try:
        # Verify user exists
        if request.user_id not in access_control_manager.users:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User {request.user_id} not found"
            )
        
        permission = await access_control_manager.grant_cluster_permission(
            user_id=request.user_id,
            cluster_id=request.cluster_id,
            access_level=request.access_level,
            granted_by=current_user["user_id"],
            expires_at=request.expires_at
        )
        
        return {
            "success": True,
            "permission": {
                "user_id": permission.user_id,
                "cluster_id": permission.cluster_id,
                "access_level": permission.access_level.value,
                "granted_at": permission.granted_at.isoformat(),
                "expires_at": permission.expires_at.isoformat() if permission.expires_at else None
            },
            "message": f"Granted {request.access_level.value} access to user {request.user_id} for cluster {request.cluster_id}"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to grant permission: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to grant permission"
        )


@router.delete("/permissions/{user_id}/{cluster_id}")
async def revoke_cluster_permission(
    user_id: str = PathParam(..., description="User ID"),
    cluster_id: str = PathParam(..., description="Cluster ID"),
    current_user: Dict[str, Any] = Depends(require_admin)
) -> Dict[str, Any]:
    """
    Revoke cluster permission from user (admin only).
    
    Removes user's access to specified cluster.
    """
    try:
        # Remove permission
        if cluster_id in access_control_manager.cluster_permissions:
            original_count = len(access_control_manager.cluster_permissions[cluster_id])
            access_control_manager.cluster_permissions[cluster_id] = [
                p for p in access_control_manager.cluster_permissions[cluster_id]
                if p.user_id != user_id
            ]
            removed = original_count - len(access_control_manager.cluster_permissions[cluster_id])
            
            if removed > 0:
                # Log the revocation
                await access_control_manager._log_audit_event(
                    user_id=current_user["user_id"],
                    cluster_id=cluster_id,
                    action="permission_revoke",
                    resource=f"user:{user_id}",
                    result="success",
                    details={}
                )
                
                return {
                    "success": True,
                    "message": f"Revoked cluster access for user {user_id} from cluster {cluster_id}"
                }
        
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No permission found for user {user_id} on cluster {cluster_id}"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to revoke permission: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to revoke permission"
        )


# API Key Management Endpoints

@router.post("/api-keys", response_model=Dict[str, Any])
async def create_api_key(
    request: CreateAPIKeyRequest,
    current_user: Dict[str, Any] = Depends(get_current_active_user)
) -> Dict[str, Any]:
    """
    Create API key for programmatic access.
    
    Generates a new API key with specified cluster permissions.
    """
    try:
        api_key, raw_key = await access_control_manager.create_api_key(
            user_id=current_user["user_id"],
            name=request.name,
            cluster_permissions=request.cluster_permissions,
            expires_at=request.expires_at
        )
        
        return {
            "success": True,
            "api_key": {
                "id": api_key.id,
                "name": api_key.name,
                "key": raw_key,  # Only returned once
                "cluster_permissions": {k: v.value for k, v in api_key.cluster_permissions.items()},
                "created_at": api_key.created_at.isoformat(),
                "expires_at": api_key.expires_at.isoformat() if api_key.expires_at else None
            },
            "message": f"API key '{request.name}' created successfully",
            "warning": "Save the API key now - it won't be shown again"
        }
        
    except Exception as e:
        logger.error(f"Failed to create API key: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create API key"
        )


@router.get("/api-keys", response_model=List[APIKeyResponse])
async def list_api_keys(
    current_user: Dict[str, Any] = Depends(get_current_active_user)
) -> List[APIKeyResponse]:
    """
    List user's API keys.
    
    Returns list of API keys belonging to the authenticated user.
    """
    try:
        api_keys = []
        user_id = current_user["user_id"]
        
        for api_key in access_control_manager.api_keys.values():
            if api_key.user_id == user_id and api_key.is_active:
                api_keys.append(APIKeyResponse(
                    id=api_key.id,
                    name=api_key.name,
                    cluster_permissions=api_key.cluster_permissions,
                    created_at=api_key.created_at,
                    last_used=api_key.last_used,
                    expires_at=api_key.expires_at
                ))
        
        return api_keys
        
    except Exception as e:
        logger.error(f"Failed to list API keys: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve API keys"
        )


@router.delete("/api-keys/{key_id}")
async def revoke_api_key(
    key_id: str = PathParam(..., description="API key ID"),
    current_user: Dict[str, Any] = Depends(get_current_active_user)
) -> Dict[str, Any]:
    """
    Revoke API key.
    
    Deactivates the specified API key.
    """
    try:
        # Check if key exists and belongs to user (or user is admin)
        if key_id not in access_control_manager.api_keys:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"API key {key_id} not found"
            )
        
        api_key = access_control_manager.api_keys[key_id]
        
        # Check ownership (unless admin)
        if not current_user.get("is_admin", False) and api_key.user_id != current_user["user_id"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Can only revoke your own API keys"
            )
        
        success = await access_control_manager.revoke_api_key(key_id, current_user["user_id"])
        
        if success:
            return {
                "success": True,
                "message": f"API key '{api_key.name}' revoked successfully"
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="API key not found"
            )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to revoke API key: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to revoke API key"
        )


# Audit Log Endpoints

@router.get("/audit-log", response_model=List[AuditLogResponse])
async def get_audit_log(
    user_id: Optional[str] = Query(None, description="Filter by user ID"),
    cluster_id: Optional[str] = Query(None, description="Filter by cluster ID"),
    action: Optional[str] = Query(None, description="Filter by action"),
    limit: int = Query(100, description="Maximum number of entries"),
    current_user: Dict[str, Any] = Depends(require_admin)
) -> List[AuditLogResponse]:
    """
    Get audit log entries (admin only).
    
    Returns filtered audit log entries for security monitoring.
    """
    try:
        entries = await access_control_manager.get_audit_log(
            user_id=user_id,
            cluster_id=cluster_id,
            action=action,
            limit=limit
        )
        
        return [
            AuditLogResponse(
                id=entry.id,
                user_id=entry.user_id,
                cluster_id=entry.cluster_id,
                action=entry.action,
                resource=entry.resource,
                result=entry.result,
                ip_address=entry.ip_address,
                user_agent=entry.user_agent,
                details=entry.details,
                timestamp=entry.timestamp
            )
            for entry in entries
        ]
        
    except Exception as e:
        logger.error(f"Failed to get audit log: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve audit log"
        )


# System Administration Endpoints

@router.post("/cleanup-expired")
async def cleanup_expired_permissions(
    current_user: Dict[str, Any] = Depends(require_admin)
) -> Dict[str, Any]:
    """
    Clean up expired permissions and API keys (admin only).
    
    Removes expired permissions and deactivates expired API keys.
    """
    try:
        cleanup_count = await access_control_manager.cleanup_expired_permissions()
        
        return {
            "success": True,
            "cleaned_up_count": cleanup_count,
            "message": f"Cleaned up {cleanup_count} expired items"
        }
        
    except Exception as e:
        logger.error(f"Failed to cleanup expired permissions: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to cleanup expired permissions"
        )


@router.get("/system/status")
async def get_security_system_status(
    current_user: Dict[str, Any] = Depends(require_admin)
) -> Dict[str, Any]:
    """
    Get security system status (admin only).
    
    Returns overview of users, permissions, API keys, and audit log.
    """
    try:
        # Count active items
        active_users = sum(1 for u in access_control_manager.users.values() if u.is_active)
        total_permissions = sum(len(perms) for perms in access_control_manager.cluster_permissions.values())
        active_api_keys = sum(1 for k in access_control_manager.api_keys.values() if k.is_active)
        audit_entries = len(access_control_manager.audit_log)
        
        return {
            "system_status": "operational",
            "statistics": {
                "total_users": len(access_control_manager.users),
                "active_users": active_users,
                "total_permissions": total_permissions,
                "active_api_keys": active_api_keys,
                "audit_log_entries": audit_entries
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get security system status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve system status"
        )