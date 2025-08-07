"""
Authentication and authorization middleware for multi-cluster support.

This module provides middleware for JWT token authentication, API key authentication,
and permission checking for cluster operations.
"""

import logging
from typing import Optional, Dict, Any, Callable, List
from fastapi import Request, HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import re

from .access_control import AccessControlManager, Permission, AuthenticationError, AuthorizationError

logger = logging.getLogger(__name__)

# Global access control manager instance
access_control_manager: Optional[AccessControlManager] = None

# Security scheme for JWT tokens
security = HTTPBearer(auto_error=False)


def init_auth_middleware(manager: AccessControlManager):
    """Initialize authentication middleware with access control manager."""
    global access_control_manager
    access_control_manager = manager


class AuthenticationMiddleware:
    """
    Middleware for handling authentication and authorization.
    """
    
    def __init__(self, access_manager: AccessControlManager):
        self.access_manager = access_manager
    
    async def __call__(self, request: Request, call_next):
        """Process request with authentication and authorization."""
        try:
            # Skip authentication for public endpoints
            if self._is_public_endpoint(request.url.path):
                return await call_next(request)
            
            # Extract and verify authentication
            user_info = await self._authenticate_request(request)
            
            if user_info:
                # Add user info to request state
                request.state.user = user_info
                request.state.authenticated = True
            else:
                request.state.authenticated = False
            
            # Continue with request processing
            response = await call_next(request)
            return response
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Authentication middleware error: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Authentication error"
            )
    
    def _is_public_endpoint(self, path: str) -> bool:
        """Check if endpoint is public (doesn't require authentication)."""
        public_patterns = [
            r'^/docs.*',
            r'^/redoc.*',
            r'^/openapi\.json$',
            r'^/health$',
            r'^/auth/login$',
            r'^/auth/register$',
            r'^/$'
        ]
        
        for pattern in public_patterns:
            if re.match(pattern, path):
                return True
        
        return False
    
    async def _authenticate_request(self, request: Request) -> Optional[Dict[str, Any]]:
        """Authenticate request using JWT token or API key."""
        # Try JWT token authentication first
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header[7:]  # Remove "Bearer " prefix
            
            # Check if it's a JWT token or API key
            if "." in token:  # JWT tokens have dots
                user_info = await self.access_manager.verify_token(token)
                if user_info:
                    return user_info
            else:  # Treat as API key
                api_key = await self.access_manager.verify_api_key(token)
                if api_key:
                    # Get user info for API key
                    user = self.access_manager.users.get(api_key.user_id)
                    if user:
                        return {
                            "user_id": user.id,
                            "username": user.username,
                            "is_admin": user.is_admin,
                            "auth_type": "api_key",
                            "api_key_id": api_key.id
                        }
        
        # Try API key from query parameter (for convenience)
        api_key_param = request.query_params.get("api_key")
        if api_key_param:
            api_key = await self.access_manager.verify_api_key(api_key_param)
            if api_key:
                user = self.access_manager.users.get(api_key.user_id)
                if user:
                    return {
                        "user_id": user.id,
                        "username": user.username,
                        "is_admin": user.is_admin,
                        "auth_type": "api_key",
                        "api_key_id": api_key.id
                    }
        
        return None


# Dependency functions for FastAPI

async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
) -> Dict[str, Any]:
    """
    Get current authenticated user.
    
    Returns:
        Dict: User information
        
    Raises:
        HTTPException: If not authenticated
    """
    if not access_control_manager:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Authentication not configured"
        )
    
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    token = credentials.credentials
    
    # Try JWT token first
    if "." in token:
        user_info = await access_control_manager.verify_token(token)
        if user_info:
            return user_info
    else:
        # Try API key
        api_key = await access_control_manager.verify_api_key(token)
        if api_key:
            user = access_control_manager.users.get(api_key.user_id)
            if user:
                return {
                    "user_id": user.id,
                    "username": user.username,
                    "is_admin": user.is_admin,
                    "auth_type": "api_key",
                    "api_key_id": api_key.id
                }
    
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid authentication credentials",
        headers={"WWW-Authenticate": "Bearer"}
    )


async def get_current_active_user(
    current_user: Dict[str, Any] = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get current active user.
    
    Returns:
        Dict: Active user information
        
    Raises:
        HTTPException: If user is inactive
    """
    user_id = current_user.get("user_id")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid user information"
        )
    
    user = access_control_manager.users.get(user_id)
    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User account is inactive"
        )
    
    return current_user


async def require_admin(
    current_user: Dict[str, Any] = Depends(get_current_active_user)
) -> Dict[str, Any]:
    """
    Require admin privileges.
    
    Returns:
        Dict: Admin user information
        
    Raises:
        HTTPException: If user is not admin
    """
    if not current_user.get("is_admin", False):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin privileges required"
        )
    
    return current_user


def require_cluster_permission(permission: Permission):
    """
    Create dependency that requires specific cluster permission.
    
    Args:
        permission: Required permission
        
    Returns:
        Callable: Dependency function
    """
    async def check_permission(
        cluster_id: str,
        current_user: Dict[str, Any] = Depends(get_current_active_user)
    ) -> Dict[str, Any]:
        """Check if user has required permission for cluster."""
        user_id = current_user.get("user_id")
        
        # Admin users have all permissions
        if current_user.get("is_admin", False):
            return current_user
        
        # Check cluster-specific permission
        has_permission = await access_control_manager.check_cluster_permission(
            user_id, cluster_id, permission
        )
        
        if not has_permission:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission '{permission.value}' required for cluster '{cluster_id}'"
            )
        
        return current_user
    
    return check_permission


def require_any_cluster_access():
    """
    Create dependency that requires access to at least one cluster.
    
    Returns:
        Callable: Dependency function
    """
    async def check_access(
        current_user: Dict[str, Any] = Depends(get_current_active_user)
    ) -> Dict[str, Any]:
        """Check if user has access to any cluster."""
        user_id = current_user.get("user_id")
        
        # Admin users have access to all clusters
        if current_user.get("is_admin", False):
            return current_user
        
        # Check if user has access to any cluster
        accessible_clusters = await access_control_manager.get_user_clusters(user_id)
        
        if not accessible_clusters:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="No cluster access permissions"
            )
        
        return current_user
    
    return check_access


# Utility functions

async def log_security_event(
    request: Request,
    action: str,
    resource: str,
    result: str,
    cluster_id: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None
):
    """
    Log security event for audit purposes.
    
    Args:
        request: FastAPI request object
        action: Action performed
        resource: Resource accessed
        result: Result of action (success, denied, error)
        cluster_id: Optional cluster ID
        details: Additional details
    """
    if not access_control_manager:
        return
    
    # Get user info from request state
    user_id = "anonymous"
    if hasattr(request.state, "user") and request.state.user:
        user_id = request.state.user.get("user_id", "anonymous")
    
    # Get client info
    ip_address = request.client.host if request.client else None
    user_agent = request.headers.get("user-agent")
    
    await access_control_manager._log_audit_event(
        user_id=user_id,
        cluster_id=cluster_id,
        action=action,
        resource=resource,
        result=result,
        ip_address=ip_address,
        user_agent=user_agent,
        details=details
    )


async def get_user_accessible_clusters(user_id: str) -> List[str]:
    """
    Get list of clusters accessible to user.
    
    Args:
        user_id: User ID
        
    Returns:
        List[str]: List of accessible cluster IDs
    """
    if not access_control_manager:
        return []
    
    return await access_control_manager.get_user_clusters(user_id)


async def check_cluster_access(user_id: str, cluster_id: str, permission: Permission) -> bool:
    """
    Check if user has specific permission for cluster.
    
    Args:
        user_id: User ID
        cluster_id: Cluster ID
        permission: Required permission
        
    Returns:
        bool: True if user has permission
    """
    if not access_control_manager:
        return False
    
    return await access_control_manager.check_cluster_permission(user_id, cluster_id, permission)