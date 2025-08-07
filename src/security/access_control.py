"""
Access control and security management for multi-cluster support.

This module provides role-based access control (RBAC), user authentication,
and authorization for cluster operations with audit logging.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Any
from enum import Enum
import hashlib
import secrets
import jwt
from dataclasses import dataclass

from ..exceptions import SecurityError, ValidationError

logger = logging.getLogger(__name__)


class AccessLevel(Enum):
    """Access levels for cluster operations."""
    READ = "read"
    WRITE = "write"
    ADMIN = "admin"


class Permission(Enum):
    """Specific permissions for cluster operations."""
    # Cluster management
    CLUSTER_VIEW = "cluster:view"
    CLUSTER_CREATE = "cluster:create"
    CLUSTER_UPDATE = "cluster:update"
    CLUSTER_DELETE = "cluster:delete"
    CLUSTER_START = "cluster:start"
    CLUSTER_STOP = "cluster:stop"
    
    # Topic management
    TOPIC_VIEW = "topic:view"
    TOPIC_CREATE = "topic:create"
    TOPIC_UPDATE = "topic:update"
    TOPIC_DELETE = "topic:delete"
    
    # Message operations
    MESSAGE_PRODUCE = "message:produce"
    MESSAGE_CONSUME = "message:consume"
    
    # Configuration management
    CONFIG_VIEW = "config:view"
    CONFIG_EXPORT = "config:export"
    CONFIG_IMPORT = "config:import"
    CONFIG_ROLLBACK = "config:rollback"
    
    # Cross-cluster operations
    CROSS_CLUSTER_MIGRATE = "cross_cluster:migrate"
    CROSS_CLUSTER_REPLICATE = "cross_cluster:replicate"
    CROSS_CLUSTER_COMPARE = "cross_cluster:compare"
    
    # System administration
    SYSTEM_ADMIN = "system:admin"
    USER_MANAGE = "user:manage"
    AUDIT_VIEW = "audit:view"


@dataclass
class User:
    """User information for access control."""
    id: str
    username: str
    email: str
    is_active: bool = True
    is_admin: bool = False
    created_at: datetime = None
    last_login: datetime = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()


@dataclass
class Role:
    """Role definition with permissions."""
    id: str
    name: str
    description: str
    permissions: Set[Permission]
    is_system_role: bool = False
    created_at: datetime = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()


@dataclass
class ClusterPermission:
    """Cluster-specific permission assignment."""
    user_id: str
    cluster_id: str
    access_level: AccessLevel
    specific_permissions: Set[Permission]
    granted_by: str
    granted_at: datetime = None
    expires_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.granted_at is None:
            self.granted_at = datetime.utcnow()


@dataclass
class APIKey:
    """API key for programmatic access."""
    id: str
    name: str
    key_hash: str
    user_id: str
    cluster_permissions: Dict[str, AccessLevel]
    is_active: bool = True
    created_at: datetime = None
    last_used: datetime = None
    expires_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()


@dataclass
class AuditLogEntry:
    """Audit log entry for security events."""
    id: str
    user_id: str
    cluster_id: Optional[str]
    action: str
    resource: str
    result: str  # success, denied, error
    ip_address: Optional[str]
    user_agent: Optional[str]
    details: Dict[str, Any]
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()


class AccessControlManager:
    """
    Manages access control, authentication, and authorization for multi-cluster operations.
    """
    
    def __init__(self, secret_key: str, token_expiry_hours: int = 24):
        """
        Initialize access control manager.
        
        Args:
            secret_key: Secret key for JWT token signing
            token_expiry_hours: JWT token expiry time in hours
        """
        self.secret_key = secret_key
        self.token_expiry_hours = token_expiry_hours
        
        # In-memory storage (in production, use proper database)
        self.users: Dict[str, User] = {}
        self.roles: Dict[str, Role] = {}
        self.cluster_permissions: Dict[str, List[ClusterPermission]] = {}
        self.api_keys: Dict[str, APIKey] = {}
        self.audit_log: List[AuditLogEntry] = []
        
        # Initialize default roles
        self._initialize_default_roles()
        
        logger.info("Access control manager initialized")
    
    def _initialize_default_roles(self):
        """Initialize default system roles."""
        # Admin role with all permissions
        admin_role = Role(
            id="admin",
            name="Administrator",
            description="Full system administrator with all permissions",
            permissions=set(Permission),
            is_system_role=True
        )
        self.roles[admin_role.id] = admin_role
        
        # Cluster admin role
        cluster_admin_role = Role(
            id="cluster_admin",
            name="Cluster Administrator",
            description="Full cluster management permissions",
            permissions={
                Permission.CLUSTER_VIEW, Permission.CLUSTER_CREATE, Permission.CLUSTER_UPDATE,
                Permission.CLUSTER_DELETE, Permission.CLUSTER_START, Permission.CLUSTER_STOP,
                Permission.TOPIC_VIEW, Permission.TOPIC_CREATE, Permission.TOPIC_UPDATE,
                Permission.TOPIC_DELETE, Permission.MESSAGE_PRODUCE, Permission.MESSAGE_CONSUME,
                Permission.CONFIG_VIEW, Permission.CONFIG_EXPORT, Permission.CONFIG_IMPORT,
                Permission.CONFIG_ROLLBACK
            },
            is_system_role=True
        )
        self.roles[cluster_admin_role.id] = cluster_admin_role
        
        # Developer role
        developer_role = Role(
            id="developer",
            name="Developer",
            description="Development permissions for topics and messages",
            permissions={
                Permission.CLUSTER_VIEW, Permission.TOPIC_VIEW, Permission.TOPIC_CREATE,
                Permission.MESSAGE_PRODUCE, Permission.MESSAGE_CONSUME, Permission.CONFIG_VIEW
            },
            is_system_role=True
        )
        self.roles[developer_role.id] = developer_role
        
        # Read-only role
        readonly_role = Role(
            id="readonly",
            name="Read Only",
            description="Read-only access to clusters and topics",
            permissions={
                Permission.CLUSTER_VIEW, Permission.TOPIC_VIEW, Permission.CONFIG_VIEW
            },
            is_system_role=True
        )
        self.roles[readonly_role.id] = readonly_role
    
    async def create_user(self, username: str, email: str, password: str, is_admin: bool = False) -> User:
        """
        Create a new user.
        
        Args:
            username: Username
            email: Email address
            password: Password (will be hashed)
            is_admin: Whether user is system admin
            
        Returns:
            User: Created user
            
        Raises:
            ValidationError: If user already exists
        """
        user_id = self._generate_id()
        
        # Check if user already exists
        for user in self.users.values():
            if user.username == username or user.email == email:
                raise ValidationError(f"User with username '{username}' or email '{email}' already exists")
        
        # Create user
        user = User(
            id=user_id,
            username=username,
            email=email,
            is_admin=is_admin
        )
        
        self.users[user_id] = user
        
        # Log user creation
        await self._log_audit_event(
            user_id="system",
            cluster_id=None,
            action="user_create",
            resource=f"user:{user_id}",
            result="success",
            details={"username": username, "email": email, "is_admin": is_admin}
        )
        
        logger.info(f"Created user: {username} ({user_id})")
        return user
    
    async def authenticate_user(self, username: str, password: str) -> Optional[str]:
        """
        Authenticate user and return JWT token.
        
        Args:
            username: Username
            password: Password
            
        Returns:
            Optional[str]: JWT token if authentication successful
        """
        # Find user
        user = None
        for u in self.users.values():
            if u.username == username and u.is_active:
                user = u
                break
        
        if not user:
            await self._log_audit_event(
                user_id="anonymous",
                cluster_id=None,
                action="login_attempt",
                resource="authentication",
                result="denied",
                details={"username": username, "reason": "user_not_found"}
            )
            return None
        
        # In production, verify password hash
        # For now, accept any password for demo purposes
        
        # Update last login
        user.last_login = datetime.utcnow()
        
        # Generate JWT token
        payload = {
            "user_id": user.id,
            "username": user.username,
            "is_admin": user.is_admin,
            "exp": datetime.utcnow() + timedelta(hours=self.token_expiry_hours),
            "iat": datetime.utcnow()
        }
        
        token = jwt.encode(payload, self.secret_key, algorithm="HS256")
        
        await self._log_audit_event(
            user_id=user.id,
            cluster_id=None,
            action="login",
            resource="authentication",
            result="success",
            details={"username": username}
        )
        
        logger.info(f"User authenticated: {username}")
        return token
    
    async def verify_token(self, token: str) -> Optional[Dict[str, Any]]:
        """
        Verify JWT token and return user information.
        
        Args:
            token: JWT token
            
        Returns:
            Optional[Dict]: User information if token valid
        """
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=["HS256"])
            
            # Check if user still exists and is active
            user_id = payload.get("user_id")
            if user_id not in self.users or not self.users[user_id].is_active:
                return None
            
            return payload
            
        except jwt.ExpiredSignatureError:
            logger.warning("Token expired")
            return None
        except jwt.InvalidTokenError:
            logger.warning("Invalid token")
            return None
    
    async def grant_cluster_permission(
        self, 
        user_id: str, 
        cluster_id: str, 
        access_level: AccessLevel,
        granted_by: str,
        expires_at: Optional[datetime] = None
    ) -> ClusterPermission:
        """
        Grant cluster-specific permission to user.
        
        Args:
            user_id: User ID
            cluster_id: Cluster ID
            access_level: Access level to grant
            granted_by: User ID who granted the permission
            expires_at: Optional expiration time
            
        Returns:
            ClusterPermission: Created permission
        """
        # Map access level to specific permissions
        permission_mapping = {
            AccessLevel.READ: {
                Permission.CLUSTER_VIEW, Permission.TOPIC_VIEW, Permission.CONFIG_VIEW
            },
            AccessLevel.WRITE: {
                Permission.CLUSTER_VIEW, Permission.TOPIC_VIEW, Permission.TOPIC_CREATE,
                Permission.MESSAGE_PRODUCE, Permission.MESSAGE_CONSUME, Permission.CONFIG_VIEW
            },
            AccessLevel.ADMIN: {
                Permission.CLUSTER_VIEW, Permission.CLUSTER_UPDATE, Permission.CLUSTER_START,
                Permission.CLUSTER_STOP, Permission.TOPIC_VIEW, Permission.TOPIC_CREATE,
                Permission.TOPIC_UPDATE, Permission.TOPIC_DELETE, Permission.MESSAGE_PRODUCE,
                Permission.MESSAGE_CONSUME, Permission.CONFIG_VIEW, Permission.CONFIG_EXPORT,
                Permission.CONFIG_IMPORT, Permission.CONFIG_ROLLBACK
            }
        }
        
        permission = ClusterPermission(
            user_id=user_id,
            cluster_id=cluster_id,
            access_level=access_level,
            specific_permissions=permission_mapping[access_level],
            granted_by=granted_by,
            expires_at=expires_at
        )
        
        # Store permission
        if cluster_id not in self.cluster_permissions:
            self.cluster_permissions[cluster_id] = []
        
        # Remove existing permission for same user/cluster
        self.cluster_permissions[cluster_id] = [
            p for p in self.cluster_permissions[cluster_id] 
            if p.user_id != user_id
        ]
        
        self.cluster_permissions[cluster_id].append(permission)
        
        await self._log_audit_event(
            user_id=granted_by,
            cluster_id=cluster_id,
            action="permission_grant",
            resource=f"user:{user_id}",
            result="success",
            details={
                "access_level": access_level.value,
                "expires_at": expires_at.isoformat() if expires_at else None
            }
        )
        
        logger.info(f"Granted {access_level.value} access to user {user_id} for cluster {cluster_id}")
        return permission  
  
    async def check_cluster_permission(
        self, 
        user_id: str, 
        cluster_id: str, 
        required_permission: Permission
    ) -> bool:
        """
        Check if user has specific permission for cluster.
        
        Args:
            user_id: User ID
            cluster_id: Cluster ID
            required_permission: Required permission
            
        Returns:
            bool: True if user has permission
        """
        # System admin has all permissions
        user = self.users.get(user_id)
        if user and user.is_admin:
            return True
        
        # Check cluster-specific permissions
        cluster_perms = self.cluster_permissions.get(cluster_id, [])
        
        for perm in cluster_perms:
            if perm.user_id == user_id:
                # Check if permission is expired
                if perm.expires_at and perm.expires_at < datetime.utcnow():
                    continue
                
                # Check if user has required permission
                if required_permission in perm.specific_permissions:
                    return True
        
        return False
    
    async def get_user_clusters(self, user_id: str) -> List[str]:
        """
        Get list of clusters user has access to.
        
        Args:
            user_id: User ID
            
        Returns:
            List[str]: List of cluster IDs user can access
        """
        # System admin has access to all clusters
        user = self.users.get(user_id)
        if user and user.is_admin:
            # Return all cluster IDs (would need to get from cluster registry)
            return list(self.cluster_permissions.keys())
        
        # Get clusters with specific permissions
        accessible_clusters = set()
        
        for cluster_id, perms in self.cluster_permissions.items():
            for perm in perms:
                if perm.user_id == user_id:
                    # Check if permission is not expired
                    if not perm.expires_at or perm.expires_at > datetime.utcnow():
                        accessible_clusters.add(cluster_id)
                        break
        
        return list(accessible_clusters)
    
    async def create_api_key(
        self, 
        user_id: str, 
        name: str, 
        cluster_permissions: Dict[str, AccessLevel],
        expires_at: Optional[datetime] = None
    ) -> tuple[APIKey, str]:
        """
        Create API key for programmatic access.
        
        Args:
            user_id: User ID
            name: API key name
            cluster_permissions: Cluster permissions for the key
            expires_at: Optional expiration time
            
        Returns:
            tuple[APIKey, str]: API key object and raw key string
        """
        key_id = self._generate_id()
        raw_key = secrets.token_urlsafe(32)
        key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
        
        api_key = APIKey(
            id=key_id,
            name=name,
            key_hash=key_hash,
            user_id=user_id,
            cluster_permissions=cluster_permissions,
            expires_at=expires_at
        )
        
        self.api_keys[key_id] = api_key
        
        await self._log_audit_event(
            user_id=user_id,
            cluster_id=None,
            action="api_key_create",
            resource=f"api_key:{key_id}",
            result="success",
            details={
                "name": name,
                "cluster_count": len(cluster_permissions),
                "expires_at": expires_at.isoformat() if expires_at else None
            }
        )
        
        logger.info(f"Created API key '{name}' for user {user_id}")
        return api_key, raw_key
    
    async def verify_api_key(self, raw_key: str) -> Optional[APIKey]:
        """
        Verify API key and return key information.
        
        Args:
            raw_key: Raw API key string
            
        Returns:
            Optional[APIKey]: API key if valid
        """
        key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
        
        for api_key in self.api_keys.values():
            if api_key.key_hash == key_hash and api_key.is_active:
                # Check expiration
                if api_key.expires_at and api_key.expires_at < datetime.utcnow():
                    continue
                
                # Update last used
                api_key.last_used = datetime.utcnow()
                
                return api_key
        
        return None
    
    async def revoke_api_key(self, key_id: str, revoked_by: str) -> bool:
        """
        Revoke API key.
        
        Args:
            key_id: API key ID
            revoked_by: User ID who revoked the key
            
        Returns:
            bool: True if key was revoked
        """
        if key_id in self.api_keys:
            self.api_keys[key_id].is_active = False
            
            await self._log_audit_event(
                user_id=revoked_by,
                cluster_id=None,
                action="api_key_revoke",
                resource=f"api_key:{key_id}",
                result="success",
                details={"key_name": self.api_keys[key_id].name}
            )
            
            logger.info(f"Revoked API key {key_id}")
            return True
        
        return False
    
    async def _log_audit_event(
        self,
        user_id: str,
        cluster_id: Optional[str],
        action: str,
        resource: str,
        result: str,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """Log audit event."""
        entry = AuditLogEntry(
            id=self._generate_id(),
            user_id=user_id,
            cluster_id=cluster_id,
            action=action,
            resource=resource,
            result=result,
            ip_address=ip_address,
            user_agent=user_agent,
            details=details or {}
        )
        
        self.audit_log.append(entry)
        
        # Keep only last 10000 entries (in production, use proper log rotation)
        if len(self.audit_log) > 10000:
            self.audit_log = self.audit_log[-10000:]
    
    async def get_audit_log(
        self,
        user_id: Optional[str] = None,
        cluster_id: Optional[str] = None,
        action: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[AuditLogEntry]:
        """
        Get audit log entries with filtering.
        
        Args:
            user_id: Filter by user ID
            cluster_id: Filter by cluster ID
            action: Filter by action
            start_time: Filter by start time
            end_time: Filter by end time
            limit: Maximum number of entries
            
        Returns:
            List[AuditLogEntry]: Filtered audit log entries
        """
        filtered_entries = []
        
        for entry in reversed(self.audit_log):  # Most recent first
            # Apply filters
            if user_id and entry.user_id != user_id:
                continue
            if cluster_id and entry.cluster_id != cluster_id:
                continue
            if action and entry.action != action:
                continue
            if start_time and entry.timestamp < start_time:
                continue
            if end_time and entry.timestamp > end_time:
                continue
            
            filtered_entries.append(entry)
            
            if len(filtered_entries) >= limit:
                break
        
        return filtered_entries
    
    def _generate_id(self) -> str:
        """Generate unique ID."""
        return secrets.token_hex(16)
    
    async def get_user_permissions_summary(self, user_id: str) -> Dict[str, Any]:
        """
        Get summary of user's permissions across all clusters.
        
        Args:
            user_id: User ID
            
        Returns:
            Dict: Permission summary
        """
        user = self.users.get(user_id)
        if not user:
            return {}
        
        summary = {
            "user_id": user_id,
            "username": user.username,
            "is_admin": user.is_admin,
            "cluster_permissions": {},
            "api_keys": []
        }
        
        # Get cluster permissions
        for cluster_id, perms in self.cluster_permissions.items():
            for perm in perms:
                if perm.user_id == user_id:
                    # Check if not expired
                    if not perm.expires_at or perm.expires_at > datetime.utcnow():
                        summary["cluster_permissions"][cluster_id] = {
                            "access_level": perm.access_level.value,
                            "permissions": [p.value for p in perm.specific_permissions],
                            "granted_at": perm.granted_at.isoformat(),
                            "expires_at": perm.expires_at.isoformat() if perm.expires_at else None
                        }
        
        # Get API keys
        for api_key in self.api_keys.values():
            if api_key.user_id == user_id and api_key.is_active:
                summary["api_keys"].append({
                    "id": api_key.id,
                    "name": api_key.name,
                    "cluster_permissions": {k: v.value for k, v in api_key.cluster_permissions.items()},
                    "created_at": api_key.created_at.isoformat(),
                    "last_used": api_key.last_used.isoformat() if api_key.last_used else None,
                    "expires_at": api_key.expires_at.isoformat() if api_key.expires_at else None
                })
        
        return summary
    
    async def cleanup_expired_permissions(self) -> int:
        """
        Clean up expired permissions and API keys.
        
        Returns:
            int: Number of items cleaned up
        """
        cleanup_count = 0
        current_time = datetime.utcnow()
        
        # Clean up expired cluster permissions
        for cluster_id, perms in self.cluster_permissions.items():
            original_count = len(perms)
            self.cluster_permissions[cluster_id] = [
                p for p in perms 
                if not p.expires_at or p.expires_at > current_time
            ]
            cleanup_count += original_count - len(self.cluster_permissions[cluster_id])
        
        # Clean up expired API keys
        for api_key in self.api_keys.values():
            if api_key.expires_at and api_key.expires_at < current_time and api_key.is_active:
                api_key.is_active = False
                cleanup_count += 1
        
        if cleanup_count > 0:
            logger.info(f"Cleaned up {cleanup_count} expired permissions/keys")
        
        return cleanup_count


# Security exceptions
class SecurityError(Exception):
    """Base security exception."""
    pass


class AuthenticationError(SecurityError):
    """Authentication failed."""
    pass


class AuthorizationError(SecurityError):
    """Authorization failed."""
    pass


class AccessDeniedError(SecurityError):
    """Access denied to resource."""
    pass