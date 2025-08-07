"""
Tests for security and access control system.

This module tests the access control manager, authentication middleware,
and authorization functionality for multi-cluster operations.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch
import jwt

from src.security.access_control import (
    AccessControlManager, User, Role, ClusterPermission, APIKey, AuditLogEntry,
    AccessLevel, Permission, SecurityError, AuthenticationError, AuthorizationError
)


class TestAccessControlManager:
    """Test access control manager functionality."""
    
    @pytest.fixture
    def access_manager(self):
        """Create access control manager for testing."""
        return AccessControlManager(secret_key="test-secret-key", token_expiry_hours=24)
    
    @pytest.mark.asyncio
    async def test_create_user(self, access_manager):
        """Test user creation."""
        user = await access_manager.create_user(
            username="testuser",
            email="test@example.com",
            password="password123",
            is_admin=False
        )
        
        assert user.username == "testuser"
        assert user.email == "test@example.com"
        assert user.is_active is True
        assert user.is_admin is False
        assert user.id in access_manager.users
    
    @pytest.mark.asyncio
    async def test_create_duplicate_user(self, access_manager):
        """Test creating duplicate user fails."""
        await access_manager.create_user("testuser", "test@example.com", "password123")
        
        with pytest.raises(Exception):  # Should raise ValidationError
            await access_manager.create_user("testuser", "other@example.com", "password123")
    
    @pytest.mark.asyncio
    async def test_authenticate_user(self, access_manager):
        """Test user authentication."""
        # Create user
        user = await access_manager.create_user("testuser", "test@example.com", "password123")
        
        # Authenticate
        token = await access_manager.authenticate_user("testuser", "password123")
        
        assert token is not None
        
        # Verify token
        payload = jwt.decode(token, access_manager.secret_key, algorithms=["HS256"])
        assert payload["user_id"] == user.id
        assert payload["username"] == "testuser"
        assert payload["is_admin"] is False
    
    @pytest.mark.asyncio
    async def test_authenticate_invalid_user(self, access_manager):
        """Test authentication with invalid credentials."""
        token = await access_manager.authenticate_user("nonexistent", "password")
        assert token is None
    
    @pytest.mark.asyncio
    async def test_verify_token(self, access_manager):
        """Test token verification."""
        # Create user and get token
        user = await access_manager.create_user("testuser", "test@example.com", "password123")
        token = await access_manager.authenticate_user("testuser", "password123")
        
        # Verify token
        payload = await access_manager.verify_token(token)
        
        assert payload is not None
        assert payload["user_id"] == user.id
        assert payload["username"] == "testuser"
    
    @pytest.mark.asyncio
    async def test_verify_invalid_token(self, access_manager):
        """Test verification of invalid token."""
        payload = await access_manager.verify_token("invalid-token")
        assert payload is None
    
    @pytest.mark.asyncio
    async def test_grant_cluster_permission(self, access_manager):
        """Test granting cluster permission."""
        # Create user
        user = await access_manager.create_user("testuser", "test@example.com", "password123")
        admin = await access_manager.create_user("admin", "admin@example.com", "password123", is_admin=True)
        
        # Grant permission
        permission = await access_manager.grant_cluster_permission(
            user_id=user.id,
            cluster_id="test-cluster",
            access_level=AccessLevel.READ,
            granted_by=admin.id
        )
        
        assert permission.user_id == user.id
        assert permission.cluster_id == "test-cluster"
        assert permission.access_level == AccessLevel.READ
        assert Permission.CLUSTER_VIEW in permission.specific_permissions
    
    @pytest.mark.asyncio
    async def test_check_cluster_permission(self, access_manager):
        """Test checking cluster permission."""
        # Create user and grant permission
        user = await access_manager.create_user("testuser", "test@example.com", "password123")
        admin = await access_manager.create_user("admin", "admin@example.com", "password123", is_admin=True)
        
        await access_manager.grant_cluster_permission(
            user_id=user.id,
            cluster_id="test-cluster",
            access_level=AccessLevel.WRITE,
            granted_by=admin.id
        )
        
        # Check permissions
        assert await access_manager.check_cluster_permission(
            user.id, "test-cluster", Permission.CLUSTER_VIEW
        ) is True
        
        assert await access_manager.check_cluster_permission(
            user.id, "test-cluster", Permission.TOPIC_CREATE
        ) is True
        
        assert await access_manager.check_cluster_permission(
            user.id, "test-cluster", Permission.CLUSTER_DELETE
        ) is False
        
        assert await access_manager.check_cluster_permission(
            user.id, "other-cluster", Permission.CLUSTER_VIEW
        ) is False
    
    @pytest.mark.asyncio
    async def test_admin_has_all_permissions(self, access_manager):
        """Test that admin users have all permissions."""
        admin = await access_manager.create_user("admin", "admin@example.com", "password123", is_admin=True)
        
        # Admin should have all permissions without explicit grants
        assert await access_manager.check_cluster_permission(
            admin.id, "any-cluster", Permission.CLUSTER_DELETE
        ) is True
        
        assert await access_manager.check_cluster_permission(
            admin.id, "any-cluster", Permission.SYSTEM_ADMIN
        ) is True
    
    @pytest.mark.asyncio
    async def test_get_user_clusters(self, access_manager):
        """Test getting user's accessible clusters."""
        # Create user and grant permissions
        user = await access_manager.create_user("testuser", "test@example.com", "password123")
        admin = await access_manager.create_user("admin", "admin@example.com", "password123", is_admin=True)
        
        await access_manager.grant_cluster_permission(
            user.id, "cluster-1", AccessLevel.READ, admin.id
        )
        await access_manager.grant_cluster_permission(
            user.id, "cluster-2", AccessLevel.WRITE, admin.id
        )
        
        clusters = await access_manager.get_user_clusters(user.id)
        
        assert "cluster-1" in clusters
        assert "cluster-2" in clusters
        assert len(clusters) == 2
    
    @pytest.mark.asyncio
    async def test_create_api_key(self, access_manager):
        """Test API key creation."""
        user = await access_manager.create_user("testuser", "test@example.com", "password123")
        
        cluster_permissions = {
            "cluster-1": AccessLevel.READ,
            "cluster-2": AccessLevel.WRITE
        }
        
        api_key, raw_key = await access_manager.create_api_key(
            user_id=user.id,
            name="test-key",
            cluster_permissions=cluster_permissions
        )
        
        assert api_key.name == "test-key"
        assert api_key.user_id == user.id
        assert api_key.cluster_permissions == cluster_permissions
        assert api_key.is_active is True
        assert raw_key is not None
        assert len(raw_key) > 20  # Should be a reasonable length
    
    @pytest.mark.asyncio
    async def test_verify_api_key(self, access_manager):
        """Test API key verification."""
        user = await access_manager.create_user("testuser", "test@example.com", "password123")
        
        api_key, raw_key = await access_manager.create_api_key(
            user_id=user.id,
            name="test-key",
            cluster_permissions={"cluster-1": AccessLevel.READ}
        )
        
        # Verify API key
        verified_key = await access_manager.verify_api_key(raw_key)
        
        assert verified_key is not None
        assert verified_key.id == api_key.id
        assert verified_key.user_id == user.id
        assert verified_key.last_used is not None
    
    @pytest.mark.asyncio
    async def test_verify_invalid_api_key(self, access_manager):
        """Test verification of invalid API key."""
        verified_key = await access_manager.verify_api_key("invalid-key")
        assert verified_key is None
    
    @pytest.mark.asyncio
    async def test_revoke_api_key(self, access_manager):
        """Test API key revocation."""
        user = await access_manager.create_user("testuser", "test@example.com", "password123")
        
        api_key, raw_key = await access_manager.create_api_key(
            user_id=user.id,
            name="test-key",
            cluster_permissions={"cluster-1": AccessLevel.READ}
        )
        
        # Revoke key
        success = await access_manager.revoke_api_key(api_key.id, user.id)
        assert success is True
        
        # Verify key is no longer valid
        verified_key = await access_manager.verify_api_key(raw_key)
        assert verified_key is None
    
    @pytest.mark.asyncio
    async def test_audit_logging(self, access_manager):
        """Test audit logging functionality."""
        # Create user (should generate audit log)
        user = await access_manager.create_user("testuser", "test@example.com", "password123")
        
        # Check audit log
        audit_entries = await access_manager.get_audit_log(limit=10)
        
        assert len(audit_entries) > 0
        
        # Find user creation entry
        user_create_entry = None
        for entry in audit_entries:
            if entry.action == "user_create" and entry.resource == f"user:{user.id}":
                user_create_entry = entry
                break
        
        assert user_create_entry is not None
        assert user_create_entry.user_id == "system"
        assert user_create_entry.result == "success"
        assert "username" in user_create_entry.details
    
    @pytest.mark.asyncio
    async def test_expired_permissions_cleanup(self, access_manager):
        """Test cleanup of expired permissions."""
        # Create user and grant permission with expiration
        user = await access_manager.create_user("testuser", "test@example.com", "password123")
        admin = await access_manager.create_user("admin", "admin@example.com", "password123", is_admin=True)
        
        # Grant permission that expires in the past
        past_time = datetime.utcnow() - timedelta(hours=1)
        await access_manager.grant_cluster_permission(
            user_id=user.id,
            cluster_id="test-cluster",
            access_level=AccessLevel.READ,
            granted_by=admin.id,
            expires_at=past_time
        )
        
        # Create API key that expires in the past
        future_time = datetime.utcnow() + timedelta(hours=1)
        api_key, raw_key = await access_manager.create_api_key(
            user_id=user.id,
            name="test-key",
            cluster_permissions={"cluster-1": AccessLevel.READ},
            expires_at=past_time
        )
        
        # Cleanup expired items
        cleanup_count = await access_manager.cleanup_expired_permissions()
        
        assert cleanup_count >= 1  # At least the expired permission should be cleaned
        
        # Verify permission is no longer valid
        has_permission = await access_manager.check_cluster_permission(
            user.id, "test-cluster", Permission.CLUSTER_VIEW
        )
        assert has_permission is False
    
    @pytest.mark.asyncio
    async def test_get_user_permissions_summary(self, access_manager):
        """Test getting user permissions summary."""
        # Create user and grant permissions
        user = await access_manager.create_user("testuser", "test@example.com", "password123")
        admin = await access_manager.create_user("admin", "admin@example.com", "password123", is_admin=True)
        
        await access_manager.grant_cluster_permission(
            user.id, "cluster-1", AccessLevel.READ, admin.id
        )
        
        api_key, raw_key = await access_manager.create_api_key(
            user_id=user.id,
            name="test-key",
            cluster_permissions={"cluster-2": AccessLevel.WRITE}
        )
        
        # Get permissions summary
        summary = await access_manager.get_user_permissions_summary(user.id)
        
        assert summary["user_id"] == user.id
        assert summary["username"] == "testuser"
        assert summary["is_admin"] is False
        assert "cluster-1" in summary["cluster_permissions"]
        assert len(summary["api_keys"]) == 1
        assert summary["api_keys"][0]["name"] == "test-key"
    
    def test_default_roles_initialization(self, access_manager):
        """Test that default roles are properly initialized."""
        # Check that default roles exist
        assert "admin" in access_manager.roles
        assert "cluster_admin" in access_manager.roles
        assert "developer" in access_manager.roles
        assert "readonly" in access_manager.roles
        
        # Check admin role has all permissions
        admin_role = access_manager.roles["admin"]
        assert len(admin_role.permissions) == len(Permission)
        
        # Check readonly role has limited permissions
        readonly_role = access_manager.roles["readonly"]
        assert Permission.CLUSTER_VIEW in readonly_role.permissions
        assert Permission.CLUSTER_DELETE not in readonly_role.permissions


class TestSecurityErrorHandling:
    """Test error handling in security system."""
    
    @pytest.fixture
    def access_manager(self):
        """Create access control manager for testing."""
        return AccessControlManager(secret_key="test-secret-key")
    
    @pytest.mark.asyncio
    async def test_expired_token_handling(self, access_manager):
        """Test handling of expired tokens."""
        # Create a token that expires immediately
        user = await access_manager.create_user("testuser", "test@example.com", "password123")
        
        # Create expired token manually
        payload = {
            "user_id": user.id,
            "username": user.username,
            "is_admin": user.is_admin,
            "exp": datetime.utcnow() - timedelta(hours=1),  # Expired
            "iat": datetime.utcnow() - timedelta(hours=2)
        }
        
        expired_token = jwt.encode(payload, access_manager.secret_key, algorithm="HS256")
        
        # Verify token should return None for expired token
        result = await access_manager.verify_token(expired_token)
        assert result is None
    
    @pytest.mark.asyncio
    async def test_inactive_user_handling(self, access_manager):
        """Test handling of inactive users."""
        # Create user and deactivate
        user = await access_manager.create_user("testuser", "test@example.com", "password123")
        user.is_active = False
        
        # Token verification should fail for inactive user
        token = jwt.encode({
            "user_id": user.id,
            "username": user.username,
            "is_admin": user.is_admin,
            "exp": datetime.utcnow() + timedelta(hours=1),
            "iat": datetime.utcnow()
        }, access_manager.secret_key, algorithm="HS256")
        
        result = await access_manager.verify_token(token)
        assert result is None


class TestSecurityIntegration:
    """Integration tests for security system."""
    
    @pytest.mark.asyncio
    async def test_full_authentication_flow(self):
        """Test complete authentication and authorization flow."""
        # This would be an integration test that tests the full flow
        # of user creation, authentication, permission granting, and access checking
        pass
    
    @pytest.mark.asyncio
    async def test_api_key_workflow(self):
        """Test complete API key workflow."""
        # This would test creating, using, and revoking API keys
        pass
    
    @pytest.mark.asyncio
    async def test_audit_logging_integration(self):
        """Test audit logging across different operations."""
        # This would test that all security operations are properly logged
        pass