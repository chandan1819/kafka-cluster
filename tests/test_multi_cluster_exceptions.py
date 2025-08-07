"""
Unit tests for multi-cluster exceptions.
"""

import pytest
from src.exceptions import (
    MultiClusterError,
    ClusterNotFoundError,
    ClusterAlreadyExistsError,
    PortAllocationError,
    CrossClusterOperationError,
    TemplateNotFoundError,
    TemplateAlreadyExistsError,
    ClusterValidationError,
    NetworkIsolationError,
    StorageBackendError,
    ErrorCode
)


class TestMultiClusterError:
    """Test MultiClusterError base class."""
    
    def test_multi_cluster_error_creation(self):
        """Test creating MultiClusterError."""
        error = MultiClusterError("Test error")
        
        assert str(error) == "Test error"
        assert error.message == "Test error"
        assert isinstance(error, Exception)
    
    def test_multi_cluster_error_with_details(self):
        """Test MultiClusterError with details."""
        details = {"cluster_id": "test-cluster", "operation": "start"}
        error = MultiClusterError("Test error", details=details)
        
        assert error.details == details
        error_dict = error.to_dict()
        assert error_dict["details"]["cluster_id"] == "test-cluster"
        assert error_dict["details"]["operation"] == "start"


class TestClusterNotFoundError:
    """Test ClusterNotFoundError."""
    
    def test_cluster_not_found_error(self):
        """Test creating ClusterNotFoundError."""
        error = ClusterNotFoundError("test-cluster")
        
        assert "Cluster 'test-cluster' not found" in str(error)
        assert error.cluster_id == "test-cluster"
        assert error.error_code == ErrorCode.NOT_FOUND
    
    def test_cluster_not_found_error_with_details(self):
        """Test ClusterNotFoundError with additional details."""
        details = {"requested_by": "user123"}
        error = ClusterNotFoundError("test-cluster", details=details)
        
        assert error.cluster_id == "test-cluster"
        assert error.details["cluster_id"] == "test-cluster"
        assert error.details["requested_by"] == "user123"
    
    def test_cluster_not_found_error_dict(self):
        """Test converting ClusterNotFoundError to dict."""
        error = ClusterNotFoundError("test-cluster")
        error_dict = error.to_dict()
        
        assert error_dict["error"] == ErrorCode.NOT_FOUND.value
        assert "test-cluster" in error_dict["message"]
        assert error_dict["details"]["cluster_id"] == "test-cluster"


class TestClusterAlreadyExistsError:
    """Test ClusterAlreadyExistsError."""
    
    def test_cluster_already_exists_error(self):
        """Test creating ClusterAlreadyExistsError."""
        error = ClusterAlreadyExistsError("existing-cluster")
        
        assert "Cluster 'existing-cluster' already exists" in str(error)
        assert error.cluster_id == "existing-cluster"
        assert error.error_code == ErrorCode.CONFLICT
    
    def test_cluster_already_exists_error_with_details(self):
        """Test ClusterAlreadyExistsError with additional details."""
        details = {"created_at": "2024-01-01T00:00:00Z"}
        error = ClusterAlreadyExistsError("existing-cluster", details=details)
        
        assert error.cluster_id == "existing-cluster"
        assert error.details["cluster_id"] == "existing-cluster"
        assert error.details["created_at"] == "2024-01-01T00:00:00Z"


class TestPortAllocationError:
    """Test PortAllocationError."""
    
    def test_port_allocation_error(self):
        """Test creating PortAllocationError."""
        requested_ports = [9092, 8082, 8080]
        conflicting_ports = [9092, 8082]
        
        error = PortAllocationError(
            "Unable to allocate ports",
            requested_ports=requested_ports,
            conflicting_ports=conflicting_ports
        )
        
        assert "Unable to allocate ports" in str(error)
        assert error.requested_ports == requested_ports
        assert error.conflicting_ports == conflicting_ports
        assert error.error_code == ErrorCode.CONFLICT
    
    def test_port_allocation_error_without_ports(self):
        """Test PortAllocationError without port lists."""
        error = PortAllocationError("Port allocation failed")
        
        assert error.requested_ports == []
        assert error.conflicting_ports == []
        assert error.details["requested_ports"] == []
        assert error.details["conflicting_ports"] == []
    
    def test_port_allocation_error_with_details(self):
        """Test PortAllocationError with additional details."""
        details = {"cluster_id": "test-cluster"}
        error = PortAllocationError(
            "Port allocation failed",
            requested_ports=[9092],
            details=details
        )
        
        assert error.details["cluster_id"] == "test-cluster"
        assert error.details["requested_ports"] == [9092]


class TestCrossClusterOperationError:
    """Test CrossClusterOperationError."""
    
    def test_cross_cluster_operation_error(self):
        """Test creating CrossClusterOperationError."""
        error = CrossClusterOperationError(
            "Migration failed",
            operation_id="op-123",
            operation_type="migrate"
        )
        
        assert "Migration failed" in str(error)
        assert error.operation_id == "op-123"
        assert error.operation_type == "migrate"
        assert error.error_code == ErrorCode.INTERNAL_SERVER_ERROR
    
    def test_cross_cluster_operation_error_with_cause(self):
        """Test CrossClusterOperationError with cause."""
        cause = ValueError("Invalid configuration")
        error = CrossClusterOperationError(
            "Operation failed",
            operation_id="op-456",
            cause=cause
        )
        
        assert error.cause == cause
        assert error.operation_id == "op-456"
        
        error_dict = error.to_dict()
        assert "Invalid configuration" in error_dict["details"]["cause"]
        assert error_dict["details"]["cause_type"] == "ValueError"
    
    def test_cross_cluster_operation_error_minimal(self):
        """Test CrossClusterOperationError with minimal parameters."""
        error = CrossClusterOperationError("Operation failed")
        
        assert error.operation_id is None
        assert error.operation_type is None
        assert error.details["operation_id"] is None
        assert error.details["operation_type"] is None


class TestTemplateNotFoundError:
    """Test TemplateNotFoundError."""
    
    def test_template_not_found_error(self):
        """Test creating TemplateNotFoundError."""
        error = TemplateNotFoundError("dev-template")
        
        assert "Template 'dev-template' not found" in str(error)
        assert error.template_id == "dev-template"
        assert error.error_code == ErrorCode.NOT_FOUND
    
    def test_template_not_found_error_with_details(self):
        """Test TemplateNotFoundError with additional details."""
        details = {"category": "development"}
        error = TemplateNotFoundError("dev-template", details=details)
        
        assert error.template_id == "dev-template"
        assert error.details["template_id"] == "dev-template"
        assert error.details["category"] == "development"


class TestTemplateAlreadyExistsError:
    """Test TemplateAlreadyExistsError."""
    
    def test_template_already_exists_error(self):
        """Test creating TemplateAlreadyExistsError."""
        error = TemplateAlreadyExistsError("existing-template")
        
        assert "Template 'existing-template' already exists" in str(error)
        assert error.template_id == "existing-template"
        assert error.error_code == ErrorCode.CONFLICT
    
    def test_template_already_exists_error_with_details(self):
        """Test TemplateAlreadyExistsError with additional details."""
        details = {"version": "1.0.0"}
        error = TemplateAlreadyExistsError("existing-template", details=details)
        
        assert error.template_id == "existing-template"
        assert error.details["template_id"] == "existing-template"
        assert error.details["version"] == "1.0.0"


class TestClusterValidationError:
    """Test ClusterValidationError."""
    
    def test_cluster_validation_error(self):
        """Test creating ClusterValidationError."""
        validation_errors = ["Invalid port range", "Missing required field"]
        error = ClusterValidationError(
            "Cluster validation failed",
            cluster_id="test-cluster",
            validation_errors=validation_errors
        )
        
        assert "Cluster validation failed" in str(error)
        assert error.cluster_id == "test-cluster"
        assert error.validation_errors == validation_errors
        assert error.error_code == ErrorCode.VALIDATION_ERROR
    
    def test_cluster_validation_error_without_errors(self):
        """Test ClusterValidationError without validation errors list."""
        error = ClusterValidationError("Validation failed")
        
        assert error.cluster_id is None
        assert error.validation_errors == []
        assert error.details["validation_errors"] == []
    
    def test_cluster_validation_error_with_details(self):
        """Test ClusterValidationError with additional details."""
        details = {"field": "kafka_config.heap_size"}
        error = ClusterValidationError(
            "Invalid configuration",
            cluster_id="test-cluster",
            validation_errors=["Invalid heap size format"],
            details=details
        )
        
        assert error.details["cluster_id"] == "test-cluster"
        assert error.details["validation_errors"] == ["Invalid heap size format"]
        assert error.details["field"] == "kafka_config.heap_size"


class TestNetworkIsolationError:
    """Test NetworkIsolationError."""
    
    def test_network_isolation_error(self):
        """Test creating NetworkIsolationError."""
        error = NetworkIsolationError(
            "Failed to create network",
            cluster_id="test-cluster",
            network_name="test-network"
        )
        
        assert "Failed to create network" in str(error)
        assert error.cluster_id == "test-cluster"
        assert error.network_name == "test-network"
        assert error.error_code == ErrorCode.INTERNAL_SERVER_ERROR
    
    def test_network_isolation_error_with_cause(self):
        """Test NetworkIsolationError with cause."""
        cause = RuntimeError("Docker network creation failed")
        error = NetworkIsolationError(
            "Network setup failed",
            cluster_id="test-cluster",
            cause=cause
        )
        
        assert error.cause == cause
        assert error.cluster_id == "test-cluster"
        assert error.network_name is None
        
        error_dict = error.to_dict()
        assert "Docker network creation failed" in error_dict["details"]["cause"]
    
    def test_network_isolation_error_minimal(self):
        """Test NetworkIsolationError with minimal parameters."""
        error = NetworkIsolationError("Network error")
        
        assert error.cluster_id is None
        assert error.network_name is None
        assert error.details["cluster_id"] is None
        assert error.details["network_name"] is None


class TestStorageBackendError:
    """Test StorageBackendError."""
    
    def test_storage_backend_error(self):
        """Test creating StorageBackendError."""
        error = StorageBackendError(
            "Failed to save cluster",
            backend_type="file",
            operation="save"
        )
        
        assert "Failed to save cluster" in str(error)
        assert error.backend_type == "file"
        assert error.operation == "save"
        assert error.error_code == ErrorCode.INTERNAL_SERVER_ERROR
    
    def test_storage_backend_error_with_cause(self):
        """Test StorageBackendError with cause."""
        cause = OSError("Permission denied")
        error = StorageBackendError(
            "Storage operation failed",
            backend_type="database",
            operation="load",
            cause=cause
        )
        
        assert error.cause == cause
        assert error.backend_type == "database"
        assert error.operation == "load"
        
        error_dict = error.to_dict()
        assert "Permission denied" in error_dict["details"]["cause"]
        assert error_dict["details"]["cause_type"] == "OSError"
    
    def test_storage_backend_error_with_details(self):
        """Test StorageBackendError with additional details."""
        details = {"file_path": "/data/clusters.json"}
        error = StorageBackendError(
            "File not found",
            backend_type="file",
            operation="load",
            details=details
        )
        
        assert error.details["backend_type"] == "file"
        assert error.details["operation"] == "load"
        assert error.details["file_path"] == "/data/clusters.json"
    
    def test_storage_backend_error_minimal(self):
        """Test StorageBackendError with minimal parameters."""
        error = StorageBackendError("Storage error")
        
        assert error.backend_type is None
        assert error.operation is None
        assert error.details["backend_type"] is None
        assert error.details["operation"] is None


class TestExceptionInheritance:
    """Test exception inheritance hierarchy."""
    
    def test_multi_cluster_error_inheritance(self):
        """Test that all multi-cluster exceptions inherit from MultiClusterError."""
        exceptions = [
            ClusterNotFoundError("test"),
            ClusterAlreadyExistsError("test"),
            PortAllocationError("test"),
            CrossClusterOperationError("test"),
            TemplateNotFoundError("test"),
            TemplateAlreadyExistsError("test"),
            ClusterValidationError("test"),
            NetworkIsolationError("test"),
            StorageBackendError("test")
        ]
        
        for exception in exceptions:
            assert isinstance(exception, MultiClusterError)
            assert isinstance(exception, Exception)
    
    def test_error_dict_format(self):
        """Test that all exceptions produce consistent error dictionaries."""
        exceptions = [
            ClusterNotFoundError("test-cluster"),
            ClusterAlreadyExistsError("test-cluster"),
            PortAllocationError("test message"),
            CrossClusterOperationError("test message"),
            TemplateNotFoundError("test-template"),
            TemplateAlreadyExistsError("test-template"),
            ClusterValidationError("test message"),
            NetworkIsolationError("test message"),
            StorageBackendError("test message")
        ]
        
        for exception in exceptions:
            error_dict = exception.to_dict()
            
            # Check required fields
            assert "error" in error_dict
            assert "message" in error_dict
            assert "details" in error_dict
            
            # Check error code is valid
            assert isinstance(error_dict["error"], str)
            assert error_dict["error"] in [code.value for code in ErrorCode]
            
            # Check message is not empty
            assert len(error_dict["message"]) > 0
            
            # Check details is a dictionary
            assert isinstance(error_dict["details"], dict)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])