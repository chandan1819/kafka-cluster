"""Tests for base data models."""

import pytest
from datetime import datetime
from pydantic import ValidationError

from src.models.base import ServiceStatus, BaseResponse, ErrorResponse


@pytest.mark.unit
class TestServiceStatus:
    """Test ServiceStatus enum."""

    def test_service_status_values(self):
        """Test all ServiceStatus enum values."""
        assert ServiceStatus.RUNNING == "running"
        assert ServiceStatus.STOPPED == "stopped"
        assert ServiceStatus.STARTING == "starting"
        assert ServiceStatus.ERROR == "error"

    def test_service_status_membership(self):
        """Test ServiceStatus membership."""
        assert ServiceStatus.RUNNING in ServiceStatus
        assert "running" in [status.value for status in ServiceStatus]
        assert "invalid" not in [status.value for status in ServiceStatus]


@pytest.mark.unit
class TestBaseResponse:
    """Test BaseResponse model."""

    def test_base_response_creation(self):
        """Test BaseResponse creation with default timestamp."""
        response = BaseResponse()
        assert isinstance(response.timestamp, datetime)
        assert response.request_id is None

    def test_base_response_with_request_id(self):
        """Test BaseResponse with request ID."""
        response = BaseResponse(request_id="test-123")
        assert response.request_id == "test-123"

    def test_base_response_json_serialization(self):
        """Test BaseResponse JSON serialization."""
        response = BaseResponse(request_id="test-123")
        json_data = response.model_dump()
        assert "timestamp" in json_data
        assert json_data["request_id"] == "test-123"


@pytest.mark.unit
class TestErrorResponse:
    """Test ErrorResponse model."""

    def test_error_response_creation(self):
        """Test ErrorResponse creation."""
        error = ErrorResponse(
            error="TEST_ERROR",
            message="Test error message"
        )
        assert error.error == "TEST_ERROR"
        assert error.message == "Test error message"
        assert error.details is None
        assert isinstance(error.timestamp, datetime)

    def test_error_response_with_details(self):
        """Test ErrorResponse with details."""
        details = {"code": 500, "service": "kafka"}
        error = ErrorResponse(
            error="SERVICE_ERROR",
            message="Service unavailable",
            details=details
        )
        assert error.details == details

    def test_error_response_validation(self):
        """Test ErrorResponse validation."""
        # Missing required fields should raise ValidationError
        with pytest.raises(ValidationError):
            ErrorResponse()
        
        with pytest.raises(ValidationError):
            ErrorResponse(error="TEST_ERROR")  # Missing message

    def test_error_response_json_serialization(self):
        """Test ErrorResponse JSON serialization."""
        error = ErrorResponse(
            error="TEST_ERROR",
            message="Test message",
            details={"key": "value"}
        )
        json_data = error.model_dump()
        assert json_data["error"] == "TEST_ERROR"
        assert json_data["message"] == "Test message"
        assert json_data["details"] == {"key": "value"}
        assert "timestamp" in json_data