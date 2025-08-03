"""Cluster-related data models."""

from typing import Dict, Optional
from pydantic import BaseModel, Field, field_validator
from .base import ServiceStatus, BaseResponse


class ServiceHealth(BaseModel):
    """Health information for a service."""
    status: ServiceStatus
    uptime: Optional[int] = Field(None, description="Service uptime in seconds")
    last_check: Optional[str] = Field(None, description="Last health check timestamp")
    error_message: Optional[str] = Field(None, description="Error message if status is ERROR")


class ClusterStatus(BaseResponse):
    """Status information for the Kafka cluster."""
    status: ServiceStatus = Field(..., description="Overall cluster status")
    broker_count: int = Field(default=0, ge=0, description="Number of active brokers")
    version: Optional[str] = Field(None, description="Kafka version")
    endpoints: Dict[str, str] = Field(default_factory=dict, description="Service endpoints")
    uptime: Optional[int] = Field(None, ge=0, description="Cluster uptime in seconds")
    services: Dict[str, ServiceHealth] = Field(
        default_factory=dict, 
        description="Health status of individual services"
    )

    @field_validator('broker_count')
    @classmethod
    def validate_broker_count(cls, v):
        """Validate broker count is reasonable."""
        if v < 0:
            raise ValueError("Broker count cannot be negative")
        if v > 100:  # Reasonable upper limit for local development
            raise ValueError("Broker count seems unreasonably high for local development")
        return v

    @field_validator('endpoints')
    @classmethod
    def validate_endpoints(cls, v):
        """Validate endpoint URLs are properly formatted."""
        for service, endpoint in v.items():
            if endpoint and not (endpoint.startswith('http://') or endpoint.startswith('https://')):
                # Allow localhost patterns without protocol
                if not endpoint.startswith('localhost:'):
                    raise ValueError(f"Invalid endpoint format for {service}: {endpoint}")
        return v


class ClusterStartRequest(BaseModel):
    """Request to start the cluster."""
    force: bool = Field(default=False, description="Force start even if already running")
    timeout: int = Field(default=60, ge=10, le=300, description="Startup timeout in seconds")


class ClusterStopRequest(BaseModel):
    """Request to stop the cluster."""
    force: bool = Field(default=False, description="Force stop containers")
    cleanup: bool = Field(default=False, description="Remove containers and volumes")
    timeout: int = Field(default=30, ge=5, le=120, description="Stop timeout in seconds")