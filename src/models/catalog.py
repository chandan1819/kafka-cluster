"""Service catalog data models."""

from typing import Dict, List, Any
from pydantic import BaseModel, Field
from .base import BaseResponse, ServiceStatus
from .cluster import ClusterStatus
from .topic import TopicInfo


class APIEndpoint(BaseModel):
    """Information about an API endpoint."""
    path: str = Field(..., description="Endpoint path")
    method: str = Field(..., description="HTTP method")
    description: str = Field(..., description="Endpoint description")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Endpoint parameters")


class ServiceInfo(BaseModel):
    """Information about a service."""
    name: str = Field(..., description="Service name")
    status: ServiceStatus = Field(..., description="Service status")
    url: str = Field(..., description="Service URL")
    version: str = Field(default="unknown", description="Service version")
    health_check_url: str = Field(default="", description="Health check endpoint")


class CatalogResponse(BaseResponse):
    """Complete service catalog response."""
    cluster: ClusterStatus = Field(..., description="Cluster status information")
    topics: List[TopicInfo] = Field(default_factory=list, description="Available topics")
    available_apis: List[APIEndpoint] = Field(
        default_factory=list, description="Available API endpoints"
    )
    services: Dict[str, ServiceInfo] = Field(
        default_factory=dict, description="Service information"
    )
    system_info: Dict[str, Any] = Field(
        default_factory=dict, description="System information"
    )