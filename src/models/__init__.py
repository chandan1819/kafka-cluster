"""Data models package for Local Kafka Manager."""

# Base models and enums
from .base import ServiceStatus, BaseResponse, ErrorResponse

# Cluster models
from .cluster import (
    ServiceHealth,
    ClusterStatus,
    ClusterStartRequest,
    ClusterStopRequest,
)

# Topic models
from .topic import (
    TopicConfig,
    TopicInfo,
    TopicListResponse,
    TopicCreateRequest,
    TopicDeleteRequest,
    TopicMetadata,
)

# Message models
from .message import (
    Message,
    ProduceRequest,
    ProduceResult,
    ConsumeRequest,
    ConsumeResponse,
    ConsumerGroup,
    ConsumerGroupListResponse,
)

# Catalog models
from .catalog import (
    APIEndpoint,
    ServiceInfo,
    CatalogResponse,
)

__all__ = [
    # Base
    "ServiceStatus",
    "BaseResponse", 
    "ErrorResponse",
    # Cluster
    "ServiceHealth",
    "ClusterStatus",
    "ClusterStartRequest",
    "ClusterStopRequest",
    # Topic
    "TopicConfig",
    "TopicInfo",
    "TopicListResponse",
    "TopicCreateRequest",
    "TopicDeleteRequest",
    "TopicMetadata",
    # Message
    "Message",
    "ProduceRequest",
    "ProduceResult",
    "ConsumeRequest",
    "ConsumeResponse",
    "ConsumerGroup",
    "ConsumerGroupListResponse",
    # Catalog
    "APIEndpoint",
    "ServiceInfo",
    "CatalogResponse",
]