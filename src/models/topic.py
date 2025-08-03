"""Topic-related data models."""

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator, ConfigDict
from .base import BaseResponse


class TopicConfig(BaseModel):
    """Configuration for creating a Kafka topic."""
    name: str = Field(..., min_length=1, max_length=249, description="Topic name")
    partitions: int = Field(default=1, ge=1, le=1000, description="Number of partitions")
    replication_factor: int = Field(default=1, ge=1, le=10, description="Replication factor")
    config: Dict[str, str] = Field(default_factory=dict, description="Topic configuration properties")

    @field_validator('name')
    @classmethod
    def validate_topic_name(cls, v):
        """Validate topic name follows Kafka naming conventions."""
        if not v:
            raise ValueError("Topic name cannot be empty")
        
        # Kafka topic name restrictions
        invalid_chars = set('\\/:*?"<>|')
        if any(char in v for char in invalid_chars):
            raise ValueError(f"Topic name contains invalid characters: {invalid_chars}")
        
        if v in ['.', '..']:
            raise ValueError("Topic name cannot be '.' or '..'")
        
        if v.startswith('__'):
            raise ValueError("Topic names starting with '__' are reserved for internal topics")
        
        return v

    @field_validator('config')
    @classmethod
    def validate_config(cls, v):
        """Validate topic configuration properties."""
        # Common Kafka topic configs with basic validation
        numeric_configs = {
            'retention.ms', 'segment.ms', 'max.message.bytes', 
            'min.insync.replicas', 'segment.bytes'
        }
        
        for key, value in v.items():
            if key in numeric_configs:
                try:
                    int_val = int(value)
                    if int_val < 0:
                        raise ValueError(f"Config {key} must be non-negative")
                except ValueError:
                    raise ValueError(f"Config {key} must be a valid integer")
        
        return v


class TopicInfo(BaseResponse):
    """Information about an existing Kafka topic."""
    name: str = Field(..., description="Topic name")
    partitions: int = Field(..., ge=1, description="Number of partitions")
    replication_factor: int = Field(..., ge=1, description="Replication factor")
    size_bytes: Optional[int] = Field(None, ge=0, description="Topic size in bytes")
    config: Dict[str, str] = Field(default_factory=dict, description="Topic configuration")
    partition_details: Optional[List[Dict[str, Any]]] = Field(
        None, description="Detailed partition information"
    )


class TopicListResponse(BaseResponse):
    """Response containing list of topics."""
    topics: List[TopicInfo] = Field(default_factory=list, description="List of topics")
    total_count: int = Field(default=0, ge=0, description="Total number of topics")


class TopicCreateRequest(TopicConfig):
    """Request to create a new topic."""
    pass


class TopicDeleteRequest(BaseModel):
    """Request to delete a topic."""
    force: bool = Field(default=False, description="Force delete even if topic has data")


class TopicMetadata(BaseResponse):
    """Detailed metadata about a topic."""
    name: str = Field(..., description="Topic name")
    partitions: List[Dict[str, Any]] = Field(default_factory=list, description="Partition details")
    config: Dict[str, str] = Field(default_factory=dict, description="Topic configuration")
    consumer_groups: List[str] = Field(default_factory=list, description="Active consumer groups")
    message_count: Optional[int] = Field(None, ge=0, description="Approximate message count")
    earliest_offset: Optional[int] = Field(None, ge=0, description="Earliest available offset")
    latest_offset: Optional[int] = Field(None, ge=0, description="Latest offset")