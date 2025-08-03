"""Message-related data models."""

from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field, field_validator
from .base import BaseResponse


class Message(BaseModel):
    """Kafka message representation."""
    topic: str = Field(..., min_length=1, description="Topic name")
    partition: int = Field(..., ge=0, description="Partition number")
    offset: int = Field(..., ge=0, description="Message offset")
    key: Optional[str] = Field(None, description="Message key")
    value: Dict[str, Any] = Field(..., description="Message value as JSON")
    timestamp: int = Field(..., ge=0, description="Message timestamp (epoch milliseconds)")
    headers: Optional[Dict[str, str]] = Field(None, description="Message headers")

    @field_validator('timestamp')
    @classmethod
    def validate_timestamp(cls, v):
        """Validate timestamp is reasonable."""
        # Check if timestamp is in milliseconds (reasonable range)
        if v < 1000000000000:  # Before year 2001 in milliseconds
            raise ValueError("Timestamp appears to be too old or in wrong format")
        if v > 9999999999999:  # After year 2286 in milliseconds
            raise ValueError("Timestamp appears to be too far in the future")
        return v


class ProduceRequest(BaseModel):
    """Request to produce a message to Kafka."""
    topic: str = Field(..., min_length=1, description="Target topic name")
    key: Optional[str] = Field(None, description="Message key")
    value: Dict[str, Any] = Field(..., description="Message value as JSON")
    partition: Optional[int] = Field(None, ge=0, description="Target partition (optional)")
    headers: Optional[Dict[str, str]] = Field(None, description="Message headers")

    @field_validator('value')
    @classmethod
    def validate_value(cls, v):
        """Validate message value is serializable."""
        if not isinstance(v, dict):
            raise ValueError("Message value must be a JSON object (dict)")
        return v


class ProduceResult(BaseResponse):
    """Result of message production."""
    topic: str = Field(..., description="Topic name")
    partition: int = Field(..., ge=0, description="Partition where message was stored")
    offset: int = Field(..., ge=0, description="Offset of the produced message")
    key: Optional[str] = Field(None, description="Message key")
    timestamp: int = Field(..., description="Message timestamp")


class ConsumeRequest(BaseModel):
    """Request to consume messages from Kafka."""
    topic: str = Field(..., min_length=1, description="Topic to consume from")
    consumer_group: str = Field(..., min_length=1, description="Consumer group ID")
    max_messages: int = Field(default=10, ge=1, le=1000, description="Maximum messages to fetch")
    timeout_ms: int = Field(default=5000, ge=100, le=60000, description="Consumer timeout in milliseconds")
    from_beginning: bool = Field(default=False, description="Start consuming from beginning")
    partition: Optional[int] = Field(None, ge=0, description="Specific partition to consume from")

    @field_validator('consumer_group')
    @classmethod
    def validate_consumer_group(cls, v):
        """Validate consumer group name."""
        if not v.strip():
            raise ValueError("Consumer group cannot be empty or whitespace")
        
        # Basic validation for consumer group naming
        invalid_chars = set('\\/:*?"<>|')
        if any(char in v for char in invalid_chars):
            raise ValueError(f"Consumer group contains invalid characters: {invalid_chars}")
        
        return v.strip()


class ConsumeResponse(BaseResponse):
    """Response containing consumed messages."""
    messages: List[Message] = Field(default_factory=list, description="Consumed messages")
    consumer_group: str = Field(..., description="Consumer group ID")
    topic: str = Field(..., description="Topic name")
    total_consumed: int = Field(default=0, ge=0, description="Number of messages consumed")
    has_more: bool = Field(default=False, description="Whether more messages are available")


class ConsumerGroup(BaseModel):
    """Information about a consumer group."""
    group_id: str = Field(..., description="Consumer group ID")
    state: str = Field(..., description="Consumer group state")
    members: List[Dict[str, Any]] = Field(default_factory=list, description="Group members")
    coordinator: Optional[str] = Field(None, description="Group coordinator")
    partition_assignments: Dict[str, List[int]] = Field(
        default_factory=dict, description="Topic partition assignments"
    )


class ConsumerGroupListResponse(BaseResponse):
    """Response containing list of consumer groups."""
    consumer_groups: List[ConsumerGroup] = Field(
        default_factory=list, description="List of consumer groups"
    )
    total_count: int = Field(default=0, ge=0, description="Total number of consumer groups")