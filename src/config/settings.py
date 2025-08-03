"""
Configuration management for Local Kafka Manager.

This module provides configuration classes for all service settings with
environment variable support, validation, and deployment environment handling.
"""

import os
from typing import Dict, Any, Optional, List
from enum import Enum
from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings


class Environment(str, Enum):
    """Supported deployment environments."""
    DEVELOPMENT = "development"
    TESTING = "testing"
    PRODUCTION = "production"


class LogLevel(str, Enum):
    """Supported logging levels."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class KafkaConfig(BaseModel):
    """Kafka broker configuration settings."""
    
    # Connection settings
    bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Kafka bootstrap servers"
    )
    broker_port: int = Field(
        default=9092,
        ge=1024,
        le=65535,
        description="Kafka broker port"
    )
    jmx_port: int = Field(
        default=9101,
        ge=1024,
        le=65535,
        description="Kafka JMX port"
    )
    
    # Cluster settings
    node_id: int = Field(
        default=1,
        ge=1,
        description="Kafka node ID"
    )
    cluster_id: str = Field(
        default="MkU3OEVBNTcwNTJENDM2Qk",
        min_length=22,
        max_length=22,
        description="Kafka cluster ID (base64 encoded UUID)"
    )
    
    # Topic defaults
    default_partitions: int = Field(
        default=1,
        ge=1,
        description="Default number of partitions for new topics"
    )
    default_replication_factor: int = Field(
        default=1,
        ge=1,
        description="Default replication factor for new topics"
    )
    
    # Timeouts and limits
    request_timeout_ms: int = Field(
        default=30000,
        ge=1000,
        description="Request timeout in milliseconds"
    )
    connection_timeout_ms: int = Field(
        default=10000,
        ge=1000,
        description="Connection timeout in milliseconds"
    )
    
    # Topic configuration defaults
    default_topic_configs: Dict[str, str] = Field(
        default_factory=lambda: {
            "cleanup.policy": "delete",
            "retention.ms": "604800000",  # 7 days
            "segment.ms": "86400000",     # 1 day
            "max.message.bytes": "1000000"  # 1MB
        },
        description="Default topic configuration properties"
    )

    @field_validator('bootstrap_servers')
    @classmethod
    def validate_bootstrap_servers(cls, v):
        """Validate bootstrap servers format."""
        if not v:
            raise ValueError("Bootstrap servers cannot be empty")
        
        # Split by comma and validate each server
        servers = [s.strip() for s in v.split(',')]
        for server in servers:
            if ':' not in server:
                raise ValueError(f"Invalid server format: {server}. Expected host:port")
            host, port = server.rsplit(':', 1)
            if not host:
                raise ValueError(f"Invalid host in server: {server}")
            try:
                port_num = int(port)
                if not (1 <= port_num <= 65535):
                    raise ValueError(f"Invalid port in server: {server}")
            except ValueError:
                raise ValueError(f"Invalid port in server: {server}")
        
        return v


class KafkaRestProxyConfig(BaseModel):
    """Kafka REST Proxy configuration settings."""
    
    host: str = Field(
        default="localhost",
        description="REST Proxy host"
    )
    port: int = Field(
        default=8082,
        ge=1024,
        le=65535,
        description="REST Proxy port"
    )
    base_url: str = Field(
        default="http://localhost:8082",
        description="REST Proxy base URL"
    )
    
    # Request settings
    request_timeout: int = Field(
        default=30,
        ge=1,
        description="Request timeout in seconds"
    )
    max_retries: int = Field(
        default=3,
        ge=0,
        description="Maximum number of retries for failed requests"
    )
    retry_delay: float = Field(
        default=1.0,
        ge=0.1,
        description="Delay between retries in seconds"
    )
    
    # Consumer settings
    consumer_timeout_ms: int = Field(
        default=5000,
        ge=1000,
        description="Consumer timeout in milliseconds"
    )
    max_messages_per_request: int = Field(
        default=100,
        ge=1,
        le=10000,
        description="Maximum messages per consume request"
    )

    @model_validator(mode='after')
    def validate_base_url(self):
        """Ensure base_url matches host and port."""
        expected_url = f"http://{self.host}:{self.port}"
        if self.base_url != expected_url:
            self.base_url = expected_url
        
        return self


class KafkaUIConfig(BaseModel):
    """Kafka UI configuration settings."""
    
    host: str = Field(
        default="localhost",
        description="Kafka UI host"
    )
    port: int = Field(
        default=8080,
        ge=1024,
        le=65535,
        description="Kafka UI port"
    )
    base_url: str = Field(
        default="http://localhost:8080",
        description="Kafka UI base URL"
    )
    
    # UI settings
    cluster_name: str = Field(
        default="local",
        description="Display name for the Kafka cluster in UI"
    )
    dynamic_config_enabled: bool = Field(
        default=True,
        description="Enable dynamic configuration in UI"
    )
    
    # Health check settings
    health_check_path: str = Field(
        default="/actuator/health",
        description="Health check endpoint path"
    )
    health_check_timeout: int = Field(
        default=10,
        ge=1,
        description="Health check timeout in seconds"
    )

    @model_validator(mode='after')
    def validate_base_url(self):
        """Ensure base_url matches host and port."""
        expected_url = f"http://{self.host}:{self.port}"
        if self.base_url != expected_url:
            self.base_url = expected_url
        
        return self


class APIConfig(BaseModel):
    """FastAPI application configuration settings."""
    
    # Server settings
    host: str = Field(
        default="0.0.0.0",
        description="API server host"
    )
    port: int = Field(
        default=8000,
        ge=1024,
        le=65535,
        description="API server port"
    )
    reload: bool = Field(
        default=False,
        description="Enable auto-reload in development"
    )
    
    # API metadata
    title: str = Field(
        default="Local Kafka Manager",
        description="API title"
    )
    description: str = Field(
        default="A self-service local Kafka cluster management solution for developers",
        description="API description"
    )
    version: str = Field(
        default="1.0.0",
        description="API version"
    )
    
    # Documentation settings
    docs_url: str = Field(
        default="/docs",
        description="Swagger UI documentation URL"
    )
    redoc_url: str = Field(
        default="/redoc",
        description="ReDoc documentation URL"
    )
    openapi_url: str = Field(
        default="/openapi.json",
        description="OpenAPI schema URL"
    )
    
    # CORS settings
    cors_origins: List[str] = Field(
        default_factory=lambda: ["*"],
        description="Allowed CORS origins"
    )
    cors_methods: List[str] = Field(
        default_factory=lambda: ["*"],
        description="Allowed CORS methods"
    )
    cors_headers: List[str] = Field(
        default_factory=lambda: ["*"],
        description="Allowed CORS headers"
    )


class DockerConfig(BaseModel):
    """Docker and Docker Compose configuration settings."""
    
    # Docker Compose settings
    compose_file: str = Field(
        default="docker-compose.yml",
        description="Docker Compose file path"
    )
    project_name: str = Field(
        default="local-kafka-manager",
        description="Docker Compose project name"
    )
    
    # Network settings
    network_name: str = Field(
        default="kafka-network",
        description="Docker network name"
    )
    
    # Volume settings
    kafka_data_volume: str = Field(
        default="kafka-data",
        description="Kafka data volume name"
    )
    
    # Container settings
    container_start_timeout: int = Field(
        default=120,
        ge=30,
        description="Container startup timeout in seconds"
    )
    container_stop_timeout: int = Field(
        default=30,
        ge=5,
        description="Container stop timeout in seconds"
    )
    
    # Health check settings
    health_check_interval: int = Field(
        default=30,
        ge=5,
        description="Health check interval in seconds"
    )
    health_check_timeout: int = Field(
        default=10,
        ge=1,
        description="Health check timeout in seconds"
    )
    health_check_retries: int = Field(
        default=3,
        ge=1,
        description="Health check retry count"
    )


class MonitoringConfig(BaseModel):
    """Health monitoring and observability configuration."""
    
    # Health monitoring
    health_check_interval: int = Field(
        default=30,
        ge=5,
        description="Health check interval in seconds"
    )
    health_check_timeout: int = Field(
        default=10,
        ge=1,
        description="Health check timeout in seconds"
    )
    
    # Logging settings
    log_level: LogLevel = Field(
        default=LogLevel.INFO,
        description="Application log level"
    )
    log_format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="Log message format"
    )
    
    # Metrics settings
    enable_metrics: bool = Field(
        default=True,
        description="Enable metrics collection"
    )
    metrics_port: int = Field(
        default=9090,
        ge=1024,
        le=65535,
        description="Metrics server port"
    )


class Settings(BaseSettings):
    """Main application settings with environment variable support."""
    
    # Environment settings
    environment: Environment = Field(
        default=Environment.DEVELOPMENT,
        description="Deployment environment"
    )
    debug: bool = Field(
        default=False,
        description="Enable debug mode"
    )
    
    # Component configurations
    kafka: KafkaConfig = Field(
        default_factory=KafkaConfig,
        description="Kafka configuration"
    )
    kafka_rest_proxy: KafkaRestProxyConfig = Field(
        default_factory=KafkaRestProxyConfig,
        description="Kafka REST Proxy configuration"
    )
    kafka_ui: KafkaUIConfig = Field(
        default_factory=KafkaUIConfig,
        description="Kafka UI configuration"
    )
    api: APIConfig = Field(
        default_factory=APIConfig,
        description="FastAPI configuration"
    )
    docker: DockerConfig = Field(
        default_factory=DockerConfig,
        description="Docker configuration"
    )
    monitoring: MonitoringConfig = Field(
        default_factory=MonitoringConfig,
        description="Monitoring configuration"
    )

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "env_nested_delimiter": "__",
        "case_sensitive": False,
        "env_prefix": "LKM_",
        "extra": "ignore"  # Ignore extra fields from .env file
    }

    @model_validator(mode='after')
    def validate_environment_specific_settings(self):
        """Apply environment-specific configuration overrides."""
        # Development environment overrides
        if self.environment == Environment.DEVELOPMENT:
            self.api.reload = True
            self.monitoring.log_level = LogLevel.DEBUG
            self.debug = True
        
        # Testing environment overrides
        elif self.environment == Environment.TESTING:
            self.monitoring.log_level = LogLevel.WARNING
            self.monitoring.enable_metrics = False
            self.debug = False
        
        # Production environment overrides
        elif self.environment == Environment.PRODUCTION:
            self.api.reload = False
            self.monitoring.log_level = LogLevel.INFO
            self.debug = False
        
        return self

    def get_kafka_bootstrap_servers(self) -> str:
        """Get Kafka bootstrap servers for client connections."""
        return self.kafka.bootstrap_servers

    def get_rest_proxy_url(self) -> str:
        """Get Kafka REST Proxy URL for HTTP requests."""
        return self.kafka_rest_proxy.base_url

    def get_kafka_ui_url(self) -> str:
        """Get Kafka UI URL for web interface."""
        return self.kafka_ui.base_url

    def get_api_base_url(self) -> str:
        """Get API base URL."""
        return f"http://{self.api.host}:{self.api.port}"

    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.environment == Environment.DEVELOPMENT

    def is_testing(self) -> bool:
        """Check if running in testing environment."""
        return self.environment == Environment.TESTING

    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment == Environment.PRODUCTION

    def to_dict(self) -> Dict[str, Any]:
        """Convert settings to dictionary."""
        return self.dict()

    def to_env_dict(self) -> Dict[str, str]:
        """Convert settings to environment variable dictionary."""
        env_dict = {}
        
        def flatten_dict(d: Dict[str, Any], prefix: str = "LKM_") -> None:
            for key, value in d.items():
                env_key = f"{prefix}{key.upper()}"
                if isinstance(value, dict):
                    flatten_dict(value, f"{env_key}__")
                elif isinstance(value, list):
                    env_dict[env_key] = ",".join(str(v) for v in value)
                else:
                    env_dict[env_key] = str(value)
        
        flatten_dict(self.dict())
        return env_dict


# Global settings instance
settings = Settings()


def get_settings() -> Settings:
    """Get the global settings instance."""
    return settings


def reload_settings() -> Settings:
    """Reload settings from environment variables and files."""
    global settings
    settings = Settings()
    return settings