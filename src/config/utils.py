"""
Configuration utilities for Local Kafka Manager.

This module provides utility functions for configuration management,
validation, and environment-specific overrides.
"""

import os
import json
import yaml
from typing import Dict, Any, Optional, Union
from pathlib import Path

from .settings import Settings, Environment


def load_config_from_file(file_path: Union[str, Path]) -> Dict[str, Any]:
    """
    Load configuration from a file (JSON or YAML).
    
    Args:
        file_path: Path to the configuration file
        
    Returns:
        Dictionary containing configuration data
        
    Raises:
        FileNotFoundError: If the file doesn't exist
        ValueError: If the file format is not supported or invalid
    """
    file_path = Path(file_path)
    
    if not file_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            if file_path.suffix.lower() in ['.json']:
                return json.load(f)
            elif file_path.suffix.lower() in ['.yml', '.yaml']:
                return yaml.safe_load(f) or {}
            else:
                raise ValueError(f"Unsupported file format: {file_path.suffix}")
    except (json.JSONDecodeError, yaml.YAMLError) as e:
        raise ValueError(f"Invalid configuration file format: {e}")


def save_config_to_file(config: Dict[str, Any], file_path: Union[str, Path]) -> None:
    """
    Save configuration to a file (JSON or YAML).
    
    Args:
        config: Configuration dictionary to save
        file_path: Path where to save the configuration file
        
    Raises:
        ValueError: If the file format is not supported
    """
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            if file_path.suffix.lower() in ['.json']:
                json.dump(config, f, indent=2, default=str)
            elif file_path.suffix.lower() in ['.yml', '.yaml']:
                yaml.dump(config, f, default_flow_style=False, indent=2)
            else:
                raise ValueError(f"Unsupported file format: {file_path.suffix}")
    except Exception as e:
        raise ValueError(f"Failed to save configuration file: {e}")


def create_settings_from_dict(config_dict: Dict[str, Any]) -> Settings:
    """
    Create Settings instance from configuration dictionary.
    
    Args:
        config_dict: Configuration dictionary
        
    Returns:
        Settings instance
    """
    # Convert nested dictionaries to match Settings structure
    settings_dict = {}
    
    # Map top-level keys to Settings fields
    field_mapping = {
        'environment': 'environment',
        'debug': 'debug',
        'kafka': 'kafka',
        'kafka_rest_proxy': 'kafka_rest_proxy',
        'kafka_ui': 'kafka_ui',
        'api': 'api',
        'docker': 'docker',
        'monitoring': 'monitoring'
    }
    
    for key, value in config_dict.items():
        if key in field_mapping:
            settings_dict[field_mapping[key]] = value
    
    return Settings(**settings_dict)


def get_environment_config_path(environment: Environment) -> Path:
    """
    Get the configuration file path for a specific environment.
    
    Args:
        environment: Target environment
        
    Returns:
        Path to the environment-specific configuration file
    """
    config_dir = Path("config")
    return config_dir / f"{environment.value}.yml"


def load_environment_config(environment: Environment) -> Optional[Dict[str, Any]]:
    """
    Load environment-specific configuration if it exists.
    
    Args:
        environment: Target environment
        
    Returns:
        Configuration dictionary or None if file doesn't exist
    """
    config_path = get_environment_config_path(environment)
    
    if config_path.exists():
        try:
            return load_config_from_file(config_path)
        except (FileNotFoundError, ValueError):
            return None
    
    return None


def create_environment_configs() -> None:
    """
    Create default environment-specific configuration files.
    """
    config_dir = Path("config")
    config_dir.mkdir(exist_ok=True)
    
    # Development configuration
    dev_config = {
        "environment": "development",
        "debug": True,
        "api": {
            "reload": True,
            "host": "0.0.0.0",
            "port": 8000
        },
        "monitoring": {
            "log_level": "DEBUG",
            "health_check_interval": 15
        },
        "kafka": {
            "bootstrap_servers": "localhost:9092",
            "request_timeout_ms": 10000
        }
    }
    
    # Testing configuration
    test_config = {
        "environment": "testing",
        "debug": False,
        "api": {
            "reload": False,
            "host": "127.0.0.1",
            "port": 8001
        },
        "monitoring": {
            "log_level": "WARNING",
            "enable_metrics": False,
            "health_check_interval": 60
        },
        "kafka": {
            "bootstrap_servers": "localhost:9093",
            "request_timeout_ms": 5000
        },
        "kafka_rest_proxy": {
            "port": 8083,
            "base_url": "http://localhost:8083"
        },
        "kafka_ui": {
            "port": 8081,
            "base_url": "http://localhost:8081"
        }
    }
    
    # Production configuration
    prod_config = {
        "environment": "production",
        "debug": False,
        "api": {
            "reload": False,
            "host": "0.0.0.0",
            "port": 8000
        },
        "monitoring": {
            "log_level": "INFO",
            "enable_metrics": True,
            "health_check_interval": 30
        },
        "kafka": {
            "bootstrap_servers": "localhost:9092",
            "request_timeout_ms": 30000
        }
    }
    
    # Save configuration files
    environments = [
        (Environment.DEVELOPMENT, dev_config),
        (Environment.TESTING, test_config),
        (Environment.PRODUCTION, prod_config)
    ]
    
    for env, config in environments:
        config_path = get_environment_config_path(env)
        if not config_path.exists():
            save_config_to_file(config, config_path)


def validate_configuration(settings: Settings) -> Dict[str, Any]:
    """
    Validate configuration settings and return validation results.
    
    Args:
        settings: Settings instance to validate
        
    Returns:
        Dictionary containing validation results
    """
    validation_results = {
        "valid": True,
        "errors": [],
        "warnings": []
    }
    
    # Validate port conflicts
    ports = [
        ("API", settings.api.port),
        ("Kafka", settings.kafka.broker_port),
        ("Kafka JMX", settings.kafka.jmx_port),
        ("Kafka REST Proxy", settings.kafka_rest_proxy.port),
        ("Kafka UI", settings.kafka_ui.port),
        ("Metrics", settings.monitoring.metrics_port)
    ]
    
    port_map = {}
    for service, port in ports:
        if port in port_map:
            validation_results["errors"].append(
                f"Port conflict: {service} and {port_map[port]} both use port {port}"
            )
            validation_results["valid"] = False
        else:
            port_map[port] = service
    
    # Validate environment-specific settings
    if settings.is_production():
        if settings.debug:
            validation_results["warnings"].append(
                "Debug mode is enabled in production environment"
            )
        if settings.api.reload:
            validation_results["warnings"].append(
                "API auto-reload is enabled in production environment"
            )
    
    # Validate Kafka configuration
    if settings.kafka.default_replication_factor > 1:
        validation_results["warnings"].append(
            "Replication factor > 1 may not work with single-node Kafka setup"
        )
    
    # Validate timeout settings
    if settings.kafka.connection_timeout_ms >= settings.kafka.request_timeout_ms:
        validation_results["errors"].append(
            "Connection timeout should be less than request timeout"
        )
        validation_results["valid"] = False
    
    return validation_results


def get_docker_compose_env_vars(settings: Settings) -> Dict[str, str]:
    """
    Generate environment variables for Docker Compose from settings.
    
    Args:
        settings: Settings instance
        
    Returns:
        Dictionary of environment variables for Docker Compose
    """
    return {
        # Kafka settings
        "KAFKA_BROKER_PORT": str(settings.kafka.broker_port),
        "KAFKA_JMX_PORT": str(settings.kafka.jmx_port),
        "KAFKA_NODE_ID": str(settings.kafka.node_id),
        "CLUSTER_ID": settings.kafka.cluster_id,
        
        # Kafka REST Proxy settings
        "KAFKA_REST_PORT": str(settings.kafka_rest_proxy.port),
        
        # Kafka UI settings
        "KAFKA_UI_PORT": str(settings.kafka_ui.port),
        "KAFKA_UI_CLUSTER_NAME": settings.kafka_ui.cluster_name,
        
        # Docker settings
        "KAFKA_NETWORK_NAME": settings.docker.network_name,
        "KAFKA_DATA_VOLUME": settings.docker.kafka_data_volume,
        
        # Health check settings
        "HEALTH_CHECK_INTERVAL": f"{settings.docker.health_check_interval}s",
        "HEALTH_CHECK_TIMEOUT": f"{settings.docker.health_check_timeout}s",
        "HEALTH_CHECK_RETRIES": str(settings.docker.health_check_retries)
    }


def merge_configurations(*configs: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge multiple configuration dictionaries with deep merging.
    
    Args:
        *configs: Configuration dictionaries to merge
        
    Returns:
        Merged configuration dictionary
    """
    def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively merge two dictionaries."""
        result = base.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = deep_merge(result[key], value)
            else:
                result[key] = value
        
        return result
    
    if not configs:
        return {}
    
    result = configs[0].copy()
    for config in configs[1:]:
        result = deep_merge(result, config)
    
    return result


def export_configuration_template() -> Dict[str, Any]:
    """
    Export a configuration template with all available settings.
    
    Returns:
        Configuration template dictionary
    """
    # Create a default settings instance
    default_settings = Settings()
    
    # Convert to dictionary and add comments
    template = {
        "_comment": "Local Kafka Manager Configuration Template",
        "_description": "This file contains all available configuration options with their default values",
        
        "environment": {
            "_comment": "Deployment environment: development, testing, production",
            "_default": default_settings.environment.value
        },
        
        "debug": {
            "_comment": "Enable debug mode",
            "_default": default_settings.debug
        },
        
        "kafka": {
            "_comment": "Kafka broker configuration",
            "bootstrap_servers": {
                "_comment": "Kafka bootstrap servers (comma-separated)",
                "_default": default_settings.kafka.bootstrap_servers
            },
            "broker_port": {
                "_comment": "Kafka broker port",
                "_default": default_settings.kafka.broker_port
            },
            "jmx_port": {
                "_comment": "Kafka JMX port",
                "_default": default_settings.kafka.jmx_port
            },
            "node_id": {
                "_comment": "Kafka node ID",
                "_default": default_settings.kafka.node_id
            },
            "cluster_id": {
                "_comment": "Kafka cluster ID (base64 encoded UUID)",
                "_default": default_settings.kafka.cluster_id
            },
            "default_partitions": {
                "_comment": "Default number of partitions for new topics",
                "_default": default_settings.kafka.default_partitions
            },
            "default_replication_factor": {
                "_comment": "Default replication factor for new topics",
                "_default": default_settings.kafka.default_replication_factor
            },
            "request_timeout_ms": {
                "_comment": "Request timeout in milliseconds",
                "_default": default_settings.kafka.request_timeout_ms
            },
            "connection_timeout_ms": {
                "_comment": "Connection timeout in milliseconds",
                "_default": default_settings.kafka.connection_timeout_ms
            }
        },
        
        "kafka_rest_proxy": {
            "_comment": "Kafka REST Proxy configuration",
            "host": {
                "_comment": "REST Proxy host",
                "_default": default_settings.kafka_rest_proxy.host
            },
            "port": {
                "_comment": "REST Proxy port",
                "_default": default_settings.kafka_rest_proxy.port
            },
            "request_timeout": {
                "_comment": "Request timeout in seconds",
                "_default": default_settings.kafka_rest_proxy.request_timeout
            },
            "max_retries": {
                "_comment": "Maximum number of retries",
                "_default": default_settings.kafka_rest_proxy.max_retries
            }
        },
        
        "kafka_ui": {
            "_comment": "Kafka UI configuration",
            "host": {
                "_comment": "Kafka UI host",
                "_default": default_settings.kafka_ui.host
            },
            "port": {
                "_comment": "Kafka UI port",
                "_default": default_settings.kafka_ui.port
            },
            "cluster_name": {
                "_comment": "Display name for the cluster in UI",
                "_default": default_settings.kafka_ui.cluster_name
            }
        },
        
        "api": {
            "_comment": "FastAPI application configuration",
            "host": {
                "_comment": "API server host",
                "_default": default_settings.api.host
            },
            "port": {
                "_comment": "API server port",
                "_default": default_settings.api.port
            },
            "reload": {
                "_comment": "Enable auto-reload in development",
                "_default": default_settings.api.reload
            }
        },
        
        "docker": {
            "_comment": "Docker and Docker Compose configuration",
            "compose_file": {
                "_comment": "Docker Compose file path",
                "_default": default_settings.docker.compose_file
            },
            "project_name": {
                "_comment": "Docker Compose project name",
                "_default": default_settings.docker.project_name
            },
            "network_name": {
                "_comment": "Docker network name",
                "_default": default_settings.docker.network_name
            }
        },
        
        "monitoring": {
            "_comment": "Health monitoring and observability configuration",
            "log_level": {
                "_comment": "Application log level: DEBUG, INFO, WARNING, ERROR, CRITICAL",
                "_default": default_settings.monitoring.log_level.value
            },
            "health_check_interval": {
                "_comment": "Health check interval in seconds",
                "_default": default_settings.monitoring.health_check_interval
            },
            "enable_metrics": {
                "_comment": "Enable metrics collection",
                "_default": default_settings.monitoring.enable_metrics
            }
        }
    }
    
    return template