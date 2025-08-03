"""
Configuration package for Local Kafka Manager.

This package provides configuration management with environment variable support,
validation, and deployment environment handling.
"""

from .settings import (
    Settings,
    KafkaConfig,
    KafkaRestProxyConfig,
    KafkaUIConfig,
    APIConfig,
    DockerConfig,
    MonitoringConfig,
    Environment,
    LogLevel,
    settings,
    get_settings,
    reload_settings
)

from .utils import (
    load_config_from_file,
    save_config_to_file,
    create_settings_from_dict,
    get_environment_config_path,
    load_environment_config,
    create_environment_configs,
    validate_configuration,
    get_docker_compose_env_vars,
    merge_configurations,
    export_configuration_template
)

__all__ = [
    # Settings classes
    "Settings",
    "KafkaConfig", 
    "KafkaRestProxyConfig",
    "KafkaUIConfig",
    "APIConfig",
    "DockerConfig",
    "MonitoringConfig",
    
    # Enums
    "Environment",
    "LogLevel",
    
    # Settings instances and functions
    "settings",
    "get_settings",
    "reload_settings",
    
    # Utility functions
    "load_config_from_file",
    "save_config_to_file",
    "create_settings_from_dict",
    "get_environment_config_path",
    "load_environment_config",
    "create_environment_configs",
    "validate_configuration",
    "get_docker_compose_env_vars",
    "merge_configurations",
    "export_configuration_template"
]