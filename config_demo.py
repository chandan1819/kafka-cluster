#!/usr/bin/env python3
"""
Configuration Management Demo Script

This script demonstrates the configuration management features of the Local Kafka Manager.
"""

import os
from src.config import (
    settings, 
    get_settings, 
    validate_configuration,
    get_docker_compose_env_vars,
    create_environment_configs,
    export_configuration_template
)


def main():
    """Demonstrate configuration management features."""
    print("=== Local Kafka Manager Configuration Demo ===\n")
    
    # Show current configuration
    print("1. Current Configuration:")
    print(f"   Environment: {settings.environment.value}")
    print(f"   Debug Mode: {settings.debug}")
    print(f"   Kafka Bootstrap Servers: {settings.get_kafka_bootstrap_servers()}")
    print(f"   REST Proxy URL: {settings.get_rest_proxy_url()}")
    print(f"   Kafka UI URL: {settings.get_kafka_ui_url()}")
    print(f"   API Base URL: {settings.get_api_base_url()}")
    print(f"   Log Level: {settings.monitoring.log_level.value}")
    print()
    
    # Validate configuration
    print("2. Configuration Validation:")
    validation_result = validate_configuration(settings)
    print(f"   Valid: {validation_result['valid']}")
    if validation_result['errors']:
        print("   Errors:")
        for error in validation_result['errors']:
            print(f"     - {error}")
    if validation_result['warnings']:
        print("   Warnings:")
        for warning in validation_result['warnings']:
            print(f"     - {warning}")
    print()
    
    # Show Docker Compose environment variables
    print("3. Docker Compose Environment Variables:")
    env_vars = get_docker_compose_env_vars(settings)
    for key, value in sorted(env_vars.items()):
        print(f"   {key}={value}")
    print()
    
    # Show environment-specific behavior
    print("4. Environment-Specific Settings:")
    print(f"   Is Development: {settings.is_development()}")
    print(f"   Is Testing: {settings.is_testing()}")
    print(f"   Is Production: {settings.is_production()}")
    print()
    
    # Show configuration structure
    print("5. Configuration Structure:")
    config_dict = settings.to_dict()
    for section, values in config_dict.items():
        if isinstance(values, dict):
            print(f"   {section}:")
            for key, value in values.items():
                print(f"     {key}: {value}")
        else:
            print(f"   {section}: {values}")
    print()
    
    # Demonstrate environment variable override
    print("6. Environment Variable Override Demo:")
    original_port = settings.api.port
    print(f"   Original API Port: {original_port}")
    
    # Set environment variable and reload
    os.environ['LKM_API__PORT'] = '8001'
    from src.config import reload_settings
    new_settings = reload_settings()
    print(f"   After setting LKM_API__PORT=8001: {new_settings.api.port}")
    
    # Clean up
    del os.environ['LKM_API__PORT']
    print()
    
    print("=== Configuration Demo Complete ===")


if __name__ == "__main__":
    main()