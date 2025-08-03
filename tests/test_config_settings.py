"""
Tests for configuration settings and management.
"""

import os
import tempfile
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from src.config.settings import (
    Settings,
    KafkaConfig,
    KafkaRestProxyConfig,
    KafkaUIConfig,
    APIConfig,
    DockerConfig,
    MonitoringConfig,
    Environment,
    LogLevel
)
from src.config.utils import (
    load_config_from_file,
    save_config_to_file,
    create_settings_from_dict,
    validate_configuration,
    get_docker_compose_env_vars,
    merge_configurations
)


class TestKafkaConfig:
    """Test Kafka configuration settings."""
    
    def test_default_values(self):
        """Test default Kafka configuration values."""
        config = KafkaConfig()
        
        assert config.bootstrap_servers == "localhost:9092"
        assert config.broker_port == 9092
        assert config.jmx_port == 9101
        assert config.node_id == 1
        assert config.cluster_id == "MkU3OEVBNTcwNTJENDM2Qk"
        assert config.default_partitions == 1
        assert config.default_replication_factor == 1
        assert config.request_timeout_ms == 30000
        assert config.connection_timeout_ms == 10000
        
        # Check default topic configs
        assert "cleanup.policy" in config.default_topic_configs
        assert config.default_topic_configs["cleanup.policy"] == "delete"
    
    def test_bootstrap_servers_validation(self):
        """Test bootstrap servers validation."""
        # Valid single server
        config = KafkaConfig(bootstrap_servers="localhost:9092")
        assert config.bootstrap_servers == "localhost:9092"
        
        # Valid multiple servers
        config = KafkaConfig(bootstrap_servers="localhost:9092,localhost:9093")
        assert config.bootstrap_servers == "localhost:9092,localhost:9093"
        
        # Invalid format - no port
        with pytest.raises(ValueError, match="Invalid server format"):
            KafkaConfig(bootstrap_servers="localhost")
        
        # Invalid format - empty host
        with pytest.raises(ValueError, match="Invalid host"):
            KafkaConfig(bootstrap_servers=":9092")
        
        # Invalid format - invalid port
        with pytest.raises(ValueError, match="Invalid port"):
            KafkaConfig(bootstrap_servers="localhost:abc")
        
        # Invalid format - port out of range
        with pytest.raises(ValueError, match="Invalid port"):
            KafkaConfig(bootstrap_servers="localhost:99999")
    
    def test_port_validation(self):
        """Test port number validation."""
        # Valid port
        config = KafkaConfig(broker_port=9092)
        assert config.broker_port == 9092
        
        # Invalid port - too low
        with pytest.raises(ValueError):
            KafkaConfig(broker_port=1023)
        
        # Invalid port - too high
        with pytest.raises(ValueError):
            KafkaConfig(broker_port=65536)


class TestKafkaRestProxyConfig:
    """Test Kafka REST Proxy configuration settings."""
    
    def test_default_values(self):
        """Test default REST Proxy configuration values."""
        config = KafkaRestProxyConfig()
        
        assert config.host == "localhost"
        assert config.port == 8082
        assert config.base_url == "http://localhost:8082"
        assert config.request_timeout == 30
        assert config.max_retries == 3
        assert config.retry_delay == 1.0
        assert config.consumer_timeout_ms == 5000
        assert config.max_messages_per_request == 100
    
    def test_base_url_validation(self):
        """Test base URL validation and auto-correction."""
        # Base URL should be auto-corrected to match host and port
        config = KafkaRestProxyConfig(
            host="example.com",
            port=8083,
            base_url="http://localhost:8082"  # This should be corrected
        )
        
        assert config.base_url == "http://example.com:8083"


class TestKafkaUIConfig:
    """Test Kafka UI configuration settings."""
    
    def test_default_values(self):
        """Test default UI configuration values."""
        config = KafkaUIConfig()
        
        assert config.host == "localhost"
        assert config.port == 8080
        assert config.base_url == "http://localhost:8080"
        assert config.cluster_name == "local"
        assert config.dynamic_config_enabled is True
        assert config.health_check_path == "/actuator/health"
        assert config.health_check_timeout == 10
    
    def test_base_url_validation(self):
        """Test base URL validation and auto-correction."""
        config = KafkaUIConfig(
            host="ui.example.com",
            port=8081,
            base_url="http://localhost:8080"  # This should be corrected
        )
        
        assert config.base_url == "http://ui.example.com:8081"


class TestAPIConfig:
    """Test FastAPI configuration settings."""
    
    def test_default_values(self):
        """Test default API configuration values."""
        config = APIConfig()
        
        assert config.host == "0.0.0.0"
        assert config.port == 8000
        assert config.reload is False
        assert config.title == "Local Kafka Manager"
        assert config.version == "1.0.0"
        assert config.docs_url == "/docs"
        assert config.redoc_url == "/redoc"
        assert config.openapi_url == "/openapi.json"
        assert config.cors_origins == ["*"]
        assert config.cors_methods == ["*"]
        assert config.cors_headers == ["*"]


class TestDockerConfig:
    """Test Docker configuration settings."""
    
    def test_default_values(self):
        """Test default Docker configuration values."""
        config = DockerConfig()
        
        assert config.compose_file == "docker-compose.yml"
        assert config.project_name == "local-kafka-manager"
        assert config.network_name == "kafka-network"
        assert config.kafka_data_volume == "kafka-data"
        assert config.container_start_timeout == 120
        assert config.container_stop_timeout == 30
        assert config.health_check_interval == 30
        assert config.health_check_timeout == 10
        assert config.health_check_retries == 3


class TestMonitoringConfig:
    """Test monitoring configuration settings."""
    
    def test_default_values(self):
        """Test default monitoring configuration values."""
        config = MonitoringConfig()
        
        assert config.health_check_interval == 30
        assert config.health_check_timeout == 10
        assert config.log_level == LogLevel.INFO
        assert config.enable_metrics is True
        assert config.metrics_port == 9090


class TestSettings:
    """Test main Settings class."""
    
    def test_default_values(self):
        """Test default settings values."""
        settings = Settings()
        
        assert settings.environment == Environment.DEVELOPMENT
        # In development environment, debug is automatically set to True
        assert settings.debug is True
        assert isinstance(settings.kafka, KafkaConfig)
        assert isinstance(settings.kafka_rest_proxy, KafkaRestProxyConfig)
        assert isinstance(settings.kafka_ui, KafkaUIConfig)
        assert isinstance(settings.api, APIConfig)
        assert isinstance(settings.docker, DockerConfig)
        assert isinstance(settings.monitoring, MonitoringConfig)
    
    def test_environment_specific_overrides(self):
        """Test environment-specific configuration overrides."""
        # Development environment
        dev_settings = Settings(environment=Environment.DEVELOPMENT)
        assert dev_settings.debug is True
        assert dev_settings.api.reload is True
        assert dev_settings.monitoring.log_level == LogLevel.DEBUG
        
        # Testing environment
        test_settings = Settings(environment=Environment.TESTING)
        assert test_settings.debug is False
        assert test_settings.monitoring.log_level == LogLevel.WARNING
        assert test_settings.monitoring.enable_metrics is False
        
        # Production environment
        prod_settings = Settings(environment=Environment.PRODUCTION)
        assert prod_settings.debug is False
        assert prod_settings.api.reload is False
        assert prod_settings.monitoring.log_level == LogLevel.INFO
    
    def test_convenience_methods(self):
        """Test convenience methods for getting URLs."""
        settings = Settings()
        
        assert settings.get_kafka_bootstrap_servers() == "localhost:9092"
        assert settings.get_rest_proxy_url() == "http://localhost:8082"
        assert settings.get_kafka_ui_url() == "http://localhost:8080"
        assert settings.get_api_base_url() == "http://0.0.0.0:8000"
    
    def test_environment_check_methods(self):
        """Test environment checking methods."""
        dev_settings = Settings(environment=Environment.DEVELOPMENT)
        assert dev_settings.is_development() is True
        assert dev_settings.is_testing() is False
        assert dev_settings.is_production() is False
        
        test_settings = Settings(environment=Environment.TESTING)
        assert test_settings.is_development() is False
        assert test_settings.is_testing() is True
        assert test_settings.is_production() is False
        
        prod_settings = Settings(environment=Environment.PRODUCTION)
        assert prod_settings.is_development() is False
        assert prod_settings.is_testing() is False
        assert prod_settings.is_production() is True
    
    @patch.dict(os.environ, {
        "LKM_ENVIRONMENT": "testing",
        "LKM_DEBUG": "true",
        "LKM_KAFKA__BROKER_PORT": "9093",
        "LKM_API__PORT": "8001"
    })
    def test_environment_variable_loading(self):
        """Test loading configuration from environment variables."""
        settings = Settings()
        
        assert settings.environment == Environment.TESTING
        # Environment overrides take precedence - testing environment sets debug=False
        assert settings.debug is False
        assert settings.kafka.broker_port == 9093
        assert settings.api.port == 8001


class TestConfigurationUtils:
    """Test configuration utility functions."""
    
    def test_load_save_config_json(self):
        """Test loading and saving JSON configuration files."""
        config_data = {
            "environment": "testing",
            "debug": True,
            "kafka": {
                "broker_port": 9093
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            config_path = f.name
        
        try:
            # Save configuration
            save_config_to_file(config_data, config_path)
            
            # Load configuration
            loaded_config = load_config_from_file(config_path)
            
            assert loaded_config == config_data
        finally:
            os.unlink(config_path)
    
    def test_load_save_config_yaml(self):
        """Test loading and saving YAML configuration files."""
        config_data = {
            "environment": "testing",
            "debug": True,
            "kafka": {
                "broker_port": 9093
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            config_path = f.name
        
        try:
            # Save configuration
            save_config_to_file(config_data, config_path)
            
            # Load configuration
            loaded_config = load_config_from_file(config_path)
            
            assert loaded_config == config_data
        finally:
            os.unlink(config_path)
    
    def test_create_settings_from_dict(self):
        """Test creating Settings from dictionary."""
        config_dict = {
            "environment": "testing",
            "debug": True,
            "kafka": {
                "broker_port": 9093,
                "bootstrap_servers": "localhost:9093"
            },
            "api": {
                "port": 8001
            }
        }
        
        settings = create_settings_from_dict(config_dict)
        
        assert settings.environment == Environment.TESTING
        # Environment overrides take precedence - testing environment sets debug=False
        assert settings.debug is False
        assert settings.kafka.broker_port == 9093
        assert settings.kafka.bootstrap_servers == "localhost:9093"
        assert settings.api.port == 8001
    
    def test_validate_configuration_success(self):
        """Test successful configuration validation."""
        settings = Settings()
        
        validation_result = validate_configuration(settings)
        
        assert validation_result["valid"] is True
        assert len(validation_result["errors"]) == 0
    
    def test_validate_configuration_port_conflict(self):
        """Test configuration validation with port conflicts."""
        # Create settings with port conflict
        settings = Settings(
            kafka=KafkaConfig(broker_port=8080),
            kafka_ui=KafkaUIConfig(port=8080)  # Same port as Kafka
        )
        
        validation_result = validate_configuration(settings)
        
        assert validation_result["valid"] is False
        assert len(validation_result["errors"]) > 0
        assert any("Port conflict" in error for error in validation_result["errors"])
    
    def test_validate_configuration_production_warnings(self):
        """Test configuration validation warnings in production."""
        # Create settings with explicit values that should trigger warnings
        # We need to bypass environment overrides for this test
        settings = Settings()
        settings.environment = Environment.PRODUCTION
        settings.debug = True  # This should trigger warning
        settings.api.reload = True  # This should trigger warning
        
        validation_result = validate_configuration(settings)
        
        assert len(validation_result["warnings"]) >= 2
        assert any("Debug mode" in warning for warning in validation_result["warnings"])
        assert any("auto-reload" in warning for warning in validation_result["warnings"])
    
    def test_get_docker_compose_env_vars(self):
        """Test generating Docker Compose environment variables."""
        settings = Settings()
        
        env_vars = get_docker_compose_env_vars(settings)
        
        assert "KAFKA_BROKER_PORT" in env_vars
        assert env_vars["KAFKA_BROKER_PORT"] == "9092"
        assert "KAFKA_REST_PORT" in env_vars
        assert env_vars["KAFKA_REST_PORT"] == "8082"
        assert "KAFKA_UI_PORT" in env_vars
        assert env_vars["KAFKA_UI_PORT"] == "8080"
        assert "CLUSTER_ID" in env_vars
        assert env_vars["CLUSTER_ID"] == "MkU3OEVBNTcwNTJENDM2Qk"
    
    def test_merge_configurations(self):
        """Test merging multiple configuration dictionaries."""
        base_config = {
            "environment": "development",
            "kafka": {
                "broker_port": 9092,
                "node_id": 1
            },
            "api": {
                "port": 8000
            }
        }
        
        override_config = {
            "environment": "testing",
            "kafka": {
                "broker_port": 9093  # Override port
                # node_id should remain from base
            },
            "monitoring": {
                "log_level": "DEBUG"
            }
        }
        
        merged = merge_configurations(base_config, override_config)
        
        assert merged["environment"] == "testing"  # Overridden
        assert merged["kafka"]["broker_port"] == 9093  # Overridden
        assert merged["kafka"]["node_id"] == 1  # From base
        assert merged["api"]["port"] == 8000  # From base
        assert merged["monitoring"]["log_level"] == "DEBUG"  # New


class TestConfigurationIntegration:
    """Integration tests for configuration management."""
    
    def test_settings_with_environment_file(self):
        """Test loading settings with environment-specific configuration file."""
        # Create temporary config file
        config_data = {
            "environment": "testing",
            "debug": False,
            "kafka": {
                "broker_port": 9093,
                "bootstrap_servers": "localhost:9093"
            },
            "api": {
                "port": 8001,
                "reload": False
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            config_path = f.name
        
        try:
            save_config_to_file(config_data, config_path)
            
            # Load settings from file
            loaded_config = load_config_from_file(config_path)
            settings = create_settings_from_dict(loaded_config)
            
            # Verify settings
            assert settings.environment == Environment.TESTING
            assert settings.debug is False  # Should be overridden by environment
            assert settings.kafka.broker_port == 9093
            assert settings.api.port == 8001
            
            # Verify environment-specific overrides still apply
            assert settings.monitoring.log_level == LogLevel.WARNING
            assert settings.monitoring.enable_metrics is False
            
        finally:
            os.unlink(config_path)
    
    @patch.dict(os.environ, {
        "LKM_ENVIRONMENT": "production",
        "LKM_KAFKA__BOOTSTRAP_SERVERS": "prod-kafka:9092",
        "LKM_API__HOST": "0.0.0.0",
        "LKM_MONITORING__LOG_LEVEL": "ERROR"
    })
    def test_production_configuration_from_env(self):
        """Test production configuration loaded from environment variables."""
        settings = Settings()
        
        assert settings.environment == Environment.PRODUCTION
        assert settings.kafka.bootstrap_servers == "prod-kafka:9092"
        assert settings.api.host == "0.0.0.0"
        # Environment overrides take precedence - production environment sets log_level=INFO
        assert settings.monitoring.log_level == LogLevel.INFO
        
        # Production environment overrides should still apply
        assert settings.debug is False
        assert settings.api.reload is False
        
        # Validate configuration
        validation_result = validate_configuration(settings)
        assert validation_result["valid"] is True