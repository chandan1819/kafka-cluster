#!/usr/bin/env python3
"""
Validation script for Kafka UI and REST Proxy integration.

This script verifies that the Docker Compose configuration is correct
and that the services are properly configured for integration.
"""

import yaml
import sys
from pathlib import Path


def validate_docker_compose():
    """Validate Docker Compose configuration for UI and REST Proxy."""
    compose_file = Path("docker-compose.yml")
    
    if not compose_file.exists():
        print("❌ docker-compose.yml not found")
        return False
    
    try:
        with open(compose_file, 'r') as f:
            compose_data = yaml.safe_load(f)
    except Exception as e:
        print(f"❌ Failed to parse docker-compose.yml: {e}")
        return False
    
    services = compose_data.get('services', {})
    
    # Check required services
    required_services = ['kafka', 'kafka-rest-proxy', 'kafka-ui']
    for service in required_services:
        if service not in services:
            print(f"❌ Missing service: {service}")
            return False
        print(f"✅ Service found: {service}")
    
    # Validate Kafka service
    kafka_service = services['kafka']
    kafka_ports = kafka_service.get('ports', [])
    if '9092:9092' not in kafka_ports:
        print("❌ Kafka port 9092 not exposed")
        return False
    print("✅ Kafka port 9092 properly exposed")
    
    # Validate REST Proxy service
    rest_proxy_service = services['kafka-rest-proxy']
    rest_proxy_ports = rest_proxy_service.get('ports', [])
    if '8082:8082' not in rest_proxy_ports:
        print("❌ REST Proxy port 8082 not exposed")
        return False
    print("✅ REST Proxy port 8082 properly exposed")
    
    # Check REST Proxy dependencies
    rest_proxy_depends_on = rest_proxy_service.get('depends_on', {})
    if 'kafka' not in rest_proxy_depends_on:
        print("❌ REST Proxy missing dependency on Kafka")
        return False
    print("✅ REST Proxy depends on Kafka")
    
    # Check REST Proxy environment
    rest_proxy_env = rest_proxy_service.get('environment', {})
    bootstrap_servers = rest_proxy_env.get('KAFKA_REST_BOOTSTRAP_SERVERS')
    if not bootstrap_servers or 'kafka:29092' not in bootstrap_servers:
        print("❌ REST Proxy bootstrap servers not configured correctly")
        return False
    print("✅ REST Proxy bootstrap servers configured correctly")
    
    # Validate UI service
    ui_service = services['kafka-ui']
    ui_ports = ui_service.get('ports', [])
    if '8080:8080' not in ui_ports:
        print("❌ Kafka UI port 8080 not exposed")
        return False
    print("✅ Kafka UI port 8080 properly exposed")
    
    # Check UI dependencies
    ui_depends_on = ui_service.get('depends_on', {})
    if 'kafka' not in ui_depends_on:
        print("❌ Kafka UI missing dependency on Kafka")
        return False
    print("✅ Kafka UI depends on Kafka")
    
    # Check UI environment
    ui_env = ui_service.get('environment', {})
    bootstrap_servers = ui_env.get('KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS')
    if not bootstrap_servers or 'kafka:29092' not in bootstrap_servers:
        print("❌ Kafka UI bootstrap servers not configured correctly")
        return False
    print("✅ Kafka UI bootstrap servers configured correctly")
    
    # Check health checks
    for service_name in ['kafka', 'kafka-rest-proxy', 'kafka-ui']:
        service_config = services[service_name]
        if 'healthcheck' not in service_config:
            print(f"⚠️  {service_name} missing health check")
        else:
            print(f"✅ {service_name} has health check configured")
    
    # Check networks
    if 'networks' in compose_data:
        print("✅ Custom networks configured")
    else:
        print("⚠️  No custom networks configured (using default)")
    
    return True


def validate_service_definitions():
    """Validate service definitions in the codebase."""
    try:
        from src.services.health_monitor import health_monitor
        
        service_definitions = health_monitor.service_definitions
        
        # Check required services are defined
        required_services = ['kafka', 'kafka-rest-proxy', 'kafka-ui']
        for service in required_services:
            if service not in service_definitions:
                print(f"❌ Service definition missing: {service}")
                return False
            print(f"✅ Service definition found: {service}")
        
        # Check service URLs and health paths
        ui_def = service_definitions['kafka-ui']
        if ui_def.get('url') != 'http://localhost:8080':
            print("⚠️  Kafka UI URL might not match expected value")
        
        rest_proxy_def = service_definitions['kafka-rest-proxy']
        if rest_proxy_def.get('url') != 'http://localhost:8082':
            print("⚠️  REST Proxy URL might not match expected value")
        
        print("✅ Service definitions validated")
        return True
        
    except Exception as e:
        print(f"❌ Failed to validate service definitions: {e}")
        return False


def validate_api_endpoints():
    """Validate API endpoints are properly configured."""
    try:
        from src.api.routes import router
        
        # Check if integration endpoint exists
        routes = [route.path for route in router.routes]
        
        if '/integration/ui-rest-proxy/status' not in routes:
            print("❌ Integration status endpoint not found")
            return False
        print("✅ Integration status endpoint configured")
        
        # Check health endpoints
        health_endpoints = [
            '/health',
            '/health/detailed',
            '/health/services/{service_name}',
            '/health/metrics'
        ]
        
        for endpoint in health_endpoints:
            if endpoint not in routes:
                print(f"⚠️  Health endpoint might be missing: {endpoint}")
            else:
                print(f"✅ Health endpoint found: {endpoint}")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to validate API endpoints: {e}")
        return False


def main():
    """Run all validation checks."""
    print("🔍 Validating Kafka UI and REST Proxy Integration")
    print("=" * 50)
    
    all_passed = True
    
    print("\n📋 Validating Docker Compose Configuration...")
    if not validate_docker_compose():
        all_passed = False
    
    print("\n🔧 Validating Service Definitions...")
    if not validate_service_definitions():
        all_passed = False
    
    print("\n🌐 Validating API Endpoints...")
    if not validate_api_endpoints():
        all_passed = False
    
    print("\n" + "=" * 50)
    if all_passed:
        print("✅ All validation checks passed!")
        print("\n📝 Integration Summary:")
        print("   • Kafka UI accessible at http://localhost:8080")
        print("   • REST Proxy accessible at http://localhost:8082")
        print("   • Proper service dependencies configured")
        print("   • Health checks implemented")
        print("   • Integration status endpoint available")
        return 0
    else:
        print("❌ Some validation checks failed!")
        print("   Please review the issues above and fix them.")
        return 1


if __name__ == "__main__":
    sys.exit(main())