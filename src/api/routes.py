"""
FastAPI routes for the Local Kafka Manager API.
"""

import logging
from datetime import datetime
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query, Path, status
from fastapi.responses import JSONResponse

from ..models.base import ErrorResponse
from ..models.cluster import ClusterStatus, ClusterStartRequest, ClusterStopRequest
from ..models.topic import (
    TopicListResponse, TopicCreateRequest, TopicDeleteRequest, 
    TopicInfo, TopicMetadata
)
from ..models.message import (
    ProduceRequest, ProduceResult, ConsumeRequest, ConsumeResponse,
    ConsumerGroupListResponse
)
from ..models.catalog import CatalogResponse
from ..services.cluster_manager import ClusterManager
from ..services.topic_manager import TopicManager
from ..services.message_manager import MessageManager
from ..services.service_catalog import ServiceCatalog
from ..exceptions import (
    LocalKafkaManagerError,
    ServiceUnavailableError,
    DockerNotAvailableError,
    KafkaNotAvailableError,
    KafkaRestProxyNotAvailableError,
    ClusterManagerError,
    TopicManagerError,
    MessageManagerError,
    ServiceCatalogError
)


logger = logging.getLogger(__name__)

# Initialize service instances
cluster_manager = ClusterManager()
topic_manager = TopicManager()
message_manager = MessageManager()
service_catalog = ServiceCatalog(cluster_manager, topic_manager)

# Multi-cluster services (will be initialized in main.py)
from ..middleware.backward_compatibility import get_backward_compatibility_manager

# Create API router
router = APIRouter()


# Error handling helper - now exceptions are handled globally in main.py
# This function is kept for backward compatibility but exceptions should bubble up
def handle_service_error(error: Exception, operation: str) -> HTTPException:
    """Convert service errors to appropriate HTTP exceptions.
    
    Note: This is deprecated. Exceptions should bubble up to be handled
    by the global exception handlers in main.py.
    """
    # Let LocalKafkaManagerError exceptions bubble up to global handlers
    if isinstance(error, LocalKafkaManagerError):
        raise error
    
    # Handle other exceptions
    if isinstance(error, (DockerNotAvailableError, KafkaNotAvailableError, KafkaRestProxyNotAvailableError)):
        return HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Service unavailable: {str(error)}"
        )
    elif isinstance(error, (ClusterManagerError, TopicManagerError, MessageManagerError, ServiceCatalogError)):
        return HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Operation failed: {str(error)}"
        )
    else:
        logger.error(f"Unexpected error in {operation}: {error}")
        return HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error during {operation}"
        )


# Service Catalog endpoints
@router.get("/catalog", response_model=CatalogResponse, tags=["Service Catalog"])
async def get_service_catalog(
    force_refresh: bool = Query(False, description="Force refresh of cached data")
) -> CatalogResponse:
    """
    Get complete service catalog with real-time status information.
    
    Returns information about cluster status, available topics, API endpoints,
    and service health status.
    
    Note: This endpoint maintains backward compatibility by routing to the default cluster
    in the multi-cluster system.
    """
    try:
        # Try to use multi-cluster system for backward compatibility
        try:
            compat_manager = get_backward_compatibility_manager()
            legacy_catalog = await compat_manager.get_legacy_service_catalog(force_refresh=force_refresh)
            
            # Convert to CatalogResponse
            return CatalogResponse(**legacy_catalog)
            
        except Exception:
            # Fall back to original single-cluster implementation
            logger.debug("Falling back to single-cluster implementation")
            catalog = await service_catalog.get_catalog(force_refresh=force_refresh)
            return catalog
    except Exception as e:
        raise handle_service_error(e, "get service catalog")


# Cluster management endpoints
@router.post("/cluster/start", response_model=ClusterStatus, tags=["Cluster Management"])
async def start_cluster(request: ClusterStartRequest = ClusterStartRequest()) -> ClusterStatus:
    """
    Start the local Kafka cluster using Docker Compose.
    
    This will launch Kafka broker, REST Proxy, and UI services.
    
    Note: This endpoint maintains backward compatibility by routing to the default cluster
    in the multi-cluster system.
    """
    try:
        # Try to use multi-cluster system for backward compatibility
        try:
            compat_manager = get_backward_compatibility_manager()
            final_status = await compat_manager.route_legacy_cluster_operation(
                "start",
                force=request.force,
                timeout=request.timeout
            )
            
            # Convert ServiceStatus to ClusterStatus for backward compatibility
            from ..models.cluster import ClusterStatus
            return ClusterStatus(
                status=final_status,
                broker_count=1,  # Default for single cluster
                version="7.4.0",
                endpoints={},
                services={}
            )
            
        except Exception:
            # Fall back to original single-cluster implementation
            logger.debug("Falling back to single-cluster implementation")
            status = await cluster_manager.start_cluster(
                force=request.force,
                timeout=request.timeout
            )
            return status
    except Exception as e:
        raise handle_service_error(e, "start cluster")


@router.post("/cluster/stop", response_model=dict, tags=["Cluster Management"])
async def stop_cluster(request: ClusterStopRequest = ClusterStopRequest()) -> dict:
    """
    Stop the local Kafka cluster.
    
    This will stop all running containers and optionally clean up resources.
    
    Note: This endpoint maintains backward compatibility by routing to the default cluster
    in the multi-cluster system.
    """
    try:
        # Try to use multi-cluster system for backward compatibility
        try:
            compat_manager = get_backward_compatibility_manager()
            success = await compat_manager.route_legacy_cluster_operation(
                "stop",
                force=request.force,
                cleanup=request.cleanup,
                timeout=request.timeout
            )
            return {"success": success, "message": "Cluster stopped successfully"}
            
        except Exception:
            # Fall back to original single-cluster implementation
            logger.debug("Falling back to single-cluster implementation")
            success = await cluster_manager.stop_cluster(
                force=request.force,
                cleanup=request.cleanup,
                timeout=request.timeout
            )
            return {"success": success, "message": "Cluster stopped successfully"}
    except Exception as e:
        raise handle_service_error(e, "stop cluster")


@router.get("/cluster/status", response_model=ClusterStatus, tags=["Cluster Management"])
async def get_cluster_status() -> ClusterStatus:
    """
    Get current status and health information for the Kafka cluster.
    
    Returns detailed information about cluster state, broker count, service health,
    and available endpoints.
    
    Note: This endpoint maintains backward compatibility by routing to the default cluster
    in the multi-cluster system.
    """
    try:
        # Try to use multi-cluster system for backward compatibility
        try:
            compat_manager = get_backward_compatibility_manager()
            service_status = await compat_manager.route_legacy_cluster_operation("status")
            
            # Convert ServiceStatus to ClusterStatus for backward compatibility
            from ..models.cluster import ClusterStatus
            return ClusterStatus(
                status=service_status,
                broker_count=1,  # Default for single cluster
                version="7.4.0",
                endpoints={},
                services={}
            )
            
        except Exception:
            # Fall back to original single-cluster implementation
            logger.debug("Falling back to single-cluster implementation")
            status = await cluster_manager.get_status()
            return status
    except Exception as e:
        raise handle_service_error(e, "get cluster status")


# Topic management endpoints
@router.get("/topics", response_model=TopicListResponse, tags=["Topic Management"])
async def list_topics(
    include_internal: bool = Query(False, description="Include internal topics (starting with __)")
) -> TopicListResponse:
    """
    List all Kafka topics with their configuration and metadata.
    
    By default, internal topics are excluded from the results.
    
    Note: This endpoint maintains backward compatibility by routing to the default cluster
    in the multi-cluster system.
    """
    try:
        # Try to use multi-cluster system for backward compatibility
        try:
            compat_manager = get_backward_compatibility_manager()
            managers = await compat_manager.get_default_cluster_manager()
            cluster_topic_manager = managers["topic_manager"]
            
            try:
                topics = await cluster_topic_manager.list_topics(include_internal=include_internal)
                return TopicListResponse(
                    topics=topics,
                    total_count=len(topics)
                )
            finally:
                cluster_topic_manager.close()
                
        except Exception:
            # Fall back to original single-cluster implementation
            logger.debug("Falling back to single-cluster implementation")
            topics = await topic_manager.list_topics(include_internal=include_internal)
            return TopicListResponse(
                topics=topics,
                total_count=len(topics)
            )
    except Exception as e:
        raise handle_service_error(e, "list topics")


@router.post("/topics", response_model=dict, status_code=status.HTTP_201_CREATED, tags=["Topic Management"])
async def create_topic(request: TopicCreateRequest) -> dict:
    """
    Create a new Kafka topic with the specified configuration.
    
    The topic will be created with the provided number of partitions,
    replication factor, and configuration properties.
    
    Note: This endpoint maintains backward compatibility by routing to the default cluster
    in the multi-cluster system.
    """
    try:
        # Try to use multi-cluster system for backward compatibility
        try:
            compat_manager = get_backward_compatibility_manager()
            managers = await compat_manager.get_default_cluster_manager()
            cluster_topic_manager = managers["topic_manager"]
            
            try:
                success = await cluster_topic_manager.create_topic(request.to_topic_config())
                return {
                    "success": success,
                    "message": f"Topic '{request.name}' created successfully",
                    "topic": request.name
                }
            finally:
                cluster_topic_manager.close()
                
        except Exception:
            # Fall back to original single-cluster implementation
            logger.debug("Falling back to single-cluster implementation")
            success = await topic_manager.create_topic(request)
            return {
                "success": success,
                "message": f"Topic '{request.name}' created successfully",
                "topic": request.name
            }
    except Exception as e:
        raise handle_service_error(e, "create topic")


@router.delete("/topics/{topic_name}", response_model=dict, tags=["Topic Management"])
async def delete_topic(
    topic_name: str = Path(..., description="Name of the topic to delete"),
    force: bool = Query(False, description="Force delete even if topic has data")
) -> dict:
    """
    Delete a Kafka topic.
    
    This operation is irreversible and will remove all data in the topic.
    """
    try:
        success = await topic_manager.delete_topic(topic_name, force=force)
        return {
            "success": success,
            "message": f"Topic '{topic_name}' deleted successfully",
            "topic": topic_name
        }
    except Exception as e:
        raise handle_service_error(e, "delete topic")


@router.get("/topics/{topic_name}/metadata", response_model=TopicMetadata, tags=["Topic Management"])
async def get_topic_metadata(
    topic_name: str = Path(..., description="Name of the topic")
) -> TopicMetadata:
    """
    Get detailed metadata for a specific topic.
    
    Returns partition details, configuration, consumer groups, and offset information.
    """
    try:
        metadata = await topic_manager.get_topic_metadata(topic_name)
        return metadata
    except Exception as e:
        raise handle_service_error(e, "get topic metadata")


# Message operations endpoints
@router.post("/produce", response_model=ProduceResult, status_code=status.HTTP_201_CREATED, tags=["Message Operations"])
async def produce_message(request: ProduceRequest) -> ProduceResult:
    """
    Produce a message to a Kafka topic via REST Proxy.
    
    The message will be sent to the specified topic with optional key and partition.
    """
    try:
        result = await message_manager.produce_message(request)
        return result
    except Exception as e:
        raise handle_service_error(e, "produce message")


@router.get("/consume", response_model=ConsumeResponse, tags=["Message Operations"])
async def consume_messages(
    topic: str = Query(..., description="Topic to consume from"),
    consumer_group: str = Query(..., description="Consumer group ID"),
    max_messages: int = Query(10, ge=1, le=1000, description="Maximum messages to fetch"),
    timeout_ms: int = Query(5000, ge=100, le=60000, description="Consumer timeout in milliseconds"),
    from_beginning: bool = Query(False, description="Start consuming from beginning"),
    partition: Optional[int] = Query(None, ge=0, description="Specific partition to consume from")
) -> ConsumeResponse:
    """
    Consume messages from a Kafka topic via REST Proxy.
    
    Creates a temporary consumer instance to fetch messages from the specified topic.
    """
    try:
        request = ConsumeRequest(
            topic=topic,
            consumer_group=consumer_group,
            max_messages=max_messages,
            timeout_ms=timeout_ms,
            from_beginning=from_beginning,
            partition=partition
        )
        response = await message_manager.consume_messages(request)
        return response
    except Exception as e:
        raise handle_service_error(e, "consume messages")


@router.get("/consumer-groups", response_model=ConsumerGroupListResponse, tags=["Message Operations"])
async def list_consumer_groups() -> ConsumerGroupListResponse:
    """
    List all consumer groups and their status.
    
    Returns information about active consumer groups, their members, and partition assignments.
    """
    try:
        groups = await message_manager.get_consumer_groups()
        return groups
    except Exception as e:
        raise handle_service_error(e, "list consumer groups")


# Health and utility endpoints
@router.get("/health", tags=["Health"])
async def health_check() -> dict:
    """
    Basic health check endpoint for the API service.
    
    Returns the health status of the API and its dependencies.
    """
    try:
        # Check cluster status to determine overall health
        cluster_status = await cluster_manager.get_status()
        
        health_status = {
            "status": "healthy",
            "service": "local-kafka-manager",
            "cluster_status": cluster_status.status.value,
            "timestamp": cluster_status.timestamp.isoformat()
        }
        
        # If cluster has errors, reflect in health status
        if cluster_status.status.value == "error":
            health_status["status"] = "degraded"
            health_status["warnings"] = ["Kafka cluster is not running"]
        
        return health_status
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "service": "local-kafka-manager",
            "error": str(e)
        }


@router.get("/health/detailed", tags=["Health"])
async def detailed_health_check(force_check: bool = Query(False, description="Force immediate health checks")) -> dict:
    """
    Detailed health check endpoint with comprehensive service monitoring.
    
    Returns detailed health information for all services including metrics,
    dependency status, and individual service health.
    """
    try:
        from ..services.health_monitor import health_monitor
        
        # Get comprehensive system health
        system_health = await health_monitor.get_system_health(force_check=force_check)
        
        return system_health.to_dict()
        
    except Exception as e:
        logger.error(f"Detailed health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }


@router.get("/health/services/{service_name}", tags=["Health"])
async def service_health_check(
    service_name: str = Path(..., description="Name of the service to check"),
    force_check: bool = Query(False, description="Force immediate health check")
) -> dict:
    """
    Get health status for a specific service.
    
    Returns detailed health information for the specified service including
    response times, error details, and service-specific metrics.
    """
    try:
        from ..services.health_monitor import health_monitor
        
        service_health = await health_monitor.get_service_health(service_name, force_check=force_check)
        
        if service_health is None:
            raise HTTPException(
                status_code=404,
                detail=f"Service '{service_name}' not found"
            )
        
        return service_health.to_dict()
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Service health check failed for {service_name}: {e}")
        return {
            "name": service_name,
            "status": "unknown",
            "error": str(e),
            "last_check": datetime.utcnow().isoformat()
        }


@router.get("/compatibility/status", tags=["Compatibility"])
async def get_backward_compatibility_status() -> dict:
    """
    Get backward compatibility status information.
    
    Returns information about the default cluster, legacy endpoint support,
    and overall backward compatibility health.
    """
    try:
        compat_manager = get_backward_compatibility_manager()
        status = await compat_manager.get_compatibility_status()
        return status
    except Exception as e:
        logger.error(f"Failed to get compatibility status: {e}")
        return {
            "backward_compatibility_enabled": False,
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }


@router.get("/health/metrics", tags=["Health"])
async def health_metrics(service_name: Optional[str] = Query(None, description="Specific service name")) -> dict:
    """
    Get health metrics for services.
    
    Returns performance metrics including success rates, response times,
    and failure statistics for monitoring and alerting.
    """
    try:
        from ..services.health_monitor import health_monitor
        
        metrics = health_monitor.get_metrics(service_name)
        
        # Convert metrics to serializable format
        result = {}
        for name, metric in metrics.items():
            result[name] = {
                "total_checks": metric.total_checks,
                "successful_checks": metric.successful_checks,
                "failed_checks": metric.failed_checks,
                "success_rate_percent": metric.success_rate(),
                "average_response_time_ms": metric.average_response_time_ms,
                "last_failure_time": metric.last_failure_time.isoformat() if metric.last_failure_time else None,
                "consecutive_failures": metric.consecutive_failures
            }
        
        return {
            "metrics": result,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Health metrics retrieval failed: {e}")
        return {
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }


@router.get("/metrics", tags=["Monitoring"])
async def get_metrics(
    since_minutes: int = Query(60, ge=1, le=1440, description="Minutes to look back for metrics"),
    metric_name: Optional[str] = Query(None, description="Specific metric name to retrieve")
) -> dict:
    """
    Get application performance metrics.
    
    Returns comprehensive metrics including API performance, service operations,
    health checks, and system resource usage.
    """
    try:
        from ..utils.metrics import get_all_metrics, get_metric_summary, get_performance_report
        from datetime import timedelta
        
        if metric_name:
            # Get specific metric
            since = datetime.utcnow() - timedelta(minutes=since_minutes)
            metric_summary = get_metric_summary(metric_name, since)
            
            if not metric_summary:
                return {
                    "error": f"Metric '{metric_name}' not found",
                    "available_metrics": list(get_all_metrics().keys())
                }
            
            return {
                "metric": {
                    "name": metric_summary.name,
                    "type": metric_summary.metric_type.value,
                    "count": metric_summary.count,
                    "sum": metric_summary.sum,
                    "min": metric_summary.min,
                    "max": metric_summary.max,
                    "avg": metric_summary.avg,
                    "p50": metric_summary.p50,
                    "p95": metric_summary.p95,
                    "p99": metric_summary.p99,
                    "last_value": metric_summary.last_value,
                    "last_updated": metric_summary.last_updated.isoformat()
                },
                "period": {
                    "since_minutes": since_minutes,
                    "since": since.isoformat()
                },
                "timestamp": datetime.utcnow().isoformat()
            }
        else:
            # Get performance report
            report = get_performance_report(since_minutes)
            return report
        
    except Exception as e:
        logger.error(f"Metrics retrieval failed: {e}")
        return {
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }


@router.get("/metrics/system", tags=["Monitoring"])
async def get_system_metrics() -> dict:
    """
    Get system resource metrics.
    
    Returns information about CPU, memory, disk usage, and process statistics
    for monitoring system health and performance.
    """
    try:
        from ..utils.metrics import get_system_metrics
        
        system_metrics = get_system_metrics()
        return {
            "system_metrics": system_metrics,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"System metrics retrieval failed: {e}")
        return {
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }


@router.get("/integration/ui-rest-proxy/status", tags=["Integration"])
async def ui_rest_proxy_integration_status() -> dict:
    """
    Get comprehensive integration status for Kafka UI and REST Proxy.
    
    Returns detailed information about UI and REST Proxy accessibility,
    functionality, and integration with the Kafka cluster.
    """
    try:
        from ..services.health_monitor import health_monitor
        
        # Get health status for UI and REST Proxy
        ui_health = await health_monitor.get_service_health("kafka-ui", force_check=True)
        rest_proxy_health = await health_monitor.get_service_health("kafka-rest-proxy", force_check=True)
        kafka_health = await health_monitor.get_service_health("kafka", force_check=True)
        
        # Get cluster status for additional context
        cluster_status = await cluster_manager.get_status()
        
        # Build comprehensive integration status
        integration_status = {
            "overall_status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "services": {
                "kafka-ui": {
                    "status": ui_health.status.value if ui_health else "unknown",
                    "accessible": ui_health.details.get("ui_accessible", False) if ui_health else False,
                    "api_accessible": ui_health.details.get("api_accessible", False) if ui_health else False,
                    "clusters_detected": ui_health.details.get("clusters_detected", 0) if ui_health else 0,
                    "response_time_ms": ui_health.response_time_ms if ui_health else None,
                    "url": cluster_status.endpoints.get("kafka-ui", "http://localhost:8080"),
                    "error": ui_health.error_message if ui_health else None
                },
                "kafka-rest-proxy": {
                    "status": rest_proxy_health.status.value if rest_proxy_health else "unknown",
                    "kafka_connectivity": rest_proxy_health.details.get("kafka_connectivity", False) if rest_proxy_health else False,
                    "topics_accessible": rest_proxy_health.details.get("topics_accessible", False) if rest_proxy_health else False,
                    "brokers_accessible": rest_proxy_health.details.get("brokers_accessible", False) if rest_proxy_health else False,
                    "topics_count": rest_proxy_health.details.get("topics_count", 0) if rest_proxy_health else 0,
                    "brokers_count": rest_proxy_health.details.get("brokers_count", 0) if rest_proxy_health else 0,
                    "response_time_ms": rest_proxy_health.response_time_ms if rest_proxy_health else None,
                    "url": cluster_status.endpoints.get("kafka-rest-proxy", "http://localhost:8082"),
                    "error": rest_proxy_health.error_message if rest_proxy_health else None
                },
                "kafka": {
                    "status": kafka_health.status.value if kafka_health else "unknown",
                    "broker_count": cluster_status.broker_count,
                    "uptime": cluster_status.uptime,
                    "error": kafka_health.error_message if kafka_health else None
                }
            },
            "integration_checks": {
                "ui_can_connect_to_kafka": False,
                "rest_proxy_can_connect_to_kafka": False,
                "services_startup_order_correct": False,
                "all_dependencies_satisfied": False
            }
        }
        
        # Perform integration checks
        if ui_health and ui_health.details:
            integration_status["integration_checks"]["ui_can_connect_to_kafka"] = (
                ui_health.details.get("clusters_detected", 0) > 0
            )
        
        if rest_proxy_health and rest_proxy_health.details:
            integration_status["integration_checks"]["rest_proxy_can_connect_to_kafka"] = (
                rest_proxy_health.details.get("kafka_connectivity", False)
            )
        
        # Check service startup order (Kafka should be running for others to work)
        kafka_running = kafka_health and kafka_health.status.value == "running"
        ui_running = ui_health and ui_health.status.value == "running"
        rest_proxy_running = rest_proxy_health and rest_proxy_health.status.value == "running"
        
        integration_status["integration_checks"]["services_startup_order_correct"] = (
            not ui_running or kafka_running
        ) and (
            not rest_proxy_running or kafka_running
        )
        
        integration_status["integration_checks"]["all_dependencies_satisfied"] = (
            kafka_running and 
            integration_status["integration_checks"]["ui_can_connect_to_kafka"] and
            integration_status["integration_checks"]["rest_proxy_can_connect_to_kafka"]
        )
        
        # Determine overall status
        if not kafka_running:
            integration_status["overall_status"] = "degraded"
        elif not integration_status["integration_checks"]["all_dependencies_satisfied"]:
            integration_status["overall_status"] = "degraded"
        elif (ui_health and ui_health.status.value == "error") or (rest_proxy_health and rest_proxy_health.status.value == "error"):
            integration_status["overall_status"] = "degraded"
        
        return integration_status
        
    except Exception as e:
        logger.error(f"UI/REST Proxy integration status check failed: {e}")
        return {
            "overall_status": "error",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }


@router.post("/health/monitoring/start", tags=["Health"])
async def start_health_monitoring(
    interval: int = Query(30, ge=10, le=300, description="Monitoring interval in seconds")
) -> dict:
    """
    Start periodic health monitoring.
    
    Begins background health monitoring with the specified interval.
    Monitoring will continue until explicitly stopped.
    """
    try:
        from ..services.health_monitor import health_monitor
        
        await health_monitor.start_monitoring(interval=interval)
        
        return {
            "status": "started",
            "message": f"Health monitoring started with {interval}s interval",
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to start health monitoring: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to start health monitoring: {str(e)}"
        )


@router.post("/health/monitoring/stop", tags=["Health"])
async def stop_health_monitoring() -> dict:
    """
    Stop periodic health monitoring.
    
    Stops the background health monitoring task and cleans up resources.
    """
    try:
        from ..services.health_monitor import health_monitor
        
        await health_monitor.stop_monitoring()
        
        return {
            "status": "stopped",
            "message": "Health monitoring stopped",
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to stop health monitoring: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to stop health monitoring: {str(e)}"
        )


# Note: Exception handlers are defined in main.py at the app level