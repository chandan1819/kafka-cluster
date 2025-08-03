"""
Local Kafka Manager - Main FastAPI application entry point
"""

import logging
import uuid
from datetime import datetime
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from .config import settings
from .api.routes import router
from .models.base import ErrorResponse
from .exceptions import (
    LocalKafkaManagerError,
    ServiceUnavailableError,
    DockerNotAvailableError,
    KafkaNotAvailableError,
    KafkaRestProxyNotAvailableError,
    ClusterManagerError,
    TopicManagerError,
    MessageManagerError,
    ServiceCatalogError,
    ValidationError,
    TimeoutError,
    ErrorCode
)
from .utils.logging import setup_logging, get_logger
from .middleware.logging_middleware import LoggingMiddleware, DebugLoggingMiddleware
from .utils.metrics import counter, gauge, get_system_metrics

# Initialize structured logging
setup_logging(
    log_level=settings.monitoring.log_level.value,
    structured=True,
    enable_request_tracking=True
)
logger = get_logger(__name__)

app = FastAPI(
    title=settings.api.title,
    description=settings.api.description,
    version=settings.api.version,
    docs_url=settings.api.docs_url,
    redoc_url=settings.api.redoc_url,
    openapi_url=settings.api.openapi_url,
    openapi_tags=[
        {
            "name": "Service Catalog",
            "description": "Service discovery and API documentation endpoints"
        },
        {
            "name": "Cluster Management", 
            "description": "Kafka cluster lifecycle management operations"
        },
        {
            "name": "Topic Management",
            "description": "Kafka topic CRUD operations and metadata"
        },
        {
            "name": "Message Operations",
            "description": "Message production and consumption via REST Proxy"
        },
        {
            "name": "Health",
            "description": "Health check and monitoring endpoints"
        }
    ]
)

# Add logging middleware
if settings.debug or settings.is_development():
    app.add_middleware(DebugLoggingMiddleware)
else:
    app.add_middleware(
        LoggingMiddleware,
        log_requests=True,
        log_responses=True,
        log_request_body=False,
        log_response_body=False,
        exclude_paths=['/health', '/metrics', '/favicon.ico']
    )

# Add CORS middleware using settings
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.api.cors_origins,
    allow_credentials=True,
    allow_methods=settings.api.cors_methods,
    allow_headers=settings.api.cors_headers,
)

# Include API routes
app.include_router(router)


# Application lifecycle events
@app.on_event("startup")
async def startup_event():
    """Initialize services on application startup."""
    logger.info(
        "Starting Local Kafka Manager application",
        extra={
            'environment': settings.environment.value,
            'debug': settings.debug,
            'log_level': settings.monitoring.log_level.value
        }
    )
    
    # Record startup metrics
    counter('application.startup', 1)
    gauge('application.config.debug', 1 if settings.debug else 0)
    
    # Start health monitoring
    try:
        from .services.health_monitor import health_monitor
        await health_monitor.start_monitoring(interval=settings.monitoring.health_check_interval)
        logger.info("Health monitoring started", extra={'interval': settings.monitoring.health_check_interval})
        counter('health.monitoring.started', 1)
    except Exception as e:
        logger.error(f"Failed to start health monitoring: {e}", exc_info=True)
        counter('health.monitoring.start_errors', 1)


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on application shutdown."""
    logger.info("Shutting down Local Kafka Manager application")
    
    # Record shutdown metrics
    counter('application.shutdown', 1)
    
    # Stop health monitoring
    try:
        from .services.health_monitor import health_monitor
        await health_monitor.stop_monitoring()
        logger.info("Health monitoring stopped")
        counter('health.monitoring.stopped', 1)
    except Exception as e:
        logger.error(f"Failed to stop health monitoring: {e}", exc_info=True)
        counter('health.monitoring.stop_errors', 1)


@app.get("/", tags=["General"])
async def root():
    """Root endpoint returning basic API information and available endpoints"""
    return {
        "name": settings.api.title,
        "version": settings.api.version,
        "description": settings.api.description,
        "environment": settings.environment.value,
        "docs_url": settings.api.docs_url,
        "redoc_url": settings.api.redoc_url,
        "openapi_url": settings.api.openapi_url,
        "endpoints": {
            "catalog": "/catalog",
            "cluster": {
                "start": "/cluster/start",
                "stop": "/cluster/stop", 
                "status": "/cluster/status"
            },
            "topics": "/topics",
            "messages": {
                "produce": "/produce",
                "consume": "/consume"
            },
            "health": "/health"
        },
        "configuration": {
            "kafka_url": settings.get_kafka_bootstrap_servers(),
            "rest_proxy_url": settings.get_rest_proxy_url(),
            "kafka_ui_url": settings.get_kafka_ui_url()
        }
    }


# Global exception handlers

def generate_request_id() -> str:
    """Generate a unique request ID for error tracking."""
    return str(uuid.uuid4())[:8]


@app.exception_handler(LocalKafkaManagerError)
async def local_kafka_manager_exception_handler(request: Request, exc: LocalKafkaManagerError):
    """Handle custom LocalKafkaManagerError exceptions with structured response."""
    request_id = generate_request_id()
    
    # Determine HTTP status code based on error type
    if isinstance(exc, ServiceUnavailableError):
        status_code = 503
    elif isinstance(exc, (TopicManagerError, MessageManagerError, ClusterManagerError)):
        if exc.error_code in [ErrorCode.TOPIC_NOT_FOUND, ErrorCode.NOT_FOUND]:
            status_code = 404
        elif exc.error_code in [ErrorCode.TOPIC_ALREADY_EXISTS, ErrorCode.CONFLICT]:
            status_code = 409
        elif exc.error_code == ErrorCode.VALIDATION_ERROR:
            status_code = 422
        elif exc.error_code == ErrorCode.TIMEOUT:
            status_code = 408
        else:
            status_code = 400
    elif isinstance(exc, ValidationError):
        status_code = 422
    elif isinstance(exc, TimeoutError):
        status_code = 408
    else:
        status_code = 500
    
    # Log the error with appropriate level
    if status_code >= 500:
        logger.error(f"[{request_id}] Server error for {request.url}: {exc}", exc_info=exc.cause)
    elif status_code >= 400:
        logger.warning(f"[{request_id}] Client error for {request.url}: {exc}")
    else:
        logger.info(f"[{request_id}] Request handled with error for {request.url}: {exc}")
    
    # Create error response
    error_dict = exc.to_dict()
    error_response = ErrorResponse(
        error=error_dict["error"],
        message=error_dict["message"],
        details=error_dict["details"],
        request_id=request_id
    )
    
    return JSONResponse(
        status_code=status_code,
        content=error_response.model_dump(mode='json')
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle request validation errors with structured response."""
    request_id = generate_request_id()
    logger.warning(f"[{request_id}] Validation error for {request.url}: {exc.errors()}")
    
    # Extract field-specific errors
    field_errors = {}
    for error in exc.errors():
        field_path = ".".join(str(loc) for loc in error["loc"])
        field_errors[field_path] = error["msg"]
    
    return JSONResponse(
        status_code=422,
        content=ErrorResponse(
            error=ErrorCode.VALIDATION_ERROR.value,
            message="Request validation failed",
            details={
                "field_errors": field_errors,
                "validation_errors": exc.errors(),
                "body": exc.body
            },
            request_id=request_id
        ).model_dump(mode='json')
    )


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    """Handle HTTP exceptions with structured response."""
    request_id = generate_request_id()
    logger.warning(f"[{request_id}] HTTP error {exc.status_code} for {request.url}: {exc.detail}")
    
    # Map HTTP status codes to error codes
    error_code_map = {
        400: ErrorCode.VALIDATION_ERROR,
        401: "UNAUTHORIZED",
        403: "FORBIDDEN", 
        404: ErrorCode.NOT_FOUND,
        405: "METHOD_NOT_ALLOWED",
        408: ErrorCode.TIMEOUT,
        409: ErrorCode.CONFLICT,
        422: ErrorCode.VALIDATION_ERROR,
        429: "RATE_LIMITED",
        500: ErrorCode.INTERNAL_SERVER_ERROR,
        502: "BAD_GATEWAY",
        503: ErrorCode.SERVICE_UNAVAILABLE,
        504: "GATEWAY_TIMEOUT"
    }
    
    error_code = error_code_map.get(exc.status_code, f"HTTP_{exc.status_code}")
    
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            error=error_code if isinstance(error_code, str) else error_code.value,
            message=exc.detail or f"HTTP {exc.status_code} error occurred",
            details={"status_code": exc.status_code},
            request_id=request_id
        ).model_dump(mode='json')
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle unexpected exceptions with structured response."""
    request_id = generate_request_id()
    logger.error(f"[{request_id}] Unexpected error for {request.url}: {exc}", exc_info=True)
    
    # Don't expose internal error details in production
    details = {
        "type": type(exc).__name__,
        "request_id": request_id
    }
    
    # In development, include more details
    if settings.is_development() or settings.debug:
        details["message"] = str(exc)
    
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            error=ErrorCode.INTERNAL_SERVER_ERROR.value,
            message="An unexpected error occurred. Please try again or contact support.",
            details=details,
            request_id=request_id
        ).model_dump(mode='json')
    )


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        app, 
        host=settings.api.host, 
        port=settings.api.port, 
        reload=settings.api.reload
    )