"""Cluster Manager for orchestrating Docker Compose lifecycle."""

import asyncio
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import docker
from docker.errors import DockerException, NotFound, APIError
from docker.models.containers import Container

from ..config import settings
from ..models.base import ServiceStatus
from ..models.cluster import ClusterStatus, ServiceHealth
from ..exceptions import (
    ClusterManagerError,
    DockerNotAvailableError,
    ClusterStartError,
    ClusterStopError,
    DockerComposeError,
    ContainerHealthError,
    TimeoutError
)
from ..utils.retry import retry_async, RetryConfig, STANDARD_RETRY, PATIENT_RETRY
from ..utils.logging import get_logger, log_performance, LogContext
from ..utils.metrics import counter, timer, gauge, record_service_metrics

logger = get_logger(__name__)


class ClusterManager:
    """Manages the lifecycle of the local Kafka cluster using Docker Compose."""
    
    def __init__(self, compose_file_path: Optional[str] = None):
        """Initialize the cluster manager.
        
        Args:
            compose_file_path: Path to the docker-compose.yml file (uses config if None)
        """
        self.compose_file_path = Path(compose_file_path or settings.docker.compose_file)
        self.project_name = settings.docker.project_name
        self._docker_client: Optional[docker.DockerClient] = None
        self._cluster_start_time: Optional[float] = None
        
        # Service container names as defined in docker-compose.yml
        self.service_containers = {
            "kafka": "kafka",
            "kafka-rest-proxy": "kafka-rest-proxy", 
            "kafka-ui": "kafka-ui"
        }
        
        # Expected service ports from configuration
        self.service_ports = {
            "kafka": settings.kafka.broker_port,
            "kafka-rest-proxy": settings.kafka_rest_proxy.port,
            "kafka-ui": settings.kafka_ui.port
        }

    @property
    def docker_client(self) -> docker.DockerClient:
        """Get Docker client, creating it if necessary."""
        if self._docker_client is None:
            try:
                self._docker_client = docker.from_env()
                # Test connection
                self._docker_client.ping()
            except DockerException as e:
                raise DockerNotAvailableError(
                    message=f"Docker is not available or not accessible: {e}",
                    cause=e
                )
        return self._docker_client

    @retry_async(PATIENT_RETRY)
    @log_performance("cluster_start")
    async def start_cluster(self, force: bool = False, timeout: int = 60) -> ClusterStatus:
        """Start the Kafka cluster using Docker Compose.
        
        Args:
            force: Force start even if already running
            timeout: Startup timeout in seconds
            
        Returns:
            ClusterStatus with the current status
            
        Raises:
            ClusterStartError: If cluster fails to start
            DockerNotAvailableError: If Docker is not available
        """
        with LogContext("cluster_start", force=force, timeout=timeout):
            logger.info("Starting Kafka cluster...", extra={'force': force, 'timeout': timeout})
            
            try:
                # Check if compose file exists
                if not self.compose_file_path.exists():
                    raise ClusterStartError(
                        message=f"Docker Compose file not found: {self.compose_file_path}",
                        details={"compose_file": str(self.compose_file_path)}
                    )
                
                # Check current status
                current_status = await self.get_status()
                if current_status.status == ServiceStatus.RUNNING and not force:
                    logger.info("Cluster is already running")
                    counter('cluster.start.already_running', 1)
                    return current_status
                
                # Stop existing containers if force is True
                if force and current_status.status == ServiceStatus.RUNNING:
                    logger.info("Force flag set, stopping existing cluster...")
                    await self.stop_cluster(force=True)
                    # Wait a bit for cleanup
                    await asyncio.sleep(2)
                
                # Start the cluster using docker-compose
                start_time = time.time()
                await self._run_compose_command(["up", "-d"], timeout=timeout)
                
                # Record start time
                self._cluster_start_time = time.time()
                
                # Wait for services to be healthy
                await self._wait_for_services_healthy(timeout)
                
                duration = time.time() - start_time
                logger.info("Kafka cluster started successfully", extra={'duration_seconds': duration})
                
                # Record metrics
                counter('cluster.start.success', 1)
                timer('cluster.start.duration_ms', duration * 1000)
                gauge('cluster.status', 1)
                
                return await self.get_status()
                
            except (ClusterStartError, DockerNotAvailableError):
                counter('cluster.start.errors', 1)
                raise
            except Exception as e:
                logger.error(f"Failed to start cluster: {e}", exc_info=True)
                counter('cluster.start.errors', 1)
                raise ClusterStartError(
                    message=str(e),
                    details={"timeout": timeout, "force": force},
                    cause=e
                )

    @retry_async(STANDARD_RETRY)
    @log_performance("cluster_stop")
    async def stop_cluster(self, force: bool = False, cleanup: bool = False, 
                          timeout: int = 30) -> bool:
        """Stop the Kafka cluster.
        
        Args:
            force: Force stop containers
            cleanup: Remove containers and volumes
            timeout: Stop timeout in seconds
            
        Returns:
            True if successfully stopped
            
        Raises:
            ClusterStopError: If cluster fails to stop
        """
        with LogContext("cluster_stop", force=force, cleanup=cleanup, timeout=timeout):
            logger.info("Stopping Kafka cluster...", extra={'force': force, 'cleanup': cleanup, 'timeout': timeout})
            
            try:
                compose_args = ["down"]
                
                if force:
                    compose_args.extend(["--timeout", "0"])
                else:
                    compose_args.extend(["--timeout", str(timeout)])
                    
                if cleanup:
                    compose_args.extend(["--volumes", "--remove-orphans"])
                
                start_time = time.time()
                await self._run_compose_command(compose_args, timeout=timeout + 10)
                duration = time.time() - start_time
                
                # Reset start time
                self._cluster_start_time = None
                
                logger.info("Kafka cluster stopped successfully", extra={'duration_seconds': duration})
                
                # Record metrics
                counter('cluster.stop.success', 1)
                timer('cluster.stop.duration_ms', duration * 1000)
                gauge('cluster.status', 0)
                
                return True
                
            except Exception as e:
                logger.error(f"Failed to stop cluster: {e}", exc_info=True)
                counter('cluster.stop.errors', 1)
                raise ClusterStopError(
                    message=str(e),
                    details={"timeout": timeout, "force": force, "cleanup": cleanup},
                    cause=e
                )

    async def get_status(self) -> ClusterStatus:
        """Get the current status of the cluster.
        
        Returns:
            ClusterStatus with current cluster information
        """
        try:
            services_health = await self._check_services_health()
            
            # Determine overall cluster status
            service_statuses = [health.status for health in services_health.values()]
            
            if not service_statuses:
                overall_status = ServiceStatus.STOPPED
            elif all(status == ServiceStatus.RUNNING for status in service_statuses):
                overall_status = ServiceStatus.RUNNING
            elif any(status == ServiceStatus.STARTING for status in service_statuses):
                overall_status = ServiceStatus.STARTING
            elif any(status == ServiceStatus.ERROR for status in service_statuses):
                overall_status = ServiceStatus.ERROR
            else:
                overall_status = ServiceStatus.STOPPED
            
            # Count running brokers (for now, just Kafka service)
            broker_count = 1 if services_health.get("kafka", ServiceHealth(status=ServiceStatus.STOPPED)).status == ServiceStatus.RUNNING else 0
            
            # Calculate uptime
            uptime = None
            if self._cluster_start_time and overall_status == ServiceStatus.RUNNING:
                uptime = int(time.time() - self._cluster_start_time)
            
            # Build endpoints
            endpoints = {}
            if overall_status == ServiceStatus.RUNNING:
                endpoints = {
                    "kafka": f"localhost:{self.service_ports['kafka']}",
                    "kafka-rest-proxy": f"http://localhost:{self.service_ports['kafka-rest-proxy']}",
                    "kafka-ui": f"http://localhost:{self.service_ports['kafka-ui']}"
                }
            
            return ClusterStatus(
                status=overall_status,
                broker_count=broker_count,
                version="7.4.0",  # From docker-compose.yml
                endpoints=endpoints,
                uptime=uptime,
                services=services_health
            )
            
        except Exception as e:
            logger.error(f"Failed to get cluster status: {e}")
            # Return error status instead of raising
            return ClusterStatus(
                status=ServiceStatus.ERROR,
                broker_count=0,
                services={"error": ServiceHealth(
                    status=ServiceStatus.ERROR,
                    error_message=str(e)
                )}
            )

    async def _check_services_health(self) -> Dict[str, ServiceHealth]:
        """Check the health of all services.
        
        Returns:
            Dictionary mapping service names to their health status
        """
        services_health = {}
        
        try:
            # Check Docker availability first
            docker_client = self.docker_client
            
            for service_name, container_name in self.service_containers.items():
                try:
                    container = docker_client.containers.get(container_name)
                    health = await self._get_container_health(container)
                    services_health[service_name] = health
                    
                except NotFound:
                    # Container doesn't exist
                    services_health[service_name] = ServiceHealth(
                        status=ServiceStatus.STOPPED
                    )
                except Exception as e:
                    logger.warning(f"Error checking {service_name} health: {e}")
                    services_health[service_name] = ServiceHealth(
                        status=ServiceStatus.ERROR,
                        error_message=str(e)
                    )
                    
        except DockerNotAvailableError:
            # Docker not available, all services are stopped
            for service_name in self.service_containers:
                services_health[service_name] = ServiceHealth(
                    status=ServiceStatus.ERROR,
                    error_message="Docker not available"
                )
        
        return services_health

    async def _get_container_health(self, container: Container) -> ServiceHealth:
        """Get health information for a specific container.
        
        Args:
            container: Docker container object
            
        Returns:
            ServiceHealth object
        """
        try:
            # Reload container to get latest status
            container.reload()
            
            # Check container status
            if container.status == "running":
                # Check if container has health check
                health_status = container.attrs.get("State", {}).get("Health", {})
                
                if health_status:
                    health_check_status = health_status.get("Status", "")
                    if health_check_status == "healthy":
                        status = ServiceStatus.RUNNING
                    elif health_check_status == "starting":
                        status = ServiceStatus.STARTING
                    else:
                        status = ServiceStatus.ERROR
                        
                    # Get last health check log
                    health_logs = health_status.get("Log", [])
                    error_message = None
                    if health_logs and status == ServiceStatus.ERROR:
                        last_log = health_logs[-1]
                        error_message = last_log.get("Output", "").strip()
                        
                else:
                    # No health check defined, assume running if container is running
                    status = ServiceStatus.RUNNING
                    error_message = None
                    
            elif container.status in ["created", "restarting"]:
                status = ServiceStatus.STARTING
                error_message = None
            else:
                status = ServiceStatus.STOPPED
                error_message = f"Container status: {container.status}"
            
            # Calculate uptime
            started_at = container.attrs.get("State", {}).get("StartedAt")
            uptime = None
            if started_at and status == ServiceStatus.RUNNING:
                try:
                    start_time = datetime.fromisoformat(started_at.replace('Z', '+00:00'))
                    uptime = int((datetime.now(start_time.tzinfo) - start_time).total_seconds())
                except Exception:
                    pass  # Ignore uptime calculation errors
            
            return ServiceHealth(
                status=status,
                uptime=uptime,
                last_check=datetime.now().isoformat(),
                error_message=error_message
            )
            
        except Exception as e:
            return ServiceHealth(
                status=ServiceStatus.ERROR,
                error_message=str(e),
                last_check=datetime.now().isoformat()
            )

    async def _wait_for_services_healthy(self, timeout: int) -> None:
        """Wait for all services to become healthy.
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Raises:
            ClusterManagerError: If services don't become healthy within timeout
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            services_health = await self._check_services_health()
            
            # Check if all services are running
            all_running = all(
                health.status == ServiceStatus.RUNNING 
                for health in services_health.values()
            )
            
            if all_running:
                logger.info("All services are healthy")
                return
            
            # Check for any error states
            error_services = [
                name for name, health in services_health.items()
                if health.status == ServiceStatus.ERROR
            ]
            
            if error_services:
                error_messages = [
                    f"{name}: {services_health[name].error_message}"
                    for name in error_services
                ]
                raise ClusterManagerError(
                    f"Services failed to start: {', '.join(error_messages)}"
                )
            
            # Wait before next check
            await asyncio.sleep(2)
        
        # Timeout reached
        services_status = {
            name: health.status.value 
            for name, health in services_health.items()
        }
        raise TimeoutError(
            operation="wait_for_services_healthy",
            timeout_seconds=timeout
        )

    async def _run_compose_command(self, args: list, timeout: int = 60) -> None:
        """Run a docker-compose command.
        
        Args:
            args: Command arguments
            timeout: Command timeout in seconds
            
        Raises:
            ClusterManagerError: If command fails
        """
        cmd = [
            "docker-compose",
            "-f", str(self.compose_file_path),
            "-p", self.project_name
        ] + args
        
        logger.debug(f"Running command: {' '.join(cmd)}")
        
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.compose_file_path.parent
            )
            
            try:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(), timeout=timeout
                )
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
                raise TimeoutError(
                    operation=f"docker-compose {' '.join(args)}",
                    timeout_seconds=timeout
                )
            
            if process.returncode != 0:
                error_msg = stderr.decode() if stderr else "Unknown error"
                raise DockerComposeError(
                    command=' '.join(args),
                    exit_code=process.returncode,
                    stderr=error_msg
                )
                
            logger.debug(f"Command output: {stdout.decode()}")
            
        except FileNotFoundError:
            raise DockerNotAvailableError(
                message="docker-compose command not found. Please install Docker Compose."
            )
        except Exception as e:
            if isinstance(e, (ClusterManagerError, DockerNotAvailableError, TimeoutError)):
                raise
            raise DockerComposeError(
                command=' '.join(args),
                exit_code=-1,
                stderr=str(e),
                cause=e
            )