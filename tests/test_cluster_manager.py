"""Unit tests for ClusterManager."""

import asyncio
import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from datetime import datetime
from pathlib import Path

import docker
from docker.errors import DockerException, NotFound, APIError

from src.services.cluster_manager import (
    ClusterManager, 
    ClusterManagerError, 
    DockerNotAvailableError
)
from src.models.base import ServiceStatus
from src.models.cluster import ClusterStatus, ServiceHealth


@pytest.mark.unit
class TestClusterManager:
    """Test cases for ClusterManager class."""

    @pytest.fixture
    def cluster_manager(self):
        """Create a ClusterManager instance for testing."""
        return ClusterManager("test-docker-compose.yml")

    @pytest.fixture
    def mock_docker_client(self):
        """Create a mock Docker client."""
        mock_client = Mock(spec=docker.DockerClient)
        mock_client.ping.return_value = True
        return mock_client

    @pytest.fixture
    def mock_container(self):
        """Create a mock Docker container."""
        container = Mock()
        container.status = "running"
        container.attrs = {
            "State": {
                "StartedAt": "2024-01-15T10:30:00.000000000Z",
                "Health": {
                    "Status": "healthy",
                    "Log": []
                }
            }
        }
        container.reload = Mock()
        return container

    def test_init(self):
        """Test ClusterManager initialization."""
        manager = ClusterManager("custom-compose.yml")
        assert manager.compose_file_path == Path("custom-compose.yml")
        assert manager.project_name == "local-kafka-manager"
        assert manager._docker_client is None
        assert manager._cluster_start_time is None

    @patch('docker.from_env')
    def test_docker_client_property_success(self, mock_from_env, cluster_manager, mock_docker_client):
        """Test successful Docker client creation."""
        mock_from_env.return_value = mock_docker_client
        
        client = cluster_manager.docker_client
        
        assert client == mock_docker_client
        mock_from_env.assert_called_once()
        mock_docker_client.ping.assert_called_once()

    @patch('docker.from_env')
    def test_docker_client_property_failure(self, mock_from_env, cluster_manager):
        """Test Docker client creation failure."""
        mock_from_env.side_effect = DockerException("Docker not available")
        
        with pytest.raises(DockerNotAvailableError, match="Docker is not available"):
            cluster_manager.docker_client

    @pytest.mark.asyncio
    @patch('src.services.cluster_manager.ClusterManager._run_compose_command')
    @patch('src.services.cluster_manager.ClusterManager._wait_for_services_healthy')
    @patch('src.services.cluster_manager.ClusterManager.get_status')
    async def test_start_cluster_success(self, mock_get_status, mock_wait_healthy, 
                                       mock_run_compose, cluster_manager):
        """Test successful cluster start."""
        # Mock compose file exists
        with patch.object(Path, 'exists', return_value=True):
            # Mock initial status as stopped
            mock_get_status.side_effect = [
                ClusterStatus(status=ServiceStatus.STOPPED, broker_count=0),
                ClusterStatus(status=ServiceStatus.RUNNING, broker_count=1)
            ]
            
            result = await cluster_manager.start_cluster()
            
            mock_run_compose.assert_called_once_with(["up", "-d"], timeout=60)
            mock_wait_healthy.assert_called_once_with(60)
            assert result.status == ServiceStatus.RUNNING
            assert cluster_manager._cluster_start_time is not None

    @pytest.mark.asyncio
    async def test_start_cluster_compose_file_not_found(self, cluster_manager):
        """Test cluster start when compose file doesn't exist."""
        with patch.object(Path, 'exists', return_value=False):
            with pytest.raises(ClusterManagerError, match="Docker Compose file not found"):
                await cluster_manager.start_cluster()

    @pytest.mark.asyncio
    @patch('src.services.cluster_manager.ClusterManager.get_status')
    async def test_start_cluster_already_running(self, mock_get_status, cluster_manager):
        """Test cluster start when already running."""
        with patch.object(Path, 'exists', return_value=True):
            mock_get_status.return_value = ClusterStatus(
                status=ServiceStatus.RUNNING, 
                broker_count=1
            )
            
            result = await cluster_manager.start_cluster()
            
            assert result.status == ServiceStatus.RUNNING

    @pytest.mark.asyncio
    @patch('src.services.cluster_manager.ClusterManager._run_compose_command')
    @patch('src.services.cluster_manager.ClusterManager._wait_for_services_healthy')
    @patch('src.services.cluster_manager.ClusterManager.get_status')
    @patch('src.services.cluster_manager.ClusterManager.stop_cluster')
    async def test_start_cluster_force_restart(self, mock_stop, mock_get_status, 
                                             mock_wait_healthy, mock_run_compose, 
                                             cluster_manager):
        """Test cluster start with force flag."""
        with patch.object(Path, 'exists', return_value=True):
            mock_get_status.side_effect = [
                ClusterStatus(status=ServiceStatus.RUNNING, broker_count=1),
                ClusterStatus(status=ServiceStatus.RUNNING, broker_count=1)
            ]
            
            with patch('asyncio.sleep'):
                result = await cluster_manager.start_cluster(force=True)
            
            mock_stop.assert_called_once_with(force=True)
            mock_run_compose.assert_called_once()
            assert result.status == ServiceStatus.RUNNING

    @pytest.mark.asyncio
    @patch('src.services.cluster_manager.ClusterManager._run_compose_command')
    async def test_stop_cluster_success(self, mock_run_compose, cluster_manager):
        """Test successful cluster stop."""
        result = await cluster_manager.stop_cluster()
        
        mock_run_compose.assert_called_once_with(
            ["down", "--timeout", "30"], 
            timeout=40
        )
        assert result is True
        assert cluster_manager._cluster_start_time is None

    @pytest.mark.asyncio
    @patch('src.services.cluster_manager.ClusterManager._run_compose_command')
    async def test_stop_cluster_with_cleanup(self, mock_run_compose, cluster_manager):
        """Test cluster stop with cleanup."""
        await cluster_manager.stop_cluster(cleanup=True)
        
        mock_run_compose.assert_called_once_with(
            ["down", "--timeout", "30", "--volumes", "--remove-orphans"], 
            timeout=40
        )

    @pytest.mark.asyncio
    @patch('src.services.cluster_manager.ClusterManager._run_compose_command')
    async def test_stop_cluster_force(self, mock_run_compose, cluster_manager):
        """Test cluster stop with force."""
        await cluster_manager.stop_cluster(force=True)
        
        mock_run_compose.assert_called_once_with(
            ["down", "--timeout", "0"], 
            timeout=40
        )

    @pytest.mark.asyncio
    @patch('src.services.cluster_manager.ClusterManager._run_compose_command')
    async def test_stop_cluster_failure(self, mock_run_compose, cluster_manager):
        """Test cluster stop failure."""
        mock_run_compose.side_effect = Exception("Stop failed")
        
        with pytest.raises(ClusterManagerError, match="Failed to stop cluster"):
            await cluster_manager.stop_cluster()

    @pytest.mark.asyncio
    @patch('src.services.cluster_manager.ClusterManager._check_services_health')
    async def test_get_status_all_running(self, mock_check_health, cluster_manager):
        """Test get_status when all services are running."""
        cluster_manager._cluster_start_time = 1642234567.0
        
        mock_check_health.return_value = {
            "kafka": ServiceHealth(status=ServiceStatus.RUNNING),
            "kafka-rest-proxy": ServiceHealth(status=ServiceStatus.RUNNING),
            "kafka-ui": ServiceHealth(status=ServiceStatus.RUNNING)
        }
        
        with patch('time.time', return_value=1642234627.0):  # 60 seconds later
            status = await cluster_manager.get_status()
        
        assert status.status == ServiceStatus.RUNNING
        assert status.broker_count == 1
        assert status.uptime == 60
        assert "kafka" in status.endpoints
        assert status.endpoints["kafka"] == "localhost:9092"

    @pytest.mark.asyncio
    @patch('src.services.cluster_manager.ClusterManager._check_services_health')
    async def test_get_status_some_starting(self, mock_check_health, cluster_manager):
        """Test get_status when some services are starting."""
        mock_check_health.return_value = {
            "kafka": ServiceHealth(status=ServiceStatus.STARTING),
            "kafka-rest-proxy": ServiceHealth(status=ServiceStatus.STOPPED),
            "kafka-ui": ServiceHealth(status=ServiceStatus.STOPPED)
        }
        
        status = await cluster_manager.get_status()
        
        assert status.status == ServiceStatus.STARTING
        assert status.broker_count == 0

    @pytest.mark.asyncio
    @patch('src.services.cluster_manager.ClusterManager._check_services_health')
    async def test_get_status_error(self, mock_check_health, cluster_manager):
        """Test get_status when services have errors."""
        mock_check_health.return_value = {
            "kafka": ServiceHealth(
                status=ServiceStatus.ERROR, 
                error_message="Container failed"
            )
        }
        
        status = await cluster_manager.get_status()
        
        assert status.status == ServiceStatus.ERROR
        assert status.broker_count == 0

    @pytest.mark.asyncio
    @patch('src.services.cluster_manager.ClusterManager._check_services_health')
    async def test_get_status_exception_handling(self, mock_check_health, cluster_manager):
        """Test get_status exception handling."""
        mock_check_health.side_effect = Exception("Health check failed")
        
        status = await cluster_manager.get_status()
        
        assert status.status == ServiceStatus.ERROR
        assert "error" in status.services

    @pytest.mark.asyncio
    @patch('docker.from_env')
    async def test_check_services_health_docker_available(self, mock_from_env, 
                                                        cluster_manager, mock_docker_client, 
                                                        mock_container):
        """Test _check_services_health when Docker is available."""
        mock_from_env.return_value = mock_docker_client
        mock_docker_client.containers.get.return_value = mock_container
        
        with patch.object(cluster_manager, '_get_container_health') as mock_get_health:
            mock_get_health.return_value = ServiceHealth(status=ServiceStatus.RUNNING)
            
            health = await cluster_manager._check_services_health()
            
            assert len(health) == 3  # kafka, kafka-rest-proxy, kafka-ui
            assert all(h.status == ServiceStatus.RUNNING for h in health.values())

    @pytest.mark.asyncio
    @patch('docker.from_env')
    async def test_check_services_health_container_not_found(self, mock_from_env, 
                                                           cluster_manager, mock_docker_client):
        """Test _check_services_health when container is not found."""
        mock_from_env.return_value = mock_docker_client
        mock_docker_client.containers.get.side_effect = NotFound("Container not found")
        
        health = await cluster_manager._check_services_health()
        
        assert all(h.status == ServiceStatus.STOPPED for h in health.values())

    @pytest.mark.asyncio
    @patch('docker.from_env')
    async def test_check_services_health_docker_not_available(self, mock_from_env, cluster_manager):
        """Test _check_services_health when Docker is not available."""
        mock_from_env.side_effect = DockerException("Docker not available")
        
        health = await cluster_manager._check_services_health()
        
        assert all(h.status == ServiceStatus.ERROR for h in health.values())
        assert all("Docker not available" in h.error_message for h in health.values())

    @pytest.mark.asyncio
    async def test_get_container_health_running_healthy(self, cluster_manager, mock_container):
        """Test _get_container_health for running healthy container."""
        mock_container.status = "running"
        mock_container.attrs = {
            "State": {
                "StartedAt": "2024-01-15T10:30:00.000000000Z",
                "Health": {
                    "Status": "healthy",
                    "Log": []
                }
            }
        }
        
        with patch('src.services.cluster_manager.datetime') as mock_datetime:
            mock_datetime.now.return_value.isoformat.return_value = "2024-01-15T10:31:00"
            
            health = await cluster_manager._get_container_health(mock_container)
        
        assert health.status == ServiceStatus.RUNNING
        assert health.error_message is None

    @pytest.mark.asyncio
    async def test_get_container_health_starting(self, cluster_manager, mock_container):
        """Test _get_container_health for starting container."""
        mock_container.status = "running"
        mock_container.attrs = {
            "State": {
                "Health": {
                    "Status": "starting",
                    "Log": []
                }
            }
        }
        
        with patch('src.services.cluster_manager.datetime') as mock_datetime:
            mock_datetime.now.return_value.isoformat.return_value = "2024-01-15T10:31:00"
            
            health = await cluster_manager._get_container_health(mock_container)
        
        assert health.status == ServiceStatus.STARTING

    @pytest.mark.asyncio
    async def test_get_container_health_unhealthy(self, cluster_manager, mock_container):
        """Test _get_container_health for unhealthy container."""
        mock_container.status = "running"
        mock_container.attrs = {
            "State": {
                "Health": {
                    "Status": "unhealthy",
                    "Log": [{"Output": "Health check failed"}]
                }
            }
        }
        
        with patch('src.services.cluster_manager.datetime') as mock_datetime:
            mock_datetime.now.return_value.isoformat.return_value = "2024-01-15T10:31:00"
            
            health = await cluster_manager._get_container_health(mock_container)
        
        assert health.status == ServiceStatus.ERROR
        assert "Health check failed" in health.error_message

    @pytest.mark.asyncio
    async def test_get_container_health_no_health_check(self, cluster_manager, mock_container):
        """Test _get_container_health for container without health check."""
        mock_container.status = "running"
        mock_container.attrs = {"State": {}}
        
        with patch('src.services.cluster_manager.datetime') as mock_datetime:
            mock_datetime.now.return_value.isoformat.return_value = "2024-01-15T10:31:00"
            
            health = await cluster_manager._get_container_health(mock_container)
        
        assert health.status == ServiceStatus.RUNNING

    @pytest.mark.asyncio
    async def test_get_container_health_stopped(self, cluster_manager, mock_container):
        """Test _get_container_health for stopped container."""
        mock_container.status = "exited"
        
        with patch('src.services.cluster_manager.datetime') as mock_datetime:
            mock_datetime.now.return_value.isoformat.return_value = "2024-01-15T10:31:00"
            
            health = await cluster_manager._get_container_health(mock_container)
        
        assert health.status == ServiceStatus.STOPPED
        assert "Container status: exited" in health.error_message

    @pytest.mark.asyncio
    @patch('src.services.cluster_manager.ClusterManager._check_services_health')
    async def test_wait_for_services_healthy_success(self, mock_check_health, cluster_manager):
        """Test _wait_for_services_healthy success case."""
        mock_check_health.return_value = {
            "kafka": ServiceHealth(status=ServiceStatus.RUNNING),
            "kafka-rest-proxy": ServiceHealth(status=ServiceStatus.RUNNING),
            "kafka-ui": ServiceHealth(status=ServiceStatus.RUNNING)
        }
        
        # Should not raise an exception
        await cluster_manager._wait_for_services_healthy(30)

    @pytest.mark.asyncio
    @patch('src.services.cluster_manager.ClusterManager._check_services_health')
    async def test_wait_for_services_healthy_error(self, mock_check_health, cluster_manager):
        """Test _wait_for_services_healthy with service errors."""
        mock_check_health.return_value = {
            "kafka": ServiceHealth(
                status=ServiceStatus.ERROR, 
                error_message="Failed to start"
            )
        }
        
        with pytest.raises(ClusterManagerError, match="Services failed to start"):
            await cluster_manager._wait_for_services_healthy(30)

    @pytest.mark.asyncio
    @patch('src.services.cluster_manager.ClusterManager._check_services_health')
    async def test_wait_for_services_healthy_timeout(self, mock_check_health, cluster_manager):
        """Test _wait_for_services_healthy timeout."""
        mock_check_health.return_value = {
            "kafka": ServiceHealth(status=ServiceStatus.STARTING)
        }
        
        with patch('asyncio.sleep'):  # Speed up the test
            with pytest.raises(ClusterManagerError, match="did not become healthy"):
                await cluster_manager._wait_for_services_healthy(1)

    @pytest.mark.asyncio
    async def test_run_compose_command_success(self, cluster_manager):
        """Test _run_compose_command success."""
        mock_process = Mock()
        mock_process.returncode = 0
        mock_process.communicate = AsyncMock(return_value=(b"Success", b""))
        
        with patch('asyncio.create_subprocess_exec', return_value=mock_process):
            with patch('asyncio.wait_for', return_value=(b"Success", b"")):
                await cluster_manager._run_compose_command(["up", "-d"])

    @pytest.mark.asyncio
    async def test_run_compose_command_failure(self, cluster_manager):
        """Test _run_compose_command failure."""
        mock_process = Mock()
        mock_process.returncode = 1
        mock_process.communicate = AsyncMock(return_value=(b"", b"Error occurred"))
        
        with patch('asyncio.create_subprocess_exec', return_value=mock_process):
            with patch('asyncio.wait_for', return_value=(b"", b"Error occurred")):
                with pytest.raises(ClusterManagerError, match="Docker Compose command failed"):
                    await cluster_manager._run_compose_command(["up", "-d"])

    @pytest.mark.asyncio
    async def test_run_compose_command_timeout(self, cluster_manager):
        """Test _run_compose_command timeout."""
        mock_process = Mock()
        mock_process.kill = Mock()
        mock_process.wait = AsyncMock()
        
        with patch('asyncio.create_subprocess_exec', return_value=mock_process):
            with patch('asyncio.wait_for', side_effect=asyncio.TimeoutError()):
                with pytest.raises(ClusterManagerError, match="timed out"):
                    await cluster_manager._run_compose_command(["up", "-d"], timeout=1)

    @pytest.mark.asyncio
    async def test_run_compose_command_not_found(self, cluster_manager):
        """Test _run_compose_command when docker-compose is not found."""
        with patch('asyncio.create_subprocess_exec', side_effect=FileNotFoundError()):
            with pytest.raises(ClusterManagerError, match="docker-compose command not found"):
                await cluster_manager._run_compose_command(["up", "-d"])