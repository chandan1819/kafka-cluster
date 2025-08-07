"""
Performance and load tests for multi-cluster Kafka manager.

This module contains performance tests that validate system behavior
under various load conditions and stress scenarios.
"""

import pytest
import asyncio
import time
import statistics
from typing import List, Dict, Any, Tuple
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch
from concurrent.futures import ThreadPoolExecutor

from src.services.multi_cluster_manager import MultiClusterManager
from src.registry.cluster_registry import ClusterRegistry
from src.services.cluster_factory import ClusterFactory
from src.services.template_manager import TemplateManager
from src.models.multi_cluster import ClusterDefinition, PortAllocation
from src.models.base import ServiceStatus
from src.storage.file_backend import FileStorageBackend
from src.networking.port_allocator import PortAllocator


class PerformanceMetrics:
    """Helper class for collecting performance metrics."""
    
    def __init__(self):
        self.metrics = {}
    
    def record_operation(self, operation: str, duration: float, success: bool = True):
        """Record an operation's performance metrics."""
        if operation not in self.metrics:
            self.metrics[operation] = {
                "durations": [],
                "successes": 0,
                "failures": 0,
                "total": 0
            }
        
        self.metrics[operation]["durations"].append(duration)
        self.metrics[operation]["total"] += 1
        
        if success:
            self.metrics[operation]["successes"] += 1
        else:
            self.metrics[operation]["failures"] += 1
    
    def get_statistics(self, operation: str) -> Dict[str, Any]:
        """Get statistics for an operation."""
        if operation not in self.metrics:
            return {}
        
        data = self.metrics[operation]
        durations = data["durations"]
        
        if not durations:
            return data
        
        return {
            **data,
            "avg_duration": statistics.mean(durations),
            "median_duration": statistics.median(durations),
            "min_duration": min(durations),
            "max_duration": max(durations),
            "p95_duration": statistics.quantiles(durations, n=20)[18] if len(durations) >= 20 else max(durations),
            "p99_duration": statistics.quantiles(durations, n=100)[98] if len(durations) >= 100 else max(durations),
            "success_rate": data["successes"] / data["total"] if data["total"] > 0 else 0,
            "throughput": data["total"] / sum(durations) if sum(durations) > 0 else 0
        }
    
    def get_all_statistics(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all operations."""
        return {op: self.get_statistics(op) for op in self.metrics.keys()}


class TestMultiClusterPerformance:
    """Performance tests for multi-cluster system."""
    
    @pytest.fixture(scope="class")
    async def performance_test_setup(self):
        """Set up performance test environment."""
        import tempfile
        import shutil
        from pathlib import Path
        
        # Create temporary directory
        temp_dir = tempfile.mkdtemp(prefix="perf_test_")
        temp_path = Path(temp_dir)
        
        try:
            # Set up storage
            storage_path = temp_path / "storage"
            storage_path.mkdir(exist_ok=True)
            storage_backend = FileStorageBackend(str(storage_path))
            await storage_backend.initialize()
            
            # Set up components
            port_allocator = PortAllocator(start_port=25000, end_port=26000)
            cluster_registry = ClusterRegistry(storage_backend)
            await cluster_registry.initialize()
            
            template_manager = TemplateManager(cluster_registry)
            await template_manager.initialize()
            
            data_dir = temp_path / "clusters"
            data_dir.mkdir(exist_ok=True)
            cluster_factory = ClusterFactory(
                registry=cluster_registry,
                port_allocator=port_allocator,
                data_directory=str(data_dir)
            )
            
            # Create multi-cluster manager
            with patch('src.services.cluster_factory.docker'):
                manager = MultiClusterManager(
                    registry=cluster_registry,
                    factory=cluster_factory,
                    template_manager=template_manager
                )
                await manager.initialize()
            
            yield {
                "manager": manager,
                "registry": cluster_registry,
                "port_allocator": port_allocator,
                "temp_path": temp_path
            }
            
        finally:
            # Cleanup
            try:
                await manager.shutdown()
            except Exception:
                pass
            shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.mark.asyncio
    async def test_cluster_creation_performance(self, performance_test_setup):
        """Test cluster creation performance under load."""
        env = performance_test_setup
        manager = env["manager"]
        port_allocator = env["port_allocator"]
        metrics = PerformanceMetrics()
        
        with patch('src.services.cluster_factory.docker'), \
             patch('src.services.cluster_manager.ClusterManager') as mock_cluster_manager:
            
            # Mock cluster manager for fast operations
            mock_manager_instance = AsyncMock()
            mock_manager_instance.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
            mock_manager_instance.start_cluster.return_value = True
            mock_cluster_manager.return_value = mock_manager_instance
            
            # Test parameters
            num_clusters = 50
            batch_size = 10
            
            async def create_cluster_batch(batch_num: int, batch_size: int) -> List[Tuple[str, float, bool]]:
                """Create a batch of clusters and measure performance."""
                results = []
                
                for i in range(batch_size):
                    cluster_id = f"perf-test-{batch_num}-{i}"
                    
                    try:
                        # Allocate ports
                        ports = await port_allocator.allocate_ports(cluster_id)
                        
                        # Create cluster definition
                        cluster_def = ClusterDefinition(
                            id=cluster_id,
                            name=f"Performance Test Cluster {batch_num}-{i}",
                            description=f"Performance test cluster {batch_num}-{i}",
                            environment="testing",
                            template_id="testing",
                            port_allocation=PortAllocation(
                                kafka_port=ports.kafka_port,
                                rest_proxy_port=ports.rest_proxy_port,
                                ui_port=ports.ui_port
                            ),
                            tags={"performance": "test", "batch": str(batch_num)}
                        )
                        
                        # Measure creation time
                        start_time = time.time()
                        created_cluster = await manager.create_cluster(cluster_def, auto_start=False)
                        end_time = time.time()
                        
                        duration = end_time - start_time
                        results.append((cluster_id, duration, True))
                        
                    except Exception as e:
                        end_time = time.time()
                        duration = end_time - start_time if 'start_time' in locals() else 0
                        results.append((cluster_id, duration, False))
                        print(f"Failed to create cluster {cluster_id}: {e}")
                
                return results
            
            # Create clusters in batches
            all_results = []
            total_start_time = time.time()
            
            for batch_num in range(0, num_clusters, batch_size):
                current_batch_size = min(batch_size, num_clusters - batch_num)
                batch_results = await create_cluster_batch(batch_num // batch_size, current_batch_size)
                all_results.extend(batch_results)
            
            total_end_time = time.time()
            total_duration = total_end_time - total_start_time
            
            # Record metrics
            for cluster_id, duration, success in all_results:
                metrics.record_operation("cluster_creation", duration, success)
            
            # Analyze results
            stats = metrics.get_statistics("cluster_creation")
            
            print(f"\nCluster Creation Performance Results:")
            print(f"Total clusters: {stats['total']}")
            print(f"Successful: {stats['successes']}")
            print(f"Failed: {stats['failures']}")
            print(f"Success rate: {stats['success_rate']:.2%}")
            print(f"Average duration: {stats['avg_duration']:.3f}s")
            print(f"Median duration: {stats['median_duration']:.3f}s")
            print(f"95th percentile: {stats['p95_duration']:.3f}s")
            print(f"Total time: {total_duration:.3f}s")
            print(f"Throughput: {stats['total'] / total_duration:.2f} clusters/second")
            
            # Performance assertions
            assert stats['success_rate'] >= 0.95  # At least 95% success rate
            assert stats['avg_duration'] < 2.0  # Average creation time under 2 seconds
            assert stats['p95_duration'] < 5.0  # 95th percentile under 5 seconds
            
            # Cleanup
            cleanup_tasks = []
            for cluster_id, _, success in all_results:
                if success:
                    task = manager.delete_cluster(cluster_id, force=True)
                    cleanup_tasks.append(task)
            
            if cleanup_tasks:
                await asyncio.gather(*cleanup_tasks, return_exceptions=True)
    
    @pytest.mark.asyncio
    async def test_concurrent_cluster_operations(self, performance_test_setup):
        """Test concurrent cluster operations performance."""
        env = performance_test_setup
        manager = env["manager"]
        port_allocator = env["port_allocator"]
        metrics = PerformanceMetrics()
        
        with patch('src.services.cluster_factory.docker'), \
             patch('src.services.cluster_manager.ClusterManager') as mock_cluster_manager:
            
            mock_manager_instance = AsyncMock()
            mock_manager_instance.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
            mock_manager_instance.start_cluster.return_value = True
            mock_manager_instance.stop_cluster.return_value = True
            mock_cluster_manager.return_value = mock_manager_instance
            
            # Create initial clusters
            num_clusters = 20
            created_clusters = []
            
            for i in range(num_clusters):
                cluster_id = f"concurrent-test-{i}"
                ports = await port_allocator.allocate_ports(cluster_id)
                
                cluster_def = ClusterDefinition(
                    id=cluster_id,
                    name=f"Concurrent Test Cluster {i}",
                    description=f"Concurrent test cluster {i}",
                    environment="testing",
                    template_id="testing",
                    port_allocation=PortAllocation(
                        kafka_port=ports.kafka_port,
                        rest_proxy_port=ports.rest_proxy_port,
                        ui_port=ports.ui_port
                    ),
                    tags={"concurrent": "test"}
                )
                
                created_cluster = await manager.create_cluster(cluster_def, auto_start=False)
                created_clusters.append(created_cluster)
            
            async def perform_operation(cluster_id: str, operation: str) -> Tuple[str, str, float, bool]:
                """Perform an operation on a cluster and measure performance."""
                start_time = time.time()
                success = True
                
                try:
                    if operation == "start":
                        await manager.start_cluster(cluster_id)
                    elif operation == "stop":
                        await manager.stop_cluster(cluster_id)
                    elif operation == "status":
                        await manager.get_cluster_status(cluster_id)
                    elif operation == "list":
                        await manager.list_clusters()
                    
                except Exception as e:
                    success = False
                    print(f"Operation {operation} failed for {cluster_id}: {e}")
                
                end_time = time.time()
                duration = end_time - start_time
                
                return cluster_id, operation, duration, success
            
            # Test concurrent start operations
            print("Testing concurrent start operations...")
            start_tasks = [
                perform_operation(cluster.id, "start") 
                for cluster in created_clusters
            ]
            
            start_results = await asyncio.gather(*start_tasks)
            
            for cluster_id, operation, duration, success in start_results:
                metrics.record_operation(f"concurrent_{operation}", duration, success)
            
            # Test concurrent status checks
            print("Testing concurrent status checks...")
            status_tasks = [
                perform_operation(cluster.id, "status") 
                for cluster in created_clusters
            ]
            
            status_results = await asyncio.gather(*status_tasks)
            
            for cluster_id, operation, duration, success in status_results:
                metrics.record_operation(f"concurrent_{operation}", duration, success)
            
            # Test concurrent list operations
            print("Testing concurrent list operations...")
            list_tasks = [
                perform_operation(cluster.id, "list") 
                for _ in range(10)  # Multiple list operations
            ]
            
            list_results = await asyncio.gather(*list_tasks)
            
            for cluster_id, operation, duration, success in list_results:
                metrics.record_operation(f"concurrent_{operation}", duration, success)
            
            # Test concurrent stop operations
            print("Testing concurrent stop operations...")
            stop_tasks = [
                perform_operation(cluster.id, "stop") 
                for cluster in created_clusters
            ]
            
            stop_results = await asyncio.gather(*stop_tasks)
            
            for cluster_id, operation, duration, success in stop_results:
                metrics.record_operation(f"concurrent_{operation}", duration, success)
            
            # Analyze results
            all_stats = metrics.get_all_statistics()
            
            print(f"\nConcurrent Operations Performance Results:")
            for operation, stats in all_stats.items():
                print(f"\n{operation}:")
                print(f"  Success rate: {stats['success_rate']:.2%}")
                print(f"  Average duration: {stats['avg_duration']:.3f}s")
                print(f"  95th percentile: {stats['p95_duration']:.3f}s")
                print(f"  Throughput: {stats['throughput']:.2f} ops/second")
            
            # Performance assertions
            for operation, stats in all_stats.items():
                assert stats['success_rate'] >= 0.90  # At least 90% success rate
                if "start" in operation or "stop" in operation:
                    assert stats['avg_duration'] < 3.0  # Start/stop under 3 seconds
                elif "status" in operation or "list" in operation:
                    assert stats['avg_duration'] < 1.0  # Status/list under 1 second
            
            # Cleanup
            cleanup_tasks = [
                manager.delete_cluster(cluster.id, force=True) 
                for cluster in created_clusters
            ]
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)
    
    @pytest.mark.asyncio
    async def test_registry_performance(self, performance_test_setup):
        """Test cluster registry performance under load."""
        env = performance_test_setup
        registry = env["registry"]
        port_allocator = env["port_allocator"]
        metrics = PerformanceMetrics()
        
        # Create test cluster definitions
        num_clusters = 100
        cluster_definitions = []
        
        for i in range(num_clusters):
            cluster_id = f"registry-perf-{i}"
            ports = await port_allocator.allocate_ports(cluster_id)
            
            cluster_def = ClusterDefinition(
                id=cluster_id,
                name=f"Registry Performance Test {i}",
                description=f"Registry performance test cluster {i}",
                environment="testing",
                template_id="testing",
                port_allocation=PortAllocation(
                    kafka_port=ports.kafka_port,
                    rest_proxy_port=ports.rest_proxy_port,
                    ui_port=ports.ui_port
                ),
                tags={"registry": "performance", "batch": str(i // 10)}
            )
            cluster_definitions.append(cluster_def)
        
        # Test registry write performance
        print("Testing registry write performance...")
        write_start_time = time.time()
        
        for cluster_def in cluster_definitions:
            start_time = time.time()
            try:
                await registry.register_cluster(cluster_def)
                success = True
            except Exception as e:
                success = False
                print(f"Failed to register cluster {cluster_def.id}: {e}")
            
            end_time = time.time()
            duration = end_time - start_time
            metrics.record_operation("registry_write", duration, success)
        
        write_end_time = time.time()
        write_total_time = write_end_time - write_start_time
        
        # Test registry read performance
        print("Testing registry read performance...")
        read_start_time = time.time()
        
        for cluster_def in cluster_definitions:
            start_time = time.time()
            try:
                retrieved_cluster = await registry.get_cluster(cluster_def.id)
                success = retrieved_cluster is not None
            except Exception as e:
                success = False
                print(f"Failed to retrieve cluster {cluster_def.id}: {e}")
            
            end_time = time.time()
            duration = end_time - start_time
            metrics.record_operation("registry_read", duration, success)
        
        read_end_time = time.time()
        read_total_time = read_end_time - read_start_time
        
        # Test registry list performance
        print("Testing registry list performance...")
        list_iterations = 20
        
        for i in range(list_iterations):
            start_time = time.time()
            try:
                clusters = await registry.list_clusters()
                success = len(clusters) > 0
            except Exception as e:
                success = False
                print(f"Failed to list clusters: {e}")
            
            end_time = time.time()
            duration = end_time - start_time
            metrics.record_operation("registry_list", duration, success)
        
        # Test registry search performance
        print("Testing registry search performance...")
        search_iterations = 10
        
        for i in range(search_iterations):
            start_time = time.time()
            try:
                results = await registry.search_clusters(
                    tags={"registry": "performance"},
                    environment="testing"
                )
                success = len(results) > 0
            except Exception as e:
                success = False
                print(f"Failed to search clusters: {e}")
            
            end_time = time.time()
            duration = end_time - start_time
            metrics.record_operation("registry_search", duration, success)
        
        # Analyze results
        all_stats = metrics.get_all_statistics()
        
        print(f"\nRegistry Performance Results:")
        for operation, stats in all_stats.items():
            print(f"\n{operation}:")
            print(f"  Total operations: {stats['total']}")
            print(f"  Success rate: {stats['success_rate']:.2%}")
            print(f"  Average duration: {stats['avg_duration']:.4f}s")
            print(f"  Median duration: {stats['median_duration']:.4f}s")
            print(f"  95th percentile: {stats['p95_duration']:.4f}s")
            print(f"  Throughput: {stats['throughput']:.2f} ops/second")
        
        print(f"\nOverall Timings:")
        print(f"  Write {num_clusters} clusters: {write_total_time:.3f}s ({num_clusters/write_total_time:.2f} ops/sec)")
        print(f"  Read {num_clusters} clusters: {read_total_time:.3f}s ({num_clusters/read_total_time:.2f} ops/sec)")
        
        # Performance assertions
        write_stats = all_stats["registry_write"]
        read_stats = all_stats["registry_read"]
        list_stats = all_stats["registry_list"]
        
        assert write_stats['success_rate'] >= 0.99  # 99% write success rate
        assert read_stats['success_rate'] >= 0.99   # 99% read success rate
        assert list_stats['success_rate'] >= 0.99   # 99% list success rate
        
        assert write_stats['avg_duration'] < 0.1    # Write under 100ms
        assert read_stats['avg_duration'] < 0.05    # Read under 50ms
        assert list_stats['avg_duration'] < 0.5     # List under 500ms
        
        # Cleanup
        for cluster_def in cluster_definitions:
            try:
                await registry.delete_cluster(cluster_def.id)
            except Exception:
                pass  # Ignore cleanup errors
    
    @pytest.mark.asyncio
    async def test_memory_usage_under_load(self, performance_test_setup):
        """Test memory usage under load."""
        env = performance_test_setup
        manager = env["manager"]
        port_allocator = env["port_allocator"]
        
        import psutil
        import gc
        
        # Get initial memory usage
        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        print(f"Initial memory usage: {initial_memory:.2f} MB")
        
        with patch('src.services.cluster_factory.docker'), \
             patch('src.services.cluster_manager.ClusterManager') as mock_cluster_manager:
            
            mock_manager_instance = AsyncMock()
            mock_manager_instance.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
            mock_manager_instance.start_cluster.return_value = True
            mock_cluster_manager.return_value = mock_manager_instance
            
            # Create many clusters to test memory usage
            num_clusters = 200
            created_clusters = []
            memory_measurements = []
            
            for i in range(num_clusters):
                cluster_id = f"memory-test-{i}"
                ports = await port_allocator.allocate_ports(cluster_id)
                
                cluster_def = ClusterDefinition(
                    id=cluster_id,
                    name=f"Memory Test Cluster {i}",
                    description=f"Memory test cluster {i}",
                    environment="testing",
                    template_id="testing",
                    port_allocation=PortAllocation(
                        kafka_port=ports.kafka_port,
                        rest_proxy_port=ports.rest_proxy_port,
                        ui_port=ports.ui_port
                    ),
                    tags={"memory": "test"}
                )
                
                created_cluster = await manager.create_cluster(cluster_def, auto_start=True)
                created_clusters.append(created_cluster)
                
                # Measure memory every 20 clusters
                if (i + 1) % 20 == 0:
                    current_memory = process.memory_info().rss / 1024 / 1024  # MB
                    memory_measurements.append((i + 1, current_memory))
                    print(f"After {i + 1} clusters: {current_memory:.2f} MB")
            
            # Final memory measurement
            final_memory = process.memory_info().rss / 1024 / 1024  # MB
            memory_measurements.append((num_clusters, final_memory))
            
            # Calculate memory growth
            memory_growth = final_memory - initial_memory
            memory_per_cluster = memory_growth / num_clusters
            
            print(f"\nMemory Usage Analysis:")
            print(f"Initial memory: {initial_memory:.2f} MB")
            print(f"Final memory: {final_memory:.2f} MB")
            print(f"Total growth: {memory_growth:.2f} MB")
            print(f"Memory per cluster: {memory_per_cluster:.3f} MB")
            
            # Test memory cleanup
            print("Testing memory cleanup...")
            
            # Delete half the clusters
            cleanup_count = num_clusters // 2
            cleanup_tasks = [
                manager.delete_cluster(cluster.id, force=True)
                for cluster in created_clusters[:cleanup_count]
            ]
            
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)
            
            # Force garbage collection
            gc.collect()
            await asyncio.sleep(1)  # Allow cleanup to complete
            
            # Measure memory after cleanup
            cleanup_memory = process.memory_info().rss / 1024 / 1024  # MB
            memory_freed = final_memory - cleanup_memory
            
            print(f"Memory after cleanup: {cleanup_memory:.2f} MB")
            print(f"Memory freed: {memory_freed:.2f} MB")
            print(f"Memory freed per cluster: {memory_freed / cleanup_count:.3f} MB")
            
            # Performance assertions
            assert memory_per_cluster < 5.0  # Less than 5MB per cluster
            assert memory_freed > 0  # Some memory should be freed
            assert memory_freed / cleanup_count > 0.1  # At least 0.1MB freed per cluster
            
            # Cleanup remaining clusters
            remaining_cleanup_tasks = [
                manager.delete_cluster(cluster.id, force=True)
                for cluster in created_clusters[cleanup_count:]
            ]
            await asyncio.gather(*remaining_cleanup_tasks, return_exceptions=True)
    
    @pytest.mark.asyncio
    async def test_system_limits(self, performance_test_setup):
        """Test system behavior at limits."""
        env = performance_test_setup
        manager = env["manager"]
        port_allocator = env["port_allocator"]
        
        with patch('src.services.cluster_factory.docker'), \
             patch('src.services.cluster_manager.ClusterManager') as mock_cluster_manager:
            
            mock_manager_instance = AsyncMock()
            mock_manager_instance.get_status.return_value = Mock(status=ServiceStatus.RUNNING)
            mock_manager_instance.start_cluster.return_value = True
            mock_cluster_manager.return_value = mock_manager_instance
            
            # Test port exhaustion
            print("Testing port allocation limits...")
            
            # Try to allocate more ports than available
            port_range = 26000 - 25000  # Available port range
            max_clusters = port_range // 3  # Each cluster needs 3 ports
            
            successful_allocations = 0
            failed_allocations = 0
            
            for i in range(max_clusters + 10):  # Try to exceed limit
                cluster_id = f"limit-test-{i}"
                try:
                    ports = await port_allocator.allocate_ports(cluster_id)
                    successful_allocations += 1
                except Exception as e:
                    failed_allocations += 1
                    if i < max_clusters:
                        print(f"Unexpected early failure at {i}: {e}")
            
            print(f"Port allocation results:")
            print(f"  Successful: {successful_allocations}")
            print(f"  Failed: {failed_allocations}")
            print(f"  Expected max: ~{max_clusters}")
            
            # Should successfully allocate up to the limit
            assert successful_allocations >= max_clusters * 0.9  # Allow some variance
            assert failed_allocations > 0  # Should eventually fail
            
            # Test rapid cluster creation/deletion cycles
            print("Testing rapid create/delete cycles...")
            
            cycle_count = 50
            cycle_metrics = PerformanceMetrics()
            
            for cycle in range(cycle_count):
                cluster_id = f"cycle-test-{cycle}"
                
                # Create
                start_time = time.time()
                try:
                    ports = await port_allocator.allocate_ports(cluster_id)
                    cluster_def = ClusterDefinition(
                        id=cluster_id,
                        name=f"Cycle Test {cycle}",
                        description=f"Cycle test cluster {cycle}",
                        environment="testing",
                        template_id="testing",
                        port_allocation=PortAllocation(
                            kafka_port=ports.kafka_port,
                            rest_proxy_port=ports.rest_proxy_port,
                            ui_port=ports.ui_port
                        ),
                        tags={"cycle": "test"}
                    )
                    
                    created_cluster = await manager.create_cluster(cluster_def, auto_start=False)
                    create_time = time.time() - start_time
                    cycle_metrics.record_operation("create", create_time, True)
                    
                    # Delete immediately
                    delete_start = time.time()
                    await manager.delete_cluster(cluster_id, force=True)
                    delete_time = time.time() - delete_start
                    cycle_metrics.record_operation("delete", delete_time, True)
                    
                except Exception as e:
                    end_time = time.time()
                    duration = end_time - start_time
                    cycle_metrics.record_operation("create", duration, False)
                    print(f"Cycle {cycle} failed: {e}")
            
            # Analyze cycle performance
            create_stats = cycle_metrics.get_statistics("create")
            delete_stats = cycle_metrics.get_statistics("delete")
            
            print(f"\nRapid cycle results:")
            print(f"Create success rate: {create_stats['success_rate']:.2%}")
            print(f"Create avg time: {create_stats['avg_duration']:.3f}s")
            print(f"Delete success rate: {delete_stats['success_rate']:.2%}")
            print(f"Delete avg time: {delete_stats['avg_duration']:.3f}s")
            
            # Performance assertions for cycles
            assert create_stats['success_rate'] >= 0.95  # 95% create success
            assert delete_stats['success_rate'] >= 0.95  # 95% delete success
            assert create_stats['avg_duration'] < 1.0    # Fast creation
            assert delete_stats['avg_duration'] < 0.5    # Fast deletion


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])