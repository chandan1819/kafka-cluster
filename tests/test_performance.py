"""
Performance tests for the Local Kafka Manager.

These tests measure throughput, latency, and resource usage
under various load conditions.
"""

import pytest
import asyncio
import time
import statistics
from typing import List, Dict, Any
from unittest.mock import patch, Mock
from concurrent.futures import ThreadPoolExecutor, as_completed

from fastapi.testclient import TestClient
from src.main import app
from src.services.message_manager import MessageManager
from src.services.topic_manager import TopicManager
from src.services.cluster_manager import ClusterManager


class PerformanceTestBase:
    """Base class for performance tests with common utilities."""
    
    @pytest.fixture(autouse=True)
    def setup_performance_test(self, performance_test_config):
        """Set up performance test environment."""
        self.config = performance_test_config
        self.client = TestClient(app)
        self.message_manager = MessageManager()
        self.topic_manager = TopicManager()
        self.cluster_manager = ClusterManager()
        
        # Performance metrics storage
        self.metrics = {
            "latencies": [],
            "throughput": [],
            "errors": [],
            "resource_usage": []
        }
    
    def measure_latency(self, func, *args, **kwargs):
        """Measure function execution latency."""
        start_time = time.perf_counter()
        try:
            result = func(*args, **kwargs)
            end_time = time.perf_counter()
            latency = (end_time - start_time) * 1000  # Convert to milliseconds
            self.metrics["latencies"].append(latency)
            return result, latency
        except Exception as e:
            end_time = time.perf_counter()
            latency = (end_time - start_time) * 1000
            self.metrics["errors"].append({"error": str(e), "latency": latency})
            raise
    
    async def measure_async_latency(self, func, *args, **kwargs):
        """Measure async function execution latency."""
        start_time = time.perf_counter()
        try:
            result = await func(*args, **kwargs)
            end_time = time.perf_counter()
            latency = (end_time - start_time) * 1000  # Convert to milliseconds
            self.metrics["latencies"].append(latency)
            return result, latency
        except Exception as e:
            end_time = time.perf_counter()
            latency = (end_time - start_time) * 1000
            self.metrics["errors"].append({"error": str(e), "latency": latency})
            raise
    
    def calculate_throughput(self, operations_count: int, duration_seconds: float) -> float:
        """Calculate operations per second."""
        return operations_count / duration_seconds if duration_seconds > 0 else 0
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance test summary."""
        latencies = self.metrics["latencies"]
        if not latencies:
            return {"error": "No latency measurements recorded"}
        
        return {
            "latency_stats": {
                "min_ms": min(latencies),
                "max_ms": max(latencies),
                "mean_ms": statistics.mean(latencies),
                "median_ms": statistics.median(latencies),
                "p95_ms": statistics.quantiles(latencies, n=20)[18] if len(latencies) >= 20 else max(latencies),
                "p99_ms": statistics.quantiles(latencies, n=100)[98] if len(latencies) >= 100 else max(latencies)
            },
            "throughput": self.metrics["throughput"],
            "error_count": len(self.metrics["errors"]),
            "total_operations": len(latencies)
        }


@pytest.mark.performance
class TestMessageThroughputPerformance(PerformanceTestBase):
    """Test message production and consumption throughput."""
    
    @pytest.mark.asyncio
    async def test_message_production_throughput(self, performance_test_config, generate_test_messages):
        """Test message production throughput under load."""
        message_count = performance_test_config["message_count"]
        test_messages = generate_test_messages(message_count, "perf-test-topic")
        
        with patch('src.services.message_manager.MessageManager.produce_message') as mock_produce:
            from src.models.message import ProduceResult
            
            # Mock fast message production
            def mock_produce_func(**kwargs):
                return ProduceResult(
                    topic=kwargs["topic"],
                    partition=0,
                    offset=hash(kwargs["key"]) % 10000,
                    key=kwargs["key"],
                    timestamp=int(time.time() * 1000)
                )
            
            mock_produce.side_effect = mock_produce_func
            
            # Measure production throughput
            start_time = time.perf_counter()
            successful_productions = 0
            
            for message in test_messages:
                try:
                    response = self.client.post("/produce", json=message)
                    if response.status_code == 201:
                        successful_productions += 1
                except Exception as e:
                    self.metrics["errors"].append({"error": str(e)})
            
            end_time = time.perf_counter()
            duration = end_time - start_time
            
            throughput = self.calculate_throughput(successful_productions, duration)
            self.metrics["throughput"].append(throughput)
            
            # Performance assertions
            assert successful_productions >= message_count * 0.95  # 95% success rate
            assert throughput >= 100  # At least 100 messages/second
            
            # Log performance results
            summary = self.get_performance_summary()
            print(f"Production Throughput: {throughput:.2f} messages/second")
            print(f"Total Messages: {successful_productions}/{message_count}")
            print(f"Duration: {duration:.2f} seconds")
    
    @pytest.mark.asyncio
    async def test_message_consumption_throughput(self, performance_test_config):
        """Test message consumption throughput."""
        with patch('src.services.message_manager.MessageManager.consume_messages') as mock_consume:
            from src.models.message import Message, ConsumeResponse
            
            # Mock message consumption with varying batch sizes
            def mock_consume_func(**kwargs):
                batch_size = min(kwargs.get("max_messages", 10), 100)
                messages = [
                    Message(
                        topic=kwargs["topic"],
                        partition=0,
                        offset=i,
                        key=f"key-{i}",
                        value={"id": i, "data": f"message-{i}"},
                        timestamp=int(time.time() * 1000) + i
                    )
                    for i in range(batch_size)
                ]
                return ConsumeResponse(
                    messages=messages,
                    consumer_group=kwargs["consumer_group"],
                    topic=kwargs["topic"],
                    total_consumed=len(messages),
                    has_more=len(messages) == batch_size
                )
            
            mock_consume.side_effect = mock_consume_func
            
            # Test consumption with different batch sizes
            batch_sizes = [1, 10, 50, 100]
            consumption_results = {}
            
            for batch_size in batch_sizes:
                start_time = time.perf_counter()
                total_consumed = 0
                iterations = 0
                
                # Consume messages for a fixed duration
                test_duration = 5  # seconds
                while time.perf_counter() - start_time < test_duration:
                    params = {
                        "topic": "perf-test-topic",
                        "consumer_group": f"perf-test-group-{batch_size}",
                        "max_messages": batch_size,
                        "timeout_ms": 1000
                    }
                    
                    response = self.client.get("/consume", params=params)
                    if response.status_code == 200:
                        data = response.json()
                        total_consumed += data["total_consumed"]
                        iterations += 1
                
                end_time = time.perf_counter()
                duration = end_time - start_time
                throughput = self.calculate_throughput(total_consumed, duration)
                
                consumption_results[batch_size] = {
                    "throughput": throughput,
                    "total_consumed": total_consumed,
                    "iterations": iterations,
                    "duration": duration
                }
            
            # Verify that larger batch sizes generally provide better throughput
            throughputs = [consumption_results[bs]["throughput"] for bs in batch_sizes]
            assert max(throughputs) >= 500  # At least 500 messages/second with optimal batch size
            
            # Log results
            for batch_size, results in consumption_results.items():
                print(f"Batch Size {batch_size}: {results['throughput']:.2f} messages/second")
    
    @pytest.mark.asyncio
    async def test_concurrent_producer_performance(self, performance_test_config):
        """Test performance with multiple concurrent producers."""
        concurrent_producers = performance_test_config["concurrent_producers"]
        messages_per_producer = performance_test_config["message_count"] // concurrent_producers
        
        with patch('src.services.message_manager.MessageManager.produce_message') as mock_produce:
            from src.models.message import ProduceResult
            
            # Mock production with slight delay to simulate real conditions
            async def mock_produce_func(**kwargs):
                await asyncio.sleep(0.001)  # 1ms delay
                return ProduceResult(
                    topic=kwargs["topic"],
                    partition=hash(kwargs["key"]) % 3,
                    offset=hash(kwargs["key"]) % 10000,
                    key=kwargs["key"],
                    timestamp=int(time.time() * 1000)
                )
            
            mock_produce.side_effect = mock_produce_func
            
            async def producer_task(producer_id: int):
                """Single producer task."""
                successful_productions = 0
                start_time = time.perf_counter()
                
                for i in range(messages_per_producer):
                    message_data = {
                        "topic": "concurrent-perf-test-topic",
                        "key": f"producer-{producer_id}-key-{i}",
                        "value": {"producer_id": producer_id, "sequence": i, "data": f"message-{i}"}
                    }
                    
                    try:
                        response = self.client.post("/produce", json=message_data)
                        if response.status_code == 201:
                            successful_productions += 1
                    except Exception as e:
                        self.metrics["errors"].append({"producer_id": producer_id, "error": str(e)})
                
                end_time = time.perf_counter()
                duration = end_time - start_time
                throughput = self.calculate_throughput(successful_productions, duration)
                
                return {
                    "producer_id": producer_id,
                    "successful_productions": successful_productions,
                    "duration": duration,
                    "throughput": throughput
                }
            
            # Run concurrent producers
            start_time = time.perf_counter()
            tasks = [producer_task(i) for i in range(concurrent_producers)]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            end_time = time.perf_counter()
            
            total_duration = end_time - start_time
            total_successful = sum(r["successful_productions"] for r in results if isinstance(r, dict))
            overall_throughput = self.calculate_throughput(total_successful, total_duration)
            
            # Performance assertions
            assert total_successful >= (messages_per_producer * concurrent_producers) * 0.9  # 90% success rate
            assert overall_throughput >= 200  # At least 200 messages/second overall
            
            # Log results
            print(f"Concurrent Producers: {concurrent_producers}")
            print(f"Overall Throughput: {overall_throughput:.2f} messages/second")
            print(f"Total Messages: {total_successful}")
            print(f"Total Duration: {total_duration:.2f} seconds")
            
            for result in results:
                if isinstance(result, dict):
                    print(f"Producer {result['producer_id']}: {result['throughput']:.2f} msg/sec")


@pytest.mark.performance
class TestAPIEndpointPerformance(PerformanceTestBase):
    """Test API endpoint performance under load."""
    
    def test_cluster_status_endpoint_performance(self, performance_test_config):
        """Test cluster status endpoint performance under load."""
        with patch('src.api.routes.cluster_manager.get_status') as mock_status:
            from src.models.cluster import ClusterStatus
            from src.models.base import ServiceStatus
            
            mock_status.return_value = ClusterStatus(
                status=ServiceStatus.RUNNING,
                broker_count=1,
                version="7.4.0",
                endpoints={"kafka": "localhost:9092"},
                uptime=3600
            )
            
            # Load test the endpoint
            request_count = 1000
            start_time = time.perf_counter()
            successful_requests = 0
            
            for i in range(request_count):
                try:
                    response = self.client.get("/cluster/status")
                    if response.status_code == 200:
                        successful_requests += 1
                except Exception as e:
                    self.metrics["errors"].append({"request": i, "error": str(e)})
            
            end_time = time.perf_counter()
            duration = end_time - start_time
            throughput = self.calculate_throughput(successful_requests, duration)
            
            # Performance assertions
            assert successful_requests >= request_count * 0.99  # 99% success rate
            assert throughput >= 1000  # At least 1000 requests/second
            assert duration < 2.0  # Complete within 2 seconds
            
            print(f"Status Endpoint Throughput: {throughput:.2f} requests/second")
            print(f"Success Rate: {(successful_requests/request_count)*100:.2f}%")
    
    def test_topics_list_endpoint_performance(self, performance_test_config):
        """Test topics list endpoint performance with many topics."""
        with patch('src.services.topic_manager.TopicManager.list_topics') as mock_list:
            from src.models.topic import TopicInfo
            
            # Mock many topics
            topic_count = 1000
            mock_topics = [
                TopicInfo(
                    name=f"topic-{i:04d}",
                    partitions=(i % 10) + 1,
                    replication_factor=1,
                    size_bytes=i * 1024
                )
                for i in range(topic_count)
            ]
            mock_list.return_value = mock_topics
            
            # Measure response time with large topic list
            start_time = time.perf_counter()
            response = self.client.get("/topics")
            end_time = time.perf_counter()
            
            response_time = (end_time - start_time) * 1000  # Convert to milliseconds
            
            assert response.status_code == 200
            data = response.json()
            assert data["total_count"] == topic_count
            assert len(data["topics"]) == topic_count
            
            # Performance assertions
            assert response_time < 1000  # Less than 1 second
            
            print(f"Topics List Response Time: {response_time:.2f}ms for {topic_count} topics")
    
    def test_service_catalog_endpoint_performance(self, performance_test_config):
        """Test service catalog endpoint performance."""
        with patch('src.services.service_catalog.ServiceCatalog.get_catalog') as mock_catalog:
            from src.models.catalog import CatalogResponse
            from src.models.cluster import ClusterStatus
            from src.models.topic import TopicInfo
            from src.models.base import ServiceStatus
            
            # Mock comprehensive catalog
            mock_catalog.return_value = CatalogResponse(
                cluster=ClusterStatus(
                    status=ServiceStatus.RUNNING,
                    broker_count=3,
                    version="7.4.0",
                    endpoints={
                        "kafka": "localhost:9092",
                        "kafka-rest-proxy": "localhost:8082",
                        "kafka-ui": "localhost:8080"
                    },
                    uptime=7200
                ),
                topics=[
                    TopicInfo(name=f"topic-{i}", partitions=3, replication_factor=1, size_bytes=i*1024)
                    for i in range(100)
                ],
                available_apis=[
                    {"path": f"/api/v{i}", "method": "GET", "description": f"API endpoint {i}"}
                    for i in range(50)
                ],
                services={
                    "kafka": ServiceStatus.RUNNING,
                    "kafka-rest-proxy": ServiceStatus.RUNNING,
                    "kafka-ui": ServiceStatus.RUNNING
                }
            )
            
            # Test catalog endpoint performance
            request_count = 100
            response_times = []
            
            for i in range(request_count):
                start_time = time.perf_counter()
                response = self.client.get("/catalog")
                end_time = time.perf_counter()
                
                response_time = (end_time - start_time) * 1000
                response_times.append(response_time)
                
                assert response.status_code == 200
            
            # Calculate statistics
            avg_response_time = statistics.mean(response_times)
            max_response_time = max(response_times)
            p95_response_time = statistics.quantiles(response_times, n=20)[18]
            
            # Performance assertions
            assert avg_response_time < 100  # Average less than 100ms
            assert max_response_time < 500  # Max less than 500ms
            assert p95_response_time < 200  # 95th percentile less than 200ms
            
            print(f"Catalog Endpoint - Avg: {avg_response_time:.2f}ms, Max: {max_response_time:.2f}ms, P95: {p95_response_time:.2f}ms")


@pytest.mark.performance
class TestResourceUsagePerformance(PerformanceTestBase):
    """Test resource usage under various load conditions."""
    
    @pytest.mark.asyncio
    async def test_memory_usage_under_load(self, performance_test_config):
        """Test memory usage during high-load operations."""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        with patch('src.services.message_manager.MessageManager.produce_message') as mock_produce:
            from src.models.message import ProduceResult
            
            # Mock message production
            mock_produce.return_value = ProduceResult(
                topic="memory-test-topic",
                partition=0,
                offset=0,
                key="test-key",
                timestamp=int(time.time() * 1000)
            )
            
            # Generate load
            message_count = 10000
            memory_samples = []
            
            for i in range(message_count):
                message_data = {
                    "topic": "memory-test-topic",
                    "key": f"key-{i}",
                    "value": {"id": i, "data": f"large-message-data-{i}" * 10}  # Larger messages
                }
                
                self.client.post("/produce", json=message_data)
                
                # Sample memory usage every 1000 messages
                if i % 1000 == 0:
                    current_memory = process.memory_info().rss / 1024 / 1024  # MB
                    memory_samples.append(current_memory)
            
            final_memory = process.memory_info().rss / 1024 / 1024  # MB
            memory_increase = final_memory - initial_memory
            max_memory = max(memory_samples) if memory_samples else final_memory
            
            # Memory usage assertions
            assert memory_increase < 100  # Less than 100MB increase
            assert max_memory < initial_memory + 150  # Peak usage reasonable
            
            print(f"Memory Usage - Initial: {initial_memory:.2f}MB, Final: {final_memory:.2f}MB")
            print(f"Memory Increase: {memory_increase:.2f}MB, Peak: {max_memory:.2f}MB")
    
    def test_concurrent_request_handling(self, performance_test_config):
        """Test handling of concurrent requests."""
        with patch('src.api.routes.cluster_manager.get_status') as mock_status:
            from src.models.cluster import ClusterStatus
            from src.models.base import ServiceStatus
            
            mock_status.return_value = ClusterStatus(
                status=ServiceStatus.RUNNING,
                broker_count=1,
                version="7.4.0",
                endpoints={"kafka": "localhost:9092"},
                uptime=3600
            )
            
            # Test concurrent requests using ThreadPoolExecutor
            concurrent_requests = 50
            request_count_per_thread = 20
            
            def make_requests(thread_id: int):
                """Make multiple requests in a single thread."""
                successful_requests = 0
                response_times = []
                
                for i in range(request_count_per_thread):
                    start_time = time.perf_counter()
                    try:
                        response = self.client.get("/cluster/status")
                        end_time = time.perf_counter()
                        
                        if response.status_code == 200:
                            successful_requests += 1
                            response_times.append((end_time - start_time) * 1000)
                    except Exception as e:
                        self.metrics["errors"].append({"thread": thread_id, "request": i, "error": str(e)})
                
                return {
                    "thread_id": thread_id,
                    "successful_requests": successful_requests,
                    "response_times": response_times
                }
            
            # Execute concurrent requests
            start_time = time.perf_counter()
            with ThreadPoolExecutor(max_workers=concurrent_requests) as executor:
                futures = [executor.submit(make_requests, i) for i in range(concurrent_requests)]
                results = [future.result() for future in as_completed(futures)]
            end_time = time.perf_counter()
            
            total_duration = end_time - start_time
            total_requests = sum(r["successful_requests"] for r in results)
            all_response_times = []
            for r in results:
                all_response_times.extend(r["response_times"])
            
            overall_throughput = self.calculate_throughput(total_requests, total_duration)
            avg_response_time = statistics.mean(all_response_times) if all_response_times else 0
            
            # Performance assertions
            expected_total = concurrent_requests * request_count_per_thread
            assert total_requests >= expected_total * 0.95  # 95% success rate
            assert overall_throughput >= 500  # At least 500 requests/second
            assert avg_response_time < 100  # Average response time less than 100ms
            
            print(f"Concurrent Requests - Threads: {concurrent_requests}, Total Requests: {total_requests}")
            print(f"Throughput: {overall_throughput:.2f} req/sec, Avg Response Time: {avg_response_time:.2f}ms")


@pytest.mark.performance
class TestLoadTestScenarios(PerformanceTestBase):
    """Test various load scenarios."""
    
    @pytest.mark.asyncio
    async def test_mixed_workload_performance(self, performance_test_config, load_test_scenarios):
        """Test performance under mixed read/write workloads."""
        for scenario in load_test_scenarios:
            print(f"\nRunning load test scenario: {scenario['name']}")
            
            with patch('src.services.message_manager.MessageManager.produce_message') as mock_produce, \
                 patch('src.services.message_manager.MessageManager.consume_messages') as mock_consume, \
                 patch('src.api.routes.cluster_manager.get_status') as mock_status:
                
                from src.models.message import ProduceResult, Message, ConsumeResponse
                from src.models.cluster import ClusterStatus
                from src.models.base import ServiceStatus
                
                # Setup mocks
                mock_produce.return_value = ProduceResult(
                    topic="load-test-topic", partition=0, offset=0, key="key", timestamp=int(time.time() * 1000)
                )
                
                mock_consume.return_value = ConsumeResponse(
                    messages=[Message(
                        topic="load-test-topic", partition=0, offset=0, key="key",
                        value={"data": "test"}, timestamp=int(time.time() * 1000)
                    )],
                    consumer_group="load-test-group", topic="load-test-topic",
                    total_consumed=1, has_more=False
                )
                
                mock_status.return_value = ClusterStatus(
                    status=ServiceStatus.RUNNING, broker_count=1, version="7.4.0",
                    endpoints={"kafka": "localhost:9092"}, uptime=3600
                )
                
                # Run mixed workload
                duration = scenario["duration_seconds"]
                target_ops_per_second = scenario["messages_per_second"]
                
                start_time = time.perf_counter()
                operations_completed = 0
                errors = 0
                
                while time.perf_counter() - start_time < duration:
                    batch_start = time.perf_counter()
                    
                    # Mix of operations: 60% produce, 30% consume, 10% status
                    for i in range(target_ops_per_second):
                        try:
                            operation_type = i % 10
                            if operation_type < 6:  # Produce message
                                message_data = {
                                    "topic": "load-test-topic",
                                    "key": f"key-{i}",
                                    "value": {"id": i, "timestamp": time.time()}
                                }
                                response = self.client.post("/produce", json=message_data)
                            elif operation_type < 9:  # Consume messages
                                params = {
                                    "topic": "load-test-topic",
                                    "consumer_group": "load-test-group",
                                    "max_messages": 5
                                }
                                response = self.client.get("/consume", params=params)
                            else:  # Check status
                                response = self.client.get("/cluster/status")
                            
                            if response.status_code in [200, 201]:
                                operations_completed += 1
                            else:
                                errors += 1
                                
                        except Exception:
                            errors += 1
                    
                    # Control rate
                    batch_duration = time.perf_counter() - batch_start
                    if batch_duration < 1.0:
                        await asyncio.sleep(1.0 - batch_duration)
                
                end_time = time.perf_counter()
                actual_duration = end_time - start_time
                actual_throughput = self.calculate_throughput(operations_completed, actual_duration)
                error_rate = (errors / (operations_completed + errors)) * 100 if (operations_completed + errors) > 0 else 0
                
                # Performance assertions based on scenario
                min_expected_throughput = target_ops_per_second * 0.8  # 80% of target
                assert actual_throughput >= min_expected_throughput
                assert error_rate < 5  # Less than 5% error rate
                
                print(f"Scenario: {scenario['name']}")
                print(f"Target: {target_ops_per_second} ops/sec, Actual: {actual_throughput:.2f} ops/sec")
                print(f"Operations: {operations_completed}, Errors: {errors}, Error Rate: {error_rate:.2f}%")
                print(f"Duration: {actual_duration:.2f} seconds")