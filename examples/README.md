# 📚 Examples

This directory contains practical examples and use cases for Local Kafka Manager.

## 📁 Directory Structure

```
examples/
├── README.md                    # This file
├── basic/                       # Basic usage examples
│   ├── producer_consumer.py     # Simple producer/consumer
│   ├── topic_management.py      # Topic operations
│   └── batch_operations.py      # Batch message handling
├── advanced/                    # Advanced patterns
│   ├── event_sourcing.py        # Event sourcing pattern
│   ├── saga_pattern.py          # Distributed transactions
│   └── stream_processing.py     # Real-time processing
├── integrations/                # Integration examples
│   ├── fastapi_integration.py   # FastAPI + Kafka
│   ├── flask_integration.py     # Flask + Kafka
│   └── django_integration.py    # Django + Kafka
├── testing/                     # Testing examples
│   ├── unit_tests.py           # Unit testing with Kafka
│   ├── integration_tests.py    # Integration testing
│   └── performance_tests.py    # Performance testing
└── scripts/                     # Utility scripts
    ├── data_generator.py        # Generate test data
    ├── load_tester.py          # Load testing
    └── monitoring.py           # Custom monitoring
```

## 🚀 Quick Start Examples

### 1. Basic Producer/Consumer
```python
# examples/basic/producer_consumer.py
import requests
import json
import time

# Configuration
API_BASE = "http://localhost:8000"
TOPIC_NAME = "user-events"

# Create topic
def create_topic():
    response = requests.post(f"{API_BASE}/topics", json={
        "name": TOPIC_NAME,
        "partitions": 3,
        "replication_factor": 1
    })
    print(f"Topic created: {response.json()}")

# Produce messages
def produce_messages():
    for i in range(10):
        message = {
            "topic": TOPIC_NAME,
            "key": f"user_{i}",
            "value": {
                "user_id": f"user_{i}",
                "action": "login",
                "timestamp": int(time.time()),
                "metadata": {"source": "web", "version": "1.0"}
            }
        }
        response = requests.post(f"{API_BASE}/produce", json=message)
        print(f"Produced: {response.json()}")
        time.sleep(0.1)

# Consume messages
def consume_messages():
    params = {
        "topic": TOPIC_NAME,
        "consumer_group": "example-group",
        "max_messages": 10
    }
    response = requests.get(f"{API_BASE}/consume", params=params)
    messages = response.json()
    print(f"Consumed {len(messages.get('messages', []))} messages:")
    for msg in messages.get('messages', []):
        print(f"  Key: {msg['key']}, Value: {msg['value']}")

if __name__ == "__main__":
    create_topic()
    time.sleep(2)  # Wait for topic creation
    produce_messages()
    time.sleep(1)  # Wait for messages to be available
    consume_messages()
```

### 2. Topic Management
```python
# examples/basic/topic_management.py
import requests
import json

API_BASE = "http://localhost:8000"

class TopicManager:
    def __init__(self, api_base=API_BASE):
        self.api_base = api_base
    
    def list_topics(self):
        """List all topics"""
        response = requests.get(f"{self.api_base}/topics")
        return response.json()
    
    def create_topic(self, name, partitions=3, replication_factor=1, config=None):
        """Create a new topic"""
        payload = {
            "name": name,
            "partitions": partitions,
            "replication_factor": replication_factor
        }
        if config:
            payload["config"] = config
        
        response = requests.post(f"{self.api_base}/topics", json=payload)
        return response.json()
    
    def get_topic_details(self, topic_name):
        """Get detailed information about a topic"""
        response = requests.get(f"{self.api_base}/topics/{topic_name}")
        return response.json()
    
    def delete_topic(self, topic_name):
        """Delete a topic"""
        response = requests.delete(f"{self.api_base}/topics/{topic_name}")
        return response.json()
    
    def create_topics_from_config(self, config_file):
        """Create multiple topics from configuration file"""
        with open(config_file, 'r') as f:
            topics_config = json.load(f)
        
        results = []
        for topic_config in topics_config['topics']:
            result = self.create_topic(**topic_config)
            results.append(result)
        
        return results

# Example usage
if __name__ == "__main__":
    tm = TopicManager()
    
    # Create topics for different use cases
    topics = [
        {"name": "user-events", "partitions": 3},
        {"name": "order-events", "partitions": 6},
        {"name": "inventory-updates", "partitions": 2},
        {"name": "notifications", "partitions": 1}
    ]
    
    for topic in topics:
        result = tm.create_topic(**topic)
        print(f"Created topic: {result}")
    
    # List all topics
    all_topics = tm.list_topics()
    print(f"All topics: {all_topics}")
    
    # Get details for a specific topic
    details = tm.get_topic_details("user-events")
    print(f"Topic details: {details}")
```

### 3. Batch Operations
```python
# examples/basic/batch_operations.py
import requests
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

API_BASE = "http://localhost:8000"

class BatchOperations:
    def __init__(self, api_base=API_BASE):
        self.api_base = api_base
    
    def batch_produce(self, topic, messages):
        """Produce multiple messages in a batch"""
        payload = {
            "topic": topic,
            "messages": messages
        }
        response = requests.post(f"{self.api_base}/produce/batch", json=payload)
        return response.json()
    
    def parallel_produce(self, topic, messages, batch_size=10, max_workers=5):
        """Produce messages in parallel batches"""
        batches = [messages[i:i + batch_size] for i in range(0, len(messages), batch_size)]
        results = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_batch = {
                executor.submit(self.batch_produce, topic, batch): batch 
                for batch in batches
            }
            
            for future in as_completed(future_to_batch):
                batch = future_to_batch[future]
                try:
                    result = future.result()
                    results.append(result)
                    print(f"Batch completed: {len(batch)} messages")
                except Exception as exc:
                    print(f"Batch failed: {exc}")
        
        return results
    
    def consume_all_messages(self, topic, consumer_group, batch_size=100):
        """Consume all available messages from a topic"""
        all_messages = []
        
        while True:
            params = {
                "topic": topic,
                "consumer_group": consumer_group,
                "max_messages": batch_size
            }
            response = requests.get(f"{self.api_base}/consume", params=params)
            batch = response.json()
            
            messages = batch.get('messages', [])
            if not messages:
                break
            
            all_messages.extend(messages)
            print(f"Consumed batch of {len(messages)} messages")
            
            if not batch.get('has_more', False):
                break
        
        return all_messages

# Example usage
if __name__ == "__main__":
    batch_ops = BatchOperations()
    topic_name = "batch-test-topic"
    
    # Create topic
    requests.post(f"{API_BASE}/topics", json={
        "name": topic_name,
        "partitions": 6,
        "replication_factor": 1
    })
    
    # Generate test messages
    messages = []
    for i in range(1000):
        messages.append({
            "key": f"key_{i}",
            "value": {
                "id": i,
                "data": f"test_data_{i}",
                "timestamp": int(time.time())
            }
        })
    
    # Produce messages in parallel batches
    print("Starting batch production...")
    start_time = time.time()
    results = batch_ops.parallel_produce(topic_name, messages, batch_size=50, max_workers=10)
    end_time = time.time()
    
    print(f"Produced {len(messages)} messages in {end_time - start_time:.2f} seconds")
    print(f"Rate: {len(messages) / (end_time - start_time):.2f} messages/second")
    
    # Consume all messages
    print("Starting consumption...")
    start_time = time.time()
    consumed_messages = batch_ops.consume_all_messages(topic_name, "batch-consumer")
    end_time = time.time()
    
    print(f"Consumed {len(consumed_messages)} messages in {end_time - start_time:.2f} seconds")
    print(f"Rate: {len(consumed_messages) / (end_time - start_time):.2f} messages/second")
```

## 🎯 Use Case Examples

### Event Sourcing Pattern
```python
# examples/advanced/event_sourcing.py
import requests
import json
import uuid
from datetime import datetime
from typing import List, Dict, Any

API_BASE = "http://localhost:8000"

class EventStore:
    def __init__(self, api_base=API_BASE):
        self.api_base = api_base
        self.events_topic = "events"
        self.snapshots_topic = "snapshots"
        self._ensure_topics()
    
    def _ensure_topics(self):
        """Ensure required topics exist"""
        topics = [
            {"name": self.events_topic, "partitions": 6},
            {"name": self.snapshots_topic, "partitions": 3}
        ]
        
        for topic in topics:
            requests.post(f"{self.api_base}/topics", json=topic)
    
    def append_event(self, aggregate_id: str, event_type: str, event_data: Dict[str, Any], expected_version: int = None):
        """Append an event to the event store"""
        event = {
            "event_id": str(uuid.uuid4()),
            "aggregate_id": aggregate_id,
            "event_type": event_type,
            "event_data": event_data,
            "timestamp": datetime.utcnow().isoformat(),
            "version": expected_version + 1 if expected_version is not None else 1
        }
        
        message = {
            "topic": self.events_topic,
            "key": aggregate_id,
            "value": event
        }
        
        response = requests.post(f"{self.api_base}/produce", json=message)
        return response.json()
    
    def get_events(self, aggregate_id: str, from_version: int = 0) -> List[Dict[str, Any]]:
        """Get all events for an aggregate"""
        # In a real implementation, you'd filter by aggregate_id and version
        # For this example, we'll consume from the topic
        params = {
            "topic": self.events_topic,
            "consumer_group": f"replay-{aggregate_id}",
            "max_messages": 1000
        }
        
        response = requests.get(f"{self.api_base}/consume", params=params)
        messages = response.json().get('messages', [])
        
        # Filter events for this aggregate
        events = [
            msg['value'] for msg in messages 
            if msg['key'] == aggregate_id and msg['value']['version'] > from_version
        ]
        
        return sorted(events, key=lambda x: x['version'])

class UserAggregate:
    def __init__(self, user_id: str, event_store: EventStore):
        self.user_id = user_id
        self.event_store = event_store
        self.version = 0
        self.email = None
        self.name = None
        self.is_active = False
        self.created_at = None
        
        # Load from event store
        self._load_from_events()
    
    def _load_from_events(self):
        """Rebuild aggregate state from events"""
        events = self.event_store.get_events(self.user_id)
        
        for event in events:
            self._apply_event(event)
    
    def _apply_event(self, event: Dict[str, Any]):
        """Apply an event to update aggregate state"""
        event_type = event['event_type']
        event_data = event['event_data']
        
        if event_type == 'UserCreated':
            self.email = event_data['email']
            self.name = event_data['name']
            self.is_active = True
            self.created_at = event['timestamp']
        elif event_type == 'UserEmailChanged':
            self.email = event_data['new_email']
        elif event_type == 'UserDeactivated':
            self.is_active = False
        elif event_type == 'UserReactivated':
            self.is_active = True
        
        self.version = event['version']
    
    def create_user(self, email: str, name: str):
        """Create a new user"""
        if self.version > 0:
            raise ValueError("User already exists")
        
        event_data = {"email": email, "name": name}
        self.event_store.append_event(
            self.user_id, 
            "UserCreated", 
            event_data, 
            self.version
        )
        
        # Apply event locally
        self._apply_event({
            "event_type": "UserCreated",
            "event_data": event_data,
            "version": self.version + 1,
            "timestamp": datetime.utcnow().isoformat()
        })
    
    def change_email(self, new_email: str):
        """Change user email"""
        if not self.is_active:
            raise ValueError("Cannot change email for inactive user")
        
        event_data = {"old_email": self.email, "new_email": new_email}
        self.event_store.append_event(
            self.user_id,
            "UserEmailChanged",
            event_data,
            self.version
        )
        
        self._apply_event({
            "event_type": "UserEmailChanged",
            "event_data": event_data,
            "version": self.version + 1,
            "timestamp": datetime.utcnow().isoformat()
        })
    
    def deactivate(self):
        """Deactivate user"""
        if not self.is_active:
            raise ValueError("User is already inactive")
        
        self.event_store.append_event(
            self.user_id,
            "UserDeactivated",
            {},
            self.version
        )
        
        self._apply_event({
            "event_type": "UserDeactivated",
            "event_data": {},
            "version": self.version + 1,
            "timestamp": datetime.utcnow().isoformat()
        })

# Example usage
if __name__ == "__main__":
    event_store = EventStore()
    user_id = str(uuid.uuid4())
    
    # Create and manipulate user
    user = UserAggregate(user_id, event_store)
    user.create_user("john@example.com", "John Doe")
    user.change_email("john.doe@example.com")
    user.deactivate()
    
    print(f"User {user.user_id}:")
    print(f"  Email: {user.email}")
    print(f"  Name: {user.name}")
    print(f"  Active: {user.is_active}")
    print(f"  Version: {user.version}")
    
    # Rebuild from events (simulating loading from persistence)
    user2 = UserAggregate(user_id, event_store)
    print(f"\\nRebuilt user {user2.user_id}:")
    print(f"  Email: {user2.email}")
    print(f"  Name: {user2.name}")
    print(f"  Active: {user2.is_active}")
    print(f"  Version: {user2.version}")
```

## 🔗 Integration Examples

### FastAPI Integration
```python
# examples/integrations/fastapi_integration.py
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import requests
import asyncio
import httpx
from typing import List, Optional

app = FastAPI(title="Kafka-Integrated API")

# Configuration
KAFKA_API_BASE = "http://localhost:8000"

# Models
class UserEvent(BaseModel):
    user_id: str
    action: str
    metadata: Optional[dict] = None

class OrderEvent(BaseModel):
    order_id: str
    user_id: str
    status: str
    items: List[dict]
    total_amount: float

# Kafka client wrapper
class KafkaClient:
    def __init__(self, base_url: str):
        self.base_url = base_url
    
    async def produce_message(self, topic: str, key: str, value: dict):
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/produce",
                json={"topic": topic, "key": key, "value": value}
            )
            return response.json()
    
    async def consume_messages(self, topic: str, consumer_group: str, max_messages: int = 10):
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/consume",
                params={
                    "topic": topic,
                    "consumer_group": consumer_group,
                    "max_messages": max_messages
                }
            )
            return response.json()

kafka_client = KafkaClient(KAFKA_API_BASE)

# API Endpoints
@app.post("/users/{user_id}/events")
async def create_user_event(user_id: str, event: UserEvent, background_tasks: BackgroundTasks):
    """Create a user event and publish to Kafka"""
    
    # Validate user exists (mock validation)
    if not user_id:
        raise HTTPException(status_code=400, detail="Invalid user ID")
    
    # Publish event to Kafka
    event_data = event.dict()
    event_data["user_id"] = user_id
    
    background_tasks.add_task(
        kafka_client.produce_message,
        "user-events",
        user_id,
        event_data
    )
    
    return {"message": "Event created", "user_id": user_id, "event": event_data}

@app.post("/orders")
async def create_order(order: OrderEvent, background_tasks: BackgroundTasks):
    """Create an order and publish events"""
    
    # Publish order event
    background_tasks.add_task(
        kafka_client.produce_message,
        "order-events",
        order.order_id,
        order.dict()
    )
    
    # Publish inventory update events
    for item in order.items:
        inventory_event = {
            "product_id": item["product_id"],
            "quantity_change": -item["quantity"],
            "reason": "order_placed",
            "order_id": order.order_id
        }
        background_tasks.add_task(
            kafka_client.produce_message,
            "inventory-updates",
            item["product_id"],
            inventory_event
        )
    
    return {"message": "Order created", "order_id": order.order_id}

@app.get("/events/{topic}")
async def get_recent_events(topic: str, limit: int = 10):
    """Get recent events from a topic"""
    
    try:
        result = await kafka_client.consume_messages(
            topic, 
            f"api-consumer-{topic}", 
            limit
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{KAFKA_API_BASE}/health")
            kafka_status = response.json()
        
        return {
            "status": "healthy",
            "kafka": kafka_status
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e)
        }

# Background event processor
@app.on_event("startup")
async def startup_event():
    """Initialize topics and start background processors"""
    
    # Create required topics
    topics = [
        {"name": "user-events", "partitions": 3},
        {"name": "order-events", "partitions": 6},
        {"name": "inventory-updates", "partitions": 3}
    ]
    
    async with httpx.AsyncClient() as client:
        for topic in topics:
            try:
                await client.post(f"{KAFKA_API_BASE}/topics", json=topic)
            except Exception as e:
                print(f"Topic creation failed: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
```

## 🧪 Testing Examples

### Integration Testing
```python
# examples/testing/integration_tests.py
import pytest
import requests
import time
import json
from typing import List, Dict

class TestKafkaIntegration:
    """Integration tests for Kafka Manager API"""
    
    @pytest.fixture(scope="class")
    def api_base(self):
        return "http://localhost:8000"
    
    @pytest.fixture(scope="class")
    def test_topic(self):
        return "integration-test-topic"
    
    @pytest.fixture(scope="class", autouse=True)
    def setup_test_environment(self, api_base, test_topic):
        """Setup test environment"""
        # Create test topic
        response = requests.post(f"{api_base}/topics", json={
            "name": test_topic,
            "partitions": 3,
            "replication_factor": 1
        })
        assert response.status_code in [200, 201, 409]  # 409 if topic exists
        
        yield
        
        # Cleanup
        try:
            requests.delete(f"{api_base}/topics/{test_topic}")
        except:
            pass
    
    def test_health_endpoint(self, api_base):
        """Test API health endpoint"""
        response = requests.get(f"{api_base}/health")
        assert response.status_code == 200
        
        data = response.json()
        assert "status" in data
        assert data["status"] in ["healthy", "degraded"]
    
    def test_topic_lifecycle(self, api_base):
        """Test complete topic lifecycle"""
        topic_name = "lifecycle-test-topic"
        
        # Create topic
        create_response = requests.post(f"{api_base}/topics", json={
            "name": topic_name,
            "partitions": 2,
            "replication_factor": 1
        })
        assert create_response.status_code in [200, 201]
        
        # List topics
        list_response = requests.get(f"{api_base}/topics")
        assert list_response.status_code == 200
        topics = list_response.json()
        topic_names = [t["name"] for t in topics.get("topics", [])]
        assert topic_name in topic_names
        
        # Get topic details
        detail_response = requests.get(f"{api_base}/topics/{topic_name}")
        assert detail_response.status_code == 200
        details = detail_response.json()
        assert details["name"] == topic_name
        assert details["partitions"] == 2
        
        # Delete topic
        delete_response = requests.delete(f"{api_base}/topics/{topic_name}")
        assert delete_response.status_code == 200
    
    def test_message_produce_consume_cycle(self, api_base, test_topic):
        """Test complete message lifecycle"""
        
        # Produce messages
        test_messages = [
            {"key": "test1", "value": {"data": "message1", "id": 1}},
            {"key": "test2", "value": {"data": "message2", "id": 2}},
            {"key": "test3", "value": {"data": "message3", "id": 3}}
        ]
        
        produced_offsets = []
        for msg in test_messages:
            response = requests.post(f"{api_base}/produce", json={
                "topic": test_topic,
                "key": msg["key"],
                "value": msg["value"]
            })
            assert response.status_code == 200
            result = response.json()
            assert "result" in result
            produced_offsets.append(result["result"]["offset"])
        
        # Wait for messages to be available
        time.sleep(1)
        
        # Consume messages
        consume_response = requests.get(f"{api_base}/consume", params={
            "topic": test_topic,
            "consumer_group": "integration-test-group",
            "max_messages": 10
        })
        assert consume_response.status_code == 200
        
        consumed_data = consume_response.json()
        consumed_messages = consumed_data.get("messages", [])
        
        # Verify messages
        assert len(consumed_messages) >= len(test_messages)
        
        consumed_keys = [msg["key"] for msg in consumed_messages]
        for test_msg in test_messages:
            assert test_msg["key"] in consumed_keys
    
    def test_batch_operations(self, api_base, test_topic):
        """Test batch produce operations"""
        
        # Prepare batch messages
        batch_messages = [
            {"key": f"batch_{i}", "value": {"batch_id": i, "data": f"batch_data_{i}"}}
            for i in range(20)
        ]
        
        # Batch produce
        batch_response = requests.post(f"{api_base}/produce/batch", json={
            "topic": test_topic,
            "messages": batch_messages
        })
        assert batch_response.status_code == 200
        
        batch_result = batch_response.json()
        assert "results" in batch_result
        assert len(batch_result["results"]) == len(batch_messages)
        assert batch_result["total_produced"] == len(batch_messages)
        assert batch_result["failed_count"] == 0
    
    def test_error_handling(self, api_base):
        """Test API error handling"""
        
        # Test invalid topic name
        invalid_topic_response = requests.post(f"{api_base}/topics", json={
            "name": "invalid-topic-name!@#",
            "partitions": 1,
            "replication_factor": 1
        })
        assert invalid_topic_response.status_code == 400
        
        # Test produce to non-existent topic
        produce_response = requests.post(f"{api_base}/produce", json={
            "topic": "non-existent-topic-12345",
            "key": "test",
            "value": {"data": "test"}
        })
        assert produce_response.status_code in [400, 404, 500]
        
        # Test consume from non-existent topic
        consume_response = requests.get(f"{api_base}/consume", params={
            "topic": "non-existent-topic-12345",
            "consumer_group": "test-group",
            "max_messages": 1
        })
        assert consume_response.status_code in [400, 404, 500]
    
    def test_concurrent_operations(self, api_base, test_topic):
        """Test concurrent produce/consume operations"""
        import threading
        import queue
        
        results_queue = queue.Queue()
        
        def producer_worker(worker_id: int, message_count: int):
            """Producer worker function"""
            try:
                for i in range(message_count):
                    response = requests.post(f"{api_base}/produce", json={
                        "topic": test_topic,
                        "key": f"worker_{worker_id}_msg_{i}",
                        "value": {"worker_id": worker_id, "message_id": i}
                    })
                    assert response.status_code == 200
                results_queue.put(("producer", worker_id, "success", message_count))
            except Exception as e:
                results_queue.put(("producer", worker_id, "error", str(e)))
        
        def consumer_worker(worker_id: int):
            """Consumer worker function"""
            try:
                response = requests.get(f"{api_base}/consume", params={
                    "topic": test_topic,
                    "consumer_group": f"concurrent-test-group-{worker_id}",
                    "max_messages": 50
                })
                assert response.status_code == 200
                data = response.json()
                message_count = len(data.get("messages", []))
                results_queue.put(("consumer", worker_id, "success", message_count))
            except Exception as e:
                results_queue.put(("consumer", worker_id, "error", str(e)))
        
        # Start producer threads
        producer_threads = []
        for i in range(3):
            thread = threading.Thread(target=producer_worker, args=(i, 10))
            thread.start()
            producer_threads.append(thread)
        
        # Wait for producers to complete
        for thread in producer_threads:
            thread.join()
        
        # Wait for messages to be available
        time.sleep(2)
        
        # Start consumer threads
        consumer_threads = []
        for i in range(2):
            thread = threading.Thread(target=consumer_worker, args=(i,))
            thread.start()
            consumer_threads.append(thread)
        
        # Wait for consumers to complete
        for thread in consumer_threads:
            thread.join()
        
        # Collect results
        results = []
        while not results_queue.empty():
            results.append(results_queue.get())
        
        # Verify results
        producer_results = [r for r in results if r[0] == "producer"]
        consumer_results = [r for r in results if r[0] == "consumer"]
        
        assert len(producer_results) == 3
        assert len(consumer_results) == 2
        
        # All operations should succeed
        for result in results:
            assert result[2] == "success", f"Operation failed: {result}"

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
```

## 🚀 Getting Started

1. **Start Local Kafka Manager**:
   ```bash
   cd /path/to/kafka-cluster
   ./start.sh
   ```

2. **Install example dependencies**:
   ```bash
   pip install requests httpx fastapi uvicorn pytest
   ```

3. **Run basic examples**:
   ```bash
   python examples/basic/producer_consumer.py
   python examples/basic/topic_management.py
   python examples/basic/batch_operations.py
   ```

4. **Run integration tests**:
   ```bash
   python examples/testing/integration_tests.py
   ```

5. **Try advanced patterns**:
   ```bash
   python examples/advanced/event_sourcing.py
   ```

## 📚 Additional Resources

- **API Documentation**: http://localhost:8000/docs
- **Kafka UI**: http://localhost:8080
- **Main Documentation**: [../README.md](../README.md)
- **Contributing Guide**: [../CONTRIBUTING.md](../CONTRIBUTING.md)

---

*These examples demonstrate real-world usage patterns. Modify them to fit your specific use cases!*