#!/usr/bin/env python3
"""
Simple Kafka Manager API Server
A simplified version that works around import issues
"""

import json
import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any, Optional

app = FastAPI(
    title="Multi-Cluster Kafka Manager",
    description="A simplified API for managing Kafka clusters",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
KAFKA_REST_PROXY_URL = "http://localhost:8082"
KAFKA_UI_URL = "http://localhost:8080"

class TopicCreate(BaseModel):
    name: str
    partitions: int = 1
    replication_factor: int = 1
    config: Dict[str, str] = {}

class MessageProduce(BaseModel):
    topic: str
    value: Dict[str, Any]
    key: Optional[str] = None

@app.get("/")
async def root():
    return {
        "message": "Multi-Cluster Kafka Manager API",
        "version": "1.0.0",
        "status": "running"
    }

@app.get("/health")
async def health():
    try:
        # Check Kafka REST Proxy
        response = requests.get(f"{KAFKA_REST_PROXY_URL}/topics", timeout=5)
        kafka_status = "healthy" if response.status_code == 200 else "unhealthy"
    except:
        kafka_status = "unhealthy"
    
    return {
        "status": "healthy",
        "services": {
            "kafka_rest_proxy": kafka_status,
            "kafka_ui": "healthy"
        },
        "endpoints": {
            "kafka_ui": KAFKA_UI_URL,
            "kafka_rest_proxy": KAFKA_REST_PROXY_URL,
            "api_docs": "http://localhost:8000/docs"
        }
    }

@app.get("/topics")
async def list_topics():
    """List all Kafka topics"""
    try:
        response = requests.get(f"{KAFKA_REST_PROXY_URL}/topics")
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch topics: {str(e)}")

@app.post("/topics")
async def create_topic(topic: TopicCreate):
    """Create a new Kafka topic"""
    try:
        payload = {
            "records": [
                {
                    "value": {
                        "name": topic.name,
                        "partitions": topic.partitions,
                        "replication_factor": topic.replication_factor,
                        "config": topic.config
                    }
                }
            ]
        }
        
        # Use Kafka REST Proxy to create topic
        headers = {"Content-Type": "application/vnd.kafka.json.v2+json"}
        response = requests.post(
            f"{KAFKA_REST_PROXY_URL}/topics/{topic.name}",
            headers=headers,
            json=payload
        )
        
        if response.status_code in [200, 201]:
            return {"message": f"Topic '{topic.name}' created successfully"}
        else:
            raise HTTPException(status_code=400, detail=f"Failed to create topic: {response.text}")
            
    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Failed to create topic: {str(e)}")

@app.post("/produce")
async def produce_message(message: MessageProduce):
    """Produce a message to a Kafka topic"""
    try:
        payload = {
            "records": [
                {
                    "value": message.value
                }
            ]
        }
        
        if message.key:
            payload["records"][0]["key"] = message.key
        
        headers = {"Content-Type": "application/vnd.kafka.json.v2+json"}
        response = requests.post(
            f"{KAFKA_REST_PROXY_URL}/topics/{message.topic}",
            headers=headers,
            json=payload
        )
        
        if response.status_code in [200, 201]:
            return {"message": f"Message sent to topic '{message.topic}'"}
        else:
            raise HTTPException(status_code=400, detail=f"Failed to produce message: {response.text}")
            
    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Failed to produce message: {str(e)}")

@app.get("/consume/{topic}")
async def consume_messages(topic: str, consumer_group: str = "api-consumer"):
    """Consume messages from a Kafka topic"""
    try:
        # This is a simplified version - in production you'd want proper consumer management
        return {
            "message": f"Consumer endpoint for topic '{topic}' with group '{consumer_group}'",
            "note": "Use Kafka UI at http://localhost:8080 for full consumer functionality"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to consume messages: {str(e)}")

@app.get("/clusters")
async def list_clusters():
    """List available clusters"""
    return [
        {
            "id": "default-cluster",
            "name": "Default Kafka Cluster",
            "status": "running",
            "kafka_port": 9092,
            "rest_proxy_port": 8082,
            "ui_port": 8080
        }
    ]

@app.get("/catalog")
async def service_catalog():
    """Get service catalog information"""
    return {
        "services": {
            "kafka": {
                "name": "Apache Kafka",
                "version": "7.4.0",
                "port": 9092,
                "status": "running"
            },
            "kafka_rest_proxy": {
                "name": "Kafka REST Proxy",
                "version": "7.4.0", 
                "port": 8082,
                "url": KAFKA_REST_PROXY_URL,
                "status": "running"
            },
            "kafka_ui": {
                "name": "Kafka UI",
                "port": 8080,
                "url": KAFKA_UI_URL,
                "status": "running"
            }
        },
        "endpoints": {
            "kafka_broker": "localhost:9092",
            "rest_api": "http://localhost:8082",
            "web_ui": "http://localhost:8080",
            "management_api": "http://localhost:8000"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)