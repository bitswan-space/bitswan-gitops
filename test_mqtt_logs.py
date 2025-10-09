#!/usr/bin/env python3
"""
Test script for MQTT log publishing functionality with OpenTelemetry format.
This script demonstrates how the new /logs MQTT topic works with OpenTelemetry-compatible messages.
"""

import asyncio
import json
import os
import uuid
from datetime import datetime

# Mock the MQTT client for testing
class MockMQTTClient:
    def __init__(self):
        self.published_messages = []
    
    def publish(self, topic, payload, qos=0, retain=False):
        message = {
            "topic": topic,
            "payload": json.loads(payload),
            "qos": qos,
            "retain": retain,
            "timestamp": datetime.now().isoformat()
        }
        self.published_messages.append(message)
        print(f"Published to {topic}: {json.dumps(message['payload'], indent=2)}")

# Mock the log publisher for testing
class MockLogPublisher:
    def __init__(self):
        self.client = MockMQTTClient()
        self.topic = "/logs"
        self.service_name = "bitswan-image-builder"
        self.service_version = "1.0.0"
    
    def _create_otel_log_record(
        self, 
        body: str, 
        image_tag: str, 
        severity: str = "INFO",
        timestamp: int = None,
        trace_id: str = None,
        span_id: str = None,
        additional_attributes: dict = None
    ):
        if timestamp is None:
            timestamp = int(datetime.now().timestamp() * 1_000_000_000)
        
        if trace_id is None:
            trace_id = format(uuid.uuid4().int, '032x')
        if span_id is None:
            span_id = format(uuid.uuid4().int, '016x')
        
        attributes = {
            "image_tag": image_tag,
            "log_type": "docker_build",
            "service.name": self.service_name,
            "service.version": self.service_version,
            "container.runtime": "docker"
        }
        
        if additional_attributes:
            attributes.update(additional_attributes)
        
        log_record = {
            "timestamp": timestamp,
            "severity": severity,
            "body": body.strip(),
            "attributes": attributes,
            "resource": {
                "service.name": self.service_name,
                "service.version": self.service_version,
                "service.instance.id": "test-instance"
            },
            "trace_context": {
                "trace_id": trace_id,
                "span_id": span_id,
                "trace_flags": "01"
            }
        }
        
        return log_record
    
    async def publish_log_line(
        self, 
        log_line: str, 
        image_tag: str, 
        severity: str = "INFO",
        timestamp: int = None,
        trace_id: str = None,
        span_id: str = None
    ):
        log_record = self._create_otel_log_record(
            body=log_line,
            image_tag=image_tag,
            severity=severity,
            timestamp=timestamp,
            trace_id=trace_id,
            span_id=span_id
        )
        self.client.publish(
            self.topic,
            payload=json.dumps(log_record),
            qos=1,
            retain=False
        )
    
    async def publish_build_start(self, image_tag: str, trace_id: str = None, span_id: str = None):
        start_message = f"Build started for {image_tag}"
        log_record = self._create_otel_log_record(
            body=start_message,
            image_tag=image_tag,
            severity="INFO",
            trace_id=trace_id,
            span_id=span_id,
            additional_attributes={
                "event_type": "build_start",
                "build.status": "started"
            }
        )
        self.client.publish(
            self.topic,
            payload=json.dumps(log_record),
            qos=1,
            retain=False
        )
    
    async def publish_build_complete(
        self, 
        image_tag: str, 
        success: bool = True, 
        error_message: str = None,
        trace_id: str = None,
        span_id: str = None
    ):
        if success:
            body = f"Build completed successfully for {image_tag}"
            severity = "INFO"
            additional_attributes = {
                "event_type": "build_complete",
                "build.status": "completed",
                "build.success": True
            }
        else:
            body = f"Build failed for {image_tag}: {error_message or 'Unknown error'}"
            severity = "ERROR"
            additional_attributes = {
                "event_type": "build_complete",
                "build.status": "failed",
                "build.success": False,
                "error.message": error_message or "Unknown error"
            }
        
        log_record = self._create_otel_log_record(
            body=body,
            image_tag=image_tag,
            severity=severity,
            trace_id=trace_id,
            span_id=span_id,
            additional_attributes=additional_attributes
        )
        self.client.publish(
            self.topic,
            payload=json.dumps(log_record),
            qos=1,
            retain=False
        )

async def test_log_publishing():
    """Test the log publishing functionality with OpenTelemetry format"""
    print("Testing MQTT log publishing functionality with OpenTelemetry format...")
    print("=" * 70)
    
    publisher = MockLogPublisher()
    image_tag = "internal/test-image"
    
    # Generate a consistent trace ID for this test
    trace_id = format(uuid.uuid4().int, '032x')
    span_id = format(uuid.uuid4().int, '016x')
    
    # Test build start
    print("\n1. Publishing build start message:")
    await publisher.publish_build_start(image_tag, trace_id=trace_id, span_id=span_id)
    
    # Test some Docker build log lines with different severity levels
    print("\n2. Publishing Docker build log lines:")
    docker_logs = [
        ("Step 1/5 : FROM python:3.9-slim", "INFO"),
        (" ---> 1234567890ab", "DEBUG"),
        ("Step 2/5 : WORKDIR /app", "INFO"),
        (" ---> Running in abcdef123456", "DEBUG"),
        (" ---> 789012345678", "DEBUG"),
        ("Step 3/5 : COPY requirements.txt .", "INFO"),
        (" ---> abcdef123456", "DEBUG"),
        ("Step 4/5 : RUN pip install -r requirements.txt", "INFO"),
        (" ---> Running in 1234567890ab", "DEBUG"),
        ("Collecting flask", "INFO"),
        ("Installing collected packages: flask", "INFO"),
        ("Successfully installed flask-2.0.1", "INFO"),
        (" ---> 789012345678", "DEBUG"),
        ("Step 5/5 : COPY . .", "INFO"),
        (" ---> abcdef123456", "DEBUG"),
        ("Successfully built abcdef123456", "INFO"),
        ("Successfully tagged internal/test-image:sha123456789", "INFO")
    ]
    
    for log_line, severity in docker_logs:
        await publisher.publish_log_line(log_line, image_tag, severity=severity, trace_id=trace_id, span_id=span_id)
    
    # Test build completion
    print("\n3. Publishing build completion message:")
    await publisher.publish_build_complete(image_tag, success=True, trace_id=trace_id, span_id=span_id)
    
    # Test error case
    print("\n4. Testing error case:")
    error_trace_id = format(uuid.uuid4().int, '032x')
    error_span_id = format(uuid.uuid4().int, '016x')
    await publisher.publish_build_complete("internal/failed-image", success=False, error_message="Dockerfile not found", trace_id=error_trace_id, span_id=error_span_id)
    
    print("\n" + "=" * 70)
    print(f"Total messages published: {len(publisher.client.published_messages)}")
    print("All messages published to topic: /logs")
    print("\nOpenTelemetry Log Record structure:")
    if publisher.client.published_messages:
        print(json.dumps(publisher.client.published_messages[0], indent=2))
    
    print("\nKey OpenTelemetry fields:")
    print("- timestamp: Nanoseconds since epoch")
    print("- severity: Log level (TRACE, DEBUG, INFO, WARN, ERROR, FATAL)")
    print("- body: The actual log message")
    print("- attributes: Key-value pairs including image_tag")
    print("- resource: Service metadata")
    print("- trace_context: Trace and span IDs for correlation")

if __name__ == "__main__":
    asyncio.run(test_log_publishing())
