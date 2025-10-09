import json
import os
import uuid
from datetime import datetime
from typing import Dict, Any, Optional
from paho.mqtt import client as mqtt_client

from .mqtt import get_mqtt_client


class ImageBuildLogPublisher:
    """
    Publishes Docker image build logs to MQTT topic /logs as OpenTelemetry-compatible JSON objects.
    Each log entry follows the OpenTelemetry Log Record format with image-tag in attributes.
    """
    
    def __init__(self):
        self.client: mqtt_client.Client | None = None
        self.topic = "/logs"
        self.service_name = os.environ.get("OTEL_SERVICE_NAME", "bitswan-image-builder")
        self.service_version = os.environ.get("OTEL_SERVICE_VERSION", "1.0.0")
    
    async def _get_client(self) -> mqtt_client.Client:
        """Get MQTT client, connecting if necessary"""
        if self.client is None:
            self.client = await get_mqtt_client()
        return self.client
    
    def _create_otel_log_record(
        self, 
        body: str, 
        image_tag: str, 
        severity: str = "INFO",
        timestamp: Optional[str] = None,
        trace_id: Optional[str] = None,
        span_id: Optional[str] = None,
        additional_attributes: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create an OpenTelemetry-compatible log record.
        
        Args:
            body: The log message body
            image_tag: The Docker image tag being built
            severity: Log severity level (TRACE, DEBUG, INFO, WARN, ERROR, FATAL)
            timestamp: Optional timestamp (defaults to current time in nanoseconds)
            trace_id: Optional trace ID for correlation
            span_id: Optional span ID for correlation
            additional_attributes: Additional attributes to include
        
        Returns:
            Dictionary representing the OpenTelemetry log record
        """
        if timestamp is None:
            # OpenTelemetry uses nanoseconds since epoch
            timestamp = int(datetime.now().timestamp() * 1_000_000_000)
        
        # Generate trace and span IDs if not provided
        if trace_id is None:
            trace_id = format(uuid.uuid4().int, '032x')
        if span_id is None:
            span_id = format(uuid.uuid4().int, '016x')
        
        # Build attributes dictionary
        attributes = {
            "image_tag": image_tag,
            "log_type": "docker_build",
            "service.name": self.service_name,
            "service.version": self.service_version,
            "container.runtime": "docker"
        }
        
        # Add additional attributes if provided
        if additional_attributes:
            attributes.update(additional_attributes)
        
        # OpenTelemetry Log Record format
        log_record = {
            "timestamp": timestamp,
            "severity": severity,
            "body": body.strip(),
            "attributes": attributes,
            "resource": {
                "service.name": self.service_name,
                "service.version": self.service_version,
                "service.instance.id": os.environ.get("HOSTNAME", "unknown")
            },
            "trace_context": {
                "trace_id": trace_id,
                "span_id": span_id,
                "trace_flags": "01"  # Sampled
            }
        }
        
        return log_record
    
    async def publish_log_line(
        self, 
        log_line: str, 
        image_tag: str, 
        severity: str = "INFO",
        timestamp: Optional[str] = None,
        trace_id: Optional[str] = None,
        span_id: Optional[str] = None
    ):
        """
        Publish a single log line to MQTT in OpenTelemetry format.
        
        Args:
            log_line: The Docker build log line
            image_tag: The Docker image tag being built
            severity: Log severity level
            timestamp: Optional timestamp
            trace_id: Optional trace ID for correlation
            span_id: Optional span ID for correlation
        """
        try:
            client = await self._get_client()
            log_record = self._create_otel_log_record(
                body=log_line,
                image_tag=image_tag,
                severity=severity,
                timestamp=timestamp,
                trace_id=trace_id,
                span_id=span_id
            )
            
            client.publish(
                self.topic,
                payload=json.dumps(log_record),
                qos=1,
                retain=False  # Don't retain individual log lines
            )
        except Exception as e:
            print(f"Failed to publish log line to MQTT: {e}")
    
    async def publish_build_start(self, image_tag: str, trace_id: Optional[str] = None, span_id: Optional[str] = None):
        """
        Publish a build start message in OpenTelemetry format.
        
        Args:
            image_tag: The Docker image tag being built
            trace_id: Optional trace ID for correlation
            span_id: Optional span ID for correlation
        """
        start_message = f"Build started for {image_tag}"
        
        try:
            client = await self._get_client()
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
            
            client.publish(
                self.topic,
                payload=json.dumps(log_record),
                qos=1,
                retain=False
            )
        except Exception as e:
            print(f"Failed to publish build start message to MQTT: {e}")
    
    async def publish_build_complete(
        self, 
        image_tag: str, 
        success: bool = True, 
        error_message: Optional[str] = None,
        trace_id: Optional[str] = None,
        span_id: Optional[str] = None
    ):
        """
        Publish a build completion message in OpenTelemetry format.
        
        Args:
            image_tag: The Docker image tag being built
            success: Whether the build was successful
            error_message: Error message if build failed
            trace_id: Optional trace ID for correlation
            span_id: Optional span ID for correlation
        """
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
        
        try:
            client = await self._get_client()
            log_record = self._create_otel_log_record(
                body=body,
                image_tag=image_tag,
                severity=severity,
                trace_id=trace_id,
                span_id=span_id,
                additional_attributes=additional_attributes
            )
            
            client.publish(
                self.topic,
                payload=json.dumps(log_record),
                qos=1,
                retain=False
            )
        except Exception as e:
            print(f"Failed to publish build complete message to MQTT: {e}")


# Global instance for use across the application
log_publisher = ImageBuildLogPublisher()
