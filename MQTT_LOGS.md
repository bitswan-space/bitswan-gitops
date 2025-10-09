# MQTT Log Streaming for Docker Image Builds (OpenTelemetry Format)

This document describes the MQTT log streaming functionality that publishes Docker image build logs to the `/logs` MQTT topic in OpenTelemetry-compatible format.

## Overview

The system publishes real-time Docker image build logs to the MQTT topic `/logs` as OpenTelemetry Log Record JSON objects. Each log message follows the OpenTelemetry specification and includes the original Docker build log data plus an `image_tag` attribute in the attributes section.

## MQTT Topic

- **Topic**: `/logs`
- **QoS**: 1 (at least once delivery)
- **Retain**: false (individual log lines are not retained)

## OpenTelemetry Log Record Format

Each message published to `/logs` follows the OpenTelemetry Log Record specification:

```json
{
  "timestamp": 1705312245123456789,
  "severity": "INFO",
  "body": "Step 1/5 : FROM python:3.9-slim",
  "attributes": {
    "image_tag": "internal/my-app",
    "log_type": "docker_build",
    "service.name": "bitswan-image-builder",
    "service.version": "1.0.0",
    "container.runtime": "docker"
  },
  "resource": {
    "service.name": "bitswan-image-builder",
    "service.version": "1.0.0",
    "service.instance.id": "hostname-123"
  },
  "trace_context": {
    "trace_id": "a1b2c3d4e5f6789012345678901234567",
    "span_id": "1234567890abcdef",
    "trace_flags": "01"
  }
}
```

### Field Descriptions

- **timestamp**: Nanoseconds since Unix epoch (integer)
- **severity**: Log severity level (`TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`, `FATAL`)
- **body**: The actual log message content
- **attributes**: Key-value pairs containing metadata
  - `image_tag`: The Docker image tag being built
  - `log_type`: Always "docker_build" for build logs
  - `service.name`: Service name (configurable via `OTEL_SERVICE_NAME`)
  - `service.version`: Service version (configurable via `OTEL_SERVICE_VERSION`)
  - `container.runtime`: Always "docker"
- **resource**: Service-level metadata
- **trace_context**: Trace correlation information
  - `trace_id`: 32-character hexadecimal trace ID
  - `span_id`: 16-character hexadecimal span ID
  - `trace_flags`: Trace sampling flags ("01" = sampled)

### Special Event Messages

#### Build Start Event
```json
{
  "timestamp": 1705312245123456789,
  "severity": "INFO",
  "body": "Build started for internal/my-app",
  "attributes": {
    "image_tag": "internal/my-app",
    "log_type": "docker_build",
    "service.name": "bitswan-image-builder",
    "service.version": "1.0.0",
    "container.runtime": "docker",
    "event_type": "build_start",
    "build.status": "started"
  },
  "resource": {
    "service.name": "bitswan-image-builder",
    "service.version": "1.0.0",
    "service.instance.id": "hostname-123"
  },
  "trace_context": {
    "trace_id": "a1b2c3d4e5f6789012345678901234567",
    "span_id": "1234567890abcdef",
    "trace_flags": "01"
  }
}
```

#### Build Complete Event (Success)
```json
{
  "timestamp": 1705312272123456789,
  "severity": "INFO",
  "body": "Build completed successfully for internal/my-app",
  "attributes": {
    "image_tag": "internal/my-app",
    "log_type": "docker_build",
    "service.name": "bitswan-image-builder",
    "service.version": "1.0.0",
    "container.runtime": "docker",
    "event_type": "build_complete",
    "build.status": "completed",
    "build.success": true
  },
  "resource": {
    "service.name": "bitswan-image-builder",
    "service.version": "1.0.0",
    "service.instance.id": "hostname-123"
  },
  "trace_context": {
    "trace_id": "a1b2c3d4e5f6789012345678901234567",
    "span_id": "1234567890abcdef",
    "trace_flags": "01"
  }
}
```

#### Build Complete Event (Error)
```json
{
  "timestamp": 1705312272123456789,
  "severity": "ERROR",
  "body": "Build failed for internal/my-app: Dockerfile not found",
  "attributes": {
    "image_tag": "internal/my-app",
    "log_type": "docker_build",
    "service.name": "bitswan-image-builder",
    "service.version": "1.0.0",
    "container.runtime": "docker",
    "event_type": "build_complete",
    "build.status": "failed",
    "build.success": false,
    "error.message": "Dockerfile not found"
  },
  "resource": {
    "service.name": "bitswan-image-builder",
    "service.version": "1.0.0",
    "service.instance.id": "hostname-123"
  },
  "trace_context": {
    "trace_id": "a1b2c3d4e5f6789012345678901234567",
    "span_id": "1234567890abcdef",
    "trace_flags": "01"
  }
}
```

## Implementation Details

### Files Modified

1. **`mqtt_publish_logs.py`** - Updated `ImageBuildLogPublisher` class with OpenTelemetry format
2. **`services/image_service.py`** - Modified to integrate OpenTelemetry MQTT log publishing during builds

### Key Components

#### ImageBuildLogPublisher Class

The `ImageBuildLogPublisher` class handles all MQTT log publishing in OpenTelemetry format:

- `_create_otel_log_record()` - Creates OpenTelemetry-compatible log records
- `publish_log_line()` - Publishes individual Docker build log lines
- `publish_build_start()` - Publishes build start events
- `publish_build_complete()` - Publishes build completion events

#### Integration with ImageService

The `ImageService._start_build_process()` method has been modified to:

1. Publish a build start message when a build begins
2. Publish each Docker build log line as it's received with appropriate severity levels
3. Publish build completion messages (success or failure)
4. Publish git operation results (for zip file uploads)

## Configuration

### Environment Variables

- `OTEL_SERVICE_NAME`: Service name for OpenTelemetry (default: "bitswan-image-builder")
- `OTEL_SERVICE_VERSION`: Service version for OpenTelemetry (default: "1.0.0")
- `HOSTNAME`: Service instance ID (default: "unknown")

### MQTT Configuration

The system uses the existing MQTT configuration from `mqtt.py`:

- Broker: `MQTT_BROKER` environment variable
- Port: `MQTT_PORT` environment variable (default: 443)
- Authentication: `MQTT_USERNAME` and `MQTT_PASSWORD` environment variables
- TLS: Enabled (certificate requirements depend on port)

## Usage

The log streaming is automatically enabled when:

1. MQTT broker is configured (via `MQTT_BROKER` environment variable)
2. Docker image builds are initiated through the ImageService

No additional configuration is required - the functionality is integrated into the existing image build process.

## Testing

A test script `test_mqtt_logs.py` is provided to demonstrate the OpenTelemetry message format and functionality. Run it with:

```bash
python test_mqtt_logs.py
```

## OpenTelemetry Compatibility

This implementation follows the OpenTelemetry Log Record specification:

- **Timestamp**: Uses nanoseconds since Unix epoch
- **Severity**: Follows OpenTelemetry severity levels
- **Body**: Contains the actual log message
- **Attributes**: Key-value metadata including `image_tag`
- **Resource**: Service-level metadata
- **Trace Context**: Trace and span IDs for correlation

The format is compatible with OpenTelemetry collectors and observability platforms that support the OTLP (OpenTelemetry Protocol) format.

## Error Handling

- MQTT publishing failures are logged but don't interrupt the build process
- Build logs continue to be written to files even if MQTT publishing fails
- All MQTT operations are wrapped in try-catch blocks to ensure robustness
- Trace and span IDs are generated automatically if not provided