"""
Async Docker client using aiohttp to communicate with Docker daemon via Unix socket.
Replaces the synchronous docker-py library for better concurrency.
"""

import asyncio
import aiohttp
import json
from typing import Any, AsyncGenerator, Optional
from urllib.parse import quote


class AsyncDockerClient:
    """Async client for Docker API over Unix socket."""

    def __init__(self, socket_path: str = "/var/run/docker.sock"):
        self.socket_path = socket_path
        self._connector = None
        self._session = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session with Unix socket connector."""
        if self._session is None or self._session.closed:
            self._connector = aiohttp.UnixConnector(path=self.socket_path)
            self._session = aiohttp.ClientSession(connector=self._connector)
        return self._session

    async def close(self):
        """Close the client session."""
        if self._session and not self._session.closed:
            await self._session.close()
        if self._connector:
            await self._connector.close()

    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict] = None,
        json_data: Optional[dict] = None,
        timeout: float = 30.0,
    ) -> tuple[int, Any]:
        """Make a request to Docker API."""
        session = await self._get_session()
        url = f"http://localhost{endpoint}"

        async with session.request(
            method,
            url,
            params=params,
            json=json_data,
            timeout=aiohttp.ClientTimeout(total=timeout),
        ) as response:
            status = response.status
            if response.content_type == "application/json":
                data = await response.json()
            else:
                data = await response.text()
            return status, data

    async def _get(
        self, endpoint: str, params: Optional[dict] = None, timeout: float = 30.0
    ) -> Any:
        """GET request to Docker API."""
        status, data = await self._request(
            "GET", endpoint, params=params, timeout=timeout
        )
        if status >= 400:
            raise DockerError(status, data)
        return data

    async def _post(
        self,
        endpoint: str,
        params: Optional[dict] = None,
        json_data: Optional[dict] = None,
        timeout: float = 30.0,
    ) -> Any:
        """POST request to Docker API."""
        status, data = await self._request(
            "POST", endpoint, params=params, json_data=json_data, timeout=timeout
        )
        if status >= 400:
            raise DockerError(status, data)
        return data

    async def _delete(
        self, endpoint: str, params: Optional[dict] = None, timeout: float = 30.0
    ) -> Any:
        """DELETE request to Docker API."""
        status, data = await self._request(
            "DELETE", endpoint, params=params, timeout=timeout
        )
        if status >= 400:
            raise DockerError(status, data)
        return data

    # Docker API methods

    async def info(self) -> dict:
        """Get Docker system info."""
        return await self._get("/info")

    async def list_images(
        self, all: bool = False, filters: Optional[dict] = None
    ) -> list[dict]:
        """List Docker images."""
        params = {"all": "true" if all else "false"}
        if filters:
            params["filters"] = json.dumps(filters)
        return await self._get("/images/json", params=params)

    async def list_containers(
        self, all: bool = False, filters: Optional[dict] = None
    ) -> list[dict]:
        """List Docker containers."""
        params = {"all": "true" if all else "false"}
        if filters:
            params["filters"] = json.dumps(filters)
        return await self._get("/containers/json", params=params)

    async def get_container(self, container_id: str) -> dict:
        """Get container details."""
        return await self._get(f"/containers/{quote(container_id, safe='')}/json")

    async def start_container(self, container_id: str) -> None:
        """Start a container."""
        await self._post(f"/containers/{quote(container_id, safe='')}/start")

    async def stop_container(self, container_id: str, timeout: int = 10) -> None:
        """Stop a container."""
        await self._post(
            f"/containers/{quote(container_id, safe='')}/stop",
            params={"t": str(timeout)},
            timeout=timeout + 5,
        )

    async def restart_container(self, container_id: str, timeout: int = 10) -> None:
        """Restart a container."""
        await self._post(
            f"/containers/{quote(container_id, safe='')}/restart",
            params={"t": str(timeout)},
            timeout=timeout + 5,
        )

    async def remove_container(self, container_id: str, force: bool = False) -> None:
        """Remove a container."""
        params = {}
        if force:
            params["force"] = "true"
        await self._delete(f"/containers/{quote(container_id, safe='')}", params=params)

    async def get_container_logs(
        self,
        container_id: str,
        tail: int = 100,
        stdout: bool = True,
        stderr: bool = True,
    ) -> str:
        """Get container logs."""
        params = {
            "tail": str(tail),
            "stdout": "true" if stdout else "false",
            "stderr": "true" if stderr else "false",
        }
        session = await self._get_session()
        url = f"http://localhost/containers/{quote(container_id, safe='')}/logs"

        async with session.get(url, params=params) as response:
            if response.status >= 400:
                data = await response.text()
                raise DockerError(response.status, data)
            # Docker logs have a multiplexed stream format with 8-byte headers
            # We need to strip these headers to get clean log output
            raw_logs = await response.read()
            return _decode_docker_logs(raw_logs)

    async def stream_container_logs(
        self,
        container_id: str,
        tail: int = 200,
        since: int = 0,
        stdout: bool = True,
        stderr: bool = True,
    ) -> AsyncGenerator[str, None]:
        """Stream container logs as an async generator. Yields decoded log lines."""
        params = {
            "follow": "true",
            "tail": str(tail),
            "timestamps": "true",
            "stdout": "true" if stdout else "false",
            "stderr": "true" if stderr else "false",
        }
        if since > 0:
            params["since"] = str(since)

        session = await self._get_session()
        url = f"http://localhost/containers/{quote(container_id, safe='')}/logs"

        try:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=0),
            ) as response:
                if response.status >= 400:
                    data = await response.text()
                    raise DockerError(response.status, data)

                buf = b""
                async for chunk in response.content.iter_any():
                    buf += chunk
                    # Docker multiplexed stream: 8-byte header + payload per frame
                    while len(buf) >= 8:
                        stream_type = buf[0]
                        if stream_type not in (0, 1, 2):
                            # Not multiplexed — treat as raw text
                            lines = buf.decode("utf-8", errors="replace").split("\n")
                            buf = b""
                            for line in lines:
                                stripped = line.rstrip("\r")
                                if stripped:
                                    yield stripped
                            break

                        frame_size = int.from_bytes(buf[4:8], byteorder="big")
                        if len(buf) < 8 + frame_size:
                            break  # wait for more data

                        frame = buf[8 : 8 + frame_size]
                        buf = buf[8 + frame_size :]
                        text = frame.decode("utf-8", errors="replace")
                        for line in text.split("\n"):
                            stripped = line.rstrip("\r")
                            if stripped:
                                yield stripped
        except (asyncio.IncompleteReadError, aiohttp.ClientPayloadError):
            # Container stopped or connection lost — end the stream gracefully
            return

    async def get_image(self, image_name: str) -> dict:
        """Get image details."""
        return await self._get(f"/images/{quote(image_name, safe='')}/json")

    async def remove_image(self, image_name: str, force: bool = False) -> list[dict]:
        """Remove an image."""
        params = {}
        if force:
            params["force"] = "true"
        return await self._delete(
            f"/images/{quote(image_name, safe='')}", params=params
        )

    async def tag_image(self, image_name: str, repo: str, tag: str) -> None:
        """Tag an image."""
        params = {"repo": repo, "tag": tag}
        await self._post(f"/images/{quote(image_name, safe='')}/tag", params=params)

    async def exec_create(
        self,
        container_id: str,
        cmd: list[str],
        stdout: bool = True,
        stderr: bool = True,
    ) -> str:
        """Create an exec instance."""
        data = await self._post(
            f"/containers/{quote(container_id, safe='')}/exec",
            json_data={
                "AttachStdout": stdout,
                "AttachStderr": stderr,
                "Cmd": cmd,
            },
        )
        return data["Id"]

    async def exec_start(self, exec_id: str) -> str:
        """Start an exec instance and return output."""
        session = await self._get_session()
        url = f"http://localhost/exec/{exec_id}/start"

        async with session.post(url, json={"Detach": False, "Tty": False}) as response:
            if response.status >= 400:
                data = await response.text()
                raise DockerError(response.status, data)
            raw_output = await response.read()
            return _decode_docker_logs(raw_output)

    async def exec_inspect(self, exec_id: str) -> dict:
        """Inspect an exec instance."""
        return await self._get(f"/exec/{exec_id}/json")

    async def watch_events(self, filters: Optional[dict] = None):
        """Watch Docker events as an async generator. Yields parsed event dicts."""
        session = await self._get_session()
        params = {}
        if filters:
            params["filters"] = json.dumps(filters)

        async with session.get(
            "http://localhost/events",
            params=params,
            timeout=aiohttp.ClientTimeout(total=0),
        ) as response:
            async for line in response.content:
                if line.strip():
                    yield json.loads(line)


class DockerError(Exception):
    """Docker API error."""

    def __init__(self, status_code: int, message: Any):
        self.status_code = status_code
        self.message = message
        if isinstance(message, dict):
            msg = message.get("message", str(message))
        else:
            msg = str(message)
        super().__init__(f"Docker API error ({status_code}): {msg}")


class ImageNotFound(DockerError):
    """Image not found error."""

    pass


class ContainerNotFound(DockerError):
    """Container not found error."""

    pass


def _decode_docker_logs(raw_logs: bytes) -> str:
    """
    Decode Docker multiplexed stream logs.
    Docker logs have 8-byte headers: [STREAM_TYPE, 0, 0, 0, SIZE1, SIZE2, SIZE3, SIZE4]
    """
    result = []
    pos = 0
    while pos < len(raw_logs):
        if pos + 8 > len(raw_logs):
            # Not enough data for header, treat rest as raw
            result.append(raw_logs[pos:].decode("utf-8", errors="replace"))
            break

        # Parse header
        header = raw_logs[pos : pos + 8]
        stream_type = header[0]

        # Check if this looks like a valid header (stream type 0, 1, or 2)
        if stream_type not in (0, 1, 2):
            # Not a multiplexed stream, return as-is
            return raw_logs.decode("utf-8", errors="replace")

        size = int.from_bytes(header[4:8], byteorder="big")
        pos += 8

        if pos + size > len(raw_logs):
            # Incomplete frame, get what we can
            result.append(raw_logs[pos:].decode("utf-8", errors="replace"))
            break

        frame = raw_logs[pos : pos + size]
        result.append(frame.decode("utf-8", errors="replace"))
        pos += size

    return "".join(result)


# Singleton instance
_client: Optional[AsyncDockerClient] = None


def get_async_docker_client() -> AsyncDockerClient:
    """Get the singleton async Docker client."""
    global _client
    if _client is None:
        _client = AsyncDockerClient()
    return _client
