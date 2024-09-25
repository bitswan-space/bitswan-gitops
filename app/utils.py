import asyncio
import hashlib
import os
from typing import Any
import zipfile
from tempfile import NamedTemporaryFile
import docker
from dockerfile_parse import DockerfileParser
import yaml


async def process_zip_file(file, deployment_id):
    with NamedTemporaryFile(delete=False) as temp_file:
        content = await file.read()
        temp_file.write(content)

    checksum = calculate_checksum(temp_file.name)
    output_dir = f"{checksum}"

    try:
        os.makedirs(output_dir, exist_ok=True)
        with zipfile.ZipFile(temp_file.name, "r") as zip_ref:
            zip_ref.extractall(output_dir)

        bitswan_home = os.environ.get("BS_BITSWAN_DIR", "/mnt/repo/bitswan")
        bitswan_yaml_path = os.path.join(bitswan_home, "bitswan.yaml")

        # Update or create bitswan.yaml
        if os.path.exists(bitswan_yaml_path):
            with open(bitswan_yaml_path, "r") as f:
                data = yaml.safe_load(f) or {}
        else:
            data = {}

        data[checksum] = deployment_id

        with open(bitswan_yaml_path, "w") as f:
            yaml.dump(data, f)

        return {
            "message": "File processed successfully",
            "output_directory": output_dir,
            "checksum": checksum,
        }
    except Exception as e:
        return {"error": f"Error processing file: {str(e)}"}
    finally:
        os.unlink(temp_file.name)


def read_bitswan_yaml(bitswan_yaml_path: str) -> dict[str, Any]:
    try:
        if os.path.exists(bitswan_yaml_path):
            with open(bitswan_yaml_path, "r") as f:
                bs_yaml: dict = yaml.safe_load(f)
                return bs_yaml
        else:
            raise FileNotFoundError("bitswan.yaml not found")
    except Exception as e:
        raise Exception(f"Error reading bitswan.yaml: {str(e)}")


async def docker_compose_up(
    bitswan_dir: str, docker_compose: str, deployment_info: dict[str, Any]
) -> None:
    ide_services = [svc + "__ide__" for svc in deployment_info.keys()]

    async def setup_asyncio_process(cmd: list[Any]) -> dict[str, Any]:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=bitswan_dir,
        )

        stdout, stderr = await proc.communicate(input=docker_compose.encode())

        return {
            "cmd": cmd,
            "stdout": stdout.decode("utf-8"),
            "stderr": stderr.decode("utf-8"),
            "returncode": proc.returncode,
        }

    build_result = await setup_asyncio_process(
        ["docker-compose", "-f", "/dev/stdin", "--pull"].extend(
            list(deployment_info.keys())
        )
    )

    up_result = await setup_asyncio_process(
        ["docker-compose", "-f", "/dev/stdin", "up", "-d", "--remove-orphans"]
    )
    ide_result = await setup_asyncio_process(
        ["docker-compose", "-f", "/dev/stdin", "up", "--no-start"].extend(ide_services)
    )

    return {
        "build_result": build_result,
        "up_result": up_result,
        "ide_result": ide_result,
    }


async def git_pull(bitswan_dir: str) -> bool:
    abspath = os.path.abspath(bitswan_dir)

    await asyncio.create_subprocess_exec(
        "git", "config", "--global", "--add", "safe.directory", abspath
    )

    await asyncio.create_subprocess_exec(
        "git", "config", "pull.rebase", "false", cwd=bitswan_dir
    )

    pull_proc = await asyncio.create_subprocess_exec("git", "pull", cwd=bitswan_dir)
    await pull_proc.wait()

    if pull_proc.returncode:
        return False

    return True


def calculate_checksum(file_path):
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()
