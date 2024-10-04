import asyncio
import hashlib
import os
from typing import Any
import zipfile
from tempfile import NamedTemporaryFile
import yaml
from filelock import FileLock
import shutil


async def process_zip_file(file, deployment_id):
    with NamedTemporaryFile(delete=False) as temp_file:
        content = await file.read()
        temp_file.write(content)

    checksum = calculate_checksum(temp_file.name)
    output_dir = f"{checksum}"
    old_deploymend_checksum = None

    try:
        bitswan_home = os.environ.get("BS_BITSWAN_DIR", "/mnt/repo/bitswan")
        bitswan_yaml_path = os.path.join(bitswan_home, "bitswan.yaml")

        output_dir = os.path.join(bitswan_home, output_dir)

        os.makedirs(output_dir, exist_ok=True)
        with zipfile.ZipFile(temp_file.name, "r") as zip_ref:
            zip_ref.extractall(output_dir)

        # Update or create bitswan.yaml
        data = None
        if os.path.exists(bitswan_yaml_path):
            with open(bitswan_yaml_path, "r") as f:
                data = yaml.safe_load(f)

        data = data or {"deployments": {}}
        deployments = data["deployments"]  # should never raise KeyError

        deployments[deployment_id] = deployments.get(deployment_id, {})
        old_deploymend_checksum = deployments[deployment_id].get("checksum")
        deployments[deployment_id]["checksum"] = checksum
        deployments[deployment_id]["active"] = True  # FIXME: How to decide this?

        data["deployments"] = deployments
        with open(bitswan_yaml_path, "w") as f:
            yaml.dump(data, f)

        await update_git(bitswan_home, deployment_id, bitswan_yaml_path, checksum)

        return {
            "message": "File processed successfully",
            "output_directory": output_dir,
            "checksum": checksum,
        }
    except Exception as e:
        shutil.rmtree(output_dir, ignore_errors=True)
        return {"error": f"Error processing file: {str(e)}"}
    finally:
        if old_deploymend_checksum:
            shutil.rmtree(
                os.path.join(bitswan_home, old_deploymend_checksum), ignore_errors=True
            )
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
    async def setup_asyncio_process(cmd: list[Any]) -> dict[str, Any]:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=bitswan_dir,
        )

        stdout, stderr = await proc.communicate(input=docker_compose.encode())

        res = {
            "cmd": cmd,
            "stdout": stdout.decode("utf-8"),
            "stderr": stderr.decode("utf-8"),
            "return_code": proc.returncode,
        }
        return res

    up_result = await setup_asyncio_process(
        ["docker", "compose", "-f", "/dev/stdin", "up", "-d", "--remove-orphans"]
    )

    return {
        "up_result": up_result,
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


async def wait_coroutine(*args, **kwargs) -> int:
    coro = await asyncio.create_subprocess_exec(*args, **kwargs)
    result = await coro.wait()
    return result


async def update_git(
    bitswan_home: str, deployment_id: str, bitswan_yaml_path: str, checksum: str
):
    if not os.path.exists(os.path.join(bitswan_home, ".git")):
        res = await wait_coroutine("git", "init", cwd=bitswan_home)
        if res:
            raise Exception("Error initializing git repository")

    lock_file = os.path.join(bitswan_home, ".git", "bitswan_git.lock")
    lock = FileLock(lock_file, timeout=30)

    with lock:
        has_remote = (
            await wait_coroutine("git", "remote", "show", "origin", cwd=bitswan_home)
            == 0
        )

        if has_remote:
            res = await git_pull(bitswan_home)
            if not res:
                raise Exception("Error pulling from git")

        add_process = await asyncio.create_subprocess_exec(
            "git", "add", bitswan_yaml_path, cwd=bitswan_home
        )
        await add_process.wait()

        commit_process = await asyncio.create_subprocess_exec(
            "git",
            "commit",
            "-m",
            f"Update deployment {deployment_id} with checksum {checksum}",
            cwd=bitswan_home,
        )
        await commit_process.wait()

        if has_remote:
            push_process = await asyncio.create_subprocess_exec(
                "git", "push", cwd=bitswan_home
            )
            await push_process.wait()
