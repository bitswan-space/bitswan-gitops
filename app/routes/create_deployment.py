import os
import hashlib
from filelock import FileLock
import yaml
import zipfile
import asyncio
import shutil
from fastapi import UploadFile, File, APIRouter
from fastapi.responses import JSONResponse
from tempfile import NamedTemporaryFile
from ..utils import wait_coroutine, git_pull

router = APIRouter()


@router.post("/create/{deployment_id}")
async def upload_zip(deployment_id: str, file: UploadFile = File(...)):
    if file.filename.endswith(".zip"):
        result = await process_zip_file(file, deployment_id)
        return JSONResponse(content=result)
    else:
        return JSONResponse(
            content={"error": "File must be a ZIP archive"}, status_code=400
        )


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
        deployments[deployment_id]["active"] = True

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


def calculate_checksum(file_path):
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


async def update_git(
    bitswan_home: str, deployment_id: str, bitswan_yaml_path: str, checksum: str
):
    async def run_command(*args):
        process: asyncio.subprocess.Process = await asyncio.create_subprocess_exec(
            *args
        )

        return await process.wait()

    async def check_git_config(config_name: str):
        return_code = await run_command("git", "config", "--get", config_name)
        return return_code == 0

    if not os.path.exists(os.path.join(bitswan_home, ".git")):
        res = await wait_coroutine("git", "init", cwd=bitswan_home)
        if res:
            raise Exception("Error initializing git repository")

    if not await check_git_config("user.email"):
        await asyncio.subprocess.create_subprocess_exec(
            "git", "config", "--global", "user.email", "pipeline-ops@bspump.com"
        )

    if not await check_git_config("user.name"):
        await asyncio.subprocess.create_subprocess_exec(
            "git", "config", "--global", "user.name", "pipeline-ops"
        )

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

