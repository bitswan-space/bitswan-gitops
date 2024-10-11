import os
import hashlib
from filelock import FileLock
import yaml
import zipfile
import shutil
from fastapi import UploadFile, File, APIRouter
from fastapi.responses import JSONResponse
from tempfile import NamedTemporaryFile
from ..utils import read_bitswan_yaml, call_git_command

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
        data = read_bitswan_yaml(bitswan_home)

        data = data or {"deployments": {}}
        deployments = data["deployments"]  # should never raise KeyError

        deployments[deployment_id] = deployments.get(deployment_id, {})
        old_deploymend_checksum = deployments[deployment_id].get("checksum")
        deployments[deployment_id]["checksum"] = checksum
        deployments[deployment_id]["active"] = True

        data["deployments"] = deployments
        with open(bitswan_yaml_path, "w") as f:
            yaml.dump(data, f)

        await update_git(bitswan_home, deployment_id, checksum)

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


async def update_git(bitswan_home: str, deployment_id: str, checksum: str):
    host_path = os.environ.get("HOST_PATH")
    if host_path:
        bitswan_home = os.environ.get("BS_HOST_DIR", "/mnt/repo/pipeline")
    bitswan_yaml_path = os.path.join(bitswan_home, "bitswan.yaml")
    lock_file = os.path.join(bitswan_home, "bitswan_git.lock")
    lock = FileLock(lock_file, timeout=30)

    with lock:
        has_remote = (
            await call_git_command("git", "remote", "show", "origin", cwd=bitswan_home)
            == 0
        )

        if has_remote:
            res = await call_git_command("git", "pull", cwd=bitswan_home)
            if res != 0:
                raise Exception("Error pulling from git")

        await call_git_command("git", "add", bitswan_yaml_path, cwd=bitswan_home)

        await call_git_command(
            "git",
            "commit",
            "--author",
            '"pipeline-ops <info@bitswan.space>"',
            "-m",
            f"Update deployment {deployment_id} with checksum {checksum}",
            cwd=bitswan_home,
        )

        if has_remote:
            await call_git_command("git", "push", cwd=bitswan_home)
