import asyncio
import json
from datetime import datetime
import os
from tempfile import NamedTemporaryFile
import zipfile
import shutil
from typing import Optional, AsyncGenerator, Dict

from app.utils import calculate_git_tree_hash, save_image

import docker
from fastapi import UploadFile, HTTPException


class ImageService:
    def __init__(self):
        self.client = docker.from_env()
        self.bs_home = os.environ.get("BITSWAN_GITOPS_DIR", "/mnt/repo/pipeline")
        self.gitops_dir = os.path.join(self.bs_home, "gitops")
        self.images_base_dir = os.path.join(self.gitops_dir, "images")
        os.makedirs(self.images_base_dir, exist_ok=True)

    def _get_image_dir(self, checksum: str) -> str:
        return os.path.join(self.images_base_dir, checksum)

    def _ensure_image_dir(self, checksum: str) -> str:
        image_dir = self._get_image_dir(checksum)
        os.makedirs(image_dir, exist_ok=True)
        return image_dir

    def _get_source_dir(self, checksum: str) -> str:
        """
        Location for build context so metadata/logs stay at the checksum root.
        """
        return os.path.join(self._ensure_image_dir(checksum), "src")

    def _metadata_path(self, checksum: str) -> str:
        return os.path.join(self._ensure_image_dir(checksum), "build_metadata.json")

    def _write_metadata(self, checksum: str, tag_root: str):
        metadata = {
            "checksum": checksum,
            "tag_root": tag_root,
            "updated_at": datetime.utcnow().isoformat(),
            "last_status": "pending",
        }
        with open(self._metadata_path(checksum), "w") as f:
            json.dump(metadata, f)

    def _read_metadata(self, checksum: str) -> Optional[dict]:
        metadata_path = self._metadata_path(checksum)
        if not os.path.exists(metadata_path):
            return None
        try:
            with open(metadata_path, "r") as f:
                return json.load(f)
        except Exception:
            return None

    def _finalize_metadata(
        self, checksum: str, status: str, tag_root: Optional[str] = None
    ):
        metadata = self._read_metadata(checksum) or {}
        if tag_root:
            metadata["tag_root"] = tag_root
        metadata.update(
            {
                "checksum": checksum,
                "updated_at": datetime.utcnow().isoformat(),
                "last_status": status,
            }
        )
        with open(self._metadata_path(checksum), "w") as f:
            json.dump(metadata, f)

    def _log_paths(self, checksum: str):
        image_dir = self._ensure_image_dir(checksum)
        building = os.path.join(image_dir, f"{checksum}.building.log")
        success = os.path.join(image_dir, f"{checksum}.build.log")
        failed = os.path.join(image_dir, f"{checksum}.failedbuild.log")
        return building, success, failed

    def _get_build_status(self, checksum: str) -> str:
        building, success, failed = self._log_paths(checksum)
        if os.path.exists(building):
            return "building"
        if os.path.exists(failed):
            return "failed"
        if os.path.exists(success):
            return "ready"
        return "ready"

    def _extract_checksum_from_tag(self, tag: str) -> Optional[str]:
        if ":sha" not in tag:
            return None
        return tag.split(":sha", 1)[1]

    def _load_metadata_entries(self) -> Dict[str, dict]:
        entries: Dict[str, dict] = {}
        if not os.path.exists(self.images_base_dir):
            return entries
        for checksum in os.listdir(self.images_base_dir):
            entry_dir = os.path.join(self.images_base_dir, checksum)
            if not os.path.isdir(entry_dir):
                continue
            metadata = self._read_metadata(checksum)
            if metadata:
                entries[checksum] = metadata
        return entries

    def _prepare_build_context(self, source_dir: str, checksum: str) -> str:
        """
        Copy build context into <checksum>/src to keep metadata/logs sibling files clean.
        """
        destination_dir = self._get_source_dir(checksum)
        source_abs = os.path.abspath(source_dir)
        destination_abs = os.path.abspath(destination_dir)

        if source_abs != destination_abs:
            if os.path.exists(destination_dir):
                shutil.rmtree(destination_dir)
            shutil.copytree(source_dir, destination_dir)

        return destination_dir

    def get_images(self):
        """
        Gets all available tagged docker images. Returns them as a list.
        """
        try:
            # Get all images and filter for those with internal/ prefix
            all_images = self.client.images.list()
            internal_images = []
            metadata_entries = self._load_metadata_entries()
            seen_tags = set()

            for image in all_images:
                for tag in image.tags:
                    if tag.startswith("internal/"):
                        checksum = self._extract_checksum_from_tag(tag)
                        build_status = (
                            self._get_build_status(checksum) if checksum else "ready"
                        )
                        internal_images.append(
                            {
                                "id": image.id,
                                "tag": tag,
                                "created": image.attrs.get("Created"),
                                "size": image.attrs.get("Size"),
                                "building": build_status == "building",
                                "build_status": build_status,
                                "checksum": checksum,
                            }
                        )
                        seen_tags.add(tag)

            # Add entries for builds that are still running or failed (no docker image yet)
            for checksum, metadata in metadata_entries.items():
                tag_root = metadata.get("tag_root")
                if not tag_root:
                    continue
                full_tag = f"{tag_root}:sha{checksum}"
                if full_tag in seen_tags:
                    continue
                status = self._get_build_status(checksum)
                if status in ("building", "failed"):
                    internal_images.append(
                        {
                            "id": None,
                            "tag": full_tag,
                            "created": metadata.get("updated_at"),
                            "size": None,
                            "building": status == "building",
                            "build_status": status,
                            "checksum": checksum,
                        }
                    )

            return internal_images
        except docker.errors.DockerException as e:
            raise HTTPException(status_code=500, detail=f"Docker error: {str(e)}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

    def _start_build_process(
        self,
        build_context_path: str,
        full_tag: str,
        tag_root: str,
        checksum: str,
        image_dir: str,
    ):
        """
        Common build process logic shared between directory and zip file builds.

        Args:
            build_context_path: Path to the build context directory
            full_tag: Full Docker tag for the image
            tag_root: Root tag (e.g., "internal/image_name")
            building_log_file_path: Path to the building log file
            final_log_file_path: Path to the final log file
        """

        building_log_file_path, success_log_file_path, failed_log_file_path = (
            self._log_paths(checksum)
        )

        def build_callback():
            final_log_path = success_log_file_path
            build_status = "success"
            try:
                # Build the Docker image and stream logs
                for line in self.client.api.build(
                    path=build_context_path, tag=full_tag, rm=True, decode=True
                ):
                    if "stream" in line:
                        with open(building_log_file_path, "a") as f:
                            f.write(line["stream"])
                # Tag with latest
                self.client.images.get(full_tag).tag(tag_root, tag="latest")

                # Build completed successfully, rename log file
                with open(building_log_file_path, "a") as f:
                    f.write(
                        f"Build completed successfully at {datetime.now().isoformat()}\n"
                    )
            except Exception as e:
                build_status = "failed"
                final_log_path = failed_log_file_path
                with open(building_log_file_path, "a") as f:
                    f.write(f"Build error: {str(e)}\n")
            finally:
                try:
                    if os.path.exists(building_log_file_path):
                        os.rename(building_log_file_path, final_log_path)
                except Exception:
                    pass

                self._finalize_metadata(checksum, build_status, tag_root)

                try:
                    asyncio.run(
                        save_image(
                            image_dir,
                            checksum,
                            tag_root,
                            copy_context=False,
                            build_status=build_status,
                            log_file_path=final_log_path,
                        )
                    )
                    with open(final_log_path, "a") as f:
                        f.write("Build context committed to git successfully\n")
                except Exception as git_error:
                    with open(final_log_path, "a") as f:
                        f.write(
                            f"Warning: Failed to commit build context to git: {str(git_error)}\n"
                        )

        # Start the build in a separate thread
        import threading

        thread = threading.Thread(target=build_callback)
        thread.daemon = True
        thread.start()

    async def create_image(
        self,
        image_tag: str,
        file: Optional[UploadFile] = None,
        checksum: Optional[str] = None,
        build_context_path: Optional[str] = None,
    ):
        # If file is provided, checksum is required
        if file and not checksum:
            raise HTTPException(
                status_code=400,
                detail="Checksum is required when uploading a file",
            )
        """
        Builds a docker image either from a zipfile upload or from a directory path.

        If file is provided: Uses zipfile uploaded to "file" as the docker context.
        If build_context_path is provided: Uses the directory directly as build context.

        Tags that image with "internal/{image_tag}"

        Launches the build operation asynchronously and returns immediately.

        Streams all logs to a file in /var/log/internal-image-build/{image_tag}

        Commits the build context to the git repository after successful build.
        """

        # Validate image tag
        if not image_tag or "/" in image_tag:
            raise HTTPException(status_code=400, detail="Invalid image tag")

        # Validate that either file or build_context_path is provided, but not both
        if not file and not build_context_path:
            raise HTTPException(
                status_code=400,
                detail="Either file or build_context_path must be provided",
            )
        if file and build_context_path:
            raise HTTPException(
                status_code=400,
                detail="Cannot provide both file and build_context_path",
            )

        tag_root = f"internal/{image_tag}"

        # Handle directory-based build
        if build_context_path:
            if not os.path.exists(build_context_path):
                raise HTTPException(
                    status_code=400,
                    detail=f"Build context directory does not exist: {build_context_path}",
                )

            # Use provided checksum or generate one from directory name
            if not checksum:
                checksum = os.path.basename(build_context_path)

            image_dir = self._prepare_build_context(build_context_path, checksum)
            full_tag = f"{tag_root}:sha{checksum}"

            self._write_metadata(checksum, tag_root)
            (
                building_log_file_path,
                success_log_file_path,
                failed_log_file_path,
            ) = self._log_paths(checksum)
            for old_log in (success_log_file_path, failed_log_file_path):
                if os.path.exists(old_log):
                    os.remove(old_log)
            if os.path.exists(building_log_file_path):
                os.remove(building_log_file_path)
            with open(building_log_file_path, "w") as log_file:
                log_file.write(f"Build started at {datetime.now().isoformat()}\n")

            # Start the build process using shared logic
            self._start_build_process(
                build_context_path=image_dir,
                full_tag=full_tag,
                tag_root=tag_root,
                checksum=checksum,
                image_dir=image_dir,
            )

            return {
                "status": "success",
                "message": f"Image {image_tag} build started from directory",
                "tag": full_tag,
            }

        # Handle zip file upload
        with NamedTemporaryFile() as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_file.flush()  # Ensure all data is written

            # Create a temporary directory to extract the zip
            with NamedTemporaryFile(suffix=".dir", delete=False) as temp_dir:
                temp_dir_path = temp_dir.name

                os.remove(temp_dir_path)  # Remove the file
                os.makedirs(temp_dir_path)  # Create a directory with the same name

                try:
                    # Extract the zip file to the temporary directory
                    with zipfile.ZipFile(temp_file.name, "r") as zip_ref:
                        zip_ref.extractall(temp_dir_path)

                    # Verify the checksum using git tree hash algorithm
                    calculated_hash = await calculate_git_tree_hash(temp_dir_path)
                    if calculated_hash != checksum:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Checksum verification failed. Expected {checksum}, got {calculated_hash}",
                        )
                    image_dir = self._prepare_build_context(temp_dir_path, checksum)
                    full_tag = f"{tag_root}:sha{checksum}"
                    self._write_metadata(checksum, tag_root)
                    (
                        building_log_file_path,
                        success_log_file_path,
                        failed_log_file_path,
                    ) = self._log_paths(checksum)
                    for old_log in (success_log_file_path, failed_log_file_path):
                        if os.path.exists(old_log):
                            os.remove(old_log)
                    if os.path.exists(building_log_file_path):
                        os.remove(building_log_file_path)
                    with open(building_log_file_path, "w") as log_file:
                        log_file.write(
                            f"Build started at {datetime.now().isoformat()}\n"
                        )

                    # Start the build process using shared logic
                    self._start_build_process(
                        build_context_path=image_dir,
                        full_tag=full_tag,
                        tag_root=tag_root,
                        checksum=checksum,
                        image_dir=image_dir,
                    )
                finally:
                    # Clean up the temporary directory
                    shutil.rmtree(temp_dir_path, ignore_errors=True)

        return {
            "status": "success",
            "message": f"Image {image_tag} build started",
            "tag": full_tag,
        }

    async def delete_image(self, image_tag: str):
        """
        Deletes a docker image with the tag "internal/{image_tag}"
        """
        full_tag = f"internal/{image_tag}"

        try:
            # Try to remove the image
            self.client.images.remove(full_tag, force=True)

            checksum = self._extract_checksum_from_tag(full_tag)
            if checksum:
                building_log_file_path, _, _ = self._log_paths(checksum)
                if os.path.exists(building_log_file_path):
                    os.remove(building_log_file_path)

            return {
                "status": "success",
                "message": f"Image {image_tag} deleted successfully",
            }
        except docker.errors.ImageNotFound:
            raise HTTPException(status_code=404, detail=f"Image {full_tag} not found")
        except docker.errors.APIError as e:
            raise HTTPException(status_code=500, detail=f"Docker API error: {str(e)}")

    def get_image_logs(self, image_tag: str, lines: int = 100):
        """
        Returns the last lines number of lines from:

        gitops/images/<checksum>/<checksum>*.log
        """
        checksum = self._extract_checksum_from_tag(f"internal/{image_tag}")
        if not checksum:
            raise HTTPException(
                status_code=404, detail=f"No logs found for image {image_tag}"
            )

        building_log_file_path, success_log_file_path, failed_log_file_path = (
            self._log_paths(checksum)
        )

        log_file_path = None
        for candidate in (
            building_log_file_path,
            success_log_file_path,
            failed_log_file_path,
        ):
            if os.path.exists(candidate):
                log_file_path = candidate
                break

        if not log_file_path:
            raise HTTPException(
                status_code=404, detail=f"No logs found for image {image_tag}"
            )

        try:
            # Read the last 'lines' lines from the log file
            with open(log_file_path, "r") as log_file:
                all_lines = log_file.readlines()
                last_lines = all_lines[-lines:] if len(all_lines) > lines else all_lines
                return {"image_tag": image_tag, "logs": last_lines}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error reading logs: {str(e)}")

    async def stream_build_logs(self, checksum: str) -> AsyncGenerator[str, None]:
        building_log_file_path, success_log_file_path, failed_log_file_path = (
            self._log_paths(checksum)
        )

        current_path = None
        current_file = None
        position = 0

        async def ensure_log_file() -> str:
            while True:
                if os.path.exists(building_log_file_path):
                    return building_log_file_path
                if os.path.exists(success_log_file_path):
                    return success_log_file_path
                if os.path.exists(failed_log_file_path):
                    return failed_log_file_path
                await asyncio.sleep(0.5)

        try:
            while True:
                next_path = await ensure_log_file()

                if next_path != current_path:
                    if current_file:
                        current_file.close()
                    current_file = open(next_path, "r")
                    position = 0
                    current_path = next_path

                current_file.seek(position)
                chunk = current_file.read()
                position = current_file.tell()

                if chunk:
                    yield chunk
                    continue

                # If we're following the building log, wait for more data
                if current_path == building_log_file_path:
                    await asyncio.sleep(0.5)
                    continue

                # Final log reached and fully read
                break
        finally:
            if current_file:
                current_file.close()
