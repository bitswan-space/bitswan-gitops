import asyncio
from datetime import datetime
import os
from tempfile import NamedTemporaryFile
import zipfile
import shutil
from typing import Optional

from app.utils import calculate_checksum, save_image

import docker
from fastapi import UploadFile, HTTPException


class ImageService:
    def __init__(self):
        self.client = docker.from_env()
        self.log_dir = "/var/log/internal-image-build"
        # Ensure log directory exists
        os.makedirs(self.log_dir, exist_ok=True)

    def get_images(self):
        """
        Gets all available tagged docker images. Returns them as a list.
        """
        try:
            # Get all images and filter for those with internal/ prefix
            all_images = self.client.images.list()
            internal_images = []

            for image in all_images:
                for tag in image.tags:
                    if tag.startswith("internal/"):
                        internal_images.append(
                            {
                                "id": image.id,
                                "tag": tag,
                                "created": image.attrs.get("Created"),
                                "size": image.attrs.get("Size"),
                                "building": False,
                            }
                        )

            # Check for images currently being built by scanning for .building log files
            if os.path.exists(self.log_dir):
                for filename in os.listdir(self.log_dir):
                    if filename.endswith(".building"):
                        # Extract image_tag from filename: {image_tag}:sha{checksum}.building
                        base_name = filename[:-9]  # Remove '.building' suffix
                        tag = f"internal/{base_name}"
                        internal_images.append(
                            {
                                "id": None,
                                "tag": tag,
                                "created": None,
                                "size": None,
                                "building": True,
                            }
                        )

            return internal_images
        except docker.errors.DockerException as e:
            raise HTTPException(status_code=500, detail=f"Docker error: {str(e)}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

    def _start_build_process(self, build_context_path: str, full_tag: str, tag_root: str, 
                           building_log_file_path: str, final_log_file_path: str):
        """
        Common build process logic shared between directory and zip file builds.
        
        Args:
            build_context_path: Path to the build context directory
            full_tag: Full Docker tag for the image
            tag_root: Root tag (e.g., "internal/image_name")
            building_log_file_path: Path to the building log file
            final_log_file_path: Path to the final log file
        """
        def build_callback():
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
                os.rename(building_log_file_path, final_log_file_path)

            except Exception as e:
                with open(building_log_file_path, "a") as f:
                    f.write(f"Build error: {str(e)}\n")
                # Build failed, still rename to final log file for error tracking
                os.rename(building_log_file_path, final_log_file_path) 

        # Start the build in a separate thread
        import threading
        thread = threading.Thread(target=build_callback)
        thread.daemon = True
        thread.start()

    async def create_image(self, image_tag: str, file: Optional[UploadFile] = None, checksum: Optional[str] = None,
    build_context_path: Optional[str] = None,):
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
            raise HTTPException(status_code=400, detail="Either file or build_context_path must be provided")
        if file and build_context_path:
            raise HTTPException(status_code=400, detail="Cannot provide both file and build_context_path")

        tag_root = f"internal/{image_tag}"

        # Handle directory-based build
        if build_context_path:
            if not os.path.exists(build_context_path):
                raise HTTPException(
                    status_code=400,
                    detail=f"Build context directory does not exist: {build_context_path}",
                )
            
            # Use provided checksum or generate one from directory
            if not checksum:
                checksum = os.path.basename(build_context_path)
            
            full_tag = tag_root + ":sha" + checksum
            # Use .building suffix during build
            building_log_file_path = os.path.join(
                self.log_dir, image_tag + ":sha" + checksum + ".building"
            )
            # Final log file path without .building suffix
            final_log_file_path = os.path.join(self.log_dir, image_tag + ":sha" + checksum)

            with open(building_log_file_path, "w") as log_file:
                log_file.write(f"Build started at {datetime.now().isoformat()}\n")

            # Start the build process using shared logic
            self._start_build_process(
                build_context_path=build_context_path,
                full_tag=full_tag,
                tag_root=tag_root,
                building_log_file_path=building_log_file_path,
                final_log_file_path=final_log_file_path
            )

            return {
                "status": "success",
                "message": f"Image {image_tag} build started from directory",
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

                    # Use provided checksum or calculate from file
                    if not checksum:
                        checksum = calculate_checksum(temp_file.name)

                    full_tag = tag_root + ":sha" + checksum
                    # Use .building suffix during build
                    building_log_file_path = os.path.join(
                        self.log_dir, image_tag + ":sha" + checksum + ".building"
                    )
                    # Final log file path without .building suffix
                    final_log_file_path = os.path.join(
                        self.log_dir, image_tag + ":sha" + checksum
                    )

                    with open(building_log_file_path, "w") as log_file:
                        log_file.write(f"Build started at {datetime.now().isoformat()}\n")

                    # Start the build process using shared logic
                    self._start_build_process(
                        build_context_path=temp_dir_path,
                        full_tag=full_tag,
                        tag_root=tag_root,
                        building_log_file_path=building_log_file_path,
                        final_log_file_path=final_log_file_path
                    )

                    # Handle git operations for zip file uploads
                    try:
                        await save_image(temp_dir_path, checksum, image_tag)
                        with open(final_log_file_path, "a") as f:
                            f.write(
                                "Build context committed to git successfully\n"
                            )
                    except Exception as git_error:
                        with open(final_log_file_path, "a") as f:
                            f.write(
                                f"Warning: Failed to commit to git: {str(git_error)}\n"
                            )
                finally:
                    # Clean up the temporary directory
                    shutil.rmtree(temp_dir_path, ignore_errors=True)

        return {
            "status": "success",
            "message": f"Image {image_tag} build started",
        }

    async def delete_image(self, image_tag: str):
        """
        Deletes a docker image with the tag "internal/{image_tag}"
        """
        full_tag = f"internal/{image_tag}"

        try:
            # Try to remove the image
            self.client.images.remove(full_tag, force=True)

            # Remove all log files associated with this image_tag
            if os.path.exists(self.log_dir):
                for filename in os.listdir(self.log_dir):
                    if filename.startswith(image_tag + ":"):
                        log_file_path = os.path.join(self.log_dir, filename)
                        if os.path.exists(log_file_path):
                            os.remove(log_file_path)

            # Also try to remove the legacy log file format for backward compatibility
            legacy_log_file_path = os.path.join(self.log_dir, image_tag)
            if os.path.exists(legacy_log_file_path):
                os.remove(legacy_log_file_path)

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

        /var/log/internal-image-build/{image_tag}
        """
        log_file_path = os.path.join(self.log_dir, image_tag)
        if not os.path.exists(log_file_path):
            log_file_path += ".building"
        if not os.path.exists(log_file_path):
            print(log_file_path)
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
