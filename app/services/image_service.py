from datetime import datetime
import os
import shutil
import threading
from tempfile import NamedTemporaryFile
import zipfile

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
                        internal_images.append({
                            "id": image.id,
                            "tag": tag,
                            "created": image.attrs.get("Created"),
                            "size": image.attrs.get("Size")
                        })

            return internal_images
        except docker.errors.DockerException as e:
            raise HTTPException(status_code=500, detail=f"Docker error: {str(e)}")

    async def create_image(self, image_tag: str, file: UploadFile):
        """
        Builds a docker image using a zipfile uploaded to "file" as the docker context.

        Tags that image with "internal/{image_tag}"

        Launches the build operation asynchronously and returns immediately.

        Streams all logs to a file in /var/log/internal-image-build/{image_tag}

        """
        # Validate image tag
        if not image_tag or "/" in image_tag:
            raise HTTPException(status_code=400, detail="Invalid image tag")

        full_tag = f"internal/{image_tag}"
        log_file_path = os.path.join(self.log_dir, image_tag)

        with NamedTemporaryFile() as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_file.flush()  # Ensure all data is written

            # Create a temporary directory to extract the zip
            with NamedTemporaryFile(suffix='.dir', delete=False) as temp_dir:
                temp_dir_path = temp_dir.name

            try:
                os.remove(temp_dir_path)  # Remove the file
                os.makedirs(temp_dir_path)  # Create a directory with the same name

                # Extract the zip file to the temporary directory
                with zipfile.ZipFile(temp_file.name, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir_path)

                # Open log file for writing
                with open(log_file_path, 'w') as log_file:
                    log_file.write(f"Build started at {datetime.now().isoformat()}\n")

                    # Start the build process asynchronously
                    def build_callback():
                        try:
                            # Build the Docker image and stream logs
                            for line in self.client.api.build(
                                path=temp_dir_path,
                                tag=full_tag,
                                rm=True,
                                decode=True
                            ):
                                if 'stream' in line:
                                    with open(log_file_path, 'a') as f:
                                        f.write(line['stream'])
                        except Exception as e:
                            with open(log_file_path, 'a') as f:
                                f.write(f"Build error: {str(e)}\n")
                        finally:
                            # Clean up the temporary directory
                            import shutil
                            shutil.rmtree(temp_dir_path, ignore_errors=True)

                # Start the build in a separate thread
                import threading
                thread = threading.Thread(target=build_callback)
                thread.daemon = True
                thread.start()

            except Exception as e:
                # Clean up and raise exception
                import shutil
                shutil.rmtree(temp_dir_path, ignore_errors=True)
                raise HTTPException(status_code=500, detail=f"Error processing upload: {str(e)}")

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

            # Also remove the log file if it exists
            log_file_path = os.path.join(self.log_dir, image_tag)
            if os.path.exists(log_file_path):
                os.remove(log_file_path)

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
            raise HTTPException(status_code=404, detail=f"No logs found for image {image_tag}")

        try:
            # Read the last 'lines' lines from the log file
            with open(log_file_path, 'r') as log_file:
                all_lines = log_file.readlines()
                last_lines = all_lines[-lines:] if len(all_lines) > lines else all_lines
                return {
                    "image_tag": image_tag,
                    "logs": "".join(last_lines)
                }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error reading logs: {str(e)}")
