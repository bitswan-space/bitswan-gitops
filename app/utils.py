import hashlib
import os
import zipfile
from tempfile import NamedTemporaryFile
import yaml

async def process_zip_file(file, deployment_id):
    with NamedTemporaryFile(delete=False) as temp_file:
        content = await file.read()
        temp_file.write(content)

    checksum = calculate_checksum(temp_file.name)
    output_dir = f"{checksum}"

    try:
        os.makedirs(output_dir, exist_ok=True)
        with zipfile.ZipFile(temp_file.name, 'r') as zip_ref:
            zip_ref.extractall(output_dir)
        
        bitswan_home = os.environ.get('BITSWAN_HOME', '')
        bitswan_yaml_path = os.path.join(bitswan_home, "bitswan.yaml")
        
        # Update or create bitswan.yaml
        if os.path.exists(bitswan_yaml_path):
            with open(bitswan_yaml_path, 'r') as f:
                data = yaml.safe_load(f) or {}
        else:
            data = {}
        
        data[checksum] = deployment_id
        
        with open(bitswan_yaml_path, 'w') as f:
            yaml.dump(data, f)
        
        return {"message": "File processed successfully", "output_directory": output_dir, "checksum": checksum}
    except Exception as e:
        return {"error": f"Error processing file: {str(e)}"}
    finally:
        os.unlink(temp_file.name)

def calculate_checksum(file_path):
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()
