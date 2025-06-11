from flask import Flask, jsonify
import os
import hashlib
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

FILE_DIR = "/home/ec2-user/files"

def compute_hash(file_name):
    """Compute the hash of a file name."""
    return int(hashlib.md5(file_name.encode()).hexdigest(), 16) & ((1 << 32) - 1)

@app.route('/get_files', methods=['GET'])
def get_files():
    """Return a list of files and their hashes stored in the file server."""
    try:
        files = os.listdir(FILE_DIR)
        file_hashes = {}
        for file in files:
            file_path = os.path.join(FILE_DIR, file)
            if os.path.isfile(file_path):
                file_hashes[file] = compute_hash(file)

        logger.info(f"Files and hashes: {file_hashes}")

        return jsonify({"status": "success", "data": file_hashes})
    except Exception as e:
        logger.error(f"Error while fetching files: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5059)