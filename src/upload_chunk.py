import os
import sys
import json
import subprocess


def calculate_chunk_size(chunk_size_mb):
    """將塊大小從MB轉換為位元組。"""
    return chunk_size_mb * 1024 * 1024


def create_metadata(file_name, total_chunks):
    """為第一個塊創建元數據。"""
    metadata = {
        "original_file_name": file_name,
        "total_chunks": total_chunks,
    }
    return json.dumps(metadata).encode("utf-8")


def split_file(file_path, chunk_size_mb):
    """
    將大檔案分割為小塊並在第一個塊中添加元數據。

    參數:
        file_path (str): 要分割的檔案路徑。
        chunk_size_mb (int): 每個塊的大小（以MB為單位）。

    返回:
        list: 分割後檔案塊的路徑清單。
    """
    chunk_size = calculate_chunk_size(chunk_size_mb)
    file_size = os.path.getsize(file_path)
    file_name = os.path.basename(file_path)
    total_chunks = (file_size + chunk_size - 1) // chunk_size  # 計算總塊數

    chunk_files = []
    with open(file_path, "rb") as f:
        for chunk_index in range(total_chunks):
            chunk = f.read(chunk_size)
            chunk_file = f"{file_path}.chunk{chunk_index}"

            # 在第一個塊中加入元數據
            if chunk_index == 0:
                metadata = create_metadata(file_name, total_chunks)
                chunk = metadata + b"\n" + chunk

            # 將塊寫入檔案
            with open(chunk_file, "wb") as chunk_f:
                chunk_f.write(chunk)

            chunk_files.append(chunk_file)
            print(f"Created chunk: {chunk_file} ({len(chunk)} bytes)")

    print(f"Split {file_name} into {total_chunks} chunks.")
    return chunk_files


def upload_chunk(chunk_file, upload_script, destination_ip):
    """使用提供的上傳腳本上傳單個檔案塊。"""
    try:
        print(f"Uploading chunk: {chunk_file} to {destination_ip}")
        subprocess.run(
            ["python3", upload_script, chunk_file, destination_ip],
            check=True,
        )
        print(f"Uploaded chunk: {chunk_file}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to upload chunk {chunk_file}: {e}")
    finally:
        # 上傳後清理本地檔案塊
        if os.path.exists(chunk_file):
            os.remove(chunk_file)
            print(f"Deleted chunk: {chunk_file}")


def upload_all_chunks(chunk_files, upload_script, destination_ip):
    """
    使用提供的上傳腳本上傳每個檔案塊。

    參數:
        chunk_files (list): 要上傳的檔案塊路徑清單。
        upload_script (str): 上傳腳本的路徑（例如 upload.py）。
        destination_ip (str): Chord 節點的IP地址。
    """
    for chunk_file in chunk_files:
        upload_chunk(chunk_file, upload_script, destination_ip)


def validate_file(file_path):
    """驗證檔案是否存在並有效。"""
    if not os.path.isfile(file_path):
        print(f"Error: File not found: {file_path}")
        sys.exit(1)


def main():
    if len(sys.argv) != 4:
        print("Usage: python chunk_upload.py <file_path> <chunk_size_mb> <destination_ip>")
        sys.exit(1)

    file_path = sys.argv[1]
    chunk_size_mb = int(sys.argv[2])
    destination_ip = sys.argv[3]

    # 驗證檔案
    validate_file(file_path)

    # 定義上傳腳本路徑
    upload_script = "upload.py"

    # 將檔案分割為塊
    chunk_files = split_file(file_path, chunk_size_mb)

    # 上傳檔案塊
    upload_all_chunks(chunk_files, upload_script, destination_ip)

    print("File upload completed.")


if __name__ == "__main__":
    main()
