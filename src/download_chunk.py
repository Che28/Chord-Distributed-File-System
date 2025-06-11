import os
import sys
import json
import subprocess


def execute_subprocess(command):
    """
    執行子程序命令並處理錯誤。

    參數:
        command (list): 以字串列表形式提供要執行的命令。

    返回:
        bool: 如果命令成功執行，返回True，否則返回False。
    """
    try:
        subprocess.run(command, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Subprocess failed: {e}")
        return False


def download_chunk_file(chunk_filename, chord_node_ip, download_script_path):
    """
    下載單個檔案塊，並返回檔案名稱。

    參數:
        chunk_filename (str): 要下載的檔案塊名稱。
        chord_node_ip (str): Chord 節點的IP地址。
        download_script_path (str): 下載腳本的路徑。

    返回:
        str: 下載後的檔案塊名稱。
    """
    print(f"Downloading chunk: {chunk_filename} from {chord_node_ip}")
    if execute_subprocess(["python3", download_script_path, chunk_filename, chord_node_ip]):
        return chunk_filename
    else:
        sys.exit(1)


def combine_file_chunks(chunks, output_filename):
    """
    將所有檔案塊組合成原始檔案。

    參數:
        chunks (list): 檔案塊的路徑清單。
        output_filename (str): 輸出組裝檔案的路徑。
    """
    with open(output_filename, "wb") as outfile:
        for chunk in chunks:
            with open(chunk, "rb") as infile:
                # 如果是第一個檔案塊，去除元數據
                if chunk == chunks[0]:
                    metadata_line = infile.readline()
                    print(f"Metadata: {metadata_line.decode('utf-8').strip()}")
                outfile.write(infile.read())
            os.remove(chunk)  # 組合後刪除檔案塊
            print(f"Deleted chunk: {chunk}")
    print(f"Assembled file saved to: {output_filename}")


def extract_metadata_from_first_chunk(chunk_filename, chord_node_ip, download_script_path):
    """
    下載第一個檔案塊並提取元數據。

    參數:
        chunk_filename (str): 第一個檔案塊的名稱。
        chord_node_ip (str): Chord 節點的IP地址。
        download_script_path (str): 下載腳本的路徑。

    返回:
        dict: 提取的元數據。
    """
    downloaded_chunk = download_chunk_file(chunk_filename, chord_node_ip, download_script_path)
    with open(downloaded_chunk, "rb") as f:
        metadata_line = f.readline()
    try:
        metadata = json.loads(metadata_line.decode("utf-8").strip())
        return metadata
    except json.JSONDecodeError:
        print(f"Error parsing metadata from {chunk_filename}.")
        sys.exit(1)


def validate_input_arguments():
    """
    驗證命令行參數。

    返回:
        tuple: 檔案名稱和節點IP。
    """
    if len(sys.argv) != 3:
        print("Usage: python chunk_download.py <file_name> <node_ip>")
        sys.exit(1)
    
    file_name = sys.argv[1]
    node_ip = sys.argv[2]
    
    return file_name, node_ip


def main():
    # 驗證參數並獲取檔案名稱和節點IP
    file_name, node_ip = validate_input_arguments()

    # 定義下載腳本路徑
    download_script_path = "download.py"

    # 下載第一個檔案塊以獲取元數據
    first_chunk_filename = f"{file_name}.chunk0"
    metadata = extract_metadata_from_first_chunk(first_chunk_filename, node_ip, download_script_path)
    total_chunks = metadata["total_chunks"]

    print(f"Metadata extracted: {metadata}")
    print(f"Downloading all {total_chunks} chunks...")

    # 下載所有檔案塊
    downloaded_chunks = []
    for i in range(total_chunks):
        chunk_filename = f"{file_name}.chunk{i}"
        downloaded_chunks.append(download_chunk_file(chunk_filename, node_ip, download_script_path))

    # 組合回原始檔案
    combine_file_chunks(downloaded_chunks, file_name)
    print("Download and assembly complete.")


if __name__ == "__main__":
    main()
