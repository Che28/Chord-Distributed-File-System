import boto3
import logging
import requests
import subprocess
import time
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import os
import hashlib
import msgpackrpc
import shutil


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

REGION = "us-east-1"
ASG_NAME = "chord2"

# 獲取當前節點的公開 IP
def get_current_ip():
    try:
        token = requests.put(
            "http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"}
        ).text
        current_ip = requests.get(
            "http://169.254.169.254/latest/meta-data/public-ipv4",
            headers={"X-aws-ec2-metadata-token": token}
        ).text
        logger.info(f"Current Node IP: {current_ip}")
        return current_ip
    except requests.RequestException as e:
        logger.error(f"Failed to get current IP: {e}")
        raise

# 獲取所有 Auto Scaling 組中的節點 IP
def get_all_instance_ips():
    try:
        autoscaling_client = boto3.client("autoscaling", region_name=REGION)
        ec2_client = boto3.client("ec2", region_name=REGION)

        response = autoscaling_client.describe_auto_scaling_groups(
            AutoScalingGroupNames=[ASG_NAME]
        )
        instance_ids = [
            instance["InstanceId"]
            for instance in response["AutoScalingGroups"][0]["Instances"]
        ]

        logger.info(f"Instance IDs: {instance_ids}")

        instance_ips = []
        for instance_id in instance_ids:
            instance_info = ec2_client.describe_instances(InstanceIds=[instance_id])
            ip_address = instance_info["Reservations"][0]["Instances"][0].get("PublicIpAddress")
            if ip_address:
                instance_ips.append(ip_address)

        logger.info(f"All Instance IPs: {instance_ips}")
        return instance_ips
    except (NoCredentialsError, PartialCredentialsError) as e:
        logger.error(f"AWS credentials error: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to get instance IPs: {e}")
        raise

# 初始化 Chord 節點
def initialize_chord(current_ip):
    try:
        logger.info("Starting as the first Chord node...")
        subprocess.Popen(["./chord", current_ip, "5057"])
        time.sleep(5)
        import msgpackrpc
        client = msgpackrpc.Client(msgpackrpc.Address(current_ip, 5057))
        client.call("create")
        logger.info("Chord ring created successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize Chord ring: {e}")
        raise

# 加入已有的 Chord 環
def join_chord_ring(current_ip, first_node_ip):
    try:
        logger.info(f"Joining Chord ring via {first_node_ip}...")
        subprocess.Popen(["./chord", current_ip, "5057"])
        time.sleep(5)
        import msgpackrpc
        client_2 = msgpackrpc.Client(msgpackrpc.Address(current_ip, 5057))
        client_1 = msgpackrpc.Client(msgpackrpc.Address(first_node_ip, 5057))
        info = client_1.call("get_info")
        client_2.call("join", info)
        logger.info("Node joined the Chord ring successfully.")
    except Exception as e:
        logger.error(f"Failed to join Chord ring: {e}")
        raise



# 配置日誌
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

FILE_DIR = "/home/ec2-user/files"
FLASK_API_URL = "http://{}:5059/get_files"  # Flask get_files API URL

def compute_hash(file_name):
    """計算檔案名稱的哈希值"""
    return int(hashlib.md5(file_name.encode()).hexdigest(), 16) & ((1 << 32) - 1)

def get_predecessor(node_ip):
    """查詢節點的前置節點"""
    try:
        time.sleep(20)
        client = msgpackrpc.Client(msgpackrpc.Address(node_ip, 5057) ,timeout = 5)
        predecessor = client.call("get_predecessor")
        logger.info(f"Predecessor of {node_ip}: {predecessor}")
        return predecessor
    except Exception as e:
        logger.error(f"Failed to get predecessor of {node_ip}: {e}")
        return None

def get_successor(node_ip, i):
    """查詢節點的i-th後繼節點"""
    try:
        time.sleep(20)
        client = msgpackrpc.Client(msgpackrpc.Address(node_ip, 5057), timeout = 5)
        successor = client.call("get_successor", i)
        logger.info(f"Successor of {node_ip} at {i}: {successor}")
        return successor
    except Exception as e:
        logger.error(f"Failed to get successor of {node_ip}: {e}")
        return None

def get_files_from_successor(successor_ip):
    """從successor節點獲取檔案清單及其哈希值"""
    try:
        response = requests.get(FLASK_API_URL.format(successor_ip))
        if response.status_code == 200:
            files = response.json().get("data", {})
            logger.info(f"Files from successor {successor_ip}: {files}")
            return files
        else:
            logger.error(f"Failed to get files from successor {successor_ip}, status code: {response.status_code}")
            return {}
    except Exception as e:
        logger.error(f"Error while fetching files from successor {successor_ip}: {e}")
        return {}

def migrate_data(new_node_ip, new_node_hash, predecessor_hash, successor_ip, successor_hash):
    """進行數據遷移：將範圍內的檔案從successor移到new_node"""
    try:
        logger.info(f"Range for migration: {predecessor_hash} -> {new_node_hash} -> {successor_hash}")

        # 從successor節點獲取檔案清單
        files_from_successor = get_files_from_successor(successor_ip)

        # 檢查範圍，從successor遷移檔案到新節點
        def is_in_range(predecessor_hash, new_node_hash, successor_hash):
            """ 判斷new_node_hash是否在predecessor_hash和successor_hash之間，並考慮環的循環性 """
            if predecessor_hash < successor_hash:
                # 範圍沒有跨越環邊界
                return predecessor_hash < new_node_hash <= successor_hash
            else:
                # 範圍跨越環邊界
                return predecessor_hash < new_node_hash or new_node_hash <= successor_hash

        for file, file_hash in files_from_successor.items():
            # 確認檔案哈希是否在遷移範圍內
            if is_in_range(predecessor_hash, file_hash, new_node_hash):
                logger.info(f"Migrating file: {file} to {new_node_ip}")
                
                # 使用download.py腳本從successor下載檔案到/chord-part-2
                download_command = f"/usr/bin/python3 /home/ec2-user/chord-part-2/download.py {file} {successor_ip}"
                subprocess.run(download_command, shell=True, check=True)

                # 移動檔案到/files目錄
                source_file_path = os.path.join("/home/ec2-user/chord-part-2", file)
                destination_file_path = os.path.join("/home/ec2-user/files", file)

                # 使用shutil.move移動檔案
                shutil.move(source_file_path, destination_file_path)

                logger.info(f"Successfully migrated file {file} to {new_node_ip} and moved to /files")

    except Exception as e:
        logger.error(f"Failed to migrate data for {new_node_ip}: {e}")


def handle_new_node(new_node_ip):
    import msgpackrpc
    client = msgpackrpc.Client(msgpackrpc.Address(new_node_ip, 5057), timeout=5)
    new_node_hash = client.call("get_info")[2]
    """處理新節點加入，並進行數據遷移"""
    predecessor_hash = get_predecessor(new_node_ip)[2]
    successor_ip = get_successor(new_node_ip, 0) [0].decode() # 假設取得第0個後繼節點
    successor_hash = get_successor(new_node_ip, 0)[2]

    if predecessor_hash and successor_ip:
        # 進行數據遷移
        migrate_data(new_node_ip, new_node_hash, predecessor_hash, successor_ip, successor_hash)
    else:
        logger.error("Failed to get predecessor or successor for migration.")


if __name__ == "__main__":
    try:
        current_ip = get_current_ip()
        instance_ips = get_all_instance_ips()

        if current_ip in instance_ips:
            instance_ips.remove(current_ip)

        if instance_ips:
            instance_ips.sort()
            first_node_ip = instance_ips[0]
            logger.info(f"First Node IP: {first_node_ip}")
            join_chord_ring(current_ip, first_node_ip)
            time.sleep(10)
            handle_new_node(current_ip)
        else:
            logger.info(f"No other nodes found. Initializing Chord ring on this node ({current_ip}).")
            initialize_chord(current_ip)
    except Exception as e:
        logger.critical(f"Script failed: {e}")