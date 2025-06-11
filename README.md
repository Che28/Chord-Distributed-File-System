# Distributed File Storage and Migration System - Chord-based

## Overview

This system implements a distributed file storage solution using the Chord DHT (Distributed Hash Table) protocol. The goal of this project is to manage large files by splitting them into chunks, distributing these chunks across multiple Chord nodes, and providing functionalities for file migration and replication. The system includes capabilities for chunking large files, uploading/downloading chunks from Chord nodes, and migrating data based on node joins.

### Key Features:
- **Data Migration**: Automatically migrate files to a new node in the Chord ring when a new node is added.
- **File Chunking**: Large files are split into smaller chunks, which are then distributed across multiple nodes.
- **Replication**: Ensures file availability across different Chord nodes.
- **Flask API**: Provides a simple API to query the file status from each node.

---

## System Components

### 1. **Chord Node Manager (`chord_node_manager.py`)**
   - Manages the lifecycle of Chord nodes.
   - Initializes the Chord ring and allows nodes to join and leave the ring.
   - Handles the migration of files based on hash range comparisons between the predecessor and successor nodes.
   
### 2. **File Chunking and Uploading (`upload_chunk.py`)**
   - Splits large files into smaller chunks based on the specified chunk size.
   - Adds metadata (original file name and total chunks) to the first chunk.
   - Uploads each chunk to the corresponding Chord node by using the node’s successor as the target.

### 3. **File Downloading and Assembly (`download_chunk.py`)**
   - Downloads the file chunks from the appropriate Chord nodes.
   - Reassembles the chunks back into the original file based on metadata.
   - Handles metadata extraction and chunk reassembly efficiently.

### 4. **Flask API (`chord_flask_api.py`)**
   - Provides a Flask-based API to query the files stored on the node.
   - Lists all files and their corresponding hashes stored on the node for migration or replication checks.

### 5. **File Storage Directory**
   - Files are stored under the `/home/ec2-user/files` directory on each node.
   - This directory holds the chunks, and new files are migrated here when a node joins the system.

---

## System Architecture

The system follows a Chord-based distributed file storage model, where each node in the system stores chunks of files based on their hash values. The key steps in file management are:

1. **File Chunking**: 
   - A large file is split into smaller chunks.
   - Each chunk is uploaded to a Chord node that corresponds to its hash value.

2. **File Migration**:
   - When a new node joins the Chord ring, data within its hash range is migrated from its predecessor’s successor.
   - The migration is handled by checking the range of each chunk’s hash and moving chunks that fall within the new node’s hash range.

3. **File Reassembly**:
   - To reconstruct a file, the chunks are downloaded from the corresponding nodes.
   - The first chunk contains metadata, which includes the total number of chunks, and the reassembly process follows this metadata to combine the chunks into the original file.

---

## Requirements

- **Python 3.x**
- **Required Python libraries**:
  - `requests`
  - `msgpack-rpc-python`
  - `boto3`
  - `flask`


