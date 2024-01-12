import os
import requests
import json
import sys
import base64
from io import BytesIO
from PIL import Image
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from urllib.parse import urlparse
from .flow import FlowServer






class Indexer:

    def __init__(self, server: FlowServer):
        self.flow = server
        # For more docs license the Professional Plan https://pixolution.io/pricing
        self.MAX_DOCS = 5000


    def index_local_images(self, root_dir: str, recursive=True, threads=2):
        if not self.flow.ping():
            print("Flow backend not reachable. Ensure Flow is up and running.")
            exit()
        else:
            abs_path = os.path.abspath(root_dir)
            files = self.scan_folder(abs_path, recursive)
            self.index(files, threads)

    def generate_id(self):
        # Each hex character is represented by 4 bits. Use multiple of 4.
        return '%016x' % random.getrandbits(64)

    def scan_folder(self, rootDir, recursive=True):
        count=0
        supported_formats = tuple(Image.registered_extensions().keys())
        image_list = []
        if (len(rootDir)>1):
            rootDir = rootDir.rstrip(os.sep)
        for root, dirs, files in os.walk(rootDir, topdown=True, followlinks=False):
            if not recursive and rootDir != root:
                # Abort as soon as we have a different folder
                break

            for fname in files:
                if not fname.lower().endswith(supported_formats):
                    continue
                if(count>=self.MAX_DOCS):
                    print(f"Doc limit of {self.MAX_DOCS} reached. Quit scanning.")
                    return image_list
                count+=1
                image_list.append(root + os.sep + fname)
        return image_list

    def get_parent_folder_name(self, filepath):
        # Normalize the path to the OS-specific format
        filepath = os.path.normpath(filepath)
        # Get the directory name
        directory = os.path.dirname(filepath)
        # Split the directory path and get the last part (folder name)
        folder_name = os.path.basename(directory)
        return folder_name

    def index(self, file_paths, threads: int = 2):
        # Parallelize client to speed up IO-bound tasks (file access, API calls)
        # Server also parallelizes to speed up CPU-bound tasks (image analysis)
        pool = ThreadPoolExecutor(threads)
        futures = []
        for path in file_paths:
            doc = {
                "id": self.generate_id(),
                "image": path,
                "filename": os.path.basename(path)
            }
            futures.append(pool.submit(self.add_doc, doc))
        if not futures:
            print(f"No images to index.")
            return
        print(f"Start indexing {len(futures)} images (this may take a while)...")
        errors=0
        try:
            # Await completion and display progress
            progress = tqdm(as_completed(futures), total=len(futures), unit="images", colour="green", smoothing=0)
            for f in progress:
                if not f.exception() == None:
                    # ONly output the first x exceptions
                    errors+=1
                    if "document limit" in str(f.exception()).lower():
                        # Reached document limit of Free Plan. Stop further indexing.
                        progress.close()
                        close_threadpool(futures, pool)
                        print(f.exception())
                        return
            if errors>0:
                print(f"{errors}/{len(futures)} images could not be indexed.")
        except KeyboardInterrupt:
            print("Abort indexing...")
            exit()
        finally:
            self.close_threadpool(futures, pool)
            # Commit updated index to make it visible
            self.flow.commit()


    def close_threadpool(self, futures, pool):
        for future in futures:
            future.cancel()
        pool.shutdown(wait=True)

    def add_doc(self, doc):
        doc["import"] = self.flow.analyze(doc["image"])
        response = requests.post(self.flow.url+"/update", json=doc)
        if not (response.json()['responseHeader']['status'] == 0):
            raise Exception(response.json()['error']['msg'])
