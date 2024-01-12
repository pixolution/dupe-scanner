import requests
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from tqdm import tqdm
from pyvis.network import Network
import networkx as nx
from PIL import Image
import base64
from io import BytesIO
from .flow import FlowServer


class Scanner:

    def __init__(self, server: FlowServer, scan_mode="balanced", threshold=0.6, threads=2):
        self.flow = server
        self.G = None
        self.threshold = threshold
        self.threads = threads
        self.approx, self.filter = self.init_scan_mode(scan_mode)

    def init_scan_mode(self, scan_mode):
        num = self.flow.num_docs()
        if scan_mode == "balanced":
            if num <= 10_000:
                return (False, "off")
            if num <= 100_000:
                return (False, "low")
            if num <= 1_000_000:
                return (True, "medium")
            return (True, "high")
        elif scan_mode == "speed":
            if num <= 100_000:
                return (False, "high")
            return (True, "high")
        else:
            raise ValueError("scan_mode must be 'balanced' or 'speed'.")

    def ids(self, limit=sys.maxsize):
        ids = []
        cursor_mark = "*"
        done = False
        bulk = 1000
        count = 0
        while not done:
            rsp = requests.get(self.flow.build_query("select", f"rows={bulk}&fl=id&sort=id asc&cursorMark={cursor_mark}")).json()
            docs = rsp["response"]["docs"]
            ids.extend(list(map(lambda doc: doc["id"], docs)))
            count += bulk
            if cursor_mark == rsp["nextCursorMark"] or count >= limit:
                # no further docs
                done = True
            cursor_mark = rsp["nextCursorMark"]
        return ids if count <= limit else ids[:limit]

    def detect(self, id):
        rsp = requests.get(self.flow.build_query("duplicate", f"rank.by.id={id}&fl=id,score,filename,image&rows=10&rank.threshold={self.threshold}&rank.approximate={self.approx}&rank.smartfilter={self.filter}"))
        dups = rsp.json()["response"]["docs"]
        # Filter irrelevant results - necessary when scanning approximately
        dups = self.remove_irrelevant_matches(dups)
        if len(dups) > 1 :
            # Add all nodes
            for dup in dups:
                self.G.add_node(dup["id"], label=self.get_label(dup), title=dup["image"], shape="image", image=dup["image"], borderWidthSelected=2)
            # After all nodes are added, connect them with query node
            for dup in dups:
                # Only draw edges to other nodes
                if id != dup["id"]:
                    self.G.add_edge(id, dup["id"], width=dup["score"]*10, title=str(round(dup["score"], 2)), color=self.interpolate_color(dup["score"]))

    def get_label(self, dup):
        if "filename" in dup:
            # Only local files fill this field
            return dup["filename"]
        else:
            return dup["id"]

    def img_to_data_uri(self, filepath, size=250):
        source = filepath
        if filepath.startswith("http"):
            response = requests.get(filepath)
            # Check if the request was successful
            if response.status_code != 200:
                return filepath
            source = BytesIO(response.content)
        # Open the image from the bytes in the response or from local filepath
        img = Image.open(source)
        # scale to thumbnail size - HighRes images slow down IO tremendously
        img.thumbnail((size, size))
        # encode as in-memory png
        bytes = BytesIO()
        img.save(bytes, format='PNG')
        # Convert to data URI
        base64_utf8_str = base64.b64encode(bytes.getvalue()).decode('utf-8')
        ext     = filepath.split('.')[-1]
        datauri = f'data:image/{ext};base64,{base64_utf8_str}'
        return datauri

    def remove_irrelevant_matches(self, docs):
        new_list = []
        for doc in docs:
            if doc["score"] >= self.threshold:
                new_list.append(doc)
        return new_list

    def parallel(self, func, inputs, unit):
        if not inputs:
            print(f"No tasks given.")
            return
        pool = ThreadPoolExecutor(self.threads)
        errors=0
        futures = []
        for input in inputs:
            futures.append(pool.submit(func, input))
        try:
            # Await completion and display progress
            progress = tqdm(as_completed(futures), total=len(futures), unit=unit, colour="green", smoothing=0)
            for f in progress:
                if not f.exception() == None:
                    # Silently count errors
                    errors+=1
            if errors>0:
                print(f"{errors}/{len(futures)} tasks could not be processed.")
        except KeyboardInterrupt:
            print("Abort ...")
            exit()
        finally:
            self.close_threadpool(futures, pool)


    def close_threadpool(self, futures, pool):
        for future in futures:
            future.cancel()
        pool.shutdown(wait=True)


    def scan(self, max=sys.maxsize):
        # Build new graph
        self.G = Network(height="1000px", width="100%", cdn_resources="in_line", neighborhood_highlight=True)
        ids = self.ids(max)
        if not ids:
            print(f"Collection is empty.")
            return
        print(f"Process {len(ids)} images. Start scanning (this may take a while)...")
        self.parallel(func=self.detect, inputs=ids, unit="scans")


    def interpolate_color(self, score, color_map={1.0: '00FF00', 0.8: 'FFFF00', 0.6: 'FF0000'}):
        # Ensure score is within the bounds [0, 1]
        score = min(max(score, 0), 1)
        # Assuming color_map is sorted and has at least two colors
        # Format: {0: 'RRGGBB', 1: 'RRGGBB'}
        lower_bound = max(k for k in color_map if k <= score)
        upper_bound = min(k for k in color_map if k >= score)
        if lower_bound == upper_bound:
            return '#' + color_map[lower_bound]
        # Interpolate between the two bounding colors
        lower_color = tuple(int(color_map[lower_bound][i:i+2], 16) for i in (0, 2, 4))
        upper_color = tuple(int(color_map[upper_bound][i:i+2], 16) for i in (0, 2, 4))
        ratio = (score - lower_bound) / (upper_bound - lower_bound)
        interpolated_color = tuple(int(lc + ratio * (uc - lc)) for lc, uc in zip(lower_color, upper_color))
        return '#' + ''.join(f'{c:02x}' for c in interpolated_color)


    def embed_img(self, node):
        node["image"] = self.img_to_data_uri(node["image"], size=250)

    def save_html(self, filename="duplicates.html"):
        if self.G.num_nodes() == 0:
            print("No duplicates found. Finished.")
            exit()
        print(f"Detected {self.G.num_nodes()} duplicate images. Export results to {filename} ...")
        # Embed thumbnail versions via Base64 Data URIs to generate a standalone HTML file
        self.parallel(func=self.embed_img, inputs=self.G.nodes, unit="thumbnails generated")
        options = {
            "nodes": {
                "font": {
                    "size": 6
                }
            },
            "interaction": {
                "hover": True,
                "multiselect": False,
                "tooltipDelay": 75
            },
            "manipulation": {
                "enabled": False,
                "initiallyActive": False
            },
            "physics": {
                "barnesHut": {
                    "springLength": 150
                },
                "minVelocity": 0.75
            }
        }
        # Convert the options dictionary to a JSON string
        self.G.set_options(json.dumps(options))
        self.G.write_html(filename, local=True, open_browser=True)
        print(f"Initiating the graph visualization in your browser now...")


    def save_json(self, output="duplicates.json"):
        nx_graph = nx.Graph()
        # Convert to networkx graph
        for node in self.G.nodes:
            nx_graph.add_node(node['id'], filepath=node['title'])
        for edge in self.G.edges:
            nx_graph.add_edge(edge['from'], edge['to'])

        subgraphs = nx.connected_components(nx_graph)
        groups = {}
        group_id = 1

        for subgraph in subgraphs:
            file_paths = []
            for id in subgraph:
                file_paths.append(nx_graph.nodes[id]["filepath"])
            group_key = f"group-{group_id}"
            groups[group_key] = file_paths
            group_id += 1

        print(f"Export {group_id-1} duplicate groups to {output}...")
        # Write to JSON file
        with open(output, 'w') as json_file:
            json.dump(groups, json_file, indent=4)
