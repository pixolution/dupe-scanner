import requests
import base64
from io import BytesIO
from PIL import Image

class FlowServer:

    def __init__(self, host = "http://localhost:8983", context_path ="api/cores", collection="my-collection"):
        s = "/"
        self.host = host.strip(s)
        self.context_path = context_path.strip(s)
        self.collection = collection.strip(s)
        # Build complete url
        self.url = self.host + s + self.context_path + s + self.collection

    def ping(self):
        try:
            response = requests.get(self.url)
            return response.status_code == 200
        except:
            return False

    def status(self):
        if self.ping():
            print(f"Flow is up and runnning at {self.host}. {self.num_docs()} images in {self.collection}.")
        else:
            print(f"Flow not reachable at {self.url}.")

    def build_query(self, endpoint, params):
        return f"{self.url}/{endpoint}?{params}"

    def num_docs(self):
        rsp = requests.get(self.url +"/select?rows=0&q*:*")
        return rsp.json()["response"]["numFound"]

    def commit(self):
        rsp = requests.post(f"{self.url}/update?softCommit=true&openSearcher=true&waitSearcher=true")

    def analyze(self, image_path, apply_modules="duplicate"):
        if image_path.startswith("http"):
            # Got web image - reference via url
            response = requests.get(f"{self.url}/analyze?modules.apply={apply_modules}&input.url={image_path}")
            return response.json()['outputs']
        else:
            # Got local image - upload
            # scale to thumbnail size - HighRes images slow down IO tremendously
            base64_image = self.img_to_data_uri(image_path, size=350)
            payload = {'input.data': base64_image}
            # set empty logParamsList= to avoid logging huge log messages when uploading base64 images as parameters
            response = requests.post(f"{self.url}/analyze?modules.apply={apply_modules}&logParamsList=", data=payload)
            return response.json()['outputs']

    def clear_collection(self):
        payload = {"delete":{"query":"*:*" }}
        response = requests.post(self.host+"/solr/"+self.collection+"/update?commit=true&openSearcher=true", json=payload)
        if not (response.json()['responseHeader']['status'] == 0):
            raise Exception(response.json()['error']['msg'])

    def img_to_data_uri(self, filepath, size=128):
         # Got local image - upload
        # scale to thumbnail size - HighRes images slow down IO tremendously
        img = Image.open(filepath)
        img.thumbnail((size, size))
        # encode as in-memory png
        bytes = BytesIO()
        img.save(bytes, format='PNG')
        # Convert to data URI
        base64_utf8_str = base64.b64encode(bytes.getvalue()).decode('utf-8')
        ext     = filepath.split('.')[-1]
        datauri = f'data:image/{ext};base64,{base64_utf8_str}'
        return datauri
