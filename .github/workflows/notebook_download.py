from urllib.parse import urlencode, unquote
from api import make_api_call
import os
import json
import base64
from IPython.display import HTML

url = unquote(f"https://v2.api.polly.elucidata.io/workspaces/{os.environ['POLLY_PROJECT_ID']}/notebooks/{os.environ['POLLY_NOTEBOOK_NAME']}.ipynb?include=jupyter_spec,versions&page[size]=10")
API_HEADERS = {
    'content-type': 'application/vnd.api+json',
    'cache-control': 'no-cache'
}
required_info = make_api_call("GET", url, {}, API_HEADERS)['included'][0]['attributes']['notebook_config']
final_url = f"https://polly.elucidata.io/manage/workspaces?action=open_polly_notebook&source=github&path=path_place_holder&kernel={required_info['kernel']}&machine={required_info['machine']}"
base_html = f"<a href=\"{final_url}\" target=\"_parent\"><img src=\"https://elucidatainc.github.io/PublicAssets/open_polly.svg\" alt=\"Open in Polly\"/></a>\n"
base_json = {
   "cell_type": "markdown",
   "metadata": {
    "colab_type": "text",
    "id": "view-in-github"
   },
   "source": [
       base_html
   ]
}
f = os.path.join("/import/", f"{os.environ['POLLY_NOTEBOOK_NAME']}.ipynb")
with open(f, encoding='utf-8') as f_p:
   data = json.load(f_p)
data["cells"].insert(0, base_json)
# ls
f1_file = os.path.join("/import/", f"{os.environ['POLLY_NOTEBOOK_NAME']}_github.ipynb")
f1 = open(f1_file, "w", encoding='utf-8')
data_write = json.dumps(json.loads(json.dumps(data)), indent=2)
f1.write(data_write)
f1.close()
