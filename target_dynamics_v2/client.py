from target_hotglue.client import HotglueSink
import requests
from singer_sdk.plugin_base import PluginBase
from typing import Dict, List, Optional
import singer
from target_dynamics_v2.auth import DynamicsAuth
LOGGER = singer.get_logger()

class DynamicsSink(HotglueSink):
    """Dynamics target sink class."""
    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        # Save config for refresh_token saving
        self.config_file = target.config
        self.target_name = "dynamics"
        if self.config.get("full_url"):
            base_url = self.config["full_url"]
        else:
            base_url = "https://{}.crm.dynamics.com".format(self.config["org"])
        
        if base_url.endswith("/"):
            base_url = base_url[:-1]
        
        self.url = f"{base_url}/api/data/v9.2/"
        self.entityid = None

    def get_auth(self):
        auth = DynamicsAuth(dict(self.config))
        r = requests.Session()
        auth = auth(r)
        return auth

    def log_request_response(self, record, response):
        self.logger.info(f"Sending payload for stream {self.name}: {record}")
        self.logger.info(f"Response: {response.text}")

    def upsert_record(self, record: dict, context: dict):
        method = "POST"
        state_dict = dict()
        id = None
        endpoint = self.endpoint
        auth = self.get_auth()
        url =  self.url
        url = f"{url}{endpoint}"
        auth.headers.update({"OData-MaxVersion":"4.0","OData-Version":"4.0","Accept":"application/json","Content-Type":"application/json","charset":"utf-8","Prefer":"return=representation"})
        response = auth.post(url,json=record)
        self.validate_response(response)
        self.log_request_response(record, response)
        if response.status_code in [200, 201]:
            state_dict["success"] = True
            id = response.json().get(self.entityid)
        #Updating records doesn't seem to work
        elif response.status_code == 204 and method == "PUT":
            state_dict["is_updated"] = True
        return id, response.ok, state_dict        