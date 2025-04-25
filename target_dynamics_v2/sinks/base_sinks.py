from typing import Dict, List, Optional

from singer_sdk.plugin_base import PluginBase
from target_hotglue.client import HotglueBatchSink

from target_dynamics_v2.client import DynamicsClient

class DynamicsBaseBatchSink(HotglueBatchSink):
    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        self.dynamics_client: DynamicsClient = self._target.dynamics_client

    def make_batch_request(self, records: List[dict]):
        headers = {"Prefer": "odata.continue-on-error"}
        
        request_data = {"requests": []}

        for record in records:
            rec_headers = record["request_params"].get("headers", {})
            data = {
                "method": record["request_params"]["method"],
                "url": record["request_params"]["url"],
                "headers": {
                    "Content-Type": "application/json",
                    "If-Match": "*",
                    **rec_headers
                },
                "body": record["payload"]
            }
            request_data["requests"].append(data)

        response = self.dynamics_client._make_request("$batch", "POST", data=request_data, headers=headers)
        return response.json()

    def handle_batch_response(self, response) -> dict:
        """
        This method should return a dict.
        It's recommended that you return a key named "state_updates".
        This key should be an array of all state updates
        """
        state_updates = []

        if responses := response.get("responses"):
            for response in responses:
                state = {}
                if response["status"] in [200, 201]:
                    state["success"] = True
                    state["id"] = response.get("body", {}).get("id")

                
                if response["status"] == 200:
                    state["is_updated"] = True

                if response["status"] >= 400:
                    state["success"] = False
                    state["error"] = response.get("body", {}).get("error")
                state_updates.append(state)

        return {"state_updates": state_updates}
