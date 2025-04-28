import hashlib
import json
from typing import Dict, List, Optional

from singer_sdk.plugin_base import PluginBase
from target_hotglue.client import HotglueBatchSink
from target_hotglue.common import HGJSONEncoder

from target_dynamics_v2.client import DynamicsClient

class DynamicsBaseBatchSink(HotglueBatchSink):
    max_size = 100

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        self.dynamics_client: DynamicsClient = self._target.dynamics_client

    def build_record_hash(self, record: dict):
        return hashlib.sha256(json.dumps(record, cls=HGJSONEncoder).encode()).hexdigest()

    def get_existing_state(self, hash: str):
        states = self.latest_state["bookmarks"][self.name]

        existing_state = next((s for s in states if hash==s.get("hash") and s.get("success")), None)

        if existing_state:
            self.latest_state["summary"][self.name]["existing"] += 1

        return existing_state

    def check_for_duplicated_records(self, records: List[dict]):
        filtered_records = []

        # filter out duplicated records from previous batches
        for record in records:
            hash = record["hash"]
            existing_state = self.get_existing_state(hash)

            if existing_state:
                continue
            filtered_records.append(record)

        # filter out duplicated records within the same batch
        seen_hashes = set()
        unique_records = []
        for record in filtered_records:
            if record["hash"] not in seen_hashes:
                seen_hashes.add(record["hash"])
                unique_records.append(record)
            else:
                self.logger.info(f"Duplicated record. Won't process it. Record: {record}")

        return unique_records

    def make_batch_request(self, records: List[dict]):
        requests_data = []

        for record in records:
            data = {
                "method": record["request_params"]["method"],
                "url": record["request_params"]["url"],
                "headers": {
                    **record["request_params"].get("headers", {})
                },
                "body": record["payload"]
            }
            requests_data.append(data)

        return self.dynamics_client.make_batch_request(requests_data)

    def handle_batch_response(self, responses: List[dict], records: List[dict]) -> dict:
        """
        This method should return a dict.
        It's recommended that you return a key named "state_updates".
        This key should be an array of all state updates.
        
        responses: a list of responses from the API
        records: a list of records used to make the request to the API
        
        responses and records have the same order, so we can relate a record to a response
        """
        state_updates = []

        for index, response in enumerate(responses):
            state = {}

            record = records[index]
            if hash := record.pop("hash", None):
                state["hash"] = hash

            if response["status"] in [200, 201]:
                state["success"] = True
                state["id"] = response.get("body", {}).get("id")

            
            if response["status"] == 200:
                state["is_updated"] = True

            if response["status"] >= 400:
                state["success"] = False
                state["record"] = record
                state["error"] = response.get("body", {}).get("error")
            state_updates.append(state)

        return {"state_updates": state_updates}
    
    def preprocess_batch(self, records: List[dict]):
        """
        Can be used to gather any additional data before processing the batch
        such as making a bulk request to get all existing records based on the
        given "records".
        """
        pass

    def hash_records(self, records: List[dict]):
        for record in records:
            record["hash"] = self.build_record_hash(record)

    def process_batch(self, context: dict) -> None:
        if not self.latest_state:
            self.init_state()

        raw_records = context["records"]

        self.preprocess_batch(raw_records)

        records = []
        for raw_record in raw_records:
            try:
                # performs record mapping from unified to Dynamics
                record = self.process_batch_record(raw_record)
                records.append(record)
            except Exception as e:
                state = {"error": str(e), "record": raw_record}
                if id := raw_record.get("id"):
                    state["id"] = id
                self.update_state(state)

        self.hash_records(records)
        records = self.check_for_duplicated_records(records)
        responses = self.make_batch_request(records)
        result = self.handle_batch_response(responses, records)

        for state in result.get("state_updates", list()):
            self.update_state(state)
