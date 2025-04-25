import json
import requests

from target_hotglue.client import HotglueSink
from target_hotglue.common import HGJSONEncoder
from target_dynamics_v2.utils import Company
from singer_sdk.plugin_base import PluginBase
from typing import Dict, List, Optional
import singer


from target_dynamics_v2.auth import DynamicsAuth

LOGGER = singer.get_logger()

class DynamicsClient:
    ref_request_endpoints = {
        "companies": "companies",
        "currencies": "companies({companyId})/currencies",
        "paymentMethods": "companies({companyId})/paymentMethods"
    }

    def __init__(self, config) -> None:
        self.config = config
        environment = self.config.get("environment_name")
        self.url = self.config.get("full_url", f"https://api.businesscentral.dynamics.com/v2.0/{environment}/api/v2.0/")
        self.auth = DynamicsAuth(dict(self.config))

    def get_auth(self):
        r = requests.Session()
        return self.auth(r)
    
    def _make_request(self, endpoint, method, data=None, params=None, headers=None):
        request_headers = {"Content-Type": "application/json"}
        if headers:
            request_headers.update(headers)

        url = self.url + endpoint
        request_params = params or {}

        request = self.get_auth()
        request.headers.update(request_headers)
        request.request(method=method, url=url, params=params)

        json_data = json.dumps(data, cls=HGJSONEncoder) if data else None

        return request.request(
            method=method,
            url=url,
            params=request_params,
            data=json_data,
            verify=True
        )
    
    def _validate_response(self, response: requests.Response) -> tuple[bool, str | None]:
        if response.status_code >= 400:
            msg = response.get("error")
            return False, msg
        else:
            return True, None
        
    def _validate_batch_response(self, response: dict) -> tuple[bool, str | None]:
        if response["status"] >= 400:
            msg = response.get("body", {}).get("error")
            return False, msg
        else:
            return True, None

    def make_batch_request(self, requests_data: List[dict]):
        """
        Performs a batch request against the API, any API endpoint can be used in batch requests.
        requests_data: list of requests, each containing a dict of url, method, headers and body
        """
        headers = {"Prefer": "odata.continue-on-error"}
        request_data = {"requests": []}

        for request in requests_data:
            req_headers = request.get("headers", {})
            data = {
                "method": request["method"],
                "url": request["url"],
                "headers": {
                    "Content-Type": "application/json",
                    "If-Match": "*",
                    **req_headers
                },
                "body": request.get("body", {})
            }
            request_data["requests"].append(data)

        response = self._make_request("$batch", "POST", data=request_data, headers=headers)
        responses = response.json().get("responses", [])
        return responses

    def get_reference_data(self, record_type: str, url_params: Optional[dict] = {}, ids: Optional[list] = []):
        endpoint = self.ref_request_endpoints[record_type].format(**url_params)
        filters = []

        if ids:
            filters += [f"id eq {id}" for id in ids]

        if filters:
            endpoint += f"?$filter={' or '.join(filters)}"

        requests_data = [{
            "url": endpoint,
            "method": "GET",
        }]

        responses = self.make_batch_request(requests_data)
        response = responses[0]

        success, error_message = self._validate_batch_response(response)
        if not success:
            return success, error_message, []
        
        return True, None, response.get("body", {}).get("value", [])

    def get_companies(self):
        _, _, companies = self.get_reference_data("companies")

        for company in companies:
            url_params = {"companyId": company["id"]}

            _, _, currencies = self.get_reference_data("currencies", url_params)
            company["currencies"] = currencies
            
            _, _, payment_methods = self.get_reference_data("paymentMethods", url_params)
            company["paymentMethods"] = payment_methods

        return True, None, companies
