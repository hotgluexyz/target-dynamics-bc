import json
import requests

from target_dynamics_v2.mappers.base_mappers import BaseMapper
from target_hotglue.common import HGJSONEncoder
from typing import Dict, List, Optional
import singer


from target_dynamics_v2.auth import DynamicsAuth

LOGGER = singer.get_logger()

class DynamicsClient:
    ref_request_endpoints = {
        "Companies": "companies",
        "Currencies": "companies({companyId})/currencies",
        "PaymentMethods": "companies({companyId})/paymentMethods",
        "Customers": "companies({companyId})/customers",
        "Dimensions": "companies({companyId})/dimensions"
    }

    def __init__(self, target) -> None:
        self.config = target.config
        environment = self.config.get("environment_name")
        self.url = self.config.get("full_url", f"https://api.businesscentral.dynamics.com/v2.0/{environment}/api/v2.0/")
        self.auth = DynamicsAuth(target)

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

    def make_batch_request(self, requests_data: List[dict], transaction_type: str = "non_atomic"):
        """
        Performs a batch request against the API, any API endpoint can be used in batch requests.
        requests_data: list of requests, each containing a dict of url, method, headers and body
        transaction_type:
            'non_atomic' = the batch requests will continue to be processed even if one of them fail
            'atomic' = if one of the requests fails all the other requests will be rolled back. It's good
                        to be used when multiple requests are needed for one entity, for example updating
                        Customer and it's default dimensions
        """
        headers = {"Prefer": "odata.continue-on-error=true"}

        if transaction_type == "atomic":
            # Prefer: odata.continue-on-error=false makes the batch request stop processing requests if one of them fail
            # Isolation: snapshot makes the batch request atomic, if one of the requests fail the operation will be rolled back
            # it's good to be used when multiple requests are needed for one entity, for example updating Customer and it's default dimensions
            headers = {"Isolation": "snapshot", "Prefer": "odata.continue-on-error=false"}

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

    def get_entities(self, record_type: str, url_params: Optional[dict] = {}, ids: Optional[list] = [], external_ids: Optional[list] = [], expand: str = None):
        """"Uses batch request to get data because the url can be of any length, allowing for long filters"""
        endpoint = self.ref_request_endpoints[record_type].format(**url_params)
        entity_filters = []

        if expand:
                expand = f"$expand={expand}"
        
        if ids:
            entity_filters.append([f"id eq {id}" for id in ids])

        if external_ids:
            entity_filters.append([f"number eq '{external_id}'" for external_id in external_ids])

        if not entity_filters:
            entity_filters = [[]]

        requests_data = []

        # Dynamics doesn't support filtering on distinct fields, so we need to make one request for each filter type
        for entity_filter in entity_filters:
            if entity_filter:
                entity_filter = f"$filter={' or '.join(entity_filter)}"

            request_url = endpoint
            query_string = "&".join(filter(None, [expand, entity_filter]))
            if query_string:
                request_url += f"?{query_string}"

            requests_data.append({
                "url": request_url,
                "method": "GET",
            })

        batch_responses = self.make_batch_request(requests_data)
        
        entities = []

        for response in batch_responses:
            success, error_message = self._validate_batch_response(response)
            if not success:
                return success, error_message, []
            entities += response.get("body", {}).get("value", [])
        
        return True, None, entities

    def get_companies(self):
        _, _, companies = self.get_entities("Companies")

        for company in companies:
            url_params = {"companyId": company["id"]}

            _, _, currencies = self.get_entities("Currencies", url_params)
            company["currencies"] = currencies
            
            _, _, payment_methods = self.get_entities("PaymentMethods", url_params)
            company["paymentMethods"] = payment_methods

            _, _, dimensions = self.get_entities("Dimensions", url_params, expand="dimensionValues")
            company["dimensions"] = dimensions

        return True, None, companies
    
    def get_existing_entities_for_records(self, companies_reference_data: List[Dict], record_type: str, records: List[Dict]) -> List[Dict]:
        """Maps records to companies and returns a list of entities based on 'records'"""
        
        # we need to map the company to query the existing customers
        company_entities_mapping = {}

        for record in records:
            company = BaseMapper.get_company_from_record(companies_reference_data, record)
            if not company:
                continue

            if company["id"] not in company_entities_mapping.keys():
                company_entities_mapping[company["id"]] = {"ids": [], "external_ids": []}
            
            if rec_id := record.get("id"):
                company_entities_mapping[company["id"]]["ids"].append(rec_id)

            if rec_external_id := record.get("externalId"):
                company_entities_mapping[company["id"]]["external_ids"].append(rec_external_id)

        existing_company_entities = {}
        # make requests to get existing entities for each company from Dynamics
        for company_id in company_entities_mapping:
            url_params = { "companyId": company_id }
            _, _, entities = self.get_entities(
                record_type,
                url_params=url_params,
                ids=company_entities_mapping[company_id]["ids"],
                external_ids=company_entities_mapping[company_id]["external_ids"],
                expand="defaultDimensions"
            )
            if company_id not in existing_company_entities.keys():
                existing_company_entities[company_id] = []
            existing_company_entities[company_id] += entities

        return existing_company_entities
    
    @staticmethod
    def create_default_dimensions_requests(record_type: str, company_id: str, entity_id: str, default_dimensions: List[dict]):
        """
        If the Entity already exists we cannot create/update defaultDimensions for it.
        We need to send a separate request for it
        """
        requests = []

        for default_dimension in default_dimensions:
            endpoint = DynamicsClient.ref_request_endpoints[record_type] + "({entityId})/defaultDimensions"
            endpoint = endpoint.format(companyId=company_id, entityId=entity_id)
            request_params = {
                "url": endpoint,
                "method": "POST"
            }

            if default_dimension_id := default_dimension.pop("id", None):
                request_params = {
                    "url": f"{endpoint}({default_dimension_id})",
                    "method": "PATCH"
                }
            requests.append({"payload": default_dimension, "request_params": request_params})

        return requests
    
    @staticmethod
    def get_entity_upsert_request_params(record_type: str, company_id: str, entity_id: str = None):
        endpoint = DynamicsClient.ref_request_endpoints[record_type]
        endpoint = endpoint.format(companyId=company_id)
        request_params = {
            "url": endpoint,
            "method": "POST"
        }

        if entity_id:
            request_params = {
                "url": f"{endpoint}({entity_id})",
                "method": "PATCH"
            }
        
        return request_params
