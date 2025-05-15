import json
from typing import Dict, List

from target_dynamics_v2.client import DynamicsClient
from target_dynamics_v2.mappers.bill_schema_mapper import BillSchemaMapper
from target_dynamics_v2.sinks.base_sinks import DynamicsBaseBatchSinkSingleUpsert
from target_hotglue.common import HGJSONEncoder

class BillSink(DynamicsBaseBatchSinkSingleUpsert):
    name = "Bills"
    record_type = "purchaseInvoices"

    def preprocess_batch(self, records: List[dict]):
        # fetch reference data related to existing customers
        # get bills for company, filter by id, number, vendorInvoiceNumber
        bill_filter_mappings = [
            {"field_from": "id", "field_to": "id", "should_quote": False},
            {"field_from": "transactionNumber", "field_to": "number", "should_quote": True},
            {"field_from": "externalId", "field_to": "vendorInvoiceNumber", "should_quote": True},
        ]
        existing_company_bills = self.dynamics_client.get_existing_entities_for_records(
            self._target.reference_data.get("companies", []),
            self.record_type,
            records,
            bill_filter_mappings,
            expand="dimensionSetLines, purchaseInvoiceLines($expand=dimensionSetLines)"
        )

        # get vendors for company, filter by id, number, displayName
        vendor_filter_mappings = [
            {"field_from": "vendorId", "field_to": "id", "should_quote": False},
            {"field_from": "vendorExternalId", "field_to": "number", "should_quote": True},
            {"field_from": "vendorName", "field_to": "displayName", "should_quote": True},
        ]
        existing_company_vendors = self.dynamics_client.get_existing_entities_for_records(
            self._target.reference_data.get("companies", []),
            "Vendors",
            records,
            vendor_filter_mappings
        )

        # get items
        items_set = set()
        # remove duplicated items across all records
        for record in records:
            items_set.update((line_item.get("itemId"), line_item.get("itemExternalId"), line_item.get("itemName"), record.get("subsidiaryId"), record.get("subsidiaryName")) for line_item in record.get("lineItems", []))
        # make a list of unique items
        items = [{"itemId": item[0], "itemExternalId": item[1], "itemName": item[2], "subsidiaryId": item[3], "subsidiaryName": item[4]} for item in items_set]
        sorted_items = sorted(items, key=lambda item: (item.get("itemId"), item.get("itemExternalId"), item.get("itemName"), item.get("subsidiaryId"), item.get("subsidiaryName")))
        item_filter_mappings = [
            {"field_from": "itemId", "field_to": "id", "should_quote": False},
            {"field_from": "itemExternalId", "field_to": "number", "should_quote": True},
            {"field_from": "itemName", "field_to": "displayName", "should_quote": True},
        ]
        existing_company_items = self.dynamics_client.get_existing_entities_for_records(
            self._target.reference_data.get("companies", []),
            "Items",
            sorted_items,
            item_filter_mappings
        ) if items else []

        self.reference_data = {**self._target.reference_data, self.name: existing_company_bills, "Vendors": existing_company_vendors, "Items": existing_company_items}

    def process_batch_record(self, record: dict) -> dict:
        # perform the mapping
        return BillSchemaMapper(record, self, self.reference_data).to_dynamics()

    def upsert_record(self, record: Dict) -> tuple[str, bool, Dict]:
        state = {}
        payload = record["payload"]
        
        company_id = record["company_id"]
        bill_id = payload.pop("id", None)
        is_update = bill_id is not None
        bill_dimensions = payload.pop("dimensionSetLines", [])
        bill_lines = payload.pop("purchaseInvoiceLines", [])

        # create/update bill
        request_params = DynamicsClient.get_entity_upsert_request_params(self.record_type, company_id, bill_id)
        bill_upsert_request_data = [{ **request_params, "body": payload }]
        bill_upsert_response = self.dynamics_client.make_batch_request(bill_upsert_request_data)[0]

        if bill_upsert_response.get("status") not in [200, 201]:
            state["error"] = bill_upsert_response.get("body", {}).get("error")
            state["record"] = json.dumps(record, cls=HGJSONEncoder, sort_keys=True)
            return bill_id, False, state
        
        bill_id = bill_upsert_response["body"]["id"]

        # we have to re-fetch the bill otherwise we don't get the inherited dimensionSetLines from the Vendor
        _, _, bills = self.dynamics_client.get_entities(self.record_type, url_params={"companyId": company_id}, filters={"id": [bill_id]}, expand="dimensionSetLines")
        upserted_bill = bills[0]

        # create/update bill dimensions
        if bill_dimensions:
            existing_bill_dimensions = upserted_bill.get("dimensionSetLines", [])
            bill_dimensions_requests = DynamicsClient.create_dimension_set_lines_requests("purchaseInvoicesDimensionSetLines", company_id, bill_id, bill_dimensions, existing_bill_dimensions)
            bill_dimensions_upsert_responses = self.dynamics_client.make_batch_request(bill_dimensions_requests)

            for bill_dimensions_upsert_response in bill_dimensions_upsert_responses:
                if bill_dimensions_upsert_response["status"] not in [200, 201]:
                    state["error"] = bill_dimensions_upsert_response.get("body", {}).get("error")
                    state["record"] = json.dumps(record, cls=HGJSONEncoder, sort_keys=True)
                    return bill_id, False, state
        
        # if there is no bill lines to upsert we are done. Success!
        if not bill_lines:
            if is_update:
                state["is_updated"] = True

            return bill_id, True, state

        # create/update lines       
        bill_lines_dimensions = []
        bill_lines_upsert_request_data = []
        url_params = {"parentId": bill_id}
        for index, bill_line in enumerate(bill_lines):
            bill_line_id = bill_line.pop("id", None)
            request_id = bill_line_id or f"temp_{index}"
            if bill_line_dimensions := bill_line.pop("dimensionSetLines", []):
                bill_lines_dimensions.append({
                    "request_id": request_id,
                    "dimension_set_lines": bill_line_dimensions
                })

            request_params = DynamicsClient.get_entity_upsert_request_params("purchaseInvoiceLines", company_id, entity_id=bill_line_id, url_params=url_params, request_id=request_id)
            bill_lines_upsert_request_data.append({ **request_params, "body": bill_line })
            

        bill_lines_upsert_responses = self.dynamics_client.make_batch_request(bill_lines_upsert_request_data) if bill_lines_upsert_request_data else []
        for bill_lines_upsert_response in bill_lines_upsert_responses:
            if bill_lines_upsert_response.get("status") not in [200, 201]:
                state["error"] = bill_lines_upsert_response.get("body", {}).get("error")
                state["record"] = json.dumps(record, cls=HGJSONEncoder, sort_keys=True)
                return bill_id, False, state

        # if there is no bill lines dimensions to upsert we are done. Success!
        if not bill_lines_dimensions:
            if is_update:
                state["is_updated"] = True

            return bill_id, True, state

        # we have to re-fetch the bill otherwise we don't get the inherited dimensionSetLines from the Vendor
        _, _, bills = self.dynamics_client.get_entities(self.record_type, url_params={"companyId": company_id}, filters={"id": [bill_id]}, expand="dimensionSetLines, purchaseInvoiceLines($expand=dimensionSetLines)")
        upserted_bill = bills[0]

        # create/update lines dimensions
        bill_lines_dimensions_requests = []
        for bill_line_dimensions in bill_lines_dimensions:
            request_id = bill_line_dimensions["request_id"]
            bill_line_dimension_set_lines = bill_line_dimensions["dimension_set_lines"]

            if not bill_line_dimension_set_lines:
                continue

            bill_line_id = next(bill_line_upsert_response["body"]["id"] for bill_line_upsert_response in bill_lines_upsert_responses if bill_line_upsert_response["id"] == request_id)

            upserted_bill_line = next((bill_line for bill_line in upserted_bill["purchaseInvoiceLines"] if bill_line["id"] == bill_line_id), None)
            existing_bill_line_dimensions = upserted_bill_line.get("dimensionSetLines", [])

            bill_lines_dimensions_requests += DynamicsClient.create_dimension_set_lines_requests("purchaseInvoiceLinesDimensionSetLines", company_id, bill_line_id, bill_line_dimension_set_lines, existing_bill_line_dimensions)
        
        bill_lines_dimensions_upsert_responses = self.dynamics_client.make_batch_request(bill_lines_dimensions_requests)

        for bill_lines_dimensions_upsert_response in bill_lines_dimensions_upsert_responses:
            if bill_lines_dimensions_upsert_response["status"] not in [200, 201]:
                state["error"] = bill_lines_dimensions_upsert_response.get("body", {}).get("error")
                state["record"] = json.dumps(record, cls=HGJSONEncoder, sort_keys=True)
                return bill_id, False, state

        if is_update:
            state["is_updated"] = True

        return bill_id, True, state
