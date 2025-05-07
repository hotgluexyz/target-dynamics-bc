from typing import List

from target_dynamics_v2.client import DynamicsClient
from target_dynamics_v2.mappers.bill_schema_mapper import BillSchemaMapper
from target_dynamics_v2.sinks.base_sinks import DynamicsBaseBatchSink

class BillSink(DynamicsBaseBatchSink):
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
        mapped_record = BillSchemaMapper(record, self, self.reference_data)
        payload = mapped_record.to_dynamics()

        record_id = payload.pop("id", None)
        request_params = DynamicsClient.get_entity_upsert_request_params(self.record_type, mapped_record.company["id"], record_id)

        dimensions_set_lines_requests = []
        if mapped_record.existing_record and payload.get("dimensionSetLines"):
            dimension_set_lines = payload.pop("dimensionSetLines")
            dimensions_set_lines_requests = DynamicsClient.create_dimension_set_lines_requests(
                self.record_type,
                mapped_record.company["id"],
                record_id,
                dimension_set_lines
            )

        lines_requests = []
        if mapped_record.existing_record and payload.get("purchaseInvoiceLines"):
            item_lines = payload.pop("purchaseInvoiceLines")
            lines_requests = DynamicsClient.create_line_items_requests(
                self.record_type,
                mapped_record.company["id"],
                record_id,
                item_lines
            )


        records = [{"payload": payload, "request_params": request_params }]
        records += dimensions_set_lines_requests
        records += lines_requests

        return {"records": records}
