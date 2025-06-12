from typing import Dict, List

from target_dynamics_bc.client import DynamicsClient
from target_dynamics_bc.mappers.bill_payment_schema_mapper import BillPaymentSchemaMapper
from target_dynamics_bc.sinks.base_sinks import DynamicsBaseBatchSinkSingleUpsert


class BillPaymentSink(DynamicsBaseBatchSinkSingleUpsert):
    name = "BillPayments"
    record_type = "vendorPayments"

    def preprocess_batch(self, records: List[dict]):
        # get vendor payment journals for company, filter by id and code
        vendor_payment_journal_filter_mappings = [
            {"field_from": "journalId", "field_to": "id", "should_quote": False},
            {"field_from": "journalExternalId", "field_to": "code", "should_quote": True}
        ]
        existing_company_vendor_payment_journals = self.dynamics_client.get_existing_entities_for_records(
            self._target.reference_data.get("companies", []),
            "vendorPaymentJournals",
            records,
            vendor_payment_journal_filter_mappings
        )

        # get bills for company, filter by id, documentNumber
        bill_payments_filter_mappings = [
            {"field_from": "id", "field_to": "id", "should_quote": False},
            {"field_from": "paymentNumber", "field_to": "documentNumber", "should_quote": True},
        ]
        existing_company_bill_payments = self.dynamics_client.get_existing_bill_payments_for_records(
            self._target.reference_data.get("companies", []),
            existing_company_vendor_payment_journals,
            records,
            bill_payments_filter_mappings
        )

        # get bills for company, filter by id, vendorInvoiceNumber
        bill_filter_mappings = [
            {"field_from": "billId", "field_to": "id", "should_quote": False},
            {"field_from": "billNumber", "field_to": "vendorInvoiceNumber", "should_quote": True},
        ]
        existing_company_bills = self.dynamics_client.get_existing_entities_for_records(
            self._target.reference_data.get("companies", []),
            "purchaseInvoices",
            records,
            bill_filter_mappings
        )

        # get vendors for company, filter by id, number, displayName
        vendor_filter_mappings = [
            {"field_from": "vendorId", "field_to": "id", "should_quote": False},
            {"field_from": "vendorNumber", "field_to": "number", "should_quote": True},
            {"field_from": "vendorName", "field_to": "displayName", "should_quote": True},
        ]
        existing_company_vendors = self.dynamics_client.get_existing_entities_for_records(
            self._target.reference_data.get("companies", []),
            "Vendors",
            records,
            vendor_filter_mappings
        )

        self.reference_data = {**self._target.reference_data, self.name: existing_company_bill_payments, "Bills": existing_company_bills, "Vendors": existing_company_vendors, "VendorPaymentJournals": existing_company_vendor_payment_journals}

    def process_batch_record(self, record: dict) -> dict:
        # perform the mapping
        return BillPaymentSchemaMapper(record, self, self.reference_data).to_dynamics()

    def upsert_record(self, record: Dict) -> tuple[str, bool, Dict]:
        state = {}
        payload = record["payload"]
        
        company_id = record["company_id"]
        bill_payment_id = payload.pop("id", None)
        journal_id = payload.pop("journalId")
        is_update = bill_payment_id is not None
        bill_payment_dimensions = payload.pop("dimensionSetLines", [])

        # create/update bill payment
        url_params = { "parentId": journal_id }
        request_params = DynamicsClient.get_entity_upsert_request_params(self.record_type, company_id, bill_payment_id, url_params=url_params)
        bill_payment_upsert_request_data = [{ **request_params, "body": payload }]
        bill_payment_upsert_response = self.dynamics_client.make_batch_request(bill_payment_upsert_request_data)[0]

        if bill_payment_upsert_response.get("status") not in [200, 201]:
            state["error"] = bill_payment_upsert_response.get("body", {}).get("error")
            return bill_payment_id, False, state
        
        bill_payment_id = bill_payment_upsert_response["body"]["id"]

        # create/update bill dimensions
        if bill_payment_dimensions:
            # we have to re-fetch the bill payment otherwise we don't get the inherited dimensionSetLines from the Vendor
            _, _, bill_payments = self.dynamics_client.get_entities(self.record_type, url_params={"companyId": company_id, "parentId": journal_id}, filters={"id": [bill_payment_id]}, expand="dimensionSetLines")
            upserted_bill_payment = bill_payments[0]

            existing_dimensions = upserted_bill_payment.get("dimensionSetLines", [])
            bill_payment_dimensions_requests = DynamicsClient.create_dimension_set_lines_requests("vendorPaymentsDimensionSetLines", company_id, bill_payment_id, bill_payment_dimensions, existing_dimensions, parentId=journal_id)
            bill_payment_dimensions_upsert_responses = self.dynamics_client.make_batch_request(bill_payment_dimensions_requests)

            for bill_payment_dimensions_upsert_response in bill_payment_dimensions_upsert_responses:
                if bill_payment_dimensions_upsert_response["status"] not in [200, 201]:
                    state["error"] = bill_payment_dimensions_upsert_response.get("body", {}).get("error")
                    return bill_payment_id, False, state

        if is_update:
            state["is_updated"] = True

        return bill_payment_id, True, state
