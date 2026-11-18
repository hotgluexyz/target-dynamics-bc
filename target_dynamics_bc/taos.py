"""Taos draft-only invoice target; no Coupa credentials or writes."""

from decimal import Decimal

from target_dynamics_bc.client import DynamicsClient
from target_dynamics_bc.sinks.bill_sink import BillSink
from target_dynamics_bc.target import TargetDynamicsV2
from target_dynamics_bc.utils import extract_error_message


class DraftInvoiceSink(BillSink):
    def preprocess_batch(self, records):
        self.batch_invoice_keys = set()
        super().preprocess_batch(records)

    def update_state(self, state, is_duplicate=False, record=None, snapshot_field_values=None):
        # The BC batch sink does not forward its 'existing' flag to the SDK counter.
        super().update_state(
            state,
            is_duplicate=is_duplicate or state.get("existing", False),
            record=record,
            snapshot_field_values=snapshot_field_values,
        )

    def process_batch_record(self, record):
        if record.get("isDraft") is not True:
            raise ValueError("Taos demo only permits draft invoices")
        expected = Decimal(str(record["totalAmount"]))
        if not expected.is_finite() or expected < 0:
            raise ValueError("Expected total must be finite and nonnegative")
        mapped = super().process_batch_record(record)
        company = mapped["company_id"]
        vendor_id = mapped["payload"]["vendorId"]
        vendor = next(v for v in self.reference_data["Vendors"][company] if v["id"] == vendor_id)
        if vendor.get("blocked") not in (" ", "", "_x0020_", None, False):
            raise ValueError("Vendor is blocked")
        key = (company, vendor_id, mapped["payload"]["vendorInvoiceNumber"])
        if key in self.batch_invoice_keys:
            raise ValueError("Duplicate company/vendor/invoice number in batch")
        self.batch_invoice_keys.add(key)
        mapped["expected_total"] = expected
        mapped["expected_currency"] = next(
            field["value"] for field in record["customFields"] if field["name"] == "coupaCurrency"
        )
        return mapped

    def fetch_draft(self, company, invoice_id):
        ok, error, invoices = self.dynamics_client.get_entities(
            "purchaseInvoices",
            url_params={"companyId": company},
            filters={"id": [invoice_id]},
            expand="purchaseInvoiceLines",
        )
        if not ok or len(invoices) != 1:
            raise ValueError(error or "Unable to retrieve the invoice for reconciliation")
        return invoices[0]

    @staticmethod
    def reconcile(draft, payload, expected, currency):
        if draft["status"] != "Draft":
            raise ValueError("Existing invoice is not a draft; no modification allowed")
        if draft["currencyCode"] != currency:
            raise ValueError("Draft currency differs from Coupa; no overwrite allowed")
        for field in ("vendorId", "vendorInvoiceNumber", "invoiceDate", "postingDate", "dueDate"):
            if field in payload and draft.get(field) != payload[field]:
                raise ValueError(f"Draft header {field} differs; no overwrite allowed")
        if abs(Decimal(str(draft["totalAmountIncludingTax"])) - expected) > Decimal("0.05"):
            raise ValueError("BC total differs from Coupa by more than 0.05")
        actual = draft.get("purchaseInvoiceLines", [])
        desired = payload["purchaseInvoiceLines"]
        if len(actual) != len(desired):
            raise ValueError("Existing draft line count differs; no overwrite allowed")
        actual = sorted(actual, key=lambda line: line["sequence"])
        desired = sorted(desired, key=lambda line: int(line["sequence"]))
        for remote, wanted in zip(actual, desired):
            for field in ("accountId", "lineType", "description", "taxCode"):
                if field not in wanted:
                    continue
                if remote.get(field) != wanted.get(field):
                    raise ValueError(f"Draft line {field} differs; no overwrite allowed")
            for field in ("quantity", "unitCost"):
                if Decimal(str(remote[field])) != Decimal(str(wanted[field])):
                    raise ValueError(f"Draft line {field} differs; no overwrite allowed")

    def upsert_record(self, record):
        company = record["company_id"]
        payload = record["payload"]
        invoice_id = payload.get("id")
        expected = record["expected_total"]
        currency = record["expected_currency"]
        if invoice_id:
            draft = self.fetch_draft(company, invoice_id)
            self.reconcile(draft, payload, expected, currency)
            return invoice_id, True, {"existing": True, "coupaExport": "pending_read_only"}

        # This demo deliberately does not support dimension writes yet.
        if payload.get("dimensionSetLines") or any(
            line.get("dimensionSetLines") for line in payload.get("purchaseInvoiceLines", [])
        ):
            raise ValueError("Dimension mappings require a separate implementation")
        if not payload.get("purchaseInvoiceLines"):
            raise ValueError("A draft must have at least one line")
        header = {
            k: v
            for k, v in payload.items()
            if k
            not in (
                "id",
                "purchaseInvoiceLines",
                "dimensionSetLines",
            )
        }
        try:
            request = DynamicsClient.get_entity_upsert_request_params("purchaseInvoices", company)
            response = self.dynamics_client.make_batch_request([{**request, "body": header}])[0]
            if response.get("status") != 201:
                raise ValueError(f"BC header rejected: {extract_error_message(response)}")
            invoice_id = response["body"]["id"]
            for line in payload["purchaseInvoiceLines"]:
                body = {k: v for k, v in line.items() if k not in ("id", "dimensionSetLines")}
                request = DynamicsClient.get_entity_upsert_request_params(
                    "purchaseInvoiceLines",
                    company,
                    url_params={"parentId": invoice_id},
                )
                response = self.dynamics_client.make_batch_request([{**request, "body": body}])[0]
                if response.get("status") != 201:
                    raise ValueError(f"BC line rejected: {extract_error_message(response)}")
            self.reconcile(self.fetch_draft(company, invoice_id), payload, expected, currency)
            return invoice_id, True, {"coupaExport": "pending_read_only"}
        except Exception as error:
            state = {"error": str(error), "coupaExport": "not_exported"}
            if invoice_id:
                try:
                    response = self.dynamics_client.make_batch_request(
                        [
                            {
                                "method": "DELETE",
                                "url": f"companies({company})/purchaseInvoices({invoice_id})",
                                "headers": {"If-Match": "*"},
                            }
                        ]
                    )[0]
                except Exception:
                    return (
                        invoice_id,
                        False,
                        {
                            **state,
                            "rollback": "unknown_requires_manual_reconciliation",
                        },
                    )
                if response.get("status") == 204:
                    state["rollback"] = "deleted_demo_draft"
                    return None, False, state
                state["rollback"] = "failed_requires_manual_reconciliation"
            else:
                state["rollback"] = "no_confirmed_id_check_remote_before_retry"
            return invoice_id, False, state


class TaosTarget(TargetDynamicsV2):
    SINK_TYPES = [DraftInvoiceSink]


if __name__ == "__main__":
    TaosTarget.cli()
