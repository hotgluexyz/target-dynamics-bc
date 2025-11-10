from target_dynamics_v2.mappers.base_mappers import BaseMapper

class BillCommentSchemaMapper(BaseMapper):
    name = "Billlines"
    field_mappings = {
        "description": "description",
    }

    def __init__(
            self,
            record,
            sink,
            reference_data,
            existing_lines
    ) -> None:
        self.existing_lines = existing_lines
        super().__init__(record, sink, reference_data)

    def to_netsuite(self) -> dict:
        payload = {
            **self._map_internal_id(),
            "lineType": "Comment",
        }

        self._map_fields(payload)

        return payload

    def _find_existing_record(self, reference_list):
        """Finds an existing record in the reference data by matching internal.
        """
        for id, invoices in reference_list.items():
            for invoice in invoices:
                line_items = invoice.get("purchaseInvoiceLines", [])
                comments = [x for x in line_items if x.get("lineType") == "Comment"]
                matching_comment = next((comment for comment in comments if comment.get("description") == self.record.get("description")), None)
                if matching_comment:
                    return matching_comment
        return None
