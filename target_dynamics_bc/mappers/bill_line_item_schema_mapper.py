from target_dynamics_bc.mappers.base_mappers import BaseMapper

class BillLineItemSchemaMapper(BaseMapper):
    name = "BillLines"
    field_mappings = {
        "externalId": "sequence",
        "description": "description",
        "taxCode": "taxCode",
        "discount": "discountAmount",
        "quantity": "quantity",
        "unitPrice": "unitCost",
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
            **self._map_item(),
            **self._map_location(),
            **self._map_dimension_set_lines(),
            "lineType": "Item"
        }

        self._map_fields(payload)

        return payload

    def _find_existing_record(self, reference_list):
        """Finds an existing record in the reference data by matching internal.
        """
        if self.company is None:
            return None
        
        found_record = None

        record_external_id = self.record.get("externalId")
        if record_external_id:
            found_record = next(
                (line for line in self.existing_lines
                if str(line["sequence"]) == record_external_id),
                None
            )

        record_item = self._map_item()
        record_item_id = record_item.get("itemId")
        record_description = self.record.get("description")
        if record_item_id and record_description:
            found_record = next(
                (line for line in self.existing_lines
                if line["description"] == record_description and line["itemId"] == record_item_id),
                None
            )

        if found_record:
            return found_record
 
    def _map_item(self):
        found_item = None

        items_reference_data = self.reference_data.get("Items", {}).get(self.company["id"], [])

        item_id = self.record.get("itemId")
        if item_id:
            found_item = next(
                (item for item in items_reference_data
                if item["id"] == item_id),
                None
            )

        item_external_id = self.record.get("itemNumber")
        if item_external_id and not found_item:
            found_item = next(
                (item for item in items_reference_data
                if item["number"] == item_external_id),
                None
            )

        item_name = self.record.get("itemExternalName")
        if item_name and not found_item:
            found_item = next(
                (item for item in items_reference_data
                if item["displayName"] == item_name),
                None
            )
        
        item_info = {}

        if found_item:
            item_info = {"itemId": found_item["id"]}

        return item_info
