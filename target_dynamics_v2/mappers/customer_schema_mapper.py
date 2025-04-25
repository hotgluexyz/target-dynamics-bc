from target_dynamics_v2.mappers.base_mappers import BaseMapper

class CustomerSchemaMapper(BaseMapper):
    field_mappings = {
        "fullName": "displayName",
        "email": "email",
        "website": "website",
        "taxable": "taxLiable"
    }

    def to_dynamics(self) -> dict:
        
        # TODO: move this to the sink
        request_params = {
            "url": "customers",
            "method": "POST",
            "headers": {
                "Company": self.company["name"],
            }
        }

        payload = {
            **self._map_internal_id(),
            **self._map_payment_method(),
            **self._map_phone_number(),
            **self._map_address(),
            **self._map_currency(),
            "blocked": " ",
            "type": "Person" if self.record.get("isPerson") else "Company",
        }

        # TODO:
        # map parentId / parentName (dimensions)
        # map categoryId / categoryName (dimensions)

        self._map_fields(payload)
        return {"payload": payload, "request_params": request_params }
    
    def _map_payment_method(self):
        if self.company is None:
            return {}
        
        found = None
        if payment_method := self.record.get("paymentMethod"):
            found = next(
                (item for item in self.company.get("paymentMethods", []) if item["id"] == payment_method or item["code"] == payment_method or item["displayName"] == payment_method),
                None
            )

            if found:
                return {"paymentMethodId": found["id"]}

        return {}