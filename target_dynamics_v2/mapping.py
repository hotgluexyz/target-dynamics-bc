from cgitb import lookup
import os
import json

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

class UnifiedMapping():
    def __init__(self) -> None:
        pass
    
    def read_json_file(self,filename):
        # read file
        with open(os.path.join(__location__, f"{filename}"), 'r') as filetoread:
            data = filetoread.read()

        # parse file
        content = json.loads(data)

        return content

    def map_salesforce_address(self,addresses,address_mapping,payload,endpoint="contact"):
        if isinstance(addresses,list):
            other_address_mapping = {}
            if len(addresses)>0:
                for key in address_mapping.keys():
                    if key in addresses[0]:
                        if addresses[0][key]:
                            payload[address_mapping[key]] = addresses[0][key]

                    if len(addresses)>1:
                        keyother = address_mapping[key].replace("Mailing","Other")
                        if endpoint=="account":
                            keyother = address_mapping[key].replace("Billing","Shipping")
                        if addresses[1][key]:
                            payload[keyother] = addresses[1][key]
                    
        return payload

    #Microsoft dynamics address mapping
    def map_dynamics_address(self,addresses,address_mapping,payload,endpoint="contact"):
        if isinstance(addresses,list):
            curr_address = 1
            if len(addresses)>0:
                for address in addresses:
                    for key in address_mapping.keys():
                        if key in address:
                            if address[key]:
                                curr_key = address_mapping[key].replace("[i]",str(curr_address))
                                payload[curr_key] = address[key]

                    
                    curr_address +=1        
                    
        return payload   

    def map_dynamics_phones(self,phones,phone_mapping,payload):
        if isinstance(phones,list):
            if len(phones)>0:
                payload[phone_mapping["number"]] = phones[0]["number"]
        return payload    

    def map_status_code(self, status, payload):
        if status in ["in_progress", "on_progress", "on_hold"]:
            payload["statecode"] = 0
        elif status in ["won", "closedwon"]:
            payload["statecode"] = 1
        elif status in ["canceled", "out-sold"]:
            payload["statecode"] = 3
        return payload        

    def map_custom_fields(self,payload,fields):
        #Populate custom fields.
        for key,val in fields:
            payload[key] = val
        return payload

    def prepare_payload(self,record,endpoint='contact',target="salesforce"):
        mapping = self.read_json_file(f"mapping_{target}.json")
        ignore = mapping["ignore"]
        mapping = mapping[endpoint]
        payload = {}
        payload_return = {}
        lookup_keys = mapping.keys()
        for lookup_key in lookup_keys:
            if lookup_key == "addresses" and target == "salesforce":
                payload = self.map_salesforce_address(record.get(lookup_key,[]),mapping[lookup_key],payload,endpoint)
            elif lookup_key == "addresses" and target == "dynamics":
                payload = self.map_dynamics_address(record.get(lookup_key,[]),mapping[lookup_key],payload,endpoint)    
            elif lookup_key == "phone_numbers" and target == "dynamics":
                self.map_dynamics_phones(record.get(lookup_key,[]),mapping[lookup_key],payload)    
            elif lookup_key == "custom_fields":
                #handle custom fields
                payload = self.map_custom_fields(payload,mapping[lookup_key])
            elif lookup_key in ["close_date"]:
                val = record.get(lookup_key,"")
                payload[mapping[lookup_key]] = val.split("T")[0]
            elif lookup_key == "status":
                payload = self.map_status_code(record.get(lookup_key), payload) 
            else:    
                val = record.get(lookup_key,"")
                if val:
                    payload[mapping[lookup_key]] = val

        #Need name for Opportunity
        if endpoint=="opportunity" or endpoint=="account":
           ignore.remove("Name")
        #filter ignored keys            
        for key in payload.keys():
            if key not in ignore:
                payload_return[key] = payload[key]            
        return payload_return