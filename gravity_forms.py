import os
import logging
import requests
from enum import Enum, auto

class OrgTypes(Enum):
    Area = auto()
    Region = auto()
    AO = auto()

class GravityForms:
    gf_key = os.environ["GRAVITYFORMS_KEY"]
    gf_secret = os.environ["GRAVITYFORMS_SECRET"]
    gf_base_url = os.environ["GRAVITYFORMS_BASEURL"]
    gf_areas_id = os.environ["GRAVITYFORMS_AREASID"]
    gf_regions_id = os.environ["GRAVITYFORMS_REGIONSID"]
    gf_aos_id = os.environ["GRAVITYFORMS_AOSID"]

    auth = (gf_key, gf_secret)

    def get_entries(self, org_type: OrgTypes) -> list:
        if org_type == OrgTypes.Area:
            form_id = self.gf_areas_id
        elif org_type == OrgTypes.Region:
            form_id = self.gf_regions_id
        elif org_type == OrgTypes.AO:
            form_id = self.gf_aos_id
        
        param = {"_labels": "1"}
        response = requests.get('https://old.f3nation.com/wp-json/gf/v2/forms/' + form_id + '/entries?paging[page_size]=10000', auth=self.auth, params=param)
        response.encoding = 'utf-8-sig'
        body = response.json()
        count = body['total_count']
        labels = body['_labels']

        if org_type == OrgTypes.Region:
            entries = [i for i in body['entries'] if not (i["1"] == "-")] # Omit the default Region that is caled "-"
        else:
            entries = body['entries']

        logging.info("Retrieved " + str(count) + " " + str(org_type) + "s. Processing.")

        entry: dict
        for entry in entries:
            
            label: str
            for label in labels:
                if type(labels[label]) == dict:
                    label_prefix = labels[label][label]
                    
                    sublabel: str
                    for sublabel in labels[label]:
                        if "." in sublabel: # skip first line, which has no decimal place, it's the prefix
                            entry[label_prefix + " " + labels[label][sublabel]] = entry.pop(sublabel)
                else:
                    entry[labels[label]] = entry.pop(label)
                        

        
        return entries
