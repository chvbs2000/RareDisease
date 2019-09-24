import os
import unicodedata
from collections import defaultdict
import xml.etree.ElementTree as ET

"""
{"_id":disease id,
    "orphanet":{
        "xref":[
            {"orphanet id":____,
             "ICD-10":____,
             "OMIMN":____,
             "UMLS":____,
             "MedDRA":____,
             "MeSH":____}
        ]

    }
}
"""

def load_data(data_access):

    docs = parse_data(data_access)

    for doc in docs:
        yield doc



def parse_data(data_access):

    file_name = "en_product1.xml"
    data_dir = os.path.join(data_access, file_name)

    # check if the file exist
    assert os.path.exists(data_dir), "input file '%s' does not exist" % data_dir

    tree = ET.parse(data_dir)
    root = tree.getroot()
    keys = ['OrphaNumber', 'Synonym', 'ICD-10', 'OMIMN', 'UMLS', 'MedDRA', 'MeSH']

    disorders = tree.findall('.//DisorderList//Disorder')
    result = []
    for d in disorders:
        disease = {}
        disease['orphanet_id'] = d.find('OrphaNumber').text
        disease['xref'] = {}
        
        if d.findall('SynonymList//Synonym')：
            synonym = d.find('SynonymList//Synonym').text
        else:
            synonym = " - "
        disease['Synonym(s)'] = synonym
        external_reference = d.findall('ExternalReferenceList//ExternalReference')
        disease['xref'] = [{e.find('Source').text : e.find('Reference').text} for e in external_reference]
        result.append(disease)

    merged_list = merge_key(result)

    return merged_list



def merge_key(my_list):

    out_dict = defaultdict(list)

    for item in result:
        update_orpha_dict = defaultdict(list)
        update_orpha_dict['_id'] = item['disease_id']
        update_orpha_dict['orphanet'] = {}
        update_orpha_dict['orphanet']['xref'] = {}
        update_orpha_dict['orphanet']['xref']['orphanet_id'] = [item['orphanet_id']]

        for ref in item['xref']:
            cur_key = list(ref.keys())[0]
            update_orpha_dict['orphanet']['xref'][cur_key] = update_orpha_dict['orphanet']['xref'].get(cur_key, [])+[ref[cur_key]]
        
        for k,v in update_orpha_dict['orphanet']['xref'].items():
            if len(v) == 1:
                update_orpha_dict['orphanet']['xref'][k] = v[0]
            else:
                update_orpha_dict['orphanet']['xref'][k] = [sub_v for sub_v in v]
        
        out_dict[update_orpha_dict['_id']].append(update_orpha_dict)

    temp_output = []
    for value in out_dict.values():
        temp_output.append({
            '_id':value[0]['_id'],
            'orphanet':{
                'xref':[v['orphanet']['xref'] for v in value]
            }
        })

    return temp_output





