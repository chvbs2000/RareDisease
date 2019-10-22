import os
import xml.etree.ElementTree as ET
from collections import defaultdict

def load_data(data_access):

    docs = parse_data(data_access)

    for doc in docs:
        yield doc

def parse_data(data_access):

    file_list = ["en_product1.xml", "en_product9_prev.xml", "en_product9_ages.xml"]

    # read files
    for idx, file in enumerate(file_list):
        data_dir = os.path.join(data_access, file)

        # check if the file exist
        assert os.path.exists(data_dir), "input file '%s' does not exist" % (data_dir)
        globals()['tree%s' % (idx+1)] = ET.parse(data_dir)

    # tree1 = ET.parse('en_product1.xml')
    # tree2 = ET.parse('en_product6.xml')
    # tree5 = ET.parse('en_product9_prev.xml')
    # tree6 = ET.parse('en_product9_ages.xml')

    disease_path = './/DisorderList//Disorder'
    disorders = tree1.findall(disease_path)
    result = []

    # find cross-reference
    for d in disorders:
        disease = {}
        disease['_id'] = d.find('OrphaNumber').text
        if d.findall('SynonymList//Synonym'):
            synonym = d.find('SynonymList//Synonym').text
        else:
            synonym = " - "
        disease['synonyms'] = synonym
        external_reference = d.findall('ExternalReferenceList//ExternalReference')
        disease['xref'] = [{e.find('Source').text : e.find('Reference').text} for e in external_reference]
        result.append(disease)

    # find prevalence
    prev_tree = tree2.findall(disease_path)
    for idx, sub_dict in enumerate(result):
        for p in prev_tree:
            if sub_dict['_id'] == p.find('OrphaNumber').text:       
                if p.findall('PrevalenceList//Prevalence//PrevalenceClass//Name'):
                    prevalence = p.find('PrevalenceList//Prevalence//PrevalenceClass//Name').text
                else:
                    prevalence = " - "   
                    
            result[idx]['prevalence'] = prevalence

    # find epidermiological data
    epi_tree = tree3.findall(disease_path)
    for idx, sub_dict in enumerate(result):
        for epi in epi_tree:
            if sub_dict['_id'] == epi.find('OrphaNumber').text:
                if epi.findall('AverageAgeOfOnsetList//AverageAgeOfOnset//Name'):
                    avg_onset = epi.findall('AverageAgeOfOnsetList//AverageAgeOfOnset')
                    result[idx]['age_of_onset'] = [e.find('Name').text for e in avg_onset]
                else:
                    result[idx]['age_of_onset'] = " - "
                
                if epi.findall('TypeOfInheritanceList//TypeOfInheritance//Name'):
                    inheritance = epi.find('TypeOfInheritanceList//TypeOfInheritance//Name').text
                else:
                    inheritance = " - "       

            result[idx]['inheritance'] = inheritance

    return merge_xref_key(result)

def merge_xref_key(list_to_merge):
    
    out_dict = defaultdict(list)

    for item in list_to_merge:
        update_dict = {}
        update_dict['_id'] = item['_id']
        update_dict['orphanet'] = {}
        update_dict['orphanet']['synonyms'] = item['synonyms']
        update_dict['orphanet']['prevalence'] = item['prevalence']
        update_dict['orphanet']['inheritance'] = item['inheritance']
        update_dict['orphanet']['age_of_onset'] = item['age_of_onset']
        update_dict['orphanet']['xref'] = {}

        for ref in item['xref']:
            cur_key = list(ref.keys())[0]
            update_dict['orphanet']['xref'][cur_key] = update_dict['orphanet']['xref'].get(cur_key, [])+[ref[cur_key]]

        for k,v in update_dict['orphanet']['xref'].items():
            if len(v) == 1:
                update_dict['orphanet']['xref'][k] = v[0]
            else:
                update_dict['orphanet']['xref'][k] = [sub_v for sub_v in v]

        out_dict[update_dict['_id']].append(update_dict)

    temp_output = []
    for value in out_dict.values():
        temp_output.append({
            '_id':value[0]['_id'],
            'orphanet':{
                'synonyms':value[0]['orphanet']['synonyms'],
                'prevalence':value[0]['orphanet']['prevalence'],
                'inheritance':value[0]['orphanet']['inheritance'],
                'age_of_onset':value[0]['orphanet']['age_of_onset'],
                'xref':[v['orphanet']['xref'] for v in value]
            }
        })

    return temp_output
