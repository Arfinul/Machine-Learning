import json
import requests
import urllib.request
import urllib.parse
import pandas as pd
import os
from requests.auth import HTTPBasicAuth

# TODO Read from config
# google custom search keys
key = "AIzaSyBRj6wM4hEpzfyVZeI9Aw9bDxlWg04aq_A "
cx = "009460746032816234403:pzgdil6_2ie"

# file path for sic codes
sic_code_file_path = "./input/sic_code.csv"

# input and output file paths
input_file_path = './input/custom_search_input.csv'
output_file_path = "./output/supplier_info.csv"

# company_house keys and url
# TODO make them final var
ch_api_key = "TJ-hMKeD2beVqk1tJeiW3i5kLKxRjlEDvNHYYVS2"
ch_company_search = "https://api.companieshouse.gov.uk/search/companies"
ch_api_url = "https://api.companieshouse.gov.uk/company/"

# url paths for API
full_contact_api_url = 'https://api.fullcontact.com/v3/company.enrich'
google_cse_api_url = 'https://www.googleapis.com/customsearch/v1'

# full contact API key
full_contact_api_auth_key = '8Yphf4QiFCuOxzqXNVEyRngs3vqkkXkC'

# extra parameter for google custom search
hq_value = 'UK'

# storing sic codes as dictionary
data_frame = pd.read_csv(sic_code_file_path)
sic_dict = dict(zip(data_frame['Code'], data_frame["Description"]))


# method to call google custom search API
def google_cse_call(query, hq_val='UK'):
    parameters = {"q": query,
                  "hq": hq_val,
                  "cx": cx,
                  "key": key,
                  }

    # make request
    page = requests.request("GET", google_cse_api_url, params=parameters)
    # process result
    api_response = json.loads(page.text)

    # get 'items' form API response
    # fetch domain value from first record in 'items'
    domains = [item["link"] for item in api_response["items"]][0]
    return domains


# method to call full contact company enrichment search API
def full_contact_service_call(domain, queries):
    req = urllib.request.Request(full_contact_api_url)
    req.add_header('Authorization', 'Bearer ' + full_contact_api_auth_key)
    data = json.dumps({
        "domain": domain}).encode('utf8')
    full_contact_api_response = urllib.request.urlopen(req, data).read().decode('utf-8')
    response_json = json.loads(full_contact_api_response)

    try:
        output_dict = {}
        name = response_json['name']
        company_name = queries
        industry_list = []
        for industry in (response_json['details']['industries']):
            industry_list.append(industry['name'])

        output_dict.update({'supplier_Name': company_name,
                            'normalized_name': name, 'industry': industry_list,
                            'keywords': response_json['details']['keywords'], 'domain': domain
                            })

        # removing '[', ']' from response values, to parse it as string
        keyword_values = ''
        for word in output_dict['keywords']:
            keyword_values += word + ","
        keyword_values = keyword_values.strip(",")

        industry_values = ''
        # concatenate industry response as string
        for word in output_dict['industry']:
            industry_values += word + ","
        industry_values = industry_values.strip(",")

        # calling company house service, in case data not found from full contact service
        if industry_values == '' or name == '':
            company_number = get_company_no_by_ch_service(queries)
            industry_values, name = get_company_details_by_number(company_number)

        # update final output
        output_dict.update({
            'normalized_name': name, 'industry': industry_values,
            'keywords': keyword_values
        })

    finally:
        output_df = pd.DataFrame.from_dict(output_dict, orient="index").transpose()
        write_output_csv(output_df)


def get_company_no_by_ch_service(queries):
    parameters = {"q": queries,
                  "items_per_page": 2,
                  "start_index": 0
                  }

    response = requests.get(ch_company_search, auth=(ch_api_key, ''), params=parameters)
    # process result
    api_response = json.loads(response.text)
    filtered_response = api_response["items"][0]
    # company number returned
    company_no = filtered_response['company_number']
    return company_no


# get basic info of company
def get_company_details_by_number(company_num):
    key_word = ''
    company_name = ''
    company_info_url = ch_api_url + company_num
    response = requests.get(company_info_url, auth=(ch_api_key, ''))
    # process result
    if response.status_code == 200:
        info_response = json.loads(response.text)
        company_name = info_response['company_name']
        if 'sic_codes' in info_response.keys():
            sic_codes_list = info_response["sic_codes"]
            for code in sic_codes_list:
                code = int(code)
                if code in sic_dict:
                    key_word = sic_dict[code]

    return key_word, company_name


def read_file(input_file):
    file_df = pd.read_csv(input_file, names=['0'])
    for company_name in file_df['0']:
        queries = company_name
        domain = google_cse_call(queries)
        full_contact_service_call(domain, queries)


# output to csv
def write_output_csv(output_df):
    if not os.path.isfile(output_file_path):
        output_df.to_csv(output_file_path, header='column_names', index=False)
    else:  # else it exists so append without writing the header
        output_df.to_csv(output_file_path, mode='a', header=False, index=False)


if __name__ == '__main__':
    print('START custom search service.......')
    read_file(input_file_path)
    print(' custom search FINISHED')
