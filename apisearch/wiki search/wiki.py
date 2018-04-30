import urllib.request
import urllib.parse
from bs4 import BeautifulSoup as Soup
import os
import pandas as pd
import json
import re
import requests

input_file_path = './companies_list.csv'
output_file_path = './company_info_out.csv'


# cx = '009460746032816234403:v2qakievzyk'
# key = "AIzaSyBRj6wM4hEpzfyVZeI9Aw9bDxlWg04aq_A"
cx = '014708891792837843645:yo28yzgyc0o'
key =  'AIzaSyDgV5ZLX5yXUaxKK88VcwqTQLHnCmcRR7Q'
google_cse_api_url = 'https://www.googleapis.com/customsearch/v1'


# method to call google custom search API
def google_cse_call(query):
    parameters = {"q": query,
                  "cx": cx,
                  "key": key,
                  }

    # make request
    page = requests.request("GET", google_cse_api_url, params=parameters)
    # process result
    api_response = json.loads(page.text)

    # get 'items' form API response
    # fetch domain value from first record in 'items'
    company_url = [item["link"] for item in api_response["items"]][0]
    print(company_url)
    return company_url


def tag_extraction(url):
    # opening up connection grabbing the page
    page = urllib.request.urlopen(url)
    page_html = page.read()
    page.close()
    dict_out = {}
    # parsing the html page
    html = Soup(page_html, "html.parser")
    company_name = html.find('h1', attrs={"class": "firstHeading"}).text

    table = html.find('table',{'class': 'infobox'})
    result = {}
    exceptional_row_count = 0

    if table:
        for tr in table.findAll('tr'):
            if tr.find('th') and tr.find('td'):
                result[tr.find('th').text] = tr.find('td').text
            else:
                # the first row Logos fall here
                exceptional_row_count += 1
        if exceptional_row_count > 1:
            pass

    keys_list = ['\nArea served\n', "Owner", '\nType\n', 'Industry', 'Headquarters',
                 "\nKey people\n", 'Website', 'Founded', 'Revenue']

    dict_out.update({"Company Name": company_name})

    if 'Type' in result.keys():
        result['\nType\n'] = result['Type']
    if '\nRevenue\n' in result.keys():
        result['Revenue'] = result['\nRevenue\n']

    for key_in_list in keys_list:
        if key_in_list not in result.keys():
            result[key_in_list] = ''
        else:
            line = result[key_in_list]
            line = re.sub(r"[\n]+", " ", line).strip()
            # line=line.strip()
            line = re.sub(r"\[\d+\]", "", line)
            result[key_in_list] = line

        dict_out.update({re.sub(r"[,\n]+", "", key_in_list): result[key_in_list]})

    # json_result = json.dumps(dict_out, indent=4, ensure_ascii=False)
    # print(json_result)

    # code to write CSV
    output_df = pd.DataFrame.from_dict(dict_out, orient="index").transpose()
    write_output_csv(output_df)


def read_file(input_file):
    file_df = pd.read_csv(input_file, names=['0'], encoding='iso-8859-1')
    for company_name in file_df['0']:
        wikipedia_url = google_cse_call(company_name)
        tag_extraction(wikipedia_url)


def write_output_csv(output_df):
    if not os.path.isfile(output_file_path):
        output_df.to_csv(output_file_path, header='column_names', index=False)
    else:  # else it exists so append without writing the header
        output_df.to_csv(output_file_path, mode='a', header=False, index=False)


if __name__ == '__main__':
    print("starting Service")
    read_file(input_file_path)
    print("Service Finished")
