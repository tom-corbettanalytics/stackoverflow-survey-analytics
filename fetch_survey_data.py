import os
import urllib
import zipfile
from typing import Dict, Iterable

import requests
from bs4 import BeautifulSoup


# Directory where the raw survey results will be stored
project_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(project_dir, 'data')
survey_zips_dir = os.path.join(data_dir, 'zips')


class Survey(object):
    pass


class Chart(object):
    pass

# Template string for url to download survey results"
download_url_template = 'https://drive.google.com/uc?export=download&id={survey_id}'

# Url with list of survey ids
survey_index_url = 'https://insights.stackoverflow.com/survey'


def iter_survey_params() -> Iterable[Dict]:
    """
    Loads the survey index url and parses out the relevant survey IDs and
    publication dates.
    """
    
    resp = requests.get(survey_index_url)
    soup = BeautifulSoup(resp.content.decode(), features="html.parser")
    tags = [string.parent for string in soup.findAll(text='Download Full Data Set (CSV)')]
    print(f"Found {len(tags)} surveys on {survey_index_url}")
    for tag in tags:
        url_params = urllib.parse.urlparse(tag['href']).query
        survey_id = urllib.parse.parse_qs(url_params)['id'].pop()
        yield {'year': tag['data-year'], 'id': survey_id}


def save_survey(survey_id: str, year: str) -> str:
    survey_url = download_url_template.format(survey_id=survey_id)
    resp = requests.get(survey_url)
    filename = os.path.join(survey_zips_dir, f"survey_results_{year}.zip")
    with open(filename, 'wb') as out:
        out.write(resp.content)
    print(f"Saved survey from year {params['year']} to {filename}")
    return filename
        

def extract_survey(filename: str, survey_id: str, year: str) -> str:
    print(f"Extracting survey from year {year}")
    for zip_filename in os.listdir(survey_zips_dir):
        zip_fullpath = os.path.join(survey_zips_dir, zip_filename)
        with zipfile.ZipFile(zip_fullpath, 'r') as zip_handler:
            print(zip_handler.namelist())
    return survey_id
    

if __name__ == "__main__":
    for params in iter_survey_params():
        saved_filename = save_survey(survey_id=params['id'], year=params['year'])
        extract_survey(filename=saved_filename, survey_id=params['id'], year=params['year'])

    