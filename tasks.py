import csv
import os
import re
import urllib
import zipfile
from typing import Dict, Iterable

import requests
import pandas as pd
import yaml
import sqlalchemy as sa
from bs4 import BeautifulSoup
from invoke import task


def load_sqlalchemy_engine():
    profiles_fn = os.path.expanduser('~/.dbt/profiles.yml')
    with open(profiles_fn) as profiles_fh:
        profiles = yaml.load(profiles_fh, Loader=yaml.FullLoader)
        stack_overflow_db_profile = profiles['default']['outputs']['stack_overflow_surveys']
        db_url = sa.engine.url.URL(**{
            'drivername': 'postgresql',
            'username': stack_overflow_db_profile['user'],
            'password': stack_overflow_db_profile['pass'],
            'host': stack_overflow_db_profile['host'],
            'port': stack_overflow_db_profile['port'],
            'database': stack_overflow_db_profile['dbname'],
        })
        return sa.engine.create_engine(db_url)


class Survey(object):
    
    # Url with list of survey ids
    survey_index_url = 'https://insights.stackoverflow.com/survey'

    # All tags containing survey params have exactly this text
    survey_index_tag_contents = 'Download Full Data Set (CSV)'
    
    # Directory where surveys are stored
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    survey_dir = os.path.join(data_dir, 'zips')
    
    @classmethod
    def iter_all_survey_params(cls, downloaded=False):
        """
        Loads the survey index url and parses out the relevant survey IDs and
        publication dates.
        """
        
        if not downloaded:
            resp = requests.get(cls.survey_index_url)
            soup = BeautifulSoup(resp.content.decode(), features="html.parser")
            tags = [string.parent for string in soup.findAll(text=cls.survey_index_tag_contents)]
    
            for tag in tags:
                url_params = urllib.parse.urlparse(tag['href']).query
                survey_id = urllib.parse.parse_qs(url_params)['id'].pop()
                year = int(tag['data-year'])
                yield {'year': year, 'survey_id': survey_id}
        else:
            for filename in os.listdir(cls.survey_dir):
                yield cls.parse_survey_params_from_filename(filename)
    
    @classmethod
    def parse_survey_params_from_filename(cls, filename):
        items = filename.replace('survey_results_', '').replace('.zip', '')
        year = items[-4:]
        survey_id = items.replace('_' + year, '')
        return {'survey_id': survey_id, 'year': int(year)}
    
    def __init__(self, survey_id: str, year: int):
        self.survey_id = survey_id
        self.year = year
        if not os.path.exists(self.survey_dir):
            os.makedirs(self.survey_dir)

    def __repr__(self):
        return f"<Survey {self.year}>"

    @property
    def url(self) -> str:
      return f'https://drive.google.com/uc?export=download&id={self.survey_id}'

    @property
    def filename(self) -> str:
        return os.path.join(self.survey_dir, f"survey_results_{self.survey_id}_{self.year}.zip")

    def load(self) -> str:
        resp = requests.get(self.url)
        with open(self.filename, 'wb') as out:
            out.write(resp.content)
            
    def extract(self) -> str:
        with zipfile.ZipFile(self.filename, 'r') as zip_handler:
            for name in zip_handler.namelist():
                if name in self.valid_zip_extract_names():
                    if 'schema' in name:
                        output_name = f"survey_{self.year}_questions"
                    else:
                        output_name = f"survey_{self.year}_responses"
                    df = pd.read_csv(zip_handler.open(name),  encoding='ISO-8859-2')
                    df.columns = self.format_csv_header(df.columns)
                    df.to_sql(output_name, load_sqlalchemy_engine(), index=False, if_exists='replace')
        return self.filename
        
    def valid_zip_extract_names(self):
        return [
            'survey_results_public.csv',
            'survey_results_schema.csv',
            f'{self.year} Stack Overflow Survey Results.csv',
            f'{self.year} Stack Overflow Survey Responses.csv',
            f'{self.year} Stack Overflow Developer Survey Responses.csv',
            f'{self.year} Stack Overflow Survey Results/{self.year} Stack Overflow Survey Responses.csv'
        ]
        
    def format_csv_header(self, columns):
        headers = []
        for n, col in enumerate(columns):
            if 'Unnamed' in col:
                headers.append(f'col_{n}_unnamed')
            else:
                headers.append(re.sub(r'(?<!^)(?=[A-Z])', '_', col).lower())
        return headers


@task 
def download(ctx):
    "Download all surveys since 2016 to ./data/zips"
    
    for params in Survey.iter_all_survey_params():
        if params['year'] >= 2016:
            survey = Survey(survey_id=params['survey_id'], year=params['year'])
            survey.load()
            print(f"Finished downloading survey from {survey.year} ({survey.survey_id})")


@task
def extract(ctx):
    "Unzip all downloaded surveys into ./data"
    
    for params in Survey.iter_all_survey_params(downloaded=True):
        survey = Survey(survey_id=params['survey_id'], year=params['year'])
        survey.extract()
        print(f"Finished extracting survey from {survey.year} ({survey.survey_id})")
    


@task
def temp(ctx):
    print(load_sqlalchemy_engine())