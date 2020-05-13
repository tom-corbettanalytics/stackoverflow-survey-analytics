import os
import requests
import urllib
from bs4 import BeautifulSoup
import zipfile
import pandas as pd
from .utils import load_sqlalchemy_engine
import re


class Survey(object):
    
    # Url with list of survey ids
    survey_index_url = 'https://insights.stackoverflow.com/survey'

    # All tags containing survey params have exactly this text
    survey_index_tag_contents = 'Download Full Data Set (CSV)'
    
    # Directory where surveys are stored
    project_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(project_dir, 'data')
    survey_dir = os.path.join(project_dir, 'sources')
    
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
        items = filename.replace('survey_', '').replace('.zip', '')
        year = items[-4:]
        survey_id = items.replace('_' + year, '')
        return {'survey_id': survey_id, 'year': int(year)}
    
    def __init__(self, survey_id: str, year: int):
        self.survey_id = survey_id
        self.year = year
        self.responses_df = None
        self.questions_df = None
        self.url = f'https://drive.google.com/uc?export=download&id={self.survey_id}'
        self.filename = os.path.join(self.survey_dir, f"survey_{self.survey_id}_{self.year}.zip")
        self.questions_tablename = f"survey_{self.year}_questions"
        self.responses_tablename = f"survey_{self.year}_responses"
        if not os.path.exists(self.survey_dir):
            os.makedirs(self.survey_dir)

    def __repr__(self):
        return f"<Survey {self.year}>"

    def download(self) -> str:
        resp = requests.get(self.url)
        with open(self.filename, 'wb') as out:
            out.write(resp.content)
            
    def load_pg(self) -> str:
        with zipfile.ZipFile(self.filename, 'r') as zip_handler:
            for name in zip_handler.namelist():
                if name in self.valid_zip_extract_names():
                    df = pd.read_csv(zip_handler.open(name),  encoding='ISO-8859-2')
                    if 'schema' in name:
                        output_name = self.questions_tablename
                        df.columns = ['column_name', 'question_text']
                        df['column_name'] = df['column_name'].apply(self.to_snake_case)
                    else:
                        output_name = self.responses_tablename
                        df.columns = self.to_snake_case_vectorized(df.columns) 
                    df.to_sql(output_name, load_sqlalchemy_engine(), index=False, if_exists='replace')
        return self.filename
        
    def load_questions_df(self) -> pd.DataFrame:
        try:
            return pd.read_sql(self.questions_tablename, load_sqlalchemy_engine())
        except Exception:
            return None
        
    def load_responses_df(self) -> pd.DataFrame:
        return pd.read_sql(self.responses_tablename, load_sqlalchemy_engine())
        
    def valid_zip_extract_names(self):
        return [
            'survey_results_public.csv',
            'survey_results_schema.csv',
            f'{self.year} Stack Overflow Survey Results.csv',
            f'{self.year} Stack Overflow Survey Responses.csv',
            f'{self.year} Stack Overflow Developer Survey Responses.csv',
            f'{self.year} Stack Overflow Survey Results/{self.year} Stack Overflow Survey Responses.csv'
        ]
        
        
    def to_snake_case(self, value):
        return re.sub(r'(?<!^)(?=[A-Z])', '_', value).lower()
        
    def to_snake_case_vectorized(self, columns):
        headers = []
        for n, col in enumerate(columns):
            if 'Unnamed' in col:
                headers.append(f'col_{n}_unnamed')
            else:
                headers.append(self.to_snake_case(col))
        return headers