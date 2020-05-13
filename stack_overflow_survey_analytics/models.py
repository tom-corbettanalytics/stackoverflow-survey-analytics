import os
import requests
import urllib
from bs4 import BeautifulSoup
import zipfile
import pandas as pd
from .utils import load_sqlalchemy_engine
import re
from matplotlib.figure import Figure
import matplotlib as mpl
import matplotlib.pyplot as plt


plt.style.use('seaborn-deep')
mpl.rcParams['axes.labelcolor'] = 'black'
mpl.rcParams['axes.titlesize'] = 16
mpl.rcParams['figure.titleweight'] = 'bold'
package_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(package_dir)


class Survey(object):
    
    # Url with list of survey ids
    survey_index_url = 'https://insights.stackoverflow.com/survey'

    # All tags containing survey params have exactly this text
    survey_index_tag_contents = 'Download Full Data Set (CSV)'
    
    # Directory where surveys are stored    
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
        
        
class Chart(object):
    
    compiled_analysis_dir = os.path.join(project_dir, 'target', 'compiled', 'stack_overflow_survey_analysis', 'analysis')
    compiled_charts_dir = os.path.join(project_dir, 'charts')
    
    def __init__(self, style, query_filename, xcol, ycols, ylabel, xlabel, title, ycol_names):
        self.style = style
        self.query_filename = query_filename
        self.query_name = self.query_filename.replace('.sql', '')
        self.xcol = xcol
        self.ycols = ycols
        self.ycol_names = ycol_names
        self.ylabel = ylabel
        self.xlabel = xlabel
        self.title = title
        self.chart_filename = os.path.join(self.compiled_charts_dir, self.query_name) + '.svg'
        self.query = self.load_query()
        self.dataset = self.load_dataset()
        self.figure = Figure()
        self.axes = self.figure.add_axes([0.1,0.1,0.8,0.8])        
        if not os.path.exists(self.compiled_charts_dir):
            os.makedirs(self.compiled_charts_dir)
        
    def load_query(self):
        query_filepath = os.path.join(self.compiled_analysis_dir, self.query_filename)
        return open(query_filepath, 'r').read()
        
    def load_dataset(self):
        return pd.read_sql(self.query, load_sqlalchemy_engine())
    
    def compile(self):
        return getattr(self, self.style)()
        
    def stacked_bars(self):
        xvals, yvals = self.dataset[self.xcol], self.dataset[self.ycols]
        max_yval = yvals.sum(axis=1).max()
        
        for n, yval in enumerate(yvals):
            print(n, )
            if n == 0:
                self.axes.bar(xvals, yvals[yval], label=self.ycol_names[n])
            else:
                self.axes.bar(xvals, yvals[yval], bottom=yvals.iloc[:,n-1], label=self.ycol_names[n])
        
        # This increase might need to be determined dynamically, based on the
        # number of yvals we're plotting. Maybe 10% per value?
        self.axes.set_ylim([0, max_yval*1.2])
        self.figure.suptitle(self.title, size=16)
        self.axes.set_ylabel(self.ylabel)
        self.axes.set_xlabel(self.xlabel)
        self.axes.set_xticks(xvals)
        self.axes.set_xticklabels([str(l) for l in xvals])
        self.axes.legend()
        self.figure.savefig(self.chart_filename)
        print(f"Compiled {self.chart_filename}")
        