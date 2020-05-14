from invoke import task
from stack_overflow_survey_analytics.models import Survey, Chart
from stack_overflow_survey_analytics.utils import load_sqlalchemy_engine, usd_formatter
import pandas as pd
import matplotlib.ticker as ticker
import os


pd.set_option('display.max_rows', 100)


@task 
def download_surveys(ctx):
    "Download all surveys since 2017 to ./sources"
    
    for params in Survey.iter_all_survey_params():
        if params['year'] >= 2017:
            survey = Survey(survey_id=params['survey_id'], year=params['year'])
            survey.download()
            print(f"Finished downloading survey from {survey.year} ({survey.survey_id})")


@task
def load_surveys(ctx):
    "Loads all downloaded surveys into the postgres database"
    
    for params in Survey.iter_all_survey_params(downloaded=True):
        survey = Survey(survey_id=params['survey_id'], year=params['year'])
        survey.load_pg()
        print(f"Finished extracting survey from {survey.year} ({survey.survey_id})")


@task 
def load_metadata(ctx, since=2017):
    "Loads a table with metadata details about the surveys"
    
    metadata_dfs = []
    
    for params in filter(lambda x: x['year'] >= since, Survey.iter_all_survey_params()):
        survey = Survey(survey_id=params['survey_id'], year=params['year'])
        responses = survey.load_responses_df()
        questions = survey.load_questions_df()
        
        first_responses = responses.head(3).transpose()
        first_responses.index = questions.index
        first_responses.columns = ['response_1', 'response_2', 'response_3']
        
        nulls = 1 - (responses.count() / len(responses.index))
        nulls.index = questions.index
        nulls.name = "nulls"
        
        year = pd.Series([survey.year for x in range(len(questions))])
        year.index = questions.index
        year.name = "year"
        
        uniques = responses.nunique()
        uniques.index = questions.index
        uniques.name = "uniques"
        
        result = pd.concat([year, questions.column_name, questions.question_text, nulls, uniques, first_responses], axis=1)
        metadata_dfs.append(result)
    else:
        metadata = pd.concat(metadata_dfs, axis='index')
        metadata.to_sql('metadata', load_sqlalchemy_engine(), if_exists='replace', index=False)
        
        
@task
def render_charts(ctx):
    "Creates the svg charts from sql queries inside of ./analysis"
    
    os.system('dbt compile')
    
    charts_config = {
        'analytics_titles_per_year.sql': {
            'style': 'stacked_bars',
            'xcol': 'year',
            'ycols': ['percent_analytics_and_other_titles', 'percent_analytics_titles_only'],
            'ycol_names': ['Analytics and other roles', 'Analytics roles only'],
            'ylabel': 'Percent of respondents',
            'xlabel': 'Year',
            'title': "Percent of respondents with Analytics titles"
        },
        # TODO: add y-label formatter
        'analytics_salary_per_year.sql': {
            'style': 'lines',
            'xcol': 'year',
            'ycols': ['with_analytics_roles', 'without_analytics_roles'],
            'ycol_names': ['Analytics roles', 'Other developers'],
            'ylabel': 'Annual salary, US respondents (USD)',
            'xlabel': 'Year',
            'title': "Average annual salary (USD)",
            'yaxis_formatter': ticker.FuncFormatter(usd_formatter)
        }
    }
    
    for query_filename, kwargs in charts_config.items():
        chart = Chart(query_filename=query_filename, **kwargs)
        chart.compile()
        
        
@task
def upload_charts(ctx):
    "Uploads charts from ./charts to the s3 bucket s3://corbett-images"
    
    bucket_name = "s3://corbett-images/stack-overflow-survey-2020/"
    os.system(f'aws s3 sync ./charts {bucket_name}')
    

@task
def view(ctx, query_filename):
    "Execute a query from ./analysis and print its results"
    
    os.system('dbt compile')
    query_path = os.path.join(Chart.compiled_analysis_dir, query_filename + ".sql")
    sql = open(query_path).read()
    df = pd.read_sql(sql, load_sqlalchemy_engine())
    print(df)
    