from invoke import task
from stack_overflow_survey_analytics.models import Survey
from stack_overflow_survey_analytics.utils import load_sqlalchemy_engine
import pandas as pd


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
        