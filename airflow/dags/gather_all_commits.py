import subprocess
import json
import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
import logging


def list_commits():
    # set up logger
    task_logger = logging.getLogger("airflow.task")
    task_logger.info("list commits beginning")
    task_logger.warning("this is a warning")

    # grab environment information for api calls and database writes
    load_dotenv()
    api_token = os.getenv("API_TOKEN")

    # initialize variables for while loop
    response_length = 30
    sha_code = ''
    commit_info = []

    # walk through all commits - recording the commit sha and commit sha url for future pulls
    # response length will be equal to 1 when we list the last commit (the first commit to the repo)
    # the github commits hook does not allow a change in direction or sorting, so we will be walking
    # from most recent commits to the commit furthest in the past.
    while response_length!=1:
        authorization = "Authorization: Bearer {}".format(api_token)
        url = "https://api.github.com/repos/artsmia/collection/commits{}".format(sha_code)
        command = ['curl', '-L', '-H', 'Accept: application/vnd.github+json', '-H', authorization, 'X-GitHub-Api-Version: 2022-11-29',  url]
        try:
            result = subprocess.run(command, capture_output=True, text=True)
        except:
            task_logger.error("Curl command to list commits errored.")
            task_logger.error("Failed command list: {}".format(command))

        if result.returncode != 0:
            task_logger.error("Curl command did not return data successfully.")
            task_logger.error("See output: {}".format(result))
            continue

        # grab results and format into json
        json_result = json.loads(result.stdout)
        # determine the response length to see where we are at in commit history
        response_length = len(json_result)
        # store the sha and url of the commit for future use
        for commit in json_result:
            commit_info.append([commit['sha'], commit['url'], commit['commit']['author']['date']])

        # update the sha to be the last listed sha, so we can walk backwards through commits
        sha_code = '?sha={}'.format(commit_info[-1][0])

    # convert sha and url list into a dataframe and drop duplicates created by previous process
    commit_info_df = pd.DataFrame(data=commit_info, columns=['commit_sha', 'commit_api_url', 'commit_date'])
    commit_info_df.drop_duplicates(inplace=True)
    task_logger.info("Total number of commits: {}".format(len(commit_info_df)))

    # define the engine for accessing the postgres instance
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    db = os.getenv('POSTGRES_DB')
    postgres_url = "postgresql+psycopg2://{}:{}@db:5432/{}".format(user, password, db)
    
    try:
        engine = create_engine(postgres_url)
    except Exception as e:
        task_logger.error("Connecting to Postgres instance failed.")
        task_logger.error("{}".format(e))

    try:
        commit_info_df.to_sql('stage1_commits', if_exists='replace', con=engine, index=False)
    except Exception as e:
        task_logger.error("Write to Postgres server failed.")
        task_logger.error({}.format(e))