# Import libraries and provide their versios inside requirements.txt
import os
import requests
import sys
import time
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta
from flask import Flask

# Set the start date of paginating github repositories (today- 1 day)
# This will load data of last 24 hours 
start_date = datetime.today() - timedelta(days=1)

# Get Key File using your Service Accounts and provide path of that file here
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="login_info.json"

# Construct a BigQuery client object.
client = bigquery.Client()

# Construct a combination of repositories and entity types to be paginated
rep_action_types  = [{"REPOSITORY":"A", "action_type":"issue"},{"REPOSITORY":"A", "action_type":"pr"},
                    {"REPOSITORY":"B", "action_type":"issue"},{"REPOSITORY":"B", "action_type":"pr"}]

# Provide github user token and organization
GITHUB_USER_TOKEN = ""
ORGANIZATION = "C"


# Function to paginate through single repositry with given entity type
def paginate_github(QUERY,GITHUB_USER_TOKEN,REPOSITORY,ENTITY_ON):

    after = ''
    df_data = []
    while True:
        request = requests.post('https://api.github.com/graphql',
                                json={'query': QUERY % after},
                                headers={"Authorization": "Bearer %s" % GITHUB_USER_TOKEN})
        result = request.json()
        edges = result['data']['search']['edges']
        for row in edges:
            if (row['node']['author']):
                row_dict = {
                    'company' : row['node']['author']['company'],
                    'github_action' : "created "+ENTITY_ON+"",
                    'updatedAt' : row['node']['updatedAt'],
                    'full_name' : row['node']['author']['name'],
                    'github_username' : row['node']['author']['login'],
                    'email' : row['node']['author']['email'],
                    'number' : row['node']['number'],
                    'state' : row['node']['state'],
                    'organization' : ORGANIZATION,
                    'repository' : REPOSITORY,
                    'title' : row['node']['title']
                }
                if not df_data:
                    df_data = [row_dict]
                else:
                    df_data.append(row_dict)
            for sub_row in row['node']['reactions']['edges']:
                if (sub_row['node']['user']):
                    row_dict = {
                            'company' : sub_row['node']['user']['company'],
                            'github_action' : "reacted to "+ENTITY_ON+"",
                            'updatedAt' : row['node']['updatedAt'],
                            'full_name' : sub_row['node']['user']['name'],
                            'github_username' : sub_row['node']['user']['login'],
                            'email' : sub_row['node']['user']['email'],
                            'number' : row['node']['number'],
                            'state' : row['node']['state'],
                            'organization' : ORGANIZATION,
                            'repository' : REPOSITORY,
                            'title' : row['node']['title']
                        }
                    if not df_data:
                        df_data = [row_dict]
                    else:
                        df_data.append(row_dict)
        search = result['data']['search']
        if not search['pageInfo']['hasNextPage']:
            print("done")
            break
        else:
            after = 'after: "%s"' % search['edges'][-1]['cursor']
            print(after)
            time.sleep(1)   
    return df_data


# Run flask app with one default URL and other with 'append_data' which will be scheduled
app = Flask(__name__)

@app.route('/')
def hello():
    return 'App is running to load github data every 24 hours!'



@app.route('/append_data')
def append_data():
    """Return a friendly HTTP greeting."""
    columns = ["company","github_action","updatedAt","full_name","github_username","email","number","state","organization","repository","title"]
    github_data_df = pd.DataFrame(columns = columns)
    client = bigquery.Client()
    for value in rep_action_types:
        REPOSITORY = value['REPOSITORY']
        ENTITY = value['action_type']
        ENTITY_ON = ""
        if (ENTITY=="issue"):
            ENTITY_ON = "Issue"
        else:
            ENTITY_ON = "PullRequest"
        QUERY = """{ search(query: "is:"""+ENTITY+""" type:"""+ENTITY+""" repo:"""+ORGANIZATION+"""/"""
        +REPOSITORY+""" updated:"""+str(start_date.date())+""".."""+str(start_date.date())+"""", type: ISSUE, first: 100, %s) {
        issueCount
		    pageInfo {
			    endCursor
			    hasNextPage
			    }    
                edges {
                    cursor
                    node {
                        ... on """+ENTITY_ON+""" {
                            updatedAt
                            state
                            title
                            number
                            url
                            author {
                                ... on User {
                                    name
                                    login
                                    email
                                    company
                                }
                            }
                                reactions(last: 100) {
                                    totalCount
                                    edges {
                                        node {
                                            createdAt
                                            content
                                            user {
                                                name
                                                login
                                                email
                                                company
                                            }
                                        }
                                    }
                                }
                        }
                    }
                }
            }
        }
        """
        data = paginate_github(QUERY,GITHUB_USER_TOKEN,REPOSITORY,ENTITY_ON)
        df = pd.DataFrame(columns = columns, data = data)
        github_data_df = pd.concat([github_data_df, df])
        print("size of dataframe = "+str(github_data_df.shape[0]))
    github_data_df['number'] = github_data_df['number'].astype(int)
    github_data_df['updatedAt'] = pd.to_datetime(github_data_df['updatedAt'])


    # provide project_id, dataset and table name in format (project_id.dataset.table)
    job = client.load_table_from_dataframe(github_data_df, 'project_id.dataset.table') # Make an API request.
    job.result()  # Wait for the job to complete.
    print("Total records logged to BigQuery =  "+str(github_data_df.shape[0]))
    return 'Results Logged!'


if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=False)
