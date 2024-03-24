import os
from dotenv import load_dotenv
import requests
import re
import pickle
import pandas as pd
from datetime import timedelta,datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import logging



def get_league_id():

    cache_file = 'league_id_cache.json'
    load_dotenv()
    headers = {
	"X-RapidAPI-Key": os.getenv('X-RapidAPI-Key'),
	"X-RapidAPI-Host": os.getenv('X-RapidAPI-Host')
                }

    # Check if cache file exists and contains league_id
    try:
        if os.path.exists(cache_file):
            with open(cache_file, 'r') as f:
                cache_data = json.load(f)
            league_id = cache_data.get('league_id')
            if league_id:
                return league_id

    # If cache file does not exist or league_id is not found, fetch it
        league_url = "https://api-football-v1.p.rapidapi.com/v3/leagues"
        league_response = requests.get(league_url, headers=headers)
        league_data = league_response.json()

    # Process the league_data to get the league_id
        leagues = league_data['response']
        for league in leagues:
            league_name = league['league']['name']
            league_country = league['country']['name']
            if league_name == 'Premier League' and league_country == 'England':
                league_id = league['league']['id']
                with open(cache_file, 'w') as f:
                    json.dump({'league_id': league_id}, f)
                logging.info('League ID fetched and cached successfully')
                return league_id
                
            
        logging.warning('Premier League (England) not found in API response')
        return None
    
    except Exception as e:
        logging.error(f'Error fetching league ID: {e}')
        return None



def get_team_id():

    team_file = 'team_id_cache.json'
    league_id=get_league_id()
    load_dotenv()
    headers = {
	"X-RapidAPI-Key": os.getenv('X-RapidAPI-Key'),
	"X-RapidAPI-Host": os.getenv('X-RapidAPI-Host')
                }

    # Check if cache file exists and contains team_id
    try:
        if os.path.exists(team_file):
            with open(team_file, 'r') as f:
                cache_data = json.load(f)
            team_id = cache_data.get('team_id')
            if team_id:
                return team_id

    # If cache file does not exist or team_id is not found, fetch it
        team_url = "https://api-football-v1.p.rapidapi.com/v3/teams"
        querystring = {'league': str(league_id), 'season': '2020'}
        team_response = requests.get(team_url, headers=headers, params=querystring)
        team_data = team_response.json()

    # Process the league_data to get the team_id
        teams = team_data['response']
        for team in teams:
            team_name = team['team']['name']
            if team_name == 'Arsenal':
                team_id = team['team']['id']
                break

    # Save league_id to cache file
        with open(team_file, 'w') as f:
            json.dump({'team_id': team_id}, f)

        logging.info('Get_team_Id function finished')
        return team_id

    except Exception as e:
        logging.error(f'Error in get_team_id: {e}')
        return None



def determine_season():
    current_date = datetime.now()
    start_date = datetime(current_date.year, 8, 1)  # Season starts in August
    end_date = datetime(current_date.year + 1, 6, 30)  # Season ends in June of the next year
    if start_date <= current_date <= end_date:
        return str(current_date.year)  # Season is from August of current year to May of next year
    elif current_date < start_date:
        return str(current_date.year - 1)  # Season starts in August of previous year
    else:
        return str(current_date.year + 1)  # Season ends in May of next year
    
    print('determine_function finished')



def fetch_fixtures():
    current_season = determine_season()
    team_id=get_team_id()
    load_dotenv()
    headers = {
	"X-RapidAPI-Key": os.getenv('X-RapidAPI-Key'),
	"X-RapidAPI-Host": os.getenv('X-RapidAPI-Host')
                }

    fixture_url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
    querystring = {"season":current_season,"team":str(team_id)}
    
    fixture_response = requests.get(fixture_url, headers=headers, params=querystring)
    fixture_data = fixture_response.json()

    fixture_df=pd.json_normalize(fixture_data['response'])
    fixture_df=fixture_df[['fixture.timezone','fixture.date','teams.home.name','teams.away.name']]

    print('fetch_fixtures function finished')
    return fixture_df



def get_today_matches():
    load_dotenv()
    filtered_fixture_df=fetch_fixtures()
    filtered_fixture_df['fixture.date']=pd.to_datetime(filtered_fixture_df['fixture.date'])
    today=datetime.now().date()
    today_matches = filtered_fixture_df[filtered_fixture_df['fixture.date'].dt.date == today]
    try:
        if not today_matches.empty:
            sender_email = os.getenv('EMAIL')
            receiver_email = os.getenv('EMAIL')
            password = os.getenv('PASSWORD')

            message = MIMEMultipart()
            message['From'] = sender_email
            message['To'] = receiver_email
            message['Subject'] = 'Today\'s Arsenal Matches'

            body = 'Arsenal Match schedule for today:\n'
            for index, row in today_matches.iterrows():
                body += f"{row['teams.home.name']} vs {row['teams.away.name']} at {row['fixture.date'].strftime('%H:%M')} UTC\n"

            message.attach(MIMEText(body, 'plain'))
            print('Connecting to server')
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(sender_email, password)
            print('Connected to server')
            text = message.as_string()
            server.sendmail(sender_email, receiver_email, text)
            print('Mail sent successfully. Enjoy your match')
            server.quit()
        else:
            print('There are no matches today')
    except Exception as e:
        print(e)


dag=DAG('football_api_dag',
        description='football api dag to get Arsenal match notification via email',
        schedule_interval='@daily',
        start_date=datetime.today())

task_get_league_id=PythonOperator(
    task_id='get_league_id',
    python_callable=get_league_id,
    dag=dag,
)

task_get_team_id=PythonOperator(
    task_id='get_team_id',
    python_callable=get_team_id,
    dag=dag,
)

task_determine_season=PythonOperator(
    task_id='determine_season',
    python_callable=determine_season,
    dag=dag,
)

task_fetch_fixtures=PythonOperator(
    task_id='fetch_fixtures',
    python_callable=fetch_fixtures,
    dag=dag,
)

task_get_today_matches=PythonOperator(
    task_id='get_today_matches',
    python_callable=get_today_matches,
    dag=dag,
)

task_get_league_id >> task_get_team_id >> task_determine_season >> task_fetch_fixtures >> task_get_today_matches