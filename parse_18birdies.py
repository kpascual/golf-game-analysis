import json
from datetime import datetime

import pandas as pd
import sqlite3
import yaml


with open("config.yml") as f:
    cfg = yaml.load(f, yaml.FullLoader)
    DB_NAME = cfg['database_file']
    RAW_DATA_DIR = cfg['raw_csv_directory']

ROOT_DIR = cfg['directory_18birdies']
FILENAME = cfg['latest_18birdies_dump']
DB = cfg['database_file']

conn = sqlite3.connect(DB)


# Load export of golf round data from 18Birdies
golf_round_filename = ROOT_DIR + FILENAME
with open(golf_round_filename, 'r') as f:
    golf_round_data = json.load(f)


courses = pd.DataFrame(golf_round_data['myData']['clubData']['playedClubs'])
courses['club_id'] = courses['clubId']

# Get summary count of holes in round, and holes completed in round
# Get summary count of holes in round, and holes completed in round
golf_rounds_list = golf_round_data['myData']['activityData']['rounds'].copy()

updated_rounds = []
for item in golf_rounds_list:
    for key, val in item.items():
        if key == 'holeStrokes':
            hole_count = len(val)
            completed_hole_count = len([x for x in val if x > 0])

            hole_strokes = {}
            for i, hole in enumerate(item[key]):
                hole_strokes['hole_{0}'.format(i + 1)] = hole
        if key == 'clubId':
            club_id = val['id']

        if key == 'timestamp':
            round_datetime = datetime.fromtimestamp(val / 1000)

        if key == 'stats':
            stats = val
            
    item['hole_count'] = hole_count
    item['completed_hole_count'] = completed_hole_count
    item['club_id'] = club_id
    item['played_at'] = round_datetime
    item['play_date'] = round_datetime.date()
    item.update(stats)
    item.update(hole_strokes)

    hole_strokes = {}
    

golf_rounds = pd.DataFrame(golf_rounds_list)
golf_rounds = golf_rounds.merge(courses, on = 'club_id', how = 'left')
golf_rounds.rename(columns={"name":"course_name"}, inplace = True)

golf_rounds.drop(columns=['clubId_x','stats','holeStrokes','clubId_y','recommendedStats']).to_sql("rounds", conn, if_exists = "replace")
