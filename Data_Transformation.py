import pandas as pd
import numpy as np


class Transformation:
    def __init__(self):
        pass

    def clean_batting_record(self):

        batting = pd.read_csv("Data/batting_raw_data.csv")

        # season value(2019) final match missing
        batting.season.fillna(2019, inplace=True)

        # Invalid datatype of season column
        batting.season = batting.season.astype(int)

        # missing record of azam khan(due to absent hurt) --> drop the row
        batting.drop(index=batting[batting.total_runs=='-'].index.values[0], axis=0, inplace=True)

        # invalid datatype of columns: runs, fours, sixes and strike rate
        batting.total_runs = batting.total_runs.astype(int)
        batting.fours = batting.fours.astype(int)
        batting.sixes = batting.sixes.astype(int)

        # strike column contains " - " for those players who played 0 ball. replace them with 0
        batting.strike_rate = batting.strike_rate.replace('-', 0)
        # invalid type
        batting.strike_rate = batting.strike_rate.astype(float)

        # Batsman name contains † invalid characters
        batting.batsman = batting.batsman.str.replace('†', '').str.strip()

        # Make new column at 6th position if a player is captain in that particular match. if yes = 1 and no = 0
        batting.insert(6, 'captain', 0)
        batting['captain'][batting.batsman.str.contains("\(c\)")] = 1

        # remove the c from batsman name
        batting.batsman = batting.batsman.str.replace("(c)", '').str.strip()

        # make new column for wicket type (bowled, catch, run out).
        # c Hussain Talat b Salman Irshad --> from this to two column one wicket type and another wicket taker
        batting.insert(7, 'wicket_type', np.nan)

        def fix_wicket_type(series):
            data = []
            if series[0] == 'b':
                data.append('bowled')
            elif series[0] == 'r':
                data.append('run out')
            elif series[0] == 'c':
                data.append('catch')
            elif series[0] == 'l':
                data.append('lbw')
            elif series[0] == 's':
                data.append('stumped')
            elif series[0] == 'h':
                data.append('hit wicket')
            elif series[0] == 'o':
                data.append('obstructing the field')
            else:
                data.append('not out')
            return data[0]

        batting.wicket_type = batting.wicket_taken.apply(fix_wicket_type)

        # add new column for wicket taker name
        batting.insert(8, 'dismissed_by', np.nan)

        def fix_wicket_taken(s):
            data = []

            if 'not out' in s:
                data.append('not out')
            elif 'run out' in s:
                data.append('run out')
            elif ' b ' in s:
                s = s[s.find(' b ') + 2:].strip()
                data.append(s)
            elif 'b ' in s:
                s = s[1:].strip()
                data.append(s)
            else:
                data.append(s)
            return data[0]

        batting['dismissed_by'] = batting.wicket_taken.apply(fix_wicket_taken)

        # the columns are extracted now drop the wicket_taken column
        batting.drop(columns='wicket_taken', inplace=True)

        # For better Column Name
        batting.rename(columns={
            'captain': 'match_captain',
            'stadium': 'venue',
            'team': 'team_name',
            'opposing_team': 'opponent_team'
        }, inplace=True)

        # export the data to csv file
        batting.to_csv('Data/batting_records_cleaned.csv', index=False)




    def clean_bowling_record(self):

        df = pd.read_csv('Data/bowling_raw_data.csv')

        # Missing value in season column. The values is missing from the final match played in national karachi stadium in season 2019
        df.season.fillna(2019, inplace=True)

        # correct the datatype
        df.season = df.season.astype(int)

        # make new column total balls
        def fix_balls(over):
            if len(str(over).split('.')) == 2:
                balls = (int(str(over).split('.')[0]) * 6) + (int(str(over).split('.')[1]))
                return balls
            else:
                balls = int(over * 6)

        total_balls = df.overs.apply(fix_balls)
        df.insert(2, 'total_balls', total_balls)

        # Rename columns
        df.rename(columns={
            'bowlers': 'bowler',
            'maiden': 'maidens',
            'runs': 'runs_conceded',
            'wickets': 'wickets_taken',
            'economy': 'economy_rate',
            'zeros': 'dot_balls',
            'fours': 'fours_conceded',
            'sixes': 'sixes_conceded',
            'wides': 'wide_balls',
            'stadium': 'venue',
            'team': 'team_name',
            'opposing_team': 'opponent_name'
        }, inplace=True)

        # export the data into csv file
        df.to_csv('Data/bowling_records_cleaned.csv',index=False)



    def clean_players_metadata(self):
        df = pd.read_csv('Data/player_raw_metadata.csv')

        # "†" unusual character in the end of name column
        df.names = df.names.str.replace('†', '')

        # extra characters (vc) and (c) in names column
        df.names = df.names.str.replace('(c)', '')
        df.names = df.names.str.replace('(vc)', '')

        # Bowling_style values are in batting_style so shift it to correct column
        row_values = df[(df['batting_style'].str.contains('Bowling')) & (df['batting_style'] != np.nan)][
            ['batting_style', 'bowling_style']].index.values

        df.iloc[row_values, [2, 3]] = df[(df['batting_style'].str.contains('Bowling')) & (df['batting_style'] != np.nan)][
            ['batting_style', 'bowling_style']].shift(axis=1)


        # Fill position column based on two other column
        # if bowling_style is null and batting_position is not null. Most probably the player position/role is Batter
        # if batting_style is null and bowling_style is not null. Most probably the player position/role is Bowler else nill

        null_row1 = df[(df['position'].isnull()) & (df['batting_style'].isnull())].index.values
        null_row2 = df[(df['position'].isnull()) & (df['bowling_style'].isnull())].index.values
        df.iloc[null_row1, 1] = "Bowler"
        df.iloc[null_row2, 1] = "Batter"
        df.position.fillna('No Information', inplace=True)

        # Remove extra character from Allrounder i.e. bowling Allrounder
        df.position = df.position.str.replace('Bowling', '').str.strip()
        df.position = df.position.str.replace('Batting', '').str.strip()

        # batting_style and bowling_style column has extra word (Batting:/Bowling:) i.e. Batting:Left hand Bat or Bowling:Left arm Medium
        df.batting_style = df.batting_style.str.replace('Batting:', '').str.strip()
        df.bowling_style = df.bowling_style.str.replace('Bowling:', '').str.strip()

        # Most of the players who are batters don't ball so this is why there are None values
        df.fillna('No Info', inplace=True)

        # better column name
        df.rename(columns={'position': 'playing_role'}, inplace=True)

        df.to_csv('Data/players_metadata_cleaned.csv', index=False)
