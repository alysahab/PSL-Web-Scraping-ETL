import time

from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.options import Options
from concurrent.futures import ThreadPoolExecutor
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import threading
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
import re


class DataExtract:
    def __init__(self):
        self.stats_links = [
            r'https://www.espncricinfo.com/series/pakistan-super-league-2015-16-923069/match-schedule-fixtures-and-results',
            r'https://www.espncricinfo.com/series/psl-2016-17-1075974/match-schedule-fixtures-and-results',
            r'https://www.espncricinfo.com/series/psl-2017-18-1128817/match-schedule-fixtures-and-results',
            r'https://www.espncricinfo.com/series/psl-2018-19-1168814/match-schedule-fixtures-and-results',
            r'https://www.espncricinfo.com/series/psl-2019-20-2020-21-1211602/match-schedule-fixtures-and-results',
            r'https://www.espncricinfo.com/series/psl-2020-21-2021-1238103/match-schedule-fixtures-and-results',
            r'https://www.espncricinfo.com/series/pakistan-super-league-2021-22-1292999/match-schedule-fixtures-and-results',
            r'https://www.espncricinfo.com/series/pakistan-super-league-2022-23-1332128/match-schedule-fixtures-and-results',
            r'https://www.espncricinfo.com/series/pakistan-super-league-2023-24-1412744/match-schedule-fixtures-and-results']

        self.metaData_links = [
            'https://www.espncricinfo.com/series/pakistan-super-league-2015-16-923069/islamabad-united-squad-967313/series-squads',
            'https://www.espncricinfo.com/series/psl-2016-17-1075974/islamabad-united-squad-1081266/series-squads',
            'https://www.espncricinfo.com/series/psl-2017-18-1128817/islamabad-united-squad-1137265/series-squads',
            'https://www.espncricinfo.com/series/psl-2018-19-1168814/islamabad-united-squad-1173965/series-squads',
            'https://www.espncricinfo.com/series/psl-2019-20-2020-21-1211602/islamabad-united-squad-1215698/series-squads',
            'https://www.espncricinfo.com/series/psl-2020-21-2021-1238103/islamabad-united-squad-1249785/series-squads',
            'https://www.espncricinfo.com/series/pakistan-super-league-2021-22-1292999/islamabad-united-squad-1293208/series-squads',
            'https://www.espncricinfo.com/series/pakistan-super-league-2022-23-1332128/islamabad-united-squad-1350209/series-squads',
            'https://www.espncricinfo.com/series/pakistan-super-league-2023-24-1412744/islamabad-united-squad-1412798/series-squads']


        # Create a file lock to ensure safe writing from multiple threads
        self.file_lock = threading.Lock()




    def write_to_file(self,content,num):
        # Use a file lock to ensure thread-safe writing to the file
        # create empty html file with the name Players_stats.html and Players_info.html

        if num == 1:
            with self.file_lock:
                with open('Html/Players_stats.html', 'a') as f:
                    f.write(content)
        else:
            with self.file_lock:
                with open('Html/Players_info.html', 'a') as f:
                    f.write(content)



    # Players stats html
    def html_extraction_stats(self, link, season_no):

        # Set up the Edge driver options
        option = Options()
        option.add_experimental_option('detach', True)

        # Path to the Edge driver executable
        s = Service(r"C:\Users\aly98\Desktop\Portfolio project\PSL project\edgedriver_win64\msedgedriver.exe")
        driver = webdriver.Edge(service=s, options=option)
        driver.maximize_window()

        driver.get(link)

        # Wait for the main content to load
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="main-container"]/div[5]/div/div[4]/div[1]/div[1]')))

        # Fetch the number of match elements
        total_matches = len(
            driver.find_elements(By.XPATH, '//*[@id="main-container"]/div[5]/div/div[4]/div[1]/div[1]/div/div/div/div'))

        i = 1
        while i<=total_matches:
            try:
                click = driver.find_element(By.XPATH,
                                            f'//*[@id="main-container"]/div[5]/div/div[4]/div[1]/div[1]/div/div/div/div[{i}]/div/div[2]/a')
                driver.execute_script("arguments[0].click();", click)

                # Wait for the div to load after clicking the match
                WebDriverWait(driver, 30).until(EC.presence_of_element_located(
                    (By.XPATH, '//*[@id="main-container"]/div[5]/div/div/div[3]/div[1]/div[2]')))

                div = driver.find_element(By.XPATH,
                                          '//*[@id="main-container"]/div[5]/div/div/div[3]/div[1]/div[2]')

                html = div.get_attribute('outerHTML')

                # Now writing to the content2.html file
                self.write_to_file(f'<!-- {season_no} season match no_{i} -->\n{html}\n\n',1)
                print(f'Processed season {season_no}, match no {i}')
                i = i+1
            except Exception as e:
                print(f"Error processing season {season_no} match {i}: {e} Trying Again")
            finally:
                driver.back()
                # wait fo 30 sec until the specific page with the following xpath is not appear
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="main-container"]/div[5]/div/div[4]')))
        driver.quit()



    # Players meta data html
    def extract_metaData_html(self,link, season_no):

        # Set up the Edge driver options
        option = Options()
        option.add_experimental_option('detach', True)

        # Path to the Edge driver executable
        s = Service(r"C:\Users\aly98\Desktop\Portfolio project\PSL project\edgedriver_win64\msedgedriver.exe")
        driver = webdriver.Edge(service=s, options=option)
        driver.maximize_window()

        # Open the webpage
        driver.get(link)
        time.sleep(5)
        # Wait for the page to load
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="main-container"]/div[5]/div/div[3]/div[2]/div[1]')))

        # Extract the first team's details
        div = driver.find_element(By.XPATH, '//*[@id="main-container"]/div[5]/div/div[3]/div[2]/div[1]')
        html = div.get_attribute('outerHTML')

        self.write_to_file(f'season {season_no} team 1 \n{html}\n\n', 2)
        print(f'season {season_no} team 1 processed')

        # Locate all <a> elements within the div
        div_xpath = '//*[@id="main-container"]/div[5]/div/div[3]/div[1]/div/div/div/div/div[2]'
        a_elements = driver.find_elements(By.XPATH, f'{div_xpath}//a')
        n = len(a_elements)

        # Loop through remaining teams
        for i in range(2, n + 1):

            click = driver.find_element(By.XPATH, f'//*[@id="main-container"]/div[5]/div/div[3]/div[1]/div/div/div/div/div[2]/a[{i}]')
            driver.execute_script("arguments[0].click();", click)
            time.sleep(5)
            # Wait for the team details to load
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="main-container"]/div[5]/div/div[3]/div[2]/div[1]')))

            div = driver.find_element(By.XPATH,
                                      '//*[@id="main-container"]/div[5]/div/div[3]/div[2]/div[1]')
            html = div.get_attribute('outerHTML')
            self.write_to_file(f'season {season_no} team {i} \n{html}\n\n',2)
            print(f'season {season_no} team {i} processed')
        driver.quit()


    # Threading to execute multiple tasks concurrently
    def concurrentExtraction(self):
        # Using ThreadPoolExecutor to run the scraping concurrently
        with ThreadPoolExecutor(max_workers=3) as executor:

            # # Players Stats
            executor.map(self.html_extraction_stats, self.stats_links, range(1, len(self.stats_links) + 1))

            # Players Meta Data
            executor.map(self.extract_metaData_html, self.metaData_links, range(1, len(self.metaData_links) + 1))



    # extract batting and bowling data from html and return data in tabular form
    def extract_players_stats(self):

        with open("Html/Players_stats.html", 'r') as f:
            html = f.read()

        soup = BeautifulSoup(html, 'html.parser')

        # for the batsmans and bowlers data
        batting_stats = {}
        bowling_stats = {}

        # All matches container
        match = len(soup.find_all('div', class_='ds-mt-3'))
        for m in range(match):

            cont = soup.find_all('div', class_='ds-mt-3')[m]
            # To check if team played a matched (not abondened)
            if len(cont.find_all('div', class_='ds-rounded-lg ds-mb-2')) > 0:

                batting_table = cont.find_all('table', class_='ds-w-full ds-table ds-table-md ds-table-auto ci-scorecard-table')
                bowling_table = cont.find_all('table', class_='ds-w-full ds-table ds-table-md ds-table-auto')
                match_detail_table = cont.find('table', class_='ds-w-full ds-table ds-table-sm ds-table-auto')


                # Team name
                # If table less then 5 i.e. 3 tables in a match means a match is abondened without other team get the chance to bat.
                num_tables = len(cont.find_all('tbody', class_=''))

                team1 = cont.find_all('span', class_='ds-text-title-xs ds-font-bold ds-capitalize')[0].text.strip()
                if num_tables > 3:
                    team2 = cont.find_all('span', class_='ds-text-title-xs ds-font-bold ds-capitalize')[1].text.strip()
                else:
                    name1 = re.sub('\d', '',match_detail_table.find_all('tr')[-1].find_all('td')[1].text.split(',')[0]).strip()
                    name2 = re.sub('\d', '', match_detail_table.find_all('tr')[-1].find_all('td')[1].text.split(',')[1]).strip()
                    if name1 == team1:
                        team2 = name2
                    else:
                        team2 = name1

                # Stadium
                stadium = match_detail_table.find_all('tr')[0].text.strip()

                # Season
                season = None
                for k in range(4, 9):
                    if 'match' in match_detail_table.find_all('tr')[k].find_all('td')[1].text.strip():
                        season = match_detail_table.find_all('tr')[k].find_all('td')[1].text.strip().split(' ')[2]
                        break

                # Iterate through batting and bowling tables record
                n_bat = len(batting_table)
                for i in range(n_bat):

                    # player team and opposing team
                    current_team = team1 if i == 0 else team2
                    opposing_team = team1 if i == 1 else team2

                    # batting records
                    n = len(batting_table[i].find('tbody').find_all('tr', class_=''))

                    # number of rows
                    for j in range(n):

                        cells = batting_table[i].find('tbody').find_all('tr', class_='')[j].find_all('td')


                        if 'Total' in cells[0].text or 'Fall' in cells[0].text:
                            continue

                        else:
                            name = cells[0].text.strip()
                            wicket_taken = cells[1].text.strip()
                            total_runs = cells[2].text.strip()
                            total_balls = cells[3].text.strip()
                            fours = cells[5].text.strip()
                            sixes = cells[6].text.strip()
                            strike_rate = cells[7].text.strip()

                            if name in batting_stats:
                                batting_stats[name]['total_runs'].append(total_runs)
                                batting_stats[name]['total_balls'].append(total_balls)
                                batting_stats[name]['wicket_taken'].append(wicket_taken)
                                batting_stats[name]['fours'].append(fours)
                                batting_stats[name]['sixes'].append(sixes)
                                batting_stats[name]['strike_rate'].append(strike_rate)
                                batting_stats[name]['team'].append(current_team)
                                batting_stats[name]['opposing_team'].append(opposing_team)
                                batting_stats[name]['season'].append(season)
                                batting_stats[name]['stadium'].append(stadium)

                            else:
                                batting_stats[name] = {

                                    'total_runs': [total_runs],
                                    'total_balls': [total_balls],
                                    'wicket_taken': [wicket_taken],
                                    'fours': [fours],
                                    'sixes': [sixes],
                                    'strike_rate': [strike_rate],
                                    'team': [current_team],
                                    'opposing_team': [opposing_team],
                                    'season': [season],
                                    'stadium': [stadium]

                                }


                    opposing_team = team1 if i == 0 else team2
                    current_team = team1 if i == 1 else team2

                    n = len(bowling_table[i].find('tbody').find_all('tr', class_=''))+1
                    for z in range(n):

                        if z == n-1:
                            # a dissimilar row class name
                            cells = bowling_table[i].find('tbody').find('tr', class_='ds-border-none').find_all('td')
                        else:
                            cells = bowling_table[i].find('tbody').find_all('tr', class_='')[z].find_all('td')

                        name = cells[0].text.strip()
                        overs = cells[1].text.strip()
                        maiden = cells[2].text.strip()
                        runs = cells[3].text.strip()
                        wickets = cells[4].text.strip()
                        economy = cells[5].text.strip()
                        zeros = cells[6].text.strip()
                        fours = cells[7].text.strip()
                        sixes = cells[8].text.strip()
                        wides = cells[9].text.strip()
                        no_balls = cells[10].text.strip()

                        if name in bowling_stats:
                            bowling_stats[name]['overs'].append(overs)
                            bowling_stats[name]['maiden'].append(maiden)
                            bowling_stats[name]['runs'].append(runs)
                            bowling_stats[name]['wickets'].append(wickets)
                            bowling_stats[name]['economy'].append(economy)
                            bowling_stats[name]['zeros'].append(zeros)
                            bowling_stats[name]['fours'].append(fours)
                            bowling_stats[name]['sixes'].append(sixes)
                            bowling_stats[name]['wides'].append(wides)
                            bowling_stats[name]['no_balls'].append(no_balls)
                            bowling_stats[name]['team'].append(current_team)
                            bowling_stats[name]['opposing_team'].append(opposing_team)
                            bowling_stats[name]['season'].append(season)
                            bowling_stats[name]['stadium'].append(stadium)



                        else:
                            bowling_stats[name] = {

                                'overs': [overs],
                                'maiden': [maiden],
                                'runs': [runs],
                                'wickets': [wickets],
                                'economy': [economy],
                                'zeros': [zeros],
                                'fours': [fours],
                                'sixes': [sixes],
                                'wides': [wides],
                                'no_balls': [no_balls],
                                'team': [current_team],
                                'opposing_team': [opposing_team],
                                'season': [season],
                                'stadium': [stadium]

                            }

            print('Match {} Data Extracted'.format(m))

        # Batting records to Dataframe format
        df1 = pd.DataFrame(
            columns=['batsman', 'total_runs', 'total_balls', 'fours', 'sixes', 'strike_rate', 'wicket_taken',
                     'season', 'stadium', 'team', 'opposing_team'])

        for key, value in batting_stats.items():

            temp_df = pd.DataFrame(batting_stats[key])
            name = []
            for i in range(temp_df.shape[0]):
                name.append(key)
            temp_df.insert(0, 'batsman', name)
            df1 = pd.concat([df1, temp_df])

        print(df1.shape)

        # Bowling records to dataframe format
        df2 = pd.DataFrame(
            columns=['bowlers', 'overs', 'maiden', 'runs', 'wickets', 'economy', 'zeros', 'fours', 'sixes', 'wides',
                     'no_balls', 'season', 'stadium', 'team', 'opposing_team'])

        for key, value in bowling_stats.items():
            temp_df = pd.DataFrame(bowling_stats[key])

            name = []
            for i in range(temp_df.shape[0]):
                name.append(key)

            temp_df.insert(0, 'bowlers', name)
            df2 = pd.concat([df2, temp_df])

        print(df2.shape)

        df1.to_csv("Data/batting_raw_data.csv",index=False)
        df2.to_csv('Data/bowling_raw_data.csv', index=False)



    # Extracting Players Meta Data
    def extract_players_metadata(self):

        path = r"Html/Players_info.html"
        with open(path, 'r') as f:
            html = f.read()

        soup = BeautifulSoup(html, 'html.parser')

        # Extract related data
        name_tag = soup.find_all('div', class_='ds-flex ds-space-x-2')
        position_tag = soup.find_all('p', class_='ds-text-tight-s ds-font-regular ds-mb-2 ds-mt-1')
        style_tag = soup.find_all('div', class_='ds-justify-between ds-text-typo-mid3')

        n = len(name_tag)
        names = []
        position = []
        batting_style = []
        bowling_style = []

        for i in range(n):
            if name_tag[i].text not in names:
                names.append(name_tag[i].text)
                position.append(position_tag[i].text)
                try:
                    batting_style.append(style_tag[i].find_all('div')[1].text)
                except:
                    batting_style.append(np.nan)
                try:
                    bowling_style.append(style_tag[i].find_all('div')[2].text)
                except:
                    bowling_style.append(np.nan)

        df = pd.DataFrame(
            {'names': names, 'position': position, 'batting_style': batting_style, 'bowling_style': bowling_style})

        # To CSV of meta data
        print(df.shape)
        df.to_csv('Data/player_raw_metadata.csv', index=False)