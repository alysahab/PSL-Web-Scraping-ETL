from Data_Extraction import DataExtract as DE
from Data_Transformation import Transformation
from Load_Data import Load

# EXTRACT
# Extracting raw data from website
# Object
extractData = DE()

# Extracting Html Content
extractData.concurrentExtraction()

# Extract batting and bowling record
extractData.extract_players_stats()

# Extract players meta data
extractData.extract_players_metadata()

#TRANSFORM
#Cleaning and Transform the data
transform = Transformation()

# Clean and transform batting records then export in csv
transform.clean_batting_record()

# Clean and transform bowling records then export in csv
transform.clean_bowling_record()

# Clean and transform Players metadata dataset
transform.clean_players_metadata()

# LOAD
# load cleaned data into cloud (AWS RDS)
load = Load()
load.load_data()