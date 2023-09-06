#!/usr/bin/env python
# coding: utf-8

# Given the risk climate change poses to the food production system, it is important to consider what locations are worth investing in for agricultural production.
# 
# This project will identify the 5 safest states for an agricultural investment, considering hazards and climate change.
# The data used in this project can be found at these locations:
# - https://hazards.fema.gov/nri/data-resources
# - https://projects.propublica.org/climate-migration/
# 
# Phase 1: Data Cleaning
# - Import data
# - Limit columns selected
# - Add year column (This is done for a larger follow up project)
# - Add identifier column
# - Resolve null values
# - Only select 48 contiguous states
# 
# Phase 2: Data Exploration
# - Remove states based on hazards
# - Generate columns used to identify safest investment
# 
# Phase 3: State Identification
# - Weight columns and sort rows based on result
# - Identify top 5 states to investigate further
# 
# Phase 4: (Follow up project) Further explore top 5 states over time.
# - A large portion of the data cleaning for this project was done to simplify Phase 4: (Follow up project)

# In[1]:


import pandas as pd
import glob

path = "C:/Users/14805/Desktop/Data Projects/FRI/RI-tables/"
files = glob.glob(path + '/*.csv')

#For this code, I want to generate a dataframe for each file in the home folder.
#Each dataframe named NRI_'year' will automatically be cleaned and added to the visuals when this code is run. (Phase 4)

data = {}

for f in files:
    d = pd.read_csv(f)
    key_name = f.replace('_Table_Counties', '').replace('.csv', '').replace('_Chng','')
    key_name = key_name.replace('C:/Users/14805/Desktop/Data Projects/FRI/RI-tables\\', '')
    data[key_name] = d
    
print(data.keys())
print('\n')
for key in data:
    print(key, ':', data[key].shape)


# Above is a list of the different dataframes in the newly created dictionary. The Expected_2050 dataframe was pulled from a Propublica article, and lists expectations for the severity of change a given county will likely experience by the year 2050.
# 
# For the NRI data the column and row count changes significantly in 2023. This change is from FEMA including more United States territories in the risk index than in previous years. Calculating the NRI for each county is a fairly new process for FEMA, so each year will contain some updates. For this analysis the goal will be tracking the changes in major risks, and I will be focused on only the contiguous states.

# In[2]:


#This is where I add a year column to better organize the data (Phase 4)

for key in data:
    data[key]['YEAR'] = key
    data[key]['YEAR'] = data[key]['YEAR'].str.replace('NRI_', '').str.replace('Expected_', '')
    data[key]['YEAR'] = data[key]['YEAR'].astype(int)


# Next, I will remove the columns that I will not be using for my analysis. Since I am interested in the change to risks and agricultural value over time, I will need three columns from FEMA for each risk I am interested in. I will keep the 'RISKR' column because that is the rating assigned by FEMA and will be used to identify the top 5 states in Phase 3.
# 
# For Phase 4 I will keep the annualized frequency to better compare year to year. I will also keep the risk score column as well. To calculate the risk score, and as a result their risk rating, FEMA utilizes the following formulas.
# 
# Risk Index Score = Expected Annual Loss × Social Vulnerability ÷ Community Resilience
# 
# Expected Annual Loss = Exposure × Annualized Frequency × Historical Loss Ratio
# 
# (Phase 4) To track changes to hazards over time I will need to use the annualized frequency column, as the other columns can be swayed by preparations due to awareness of a hazard, which will minimize the exposure.

# In[3]:


#This is where I create a new dictionary with only the FEMA NRI datasets.

import re

nri_select = []

for key in data:
    nri_select.append(key)
    
print('nri_select :',nri_select)

wanted = []
nri_expression = '(NRI_20)\d*'

for title in nri_select:
    if re.search(nri_expression, title):
        wanted.append(title)

nri_county = {k: data[k] for k in data.keys() & set(wanted)}

print('\n')
print(nri_county.keys())


# In[4]:


#The RISKR column will be used on this project, the RISKS and AFREQ columns will be utilized in Phase 4

keepers = ['NRI_ID', 'STATE', 'STATEABBRV', 'COUNTY', 'COUNTYTYPE', 'AGRIVALUE', 'AREA', 'RISK_SCORE', 'RISK_RATNG', 'CFLD_AFREQ', 'CFLD_RISKS', 'CFLD_RISKR',
           'CWAV_AFREQ', 'CWAV_RISKS', 'CWAV_RISKR', 'DRGT_AFREQ', 'DRGT_RISKS', 'DRGT_RISKR', 'HWAV_AFREQ', 'HWAV_RISKS',
           'HWAV_RISKR', 'HRCN_AFREQ', 'HRCN_RISKS', 'HRCN_RISKR', 'ISTM_AFREQ', 'ISTM_RISKS', 'ISTM_RISKR', 'RFLD_AFREQ',
           'RFLD_RISKS', 'RFLD_RISKR', 'TRND_AFREQ', 'TRND_RISKS', 'TRND_RISKR', 'WFIR_AFREQ', 'WFIR_RISKS', 'WFIR_RISKR',
           'WNTW_AFREQ', 'WNTW_RISKS', 'WNTW_RISKR', 'YEAR']

for key in nri_county:
    nri_county[key] = nri_county[key].loc[:, keepers]
    
for key in nri_county:
    print(key, ':', nri_county[key].shape)


# The new data frames now have the number of columns that I am looking for. Next, I need to ensure that the continental United States are the only locations selected in the dataframes. First, I am going to create a unique identifier for each line by creating a column that combines the year and the NRI_ID. That identifier will be used to more simply ensure that county's, states, and years, are not accidentally duplicated.

# In[5]:


#This identifier was created for Phase 4, but was useful with identifying duplicated data when merging dataframes in phase 3

def zfil(string):
    return str(string).zfill(2)

for key in nri_county:
    nri_county[key]['IDENTIFIER'] = nri_county[key]['NRI_ID'] + '_' + nri_county[key]['YEAR'].apply(zfil)
    
for key in nri_county:
    print(key, ':', nri_county[key]['IDENTIFIER'].info)


# In[6]:


#This is to identify the list of states in the data set

states_full = nri_county['NRI_2020']['STATE'].unique()
print(states_full)
print('\n')
print(len(states_full))


# In[7]:


#These are the states I will be using in my analysis, I printed the length to confirm the correct amount were listed.

states = ['Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan',
 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada',
 'New Hampshire', 'New Jersey', 'Indiana', 'Iowa', 'New Mexico', 'New York',
 'Kansas', 'Alabama', 'Arizona', 'Arkansas', 'California', 'Colorado',
 'Connecticut', 'Delaware', 'Florida', 'Georgia',
 'Idaho', 'Illinois', 'North Carolina', 'North Dakota', 'Ohio',
 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina',
 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia',
 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming']

print(len(states))


# In[8]:


#The NRI dataframes now only contain the columns I want, and the contiguous United States

for key in nri_county:
    nri_county[key] = nri_county[key][nri_county[key]['STATE'].isin(states)]
    
for key in nri_county:
    print(key, ':', nri_county[key].shape)


# The last thing I will need to do before merging the datasets (Phase 4) is check on the missing variables for each of the columns, and then decide how I want to resolve the Nan variables.
# 
# We can see below that even in the original data set there are some missing variables. This is because some locations do not deal with certain types of hazards, or those hazards are so infrequent they are difficult to measure. FEMA simply does not add any data for the missing disaster types. For my analysis replacing the missing data types with a 0 will be sufficient.

# In[9]:


#Checking to see if there any null values in my data

for key in nri_county:
    print(key, ':', nri_county[key].info())


# In[10]:


#Resolve all null values by replacing with 0

for key in nri_county:
    nri_county[key] = nri_county[key].fillna(0)
    
for key in nri_county:
    print(key, ':', nri_county[key].info())


# My FEMA data is now uniform and will automatically incorporate new .csv files I add to the original folder, as long as they are named correctly. Each data set now has the same state count and column count, unique identifiers, no missing values, and year columns. The rest of this project will focus strictly on Phase 3.
# 
# The dataset 'Expected_2050' was published in 2020, and because it is a predictive dataset, it is important that it is compared with a dataset from that same year. This is because data is a useful snapshot in time, and it will line up best with the data from the same time frame. The data recorded later will be used to identify if the trend predicted is playing out (Phase 4).

# In[11]:


#Examining the Expected_2050 data for the first time, we can see that the county column does not only contain the county name.
#To avoid duplicates, I will need to leave in the county type: ex - St. Louis City and St. Louis County

print(data['Expected_2050'].head())

print(data['Expected_2050'].info())


# In[12]:


#The Expected_2050 dataset had several typos in the County and State columns
#The dataset needed several suffixes removed from the county column, it had a state misspelled and, a county in the wrong state.

Expected_2050 = data['Expected_2050'].copy()

Expected_2050['County'] = Expected_2050['County'].str.replace(', \w+', '', regex=True)
Expected_2050['State'] = Expected_2050['State'].str.replace('Deleware', 'Delaware')
Expected_2050.loc[Expected_2050['County'] == 'Kalamazoo County', 'State'] = 'Michigan'

Expected_2050.head()


# In[13]:


#To simplify the results on this project I will be removing columns not needed for Phase 3
keepers = ['STATE','COUNTY', 'COUNTYTYPE', 'AGRIVALUE', 'AREA', 'CFLD_RISKR', 'CWAV_RISKR', 'DRGT_RISKR',
           'HWAV_RISKR', 'HRCN_RISKR', 'ISTM_RISKR', 'RFLD_RISKR', 'TRND_RISKR', 'WFIR_RISKR', 'WNTW_RISKR', 'YEAR']

predictive_nri = nri_county['NRI_2020'].copy()
predictive_nri = predictive_nri.loc[:, keepers]
    
print(predictive_nri.shape)


# In[14]:


#Here I will generate a unique identifier for each state county combo to simplify merging and identify missing/duplicate data.
#I will also remove unnecessary columns from the Expected_2050 Dataset

predictive_nri['STATE_COUNTY'] = predictive_nri['STATE'] + ', ' + predictive_nri['COUNTY'] + ' ' + predictive_nri['COUNTYTYPE']
predictive_nri['STATE_COUNTY'] = predictive_nri['STATE_COUNTY'].str.lower()

Expected_2050['State_county'] = Expected_2050['State'] + ', ' + Expected_2050['County']
Expected_2050['State_county'] = Expected_2050['State_county'].str.lower()
Agrivalue_2050 = Expected_2050[['State_county', 'Farm crop Yields']]

print(predictive_nri['STATE_COUNTY'].head(), predictive_nri.info())
print('\n')
print(Agrivalue_2050['State_county'].head(), Agrivalue_2050.info())


# In[15]:


#All rows merged properly, and the extra row for Washington DC was removed. This is the expected result.

Agrivalue_2050 = Agrivalue_2050.merge(predictive_nri, how='right', left_on=('State_county'), right_on=('STATE_COUNTY'))
Agrivalue_2050.info()


# In[16]:


#This identifies our agricultural value per square mile
#This value will be a better indicator of small farm investment than the total agricultural value
Agrivalue_2050['Agrivalue_SQML'] = Agrivalue_2050['AGRIVALUE']/Agrivalue_2050['AREA']

#Here I make the assumption that each integer increase in the expectation that agricultural value will be negatively impacted
#will result in a 1/10 decline in agricultural production in 2050 compared to what was produced in 2020
Agrivalue_2050['Farm_Yield_Probability'] = Agrivalue_2050['Farm crop Yields']/10

#This is the expected future value of each square mile in a county, assuming the expected loss, in 2020 dollars.
Agrivalue_2050['Expected_Value'] = Agrivalue_2050['Agrivalue_SQML']*(1-Agrivalue_2050['Farm_Yield_Probability'])

Agrivalue_2050.head()


# Tracking down a hard number for the occurrence rate of a given hazard is difficult. The National Risk Index that FEMA provides has an annualized frequency, which is a calculated measure, using several other variables. This annualized frequency is also used to generate a counties Risk Score, which is then sorted and grouped in the Risk Rating column for each hazard.
# 
# FEMA's process on how these variables are created can be found here:
# https://hazards.fema.gov/nri/understanding-scores-ratings
# 
# Many of these variables used to calculate the risk scores are not something I have access to. As a result for this analysis, I will need to trust FEMA's analysis that the RISKR column accurately represents the risk a county faces from a given hazard. This column will be used to remove counties that are a greater risk to producing agricultural goods.

# For each of the following categories the following ratings are available:
# 
# 'No Rating', 'Very Low', 'Relatively Low', 'Relatively Moderate', 'Relatively High', 'Very High'
# 
# The risk rating used will remove all risk ratings higher than the chosen rating.
# 
# CFLD_RISKR
# - I want to ensure that the expected sea level rise, which will increase floodings, will not impact the surrounding infrastructure, I will use a 'Relatively Low' risk rating.
# 
# 
# 
# CWAV_RISKR
# - Cold waves have a severe impact on crops, in particular livestock and trees, but are common across the states. I will use a 'Relatively Moderate' risk rating.
# 
# 
# 
# DRGT_RISKR
# - Drought conditions can change and can often be offset if a viable water source is nearby. I will use a 'Relatively Moderate' risk rating.
# 
# 
# 
# HWAV_RISKR
# - Heat waves are not as damaging to crops as cold waves are, but still pose a risk to livestock. I will use a 'Relatively Low' risk rating.
# 
# 
# 
# HRCN_RISKR
# - Hurricane and the storms they blow in can be quite severe in nature, however they also bring rainfall. I will use a 'Relatively Moderate' risk rating.
# 
# 
# 
# ISTM_RISKR
# - Ice storms receives the same rating as cold waves because these could cause sever damage. I will use a 'Relatively Moderate' risk rating.
# 
# 
# 
# RFLD_RISKR
# - Ravine flooding can often be offset by choosing the correct location, as a result only counties with a severe risk will be removed. I will use a 'Realtively High' risk rating.
# 
# 
# 
# TRND_RISKR
# - Tornados can cause severe damage to property, and for this reason any counties with a relatively high risk will be avoided. I will use a 'Relatively High' risk rating.
# 
# 
# 
# WFIR_RISKR
# - Steps can be taken to mitigate the damage from wildfires, however locations with too high of a risk will be removed. I will use a 'Relatively High' risk rating.
# 
# 
# 
# WNTW_RISKR
# - Winter weather cannot be escaped, and this category includes snow. Locations with severe winter weather will be avoided. I will use a 'Relatively High' risk rating.
# 
# 
# Next, I will create a new dataframe that removes counties based on the hazard risk rating identified above. Following this, I will identify the percentage of each state that passes through this hazard filter. That percentage will then be multiplied against the average agricultural value per square mile for that state, to identify the top 5 states for further analysis.

# In[17]:


State_area = pd.DataFrame(Agrivalue_2050['AREA'].groupby(by=Agrivalue_2050['STATE']).sum().reset_index())
State_avg_value = pd.DataFrame(Agrivalue_2050['Expected_Value'].groupby(by=Agrivalue_2050['STATE']).mean().reset_index())

Agrivalue_2050['Expected_Value'].describe()

#These are the summary statistics for the expected future agricultural value per squaremile, the 'Expected_Value column'
#I Will use the mean value to remove the lower quality candidates.


# In[18]:


#This is where I will identify the counties with accepted risk ratings

moderate = ['No Rating', 'Very Low', 'Relatively Low', 'Relatively Moderate']
r_low = ['No Rating', 'Very Low', 'Relatively Low']
r_high = ['No Rating', 'Very Low', 'Relatively Low', 'Relatively Moderate', 'Relatively High']

Agrivalue_2050 = Agrivalue_2050[Agrivalue_2050['CFLD_RISKR'].isin(r_low)]
Agrivalue_2050 = Agrivalue_2050[Agrivalue_2050['CWAV_RISKR'].isin(moderate)]
Agrivalue_2050 = Agrivalue_2050[Agrivalue_2050['DRGT_RISKR'].isin(moderate)]
Agrivalue_2050 = Agrivalue_2050[Agrivalue_2050['HWAV_RISKR'].isin(moderate)]
Agrivalue_2050 = Agrivalue_2050[Agrivalue_2050['HRCN_RISKR'].isin(r_low)]
Agrivalue_2050 = Agrivalue_2050[Agrivalue_2050['ISTM_RISKR'].isin(moderate)]
Agrivalue_2050 = Agrivalue_2050[Agrivalue_2050['RFLD_RISKR'].isin(r_high)]
Agrivalue_2050 = Agrivalue_2050[Agrivalue_2050['TRND_RISKR'].isin(r_high)]
Agrivalue_2050 = Agrivalue_2050[Agrivalue_2050['WFIR_RISKR'].isin(r_high)]
Agrivalue_2050 = Agrivalue_2050[Agrivalue_2050['WNTW_RISKR'].isin(r_high)]


# In[19]:


#I will look at all counties with an Expected_Value above the average of $86,785.

Agrivalue_2050 = Agrivalue_2050[Agrivalue_2050['Expected_Value'] >= 86785]
Agrivalue_2050.info()

#Following this approach, there are 33 counties that pass through our filter for state identification.


# In[20]:


new_area = pd.DataFrame(Agrivalue_2050['AREA'].groupby(by=Agrivalue_2050['STATE']).sum().reset_index())
new_area.rename(columns={'AREA': 'NEW_AREA'}, inplace=True)
print(new_area)


# In[21]:


#Here I merged the different value counts dataframes to one dataframe, used to identify the top 5 states.

State_area = State_area.merge(new_area, how='right', left_on='STATE', right_on='STATE')
State_area['Percent_Ideal'] = State_area['NEW_AREA']/State_area['AREA']
State_area = State_area.merge(State_avg_value, how='left', left_on='STATE', right_on='STATE')
State_area.sort_values(by=['Expected_Value'], ignore_index=True, ascending=False, inplace=True)
print(State_area.head())


# In[22]:


#According my analyses, these are the top 5 state to consider investing in a small farm.

State_area['Weighted_Expectation'] = State_area['Percent_Ideal'] * State_area['Expected_Value']
State_area.sort_values(by=['Weighted_Expectation'], ignore_index=True, ascending=False, inplace=True)
State_area.head()


# In[23]:


import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')
plotted_5 = State_area.head()

plt.bar(plotted_5['STATE'], plotted_5['Weighted_Expectation'])
plt.show


# ## Next Steps
# 
# For the next phase of this project (Phase 4) I will look at the top 5 states identified in this intial analyses, and see if there is a clear front runner. Removing the counties with the risk level analysis most certaintly helps us identify less risky investment locations, and prioritizing those investments based on the agricultural value ensure that the top 5 states have the value desired. This initial analyses is very restrictive, and is limited, in it's ability to dientify a state we should ivnest in. It can identify states to consider further, but to compare those states side by side is what will happen in pahse 4. For example, while in this anakysis Delware is the clear winner, it is also surrounded by ocean, is limited in area, and has a realtively high cost of living. All of those factors will play a role in the feasability and return on an agricultural investment. This is not something this analysis can take into consdieration.
