import edgar
import os, time
from pathlib2 import Path
import pandas as pd
import numpy as np
import requests
from tqdm import tqdm
import ProjectDirectory as directory
import re

pd.options.mode.chained_assignment = None
DOWNLOAD_FROM_EDGAR = True

# ## generate df with all companies and URLs
project_dir = directory.get_project_dir()
os.chdir(os.path.join(project_dir, 'sec-filings-index'))

# filing_year = 2020   # uncomment to run, choose year to get all edgar filings from
# edgar.download_index(os.getcwd(), filing_year)

# Get list of all DFs 
table_list = []

for i in os.listdir():
    if not os.path.isdir(i) and i.endswith('.tsv'):
        table_list.append(pd.read_csv(i, sep='|', header=None, encoding='latin-1', parse_dates=[3], dtype={0: int}))

# append all dfs into a single df
df = pd.DataFrame(columns=[0,1,2,3,4,5])   # downloaded file has 6 columns

for i in range(len(table_list)):
        df = pd.concat([df, table_list[i]], ignore_index=True, axis=0)

df.columns= ['cik', 'company_name', 'filing_type', 'filing_date', 'url', 'url2']

# Fix up company names
df['company_name'] = [re.sub(r'\s*\\.*|/.*|[\.\,]*', '', str(x)) for x in df['company_name']]

# ## Check if dataframe correctly generated
count_list = []
for i in range(len(table_list)):
    count_list.append(len(table_list[i]))

if df.shape[0] == sum(count_list):
    print('df tallies with individual files. Total rows = {}'.format(df.shape[0]))
else:
    print('ERROR. df does not tally!!')

# ## Get CIK df
# cik_ticker_list.csv contains cik tickets of companies
# df_cik = pd.read_csv(os.path.join(project_dir, 'data', 'cik_ticker_list.csv'))
# df_cik = pd.read_csv(os.path.join(project_dir, 'data', '1_analysts_202010151718.csv'))
df_cik = pd.read_csv(os.path.join(project_dir, 'data', 'market_cap_GT_1B.csv'))

def company_name_search(df, company_name_list):
    for company in company_name_list:
        df_company = df[df['Name'].str.contains(company, case=False)]
        print('*' * 50)
        print('SEARCH TERM: {}'.format(company))
        print('RESULTS:')
        for i in df_company['Name'].tolist():
            for j in df_company['CIK'].tolist():
                print(i, j)
        print('*' * 50)
        
def get_cik_from_company_name(df, company_name_list=None):
    cik_list = []
    if company_name_list is not None:
        for company in company_name_list:
            cik_series = df[df['Name'].str.contains(company, case=False)]['CIK']
            cik_list.append(cik_series.values[0])
    else:
        cik_list = df['CIK'].tolist()        
    return cik_list

def get_company_name_from_cik(df, cik_list):
    company_list = []
    for cik in cik_list:
        company_series = df[df['CIK'] == cik]
        company_list.append(company_series.values[0])
    return company_list

companies_list = ['']

# company_name_search(df_cik, companies_list)

# cik_list = get_cik_from_company_name(df_cik, companies_list)    # Just get these companies' data
cik_list = get_cik_from_company_name(df_cik)  # Get all company data

# ## download data
def download_filings(cik_num_list, from_date='2016-01-01'):
    """Function to filter the appropriate filings and download them in the folder"""
    
    project_dir = directory.get_project_dir()
    
    # filter df with company CIK,filing type (10-K and 10-Q) and date  
    df_filtered = df [(df['cik'].isin(cik_num_list)) & 
                      ((df['filing_type']=='10-K') | (df['filing_type'] == '10-Q')) & 
                      (df['filing_date'] > from_date)]
    
    company_names = df_filtered['company_name'].unique().tolist()
    
    # check if folders for each company already exists    
    sec_filings_dir = os.path.join(project_dir, 'sec-filings-downloaded')  # dir to download SEC filingsa
    os.chdir(sec_filings_dir)

    for company in company_names:
        company_dir = os.path.join(sec_filings_dir, company)

        if not os.path.exists(company_dir):
            os.makedirs(company_dir)
            print('\n created dir: {}'.format(company))
        else:
            print('\n{} directory exists'.format(company))
            
        os.chdir(company_dir)
        
        # create company specific df to iterate over    
        df_filtered_co = df_filtered[df_filtered['company_name'] == company]  # get df with the company only
        df_filtered_co['filing_date'] = df_filtered_co['filing_date'].astype(str)   # convert to 'object' to name file
        
        for i in range(len(df_filtered_co)):
            url_prefix = 'https://www.sec.gov/Archives/'
            row = df_filtered_co.iloc[i,:]
            url = url_prefix + row['url']
            
            filing_name = row['filing_date'] + str('_') + row['filing_type']
            if os.path.isfile(filing_name):
                print('{} file already exists'.format(filing_name))
            else:
                if DOWNLOAD_FROM_EDGAR:
                    print('Downloading: {}'.format(filing_name))
                    response = requests.get(url, stream=True, timeout=30)
                    with open('{}'.format(filing_name), 'wb') as handle:
                        for data in tqdm(response.iter_content()):
                            handle.write(data)
                else:
                    # Instead of downloading here, we'll move from existing directory. This should be a command line parameter
                    # Parse the filing date as '2013-03-04'
                    filing_date = row['filing_date']
                    get_date = filing_date[0:10]
                    get_year = filing_date[0:4]
                    get_month = int(filing_date[5:7])

                    if get_month >= 1 and get_month <= 3:
                        filing_quarter = 'Q1'
                    elif get_month >= 4 and get_month <= 6:
                        filing_quarter = 'Q2'
                    elif get_month >= 7 and get_month <= 9:
                        filing_quarter = 'Q3'
                    else:
                        filing_quarter = 'Q4'

                    filename = '../' + row['filing_type'] + '/' + get_year + '/' + filing_quarter + '/' + url[url.rfind('/')+1:]
                    print(f'Moving: {filename} to {filing_name}')
                    try:
                        os.rename(filename, filing_name)
                        cleaned_filename = '../' + row['filing_type'] + '/' + get_year + '/' + filing_quarter + '/' + 'cleaned_' + url[url.rfind('/')+1:]
                        cleaned_filing_name = 'cleaned_' + filing_name
                        print(f'Moving: {cleaned_filename} to {cleaned_filing_name}')
                        os.rename(cleaned_filename, cleaned_filing_name)
                    except:
                        print('Not found. Skipped.')
                        continue

# ### ↓ Automated download of filings. If the filing exists in the directory, the download will skip and move on the the next filing
download_filings(cik_list)
