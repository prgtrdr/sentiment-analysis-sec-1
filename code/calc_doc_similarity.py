import pandas as pd
import numpy as np
import os
from pathlib2 import Path
import re
import shutil
import ProjectDirectory as directory
import json

# preprocess filings
import string
from nltk import word_tokenize
from nltk.stem import PorterStemmer

# to vectorize filing
from sklearn.feature_extraction.text import CountVectorizer

PROCESS_10K = False
PROCESS_10Q = True

items_10K = [
    'item 1',    #0
    'item 1a',   #1
    'item 1b',   #2
    'item 2',    #3
    'item 3',    #4
    'item 3a',   #5
    'item 4',    #6
    'item 4a',   #7
    'item 5',    #8
    'item 5a',   #9
    'item 6',    #10
    'item 7',    #11
    'item 7a',   #12
    'item 8',    #13
    'item 8a',   #14
    'item 8b',   #15
    'item 9',    #16
    'item 9a',   #17
    'item 9b',   #18
    'item 10',   #19
    'item 11',   #20
    'item 12',   #21
    'item 12a',  #22
    'item 12b',  #23
    'item 13',   #24
    'item 14',   #25
    'item 15',   #26
    'item 15a',  #27
    'item 15b',  #28
    'item 16'    #29
]

items_10Q = [
    'item 1',   #0
    'item 2',   #1
    'item 3',   #2
    'item 4',   #3
    'item 21',  #4
    'item 21a', #5
    'item 22',  #6
    'item 23',  #7
    'item 24',  #8
    'item 25',  #9
    'item 26'   #10
]

# # Preprocessing

# **Import stopwords from LoughranMcDonald Master Dictionary**

def import_master_dict_stopwords(stopwords_file_path = os.path.join(directory.get_project_dir(), 'master-dict', 'StopWords_Generic.txt')):
#     os.chdir(stopwords_file_dir)
#     stopwords = pd.read_csv('StopWords_Generic.txt', header=None)
    stopwords = pd.read_csv(stopwords_file_path, header=None)[0].tolist()
    stopwords = frozenset([word.lower() for word in stopwords])
    return stopwords


def preprocess_filing(text, stopwords=True, stemming=False):
    
    # remove punctuations
    punctuation_list = set(string.punctuation)
    text = ''.join(word for word in text if word not in punctuation_list)
    
    # Remove n-digit numbers (perhaps do this during clean?)
    text = re.sub(pattern=r'\b\d+\b', repl='', string=text)
    
    tokens = word_tokenize(text)
    
    if stopwords:
        stopwords = import_master_dict_stopwords()
        tokens = [word for word in tokens if word not in stopwords]
        tokens = [word.lower() for word in tokens]

    if stemming:
        stemmer = PorterStemmer()
        tokens = [stemmer.stem(word) for word in tokens]
                
    return tokens


def vectorize_and_preprocess_filings(filings_list):
    """vectorizes and preprocesses filings for each company"""
    
    vectorizer = CountVectorizer(tokenizer=preprocess_filing)
    X = vectorizer.fit_transform(filings_list)
    return X


# # Calculating Similarity
def calculate_cosine_similarity(a, b):
    cos_sim = np.dot(a,b) / ( np.linalg.norm(a) * np.linalg.norm(b) )
    return cos_sim


project_dir = directory.get_project_dir()
company_dir_list = os.listdir(os.chdir(os.path.join(project_dir, 'sec-filings-downloaded')))

# initialize empty dataframes with appropriate cols
df_ten_k_results = pd.DataFrame(columns=['company', 'comp_URL', 'cosine_similarity', 'latest_filing_dt', 'previous_filing_dt'] + items_10K)
df_ten_q_results = pd.DataFrame(columns=['company', 'comp_URL', 'cosine_similarity', 'latest_filing_dt', 'latest_filing_quarter', 
                                        'previous_filing_dt', 'previous_filing_quarter'] + items_10Q)

companies_done = 0
for company in company_dir_list:
    # if 'INTERNATIONAL BUSINESS' not in company:
    #     continue
    company_dir = os.path.join(project_dir, 'sec-filings-downloaded', company)
    os.chdir(os.path.join(company_dir, 'cleaned_filings'))
    companies_done += 1
    
    ten_k_dict = {}
    ten_q_dict = {}
    
    for file in os.listdir():
        if file.endswith('10-K'): 
            filing_year = int(file[8:12])
            ten_k_dict[filing_year] = file
            
        if file.endswith('10-Q'):
            filing_quarter = str(file[8:10])
            filing_year = file[11:15]            
            ten_q_dict[str(filing_quarter) + '_' + filing_year] = file

    if PROCESS_10K:            
        # Calculate cosine similarity for 10-K and append to df
        try:
            max_ten_k_year = max(ten_k_dict, key=ten_k_dict.get)
            year_before_max_ten_k = max_ten_k_year - 1
            print(f'{companies_done}: Calc 10-K sim {company}: {max_ten_k_year} vs {year_before_max_ten_k}')

            with open(ten_k_dict[max_ten_k_year], 'r', encoding='utf-8') as file:
                latest_ten_k_header_data = file.readline()
                latest_ten_k_header = json.loads(latest_ten_k_header_data)
                latest_ten_k = file.read()
            with open(ten_k_dict[year_before_max_ten_k], 'r', encoding='utf-8') as file:
                previous_ten_k_header_data = file.readline()
                previous_ten_k_header = json.loads(previous_ten_k_header_data)
                previous_ten_k = file.read()

            # Calculate similarity for entire document
            ten_k_vec = vectorize_and_preprocess_filings([latest_ten_k, previous_ten_k])
            cosine_sim_ten_k = calculate_cosine_similarity(ten_k_vec.toarray()[0], ten_k_vec.toarray()[1])

            ten_k_result_dict = {
                'company': company,
                # 'comp_URL': f'https://docoh.com/filing/{previous_ten_k_header["CIK"]}/{previous_ten_k_header["edgar_accession"]}/diff/{latest_ten_k_header["edgar_accession"]}',
                'comp_URL': f'https://localhost:8000/abcomp/{previous_ten_k_header["CIK"]}/{previous_ten_k_header["edgar_accession"]}/{previous_ten_k_header["edgar_filename"]}/{latest_ten_k_header["edgar_accession"]}/{latest_ten_k_header["edgar_filename"]}',
                'latest_filing_dt': ten_k_dict[max_ten_k_year][8:18],
                'previous_filing_dt': ten_k_dict[year_before_max_ten_k][8:18],
                'cosine_similarity': cosine_sim_ten_k
            }

            # Split each document into individual sections (items)
            latest_ten_k_sections = dict((k.lower(), v) for k,v in dict(x.split(".",1) for x in filter(None, latest_ten_k.split('Â°'))).items())
            previous_ten_k_sections = dict((k.lower(), v) for k,v in dict(x.split(".",1) for x in filter(None, previous_ten_k.split('Â°'))).items())

            # Calculate similarity for each individual section
            for latest_section, latest_text in latest_ten_k_sections.items():
                if latest_section not in previous_ten_k_sections:
                    continue
                else:
                    ten_k_vec = vectorize_and_preprocess_filings([latest_text, previous_ten_k_sections[latest_section]])
                    cosine_sim_ten_k = calculate_cosine_similarity(ten_k_vec.toarray()[0], ten_k_vec.toarray()[1])
                    ten_k_result_dict[latest_section] = cosine_sim_ten_k
                    ten_k_result_dict[(latest_section+'_lwc')] = len(word_tokenize(latest_text))
                    ten_k_result_dict[(latest_section+'_pwc')] = len(word_tokenize(previous_ten_k_sections[latest_section]))

            df_ten_k_results = df_ten_k_results.append(ten_k_result_dict, ignore_index=True)
        except BaseException as e:
            print('Exception during 10-K calc for {}: {}'.format(company, e))
            continue

    # calculate cosine similarity for 10-Q and append to df
    if PROCESS_10Q:
        try:
            max_ten_q_quarter_year = max(ten_q_dict, key=ten_q_dict.get)
            year_before_max_ten_q = max_ten_q_quarter_year[0:3]+str(int(filing_year)-1)
            print(f'{companies_done}: Calc 10-Q sim {company}: {max_ten_q_quarter_year} vs {year_before_max_ten_q}')

            with open(ten_q_dict[max_ten_q_quarter_year], 'r', encoding='utf-8') as file:
                latest_ten_q_header_data = file.readline()
                latest_ten_q_header = json.loads(latest_ten_q_header_data)
                latest_ten_q = file.read()
            with open(ten_q_dict[year_before_max_ten_q], 'r', encoding='utf-8') as file:
                previous_ten_q_header_data = file.readline()
                previous_ten_q_header = json.loads(previous_ten_q_header_data)
                previous_ten_q = file.read()

            ten_q_vec = vectorize_and_preprocess_filings([latest_ten_q, previous_ten_q])
            cosine_sim_ten_q = calculate_cosine_similarity(ten_q_vec.toarray()[0], ten_q_vec.toarray()[1])

            ten_q_result_dict = {
                'company': company,
                'comp_URL': f'https://docoh.com/filing/{previous_ten_q_header["CIK"]}/{previous_ten_q_header["edgar_accession"]}/diff/{latest_ten_q_header["edgar_accession"]}',
                'latest_filing_dt': ten_q_dict[max_ten_q_quarter_year][11:21],
                'latest_filing_quarter': ten_q_dict[max_ten_q_quarter_year][8:10],
                'previous_filing_dt': ten_q_dict[year_before_max_ten_q][11:21],
                'previous_filing_quarter': ten_q_dict[year_before_max_ten_q][8:10],
                'cosine_similarity': cosine_sim_ten_q
            }

            # Split each document into individual sections (items)
            latest_ten_q_sections = dict((k.lower(), v) for k,v in dict(x.split(".",1) for x in filter(None, latest_ten_q.split('Â°'))).items())
            previous_ten_q_sections = dict((k.lower(), v) for k,v in dict(x.split(".",1) for x in filter(None, previous_ten_q.split('Â°'))).items())

            # Calculate similarity for each individual section
            for latest_section, latest_text in latest_ten_q_sections.items():
                if latest_section not in previous_ten_q_sections:
                    continue
                else:
                    ten_q_vec = vectorize_and_preprocess_filings([latest_text, previous_ten_q_sections[latest_section]])
                    cosine_sim_ten_q = calculate_cosine_similarity(ten_q_vec.toarray()[0], ten_q_vec.toarray()[1])
                    ten_q_result_dict[latest_section] = cosine_sim_ten_q
                    ten_q_result_dict[(latest_section+'_lwc')] = len(word_tokenize(latest_text))
                    ten_q_result_dict[(latest_section+'_pwc')] = len(word_tokenize(previous_ten_q_sections[latest_section]))

            df_ten_q_results = df_ten_q_results.append(ten_q_result_dict, ignore_index=True)
        except BaseException as e:
            print('Exception during 10-Q calc for {}: {}'.format(company, e))
            continue
if PROCESS_10K:
    df_ten_k_results.to_csv('../../../data/ten_k_results.csv', encoding='utf-8', index=False)

if PROCESS_10Q:
    df_ten_q_results.to_csv('../../../data/ten_q_results.csv', encoding='utf-8', index=False)
