# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import pandas as pd
import numpy as np
import os
from pathlib2 import Path
import re
import shutil
import ProjectDirectory as directory

# preprocess filings
import string
from nltk import word_tokenize
from nltk.stem import PorterStemmer

# to vectorize filing
from sklearn.feature_extraction.text import CountVectorizer


# # Preprocessing

# **Import stopwords from LoughranMcDonald Master Dictionary**

# %%
def import_master_dict_stopwords(stopwords_file_path = os.path.join(directory.get_project_dir(), 'master-dict', 'StopWords_Generic.txt')):
#     os.chdir(stopwords_file_dir)
#     stopwords = pd.read_csv('StopWords_Generic.txt', header=None)
    stopwords = pd.read_csv(stopwords_file_path, header=None)[0].tolist()
    stopwords = frozenset([word.lower() for word in stopwords])
    return stopwords


# %%
def preprocess_filing(text, stopwords=True, stemming=False):
    
    # remove punctuations
    punctuation_list = set(string.punctuation)
    text = ''.join(word for word in text if word not in punctuation_list)
    
    tokens = word_tokenize(text)
    
    if stopwords:
        stopwords = import_master_dict_stopwords()
        tokens = [word for word in tokens if word not in stopwords]
        tokens = [word.lower() for word in tokens]

    if stemming:
        stemmer = PorterStemmer()
        tokens = [stemmer.stem(word) for word in tokens]
                
    return tokens


# %%
def vectorize_and_preprocess_filings(filings_list):
    """vectorizes and preprocesses filings for each company"""
    
    vectorizer = CountVectorizer(tokenizer=preprocess_filing)
    X = vectorizer.fit_transform(filings_list)
    return X


# # Calculating Similarity

# %%
def calculate_consine_similarity(a, b):
    cos_sim = np.dot(a,b) / ( np.linalg.norm(a) * np.linalg.norm(b) )
    return cos_sim


# %%
project_dir = directory.get_project_dir()
company_dir_list = os.listdir(os.chdir(os.path.join(project_dir, 'sec-filings-downloaded')))

# initialize empty dataframes with appropriate cols
df_ten_k_results = pd.DataFrame(columns=['company', 'cosine_similarity', 'latest_filing_dt', 'previous_filing_dt'])
df_ten_q_results = pd.DataFrame(columns=['company', 'cosine_similarity', 'latest_filing_dt', 'latest_filing_quarter', 
                                        'previous_filing_dt', 'previous_filing_quarter'])

for company in company_dir_list:
    company_dir = os.path.join(project_dir, 'sec-filings-downloaded', company)
    os.chdir(os.path.join(company_dir, 'cleaned_filings'))
    
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
            
    # Calculate consine similarity for 10-K and append to df
    max_ten_k_year = max(ten_k_dict, key=ten_k_dict.get)
    year_before_max_ten_k = max_ten_k_year - 1

    with open(ten_k_dict[max_ten_k_year]) as file:
        latest_ten_k = file.readline()
    with open(ten_k_dict[year_before_max_ten_k]) as file:
        previous_ten_k = file.readline()

    ten_k_vec = vectorize_and_preprocess_filings([latest_ten_k, previous_ten_k])
    cosine_sim_ten_k = calculate_consine_similarity(ten_k_vec.toarray()[0], ten_k_vec.toarray()[1])
    df_ten_k_results = df_ten_k_results.append({'company': company, 
                                                'cosine_similarity': cosine_sim_ten_k, 
                                                'latest_filing_dt': ten_k_dict[max_ten_k_year][8:18],
                                                'previous_filing_dt': ten_k_dict[year_before_max_ten_k][8:18]},
                                               ignore_index=True)
    
    # calculate consine similarity for 10-Q and append to df
#     try:
    max_ten_q_quarter_year = max(ten_q_dict, key=ten_q_dict.get)
    year_before_max_ten_q = max_ten_q_quarter_year[0:3]+str(int(filing_year)-1)

    # build filename for previous 10Q (not year earlier)
    qtrs = ['Q1', 'Q2', 'Q3' ]
    max_ten_q_quarter = max_10_q_quarter_year[0:3]+str(int(filing_year)-1)
    qtr_before_max_10_q = max_10_q_quarter_year[0:3] == else 

    with open(ten_q_dict[max_ten_q_quarter_year]) as file:
        latest_ten_q = file.readline()
    with open(ten_q_dict[year_before_max_ten_q]) as file:
        previous_ten_q = file.readline()

    ten_q_vec = vectorize_and_preprocess_filings([latest_ten_q, previous_ten_q])
    consine_sim_ten_q = calculate_consine_similarity(ten_q_vec.toarray()[0], ten_q_vec.toarray()[1])
    df_ten_q_results = df_ten_q_results.append({'company': company, 
                                                'cosine_similarity': consine_sim_ten_q, 
                                                'latest_filing_dt': ten_q_dict[max_ten_q_quarter_year][11:21],
                                                'latest_filing_quarter': ten_q_dict[max_ten_q_quarter_year][8:10],
                                                'previous_filing_dt': ten_q_dict[year_before_max_ten_q][11:21],
                                                'previous_filing_quarter': ten_q_dict[year_before_max_ten_q][8:10]}, 
                                               ignore_index=True)
#     except BaseException as e:
#         print('{}: {}'.format(company, e))


# %%
df_ten_k_results


# %%
df_ten_q_results


# %%



