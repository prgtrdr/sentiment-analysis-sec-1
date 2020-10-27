import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import os
from pathlib2 import Path
import re
import shutil
import ProjectDirectory as directory

def clean_filing(input_filename, filing_type, output_filename):
    """
    Cleans a 10-K or 10-Q filing. All arguments take strings as input
    input_filename: name of the file to be cleaned
    filing_type: either 10-K or 10-Q
    outuput_filename: name of output file
    """
    
    # open file
    with open (input_filename, 'r', encoding='utf-8') as f:
        data = f.read()
    
    if 'K' in input_filename:
        # Process 10K
        print("processing 10K")

        # Remove tables that do not contain the word 'item.' This fixes docs that incorrectly
        # use tables as page headers.
        data = re.sub(pattern="(?s)(?i)<TABLE((?!Item).)*?</TABLE>", repl='', string=data)

        # Regex to find <DOCUMENT> tags
        doc_start_pattern = re.compile(r'<DOCUMENT>')
        doc_end_pattern = re.compile(r'</DOCUMENT>')
        # Regex to find <TYPE> tag prceeding any characters, terminating at new line
        type_pattern = re.compile(r'<TYPE>[^\n]+')

        doc_start_is = [x.end() for x in doc_start_pattern.finditer(data)]
        doc_end_is = [x.start() for x in doc_end_pattern.finditer(data)]
        doc_types = [x[len('<TYPE>'):] for x in type_pattern.findall(data)]

        # Create a Dictionary for the 10-K
        # 
        # In the code below, we create a dictionary which has the key `10-K` and as value the contents of the `10-K` section
        # found above. To do this, we will create a loop, to go through all the sections found above, and if the section
        # type is `10-K` then save it to the dictionary. Use the indices in  `doc_start_is` and `doc_end_is`to slice the
        # `data` file.
        document = {}

        # Create a loop to go through each section type and save only the 10-K section in the dictionary
        for doc_type, doc_start, doc_end in zip(doc_types, doc_start_is, doc_end_is):
            if doc_type == '10-K':
                document[doc_type] = data[doc_start:doc_end]

        # STEP 3 : Apply REGEXes to find Item 1A, 7, and 7A under 10-K Section 
        regex = re.compile(r'(>(Item|ITEM)(\s|&#160;|&nbsp;)(1A|1B|7A|7|8)\.{0,1})')

        # Use finditer to math the regex
        matches = regex.finditer(document['10-K'])

        # Matches
        matches = regex.finditer(document['10-K'])

        # Create the dataframe
        test_df = pd.DataFrame([(x.group(), x.start(), x.end()) for x in matches])
        test_df.columns = ['item', 'start', 'end']
        test_df['item'] = test_df.item.str.lower()

        # Get rid of unnesesary charcters from the dataframe
        test_df.replace('&#160;',' ',regex=True,inplace=True)
        test_df.replace('&nbsp;',' ',regex=True,inplace=True)
        test_df.replace(' ','',regex=True,inplace=True)
        test_df.replace('\.','',regex=True,inplace=True)
        test_df.replace('>','',regex=True,inplace=True)

        # Drop duplicates
        pos_dat = test_df.sort_values('start', ascending=True).drop_duplicates(subset=['item'], keep='last')

        # Set item as the dataframe index
        pos_dat.set_index('item', inplace=True)

        # Get Item 1a
        item_1a_raw = document['10-K'][pos_dat['start'].loc['item1a']+1:pos_dat['start'].loc['item1b']]

        # Get Item 7
        item_7_raw = document['10-K'][pos_dat['start'].loc['item7']+1:pos_dat['start'].loc['item7a']]

        # Get Item 7a
        item_7a_raw = document['10-K'][pos_dat['start'].loc['item7a']+1:pos_dat['start'].loc['item8']]

        ### First convert the raw text we have to exrtacted to BeautifulSoup object 
        item_1a_content = BeautifulSoup(item_1a_raw, 'lxml')
        item_7_content = BeautifulSoup(item_7_raw, 'lxml')
        item_7a_content = BeautifulSoup(item_7a_raw, 'lxml')

        item_1a_text = re.sub(r'\s+\d+\s+Table of Contents', ' ', item_1a_content.get_text(' '), flags=re.IGNORECASE)
        item_1a_text = re.sub(r'\n\s+\n', '', item_1a_text)
        item_7_text =  re.sub(r'\s+\d+\s+Table of Contents', ' ', item_7_content.get_text(' '), flags=re.IGNORECASE)
        item_7_text = re.sub(r'\n\s+\n', '', item_7_text)
        item_7a_text = re.sub(r'\s+\d+\s+Table of Contents', ' ', item_7a_content.get_text(' '), flags=re.IGNORECASE)
        item_7a_text = re.sub(r'\n\s+\n', '', item_7a_text)

        with open(output_filename + '_item1A', 'w', encoding='utf-8') as output:
            output.write(item_1a_text)

        with open(output_filename + '_item7', 'w', encoding='utf-8') as output:
            output.write(item_7_text)

        with open(output_filename + '_item7A', 'w', encoding='utf-8') as output:
            output.write(item_7a_text)
    else:
        # Process 10Q
        print("Processing 10Q")


def clean_all_filings():
    """Clean all filings in sec-filings directory"""
    print("cleaning...")
    
    project_dir = directory.get_project_dir()
    company_list = os.listdir(os.path.join(project_dir, 'sec-filings-downloaded'))  

    for company in company_list:
        company_dir = os.path.join(project_dir, 'sec-filings-downloaded', company)
        os.chdir(company_dir) # abs path to each company directory
        
        print('***Cleaning: {}***'.format(company))
        for file in os.listdir():  # iterate through all files in the respective company directory
            
            # cleaning files
            if file.startswith('cleaned'): 
                continue
            
            if file.endswith('10-K'): filing_type = '10-K'
            else: filing_type = '10-Q'
            
            if file.endswith('10-K') or file.endswith('10-Q'):
                clean_filing(input_filename=file, filing_type=filing_type, output_filename='cleaned_' + str(file))
                print('{} filing cleaned'.format(file))

def rename_10_Q_filings():
    """Rename 10Q filigns to include the quarter of the filing in the filing name"""
    
    project_dir = directory.get_project_dir()
    company_list = os.listdir(os.path.join(project_dir, 'sec-filings-downloaded'))  
    
    for company in company_list:
        company_dir = os.path.join(project_dir, 'sec-filings-downloaded', company)
        os.chdir(company_dir)
        
        print('***{}***'.format(company))
        for file in os.listdir():
            if file.startswith('cleaned_filings') or file.startswith('cleaned_Q'): 
                continue
                
            if file.startswith('cleaned') and file.endswith('10-Q'):
                get_date = file[8:18]
                get_year = file[8:12]
                get_month = int(file[13:15])

                if get_month >= 1 and get_month <= 5:
                    filing_quarter = 'Q1'
                elif get_month >= 6 and get_month <= 8:
                    filing_quarter = 'Q2'
                else:
                    filing_quarter = 'Q3'

                os.rename(file, ('cleaned_'+str(filing_quarter)+'_'+str(get_date)+'_'+'10-Q'))
                print('{} renamed'.format(file))
            
            else:
                print('{} not renamed'.format(file))

def move_10k_10q_to_folder():
    """Move filings to the appropriate folders in each company directory"""
    
    project_dir = directory.get_project_dir()
    
    company_list = os.listdir(os.path.join(project_dir, 'sec-filings-downloaded'))  

    for company in company_list:    
        # make directory of cleaned files
        cleaned_files_dir = os.path.join(project_dir, 'sec-filings-downloaded', company, 'cleaned_filings')
        if not os.path.exists(cleaned_files_dir): os.makedirs(cleaned_files_dir)
        
        company_dir = os.path.join(project_dir, 'sec-filings-downloaded', company)
        os.chdir(company_dir) # abs path to each company directory    
        
        print('***{}***'.format(company))
        for file in os.listdir():
            if file.startswith('cleaned_filings'): continue  # cleaned_filings directory
            if file.startswith('clean') and ('10-Q' in file or '10-K' in file):
                try:
                    shutil.move(os.path.join(company_dir, file), os.path.join(cleaned_files_dir, file))
                    print('{} moved to cleaned files folder'.format(file))
                except Exception as e:
                    os.remove(os.path.join(cleaned_files_dir, file))
                    shutil.move(os.path.join(company_dir, file), os.path.join(cleaned_files_dir, file))
                    print('{} moved to cleaned files folder'.format(file))

clean_all_filings()

rename_10_Q_filings()

move_10k_10q_to_folder()



