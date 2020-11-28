#
#   Secondary parsing method for 10-Q filings.
#
#       Rather than trying to find each "Item" string in the document (some filings omit these strings, especially
#       for Item 1), scan the Index or Table of Contents usually found at the beginning of the document. Use that
#       as a map for each section.

import os
import glob
from pathlib2 import Path
import re
import shutil
import ProjectDirectory as directory
from bs4 import BeautifulSoup
import unicodedata
import pandas as pd
from html.parser import HTMLParser
from io import StringIO

CLEAN_10K = False
CLEAN_10Q = True
WRITE_OUTPUT_FILE = False
SECTION_MARKER = 'Â°'

# Strip out HTML from string
class MLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs= True
        self.text = StringIO()
    def handle_data(self, d):
        self.text.write(d)
    def get_data(self):
        return self.text.getvalue()

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()

def clean_filing(input_filename, filing_type, output_filename):
    """
    Cleans a 10-K or 10-Q filing. All arguments take strings as input
    input_filename: name of the file to be cleaned
    filing_type: either 10-K or 10-Q
    output_filename: name of output file
    """
    # open file
    with open (input_filename, 'r', encoding='utf-8') as f:
        data = f.read()

    data = unicodedata.normalize("NFKD", data)

    # Extract EDGAR CIK and filename
    CIK = re.search(r'(?:CENTRAL INDEX KEY:\s+)(\d+)', data, re.IGNORECASE)[1]
    edgar_accession = re.search(r'(?:ACCESSION NUMBER:\s+)([\d-]+)', data, re.IGNORECASE)[1]
    edgar_filename = re.search(r'(?:<FILENAME>)(.+)\n', data, re.IGNORECASE)[1]
    EDGAR_PATH = f'https://www.sec.gov/Archives/edgar/data/{CIK}/{edgar_accession.replace("-", "")}/{edgar_filename}'
    print(f'Parsing {EDGAR_PATH}')

    if filing_type == '10-Q':
        # Step 1. Remove all the encoded sections
        data = re.sub(r'<DOCUMENT>\n<TYPE>GRAPHIC.*?</DOCUMENT>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<DOCUMENT>\n<TYPE>ZIP.*?</DOCUMENT>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<DOCUMENT>\n<TYPE>EXCEL.*?</DOCUMENT>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<DOCUMENT>\n<TYPE>JSON.*?</DOCUMENT>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<DOCUMENT>\n<TYPE>PDF.*?</DOCUMENT>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<DOCUMENT>\n<TYPE>XML.*?</DOCUMENT>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<DOCUMENT>\n<TYPE>EX.*?</DOCUMENT>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<ix:header.*?</ix:header>', '', data, flags=re.S | re.A | re.I )

        soup = BeautifulSoup(data, 'html.parser')

        index_table_hdr = soup.find(string=re.compile('(?i)Part.*?I'))
        if not index_table_hdr:
            print('Could not find Index table header')
            return
        index_table = index_table_hdr.find_parent('table')
        if index_table.name != 'table':
            print(f"index_table tag is not table, it's {index_table.name}")
            return

        # Found the index table. Iterate through each line and find proper anchor tag references
        contents_df = pd.DataFrame(columns = ['Item', 'Begin_tag', 'Begin_line']) 
        in_part = 1     # initially in Part I
        for row in index_table.find_all('tr'):
            if row.find(string=re.compile('(?i)Part.*?II')):
                in_part = 2

            # First get the item text, if any. Clean it up depending on what Part it's in.
            item_text = row.find(string=re.compile('(?i)item'))
            if not item_text:
                print(f'item text not in row:\n{row}')
                continue    # Here we should look to see if related to previous item
            if in_part == 1:
                item_text = re.sub(r'item\s+(\d+)(a|b)?\.?', 'item \\1\\2.', item_text, flags=re.IGNORECASE)
            else:
                item_text = re.sub(r'item\s+(\d+)(a|b)?\.?', 'item 2\\1\\2.', item_text, flags=re.IGNORECASE)

            # Found an item, look for an anchor
            item_anchor = row.find('a')
            if item_anchor:
                anchor_goto = item_anchor['href'][1:]
                found_anchor = soup.find('a', {"id":anchor_goto})
                if not found_anchor:
                    found_anchor = soup.find('a', {"name":anchor_goto})
                    if not found_anchor:
                        print(f'Could not find anchor target for {anchor_goto}.')
                        # Save item name and placeholder for anchor element in dataframe
                        contents_df = contents_df.append({'Item': item_text}, ignore_index=True)
                        return
                    # Save item_text and found_anchor element in dataframe
                    contents_df = contents_df.append({'Item': item_text, 'Begin_tag': found_anchor, 'Begin_line': found_anchor.sourceline}, ignore_index=True)
            else:
                print(f'Item anchor not found in row={row}')

        with open(output_filename, 'w', encoding='utf-8') as output:
            for i in range(0, len(contents_df)):
                start = contents_df['Begin_line'][i]
                stop = 1000000 if i+1 == len(contents_df) else contents_df['Begin_line'][i+1]-1
                aggregate_text = '\n'.join(data.splitlines()[start: stop])
                aggregate_text = re.sub(r'<TABLE.*?</TABLE>', repl='', string=aggregate_text, flags=re.S | re.A | re.IGNORECASE)
                aggregate_text = strip_tags(aggregate_text)
                aggregate_text = re.sub(r'\s*\d*\s*(Table of Contents|PART I).*?\n+', ' ', aggregate_text, flags=re.DOTALL| re.IGNORECASE)
                aggregate_text = re.sub(r'\n\s*\d*\s*\n', '\n', aggregate_text)
                print(f"Item={contents_df['Item'][i]}\n*************************\n{SECTION_MARKER}{aggregate_text}\n************************\n")
                if WRITE_OUTPUT_FILE:
                    output.write(SECTION_MARKER + aggregate_text)

def clean_all_filings():
    """Clean all filings in sec-filings directory"""
    print("cleaning...")
    
    project_dir = directory.get_project_dir()

    company_list = os.listdir(os.path.join(project_dir, 'sec-filings-downloaded'))  

    for company in company_list:
        # DEBUGGING PURPOSES *************************
        # if 'INTERNATIONAL BUSINESS' not in company:
        #     continue

        company_dir = os.path.join(project_dir, 'sec-filings-downloaded', company)
        os.chdir(company_dir) # abs path to each company directory

        print('***Cleaning: {}***'.format(company))
        for file in os.listdir():  # iterate through all files in the respective company directory
            
            # cleaning files
            if 'error' not in file or file.endswith('txt'): 
                continue

            file = re.sub(r'error_(NFI_)?(seq_)?cleaned_', '', file )
            
            if file.endswith('10-K'): filing_type = '10-K'
            else: filing_type = '10-Q'
            
            if (CLEAN_10K and file.endswith('10-K')) or (CLEAN_10Q and file.endswith('10-Q')):
                clean_filing(input_filename=file, filing_type=filing_type, output_filename='cleaned_' + str(file))
                print('{} filing cleaned'.format(file))

def rename_10_Q_filings():
    """Rename 10Q filings to include the quarter of the filing in the filing name"""
    
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
                # get_year = file[8:12]
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
                except:
                    os.remove(os.path.join(cleaned_files_dir, file))
                    shutil.move(os.path.join(company_dir, file), os.path.join(cleaned_files_dir, file))
                    print('{} moved to cleaned files folder'.format(file))

# Mainline code execution
clean_all_filings()
#rename_10_Q_filings()
#move_10k_10q_to_folder()