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
import html
import math
import json


CLEAN_10K = False
CLEAN_10Q = True
OVERWRITE_EXISTING = True  # If True, overwrite existing cleaned files, else skip
WRITE_OUTPUT_FILE = True    # Write the clean_ output file, else just print
SECTION_MARKER = 'Â°'
COMPANY_SCAN_LIST = ['']   # List of company name strings to limit parse, e.g., ['ABBOTT', 'AMERICAN FINANCIAL']
COMPANY_SCAN_CONTINUE = True        # If True, continue scanning when done with first company in list

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

"""
    Function to identify removable tables. Methodology suggested by
    Loughran-MacDonald https://sraf.nd.edu/data/stage-one-10-x-parse-data/
"""
def tablerep(matchobj):
    text = matchobj.group(0)

    # Strip out all html
    text = strip_tags(text)

    # Count the letters and numbers
    numbers = sum(c.isdigit() for c in text)
    letters = sum(c.isalpha() for c in text)
    divisor = letters + numbers
    if divisor == 0:
        return ''

    # If less than 8% of the chars in the table are numbers, keep it, else delete
    if (numbers / divisor) < 0.1 or 'Item 1' in text: 
        return(re.sub(r'</td>', '\n', text, re.IGNORECASE | re.DOTALL))
#        return matchobj.group(0)
    else:
        return ''

def delete_repeated_item(index_text, section_text):
    """
    Compare two strings without regard to whitespace characters
    Input: two strings to be compared
    Output: 0 if strings do not match, else position of last matching character in section_text
    """
    index_text_split = index_text.split()
    if index_text_split[0] == 'item':
        index_text_split[0] += r'(s)?'   # Allow for the text to contain 'Items' or 'Item'
    else:
        index_text_split[0] = re.escape(index_text_split[0])
    index_text_split[1] = re.sub(r'2?(\d.*)', r'\1', index_text_split[1])
    index_text_template = r'\s*' + r'\s*'.join(index_text_split)
    index_text_match = re.match(index_text_template, section_text, flags=re.IGNORECASE)
    if not index_text_match:
        return 0
    else:
        return index_text_match.end()

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

    header_data = {
        "CIK": CIK,
        "edgar_accession": edgar_accession,
        "edgar_filename": edgar_filename
    }

    if filing_type == '10-Q':
        # Step 1. Remove all the encoded sections
        data = re.sub(r'<DOCUMENT>\n<TYPE>GRAPHIC.*?</DOCUMENT>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<DOCUMENT>\n<TYPE>ZIP.*?</DOCUMENT>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<DOCUMENT>\n<TYPE>EXCEL.*?</DOCUMENT>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<DOCUMENT>\n<TYPE>JSON.*?</DOCUMENT>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<DOCUMENT>\n<TYPE>PDF.*?</DOCUMENT>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<PDF.*?</PDF>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<DOCUMENT>\n<TYPE>XML.*?</DOCUMENT>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<DOCUMENT>\n<TYPE>EX.*?</DOCUMENT>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<ix:header.*?</ix:header>', '', data, flags=re.S | re.A | re.I )

        data = html.unescape(data)    # This function doesn't convert all the characters as needed, like xa0 and apostrophes
        data = data.replace('\xa0', ' ')
        data = data.replace('\u200b', ' ')
        data = data.replace("’", "'")
        data = data.replace('“', '"')
        data = data.replace('”', '"')

        soup = BeautifulSoup(data, 'html.parser')

        index_table_hdr = soup.find(string=re.compile(r'(?i)^\s*Part\s+I'))
        if not index_table_hdr:
            error_text = f'{EDGAR_PATH}\nCould not find Index table header'
            print(error_text)
            with open('error_not_cleaned_ITH_' + input_filename, 'w') as f:
                f.write(error_text)                
            return
        index_table = index_table_hdr.find_parent('table')
        if not index_table:
            # Alternate method of finding the index table
            for index_table in soup.find_all('table'):
                if len(index_table.find_all(string=re.compile(r'(?i)^\s*item\s+(\d+)(a|b)?\.?'))) > 2:
                    break
            else:
                error_text = f"{EDGAR_PATH}\ncould not find index_table parent tag."
                print(error_text)
                with open('error_not_cleaned_ITT_' + input_filename, 'w') as f:
                    f.write(error_text)                
                return

        # Found the index table. Create a dataframe to store locations of data items.
        contents_df = pd.DataFrame(columns = ['Item', 'Begin_tag', 'Begin_line', 'Begin_pos']) 
        contents_df['Begin_tag'] = contents_df['Begin_tag'].astype(str)
        contents_df[['Begin_line', 'Begin_pos']] = contents_df[['Begin_line', 'Begin_pos']].astype(float)

        # Iterate through each line and find proper anchor tag references
        in_part = 1     # initially in Part I
        for row in index_table.find_all('tr'):
            if row.find(string=re.compile('(?i)Part.*?II')):
                in_part = 2

            # First get the item text, if any. Clean it up depending on what Part it's in.
            item_text = row.find(string=re.compile(r'(?i)item\s+\d'))
            if item_text:
                if in_part == 1:
                    item_text = re.sub(r'item\s+(\d+)(a|b)?\.?', 'item \\1\\2.', item_text, flags=re.IGNORECASE)
                else:
                    item_text = re.sub(r'item\s+(\d+)(a|b)?\.?', 'item 2\\1\\2.', item_text, flags=re.IGNORECASE)

                # Found an item, look for a usable anchor
                for item_anchor in row.find_all('a'):
                    anchor_goto = item_anchor.get('href')[1:]
                    found_anchor = soup.find(id=anchor_goto)
                    if not found_anchor:
                        found_anchor = soup.find(attrs={"name":anchor_goto})
                        if not found_anchor:
                            continue    # No match for this anchor, try more anchors if exist
                    # If we got here we have a viable anchor. Save it in dataframe
                    contents_df = contents_df.append({
                        'Item': item_text,
                        'Begin_tag': found_anchor,
                        'Begin_line': found_anchor.sourceline,
                        'Begin_pos': found_anchor.sourcepos
                    }, ignore_index=True)
                    break   # Done with this row
                else:   # Unique Pythonic for-else
                    # Valid anchor not found for this row. Save item name and placeholder for anchor element
                    contents_df = contents_df.append({
                        'Item': item_text,
                        'Begin_tag': None,
                        'Begin_line': float('nan'),
                        'Begin_pos': float('nan')
                    }, ignore_index=True)
            else:
                # Did not find an item in this row. If there's something missing from previous row
                # see if we can fill in the blanks.
                if len(contents_df) > 0 and math.isnan(contents_df.iloc[len(contents_df)-1,2]):
                    # Found an item, look for a usable anchor
                    for item_anchor in row.find_all('a'):
                        anchor_goto = item_anchor.get('href')[1:]
                        found_anchor = soup.find(id=anchor_goto)
                        if not found_anchor:
                            found_anchor = soup.find(attrs={"name":anchor_goto})
                            if not found_anchor:
                                continue    # No match for this anchor, try more anchors if exist
                        # If we got here we have a viable anchor. Save it in dataframe
                        contents_df.iloc[len(contents_df)-1, 1:4] = [
                            found_anchor.text,
                            found_anchor.sourceline,
                            found_anchor.sourcepos
                        ]
                        break   # Found valid anchor in this row
                    else:   # Unique Pythonic for-else
                        continue   # No item and no anchor found in this row
                else:
                    continue    # No item and no previous row item

        # Clean up the contents dataframe. If we couldn't find a section, remove it.
        contents_df = contents_df.dropna()
        contents_df = contents_df.reset_index(drop=True)

        # Extract the identified section and write to output file
        try:
            contents_df[['Begin_line', 'Begin_pos']] = contents_df[['Begin_line', 'Begin_pos']].astype(int)
        except:
            # ** Should never reach this code
            error_text = f"Couldn't find some tags for {EDGAR_PATH}.\n{contents_df}"
            print(error_text)
            with open('error_not_cleaned_MT_' + input_filename, 'w') as f:
                f.write(error_text)                
            return  # Probably means we couldn't find tags, abort this file

        with open(output_filename, 'w', encoding='utf-8') as output:
            output.write(json.dumps(header_data) + '\n')
            for i in range(0, len(contents_df)):
                # Use Beautiful Soup sourceline and sourcepos to determine where text is in the main buffer
                sl = data.splitlines()
                start = contents_df['Begin_line'][i]-1
                stop = len(sl)-1 if i+1 == len(contents_df) else contents_df['Begin_line'][i+1]-1

                # Must do the end slice first in case same line
                sl[stop] = sl[stop] if i+1 == len(contents_df) else sl[stop][:contents_df['Begin_pos'][i+1]]
                sl[start] = sl[start][contents_df['Begin_pos'][i]:]
                aggregate_text = '\n'.join(sl[start: stop+1])

                # Clean the text
                aggregate_text = re.sub(r'<TABLE.*?</TABLE>', repl=tablerep, string=aggregate_text, flags=re.S | re.A | re.IGNORECASE)
                aggregate_text = re.sub(r'<div.*?>', '\n', aggregate_text, flags=re.IGNORECASE)
                aggregate_text = strip_tags(aggregate_text)
                aggregate_text = re.sub(r'\s*\d*\s*Table of Contents', ' ', aggregate_text, flags=re.DOTALL| re.IGNORECASE | re.MULTILINE)
                # Delete repetitive item text at beginning
                aggregate_text = aggregate_text[delete_repeated_item(contents_df['Item'][i], aggregate_text):]
                aggregate_text = re.sub(r'\n(item\s+\d+(?:a|b)?\..*)$', '', aggregate_text, flags=re.IGNORECASE)
                aggregate_text = re.sub(r'^\s*PART II?.*?\n?$', ' ', aggregate_text, flags=re.DOTALL| re.IGNORECASE | re.MULTILINE)
                aggregate_text = re.sub(r'\n\s*\d*\s*\n', '\n', aggregate_text)
                if WRITE_OUTPUT_FILE:
                    output.write(SECTION_MARKER + contents_df['Item'][i] + ' ' + aggregate_text)
                else:
                    print(f"*****\n{SECTION_MARKER}{contents_df['Item'][i]} {aggregate_text}\n*****")

def clean_all_filings():
    """Clean all filings in sec-filings directory"""
    print("cleaning...")
    
    project_dir = directory.get_project_dir()

    company_list = os.listdir(os.path.join(project_dir, 'sec-filings-downloaded'))  

    keep_going = False
    for company in company_list:
        # User can specify a list of company names to include
        if not any(x in company for x in COMPANY_SCAN_LIST) and not keep_going:
            continue
        else:
            if COMPANY_SCAN_CONTINUE:
                keep_going = True

        company_dir = os.path.join(project_dir, 'sec-filings-downloaded', company)
        os.chdir(company_dir) # abs path to each company directory

        print('***Cleaning: {}***'.format(company))
        for file in os.listdir():  # iterate through all files in the respective company directory
            
            # cleaning files
            if 'error' not in file or file.endswith('txt'): 
                continue

            file = re.sub(r'error_(not_)?(NFI_)?(NFII_)?(seq_)?cleaned_(MT_|ITH_|ITT_)?', '', file )

            if not OVERWRITE_EXISTING:
                if os.path.exists('cleaned_' + str(file)):
                    continue
            
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
#clean_all_filings()
rename_10_Q_filings()
move_10k_10q_to_folder()