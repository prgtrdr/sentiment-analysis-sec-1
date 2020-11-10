import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import os
import glob
from pathlib2 import Path
import re
import shutil
import ProjectDirectory as directory
from io import StringIO
from html.parser import HTMLParser

# Utility functions
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

def extract_raw(in_document, pos_map, item): 
    item_raw = in_document[pos_map['start'].loc[item]+1:pos_map['end'].loc[item]]
    item_content = BeautifulSoup(item_raw, 'lxml')
    item_text = re.sub(r'\s+\d+\s+Table of Contents', ' ', item_content.get_text(' '), flags=re.IGNORECASE)
    item_text = re.sub(r'\n\s*\d*\s*\n', '', item_text)
    return item_text

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

    if (numbers / divisor) > 0.1: 
        return ''
    else:
        return matchobj.group(0)

    
def clean_filing(input_filename, filing_type, output_filename):
    """
    Cleans a 10-K or 10-Q filing. All arguments take strings as input
    input_filename: name of the file to be cleaned
    filing_type: either 10-K or 10-Q
    outuput_filename: name of output file
    """

    SECTION_MARKER = 'Â°'

    # open file
    with open (input_filename, 'r', encoding='utf-8') as f:
        data = f.read()
    
    if filing_type == '10-K':
        # Step 1. Remove all the encoded sections
        data = re.sub(r'<DOCUMENT>\n<TYPE>GRAPHIC.*?</DOCUMENT>', '', data, flags=re.S | re.A )
        data = re.sub(r'<DOCUMENT>\n<TYPE>ZIP.*?</DOCUMENT>', '', data, flags=re.S | re.A )
        data = re.sub(r'<DOCUMENT>\n<TYPE>EXCEL.*?</DOCUMENT>', '', data, flags=re.S | re.A )
        data = re.sub(r'<DOCUMENT>\n<TYPE>JSON.*?</DOCUMENT>', '', data, flags=re.S | re.A )
        data = re.sub(r'<DOCUMENT>\n<TYPE>PDF.*?</DOCUMENT>', '', data, flags=re.S | re.A )
        data = re.sub(r'<DOCUMENT>\n<TYPE>XML.*?</DOCUMENT>', '', data, flags=re.S | re.A )
        data = re.sub(r'<DOCUMENT>\n<TYPE>EX.*?</DOCUMENT>', '', data, flags=re.S | re.A )
        #data = re.sub(r'<XBRL.*?</XBRL>', '', data, flags=re.S | re.A | re.IGNORECASE )

        # Remove tables that do not contain the word 'item.' This fixes docs that incorrectly
        # use tables as page headers.
        data = re.sub(pattern="(?s)(?i)</?(FONT|SPAN|A).*?>", repl='', string=data)
        data = re.sub(pattern='(?s)(?i)(&#160;|&#32;|&nbsp;|&#xa0;)', repl=' ', string=data)

        # Code to intelligently remove tables. Some people use tables as text alignment so 
        # data = re.sub(pattern="(?s)(?i)<TABLE((?!Item).)*?</TABLE>", repl='', string=data)
        data = re.sub(r'<TABLE.*?</TABLE>', repl=tablerep, string=data, flags=re.S | re.A | re.IGNORECASE)

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

        # Create a loop to go through each section type and save only the first 10-K section in the dictionary
        for doc_type, doc_start, doc_end in zip(doc_types, doc_start_is, doc_end_is):
            if doc_type == '10-K':
                document[doc_type] = data[doc_start:doc_end]
                break

        # Validity check
        if '10-K' not in document:
            with open('error_' + output_filename, 'w', encoding='utf-8') as output:
                output.write('Could not find document[10-K]\n' + '\n' + doc_start_is + '\n' + doc_end_is + '\n' +doc_types)
                return


        # STEP 3 : Apply REGEXes to find Item 1A, 7, and 7A under 10-K Section 
        # document['10-K'] = re.sub(r'>(\s|&#32;|&#160;|Part I, |Part II, )*?(I?TEM)(?:<.*?>)?(\s|&#32;|&#160;|&nbsp;)+(<.*?>)?(1\(?A\)?|1\(?B\)?|2|7\(?A\)?|7|8)(\.{0,1})', '>item \\5\\6', document['10-K'], 0, re.IGNORECASE)
        document['10-K'] = re.sub(r'>(\s|Part I, |Part I. |Part II, |Part II. )*?(I?TEM)(?:<.*?>)?(\s)+(<.*?>)?(1|2|7|8|9)(?:\s)?\(?(A|B)?\)?(\.{0,1})', '>item \\5\\6\\7', document['10-K'], 0, re.IGNORECASE)
        regex = re.compile(r'>item\s(1\(?A\)?|1\(?B\)?|2|7\(?A\)?|7|8|9\(?A\)?|9\(?B\)?|9)\.{0,1}', re.IGNORECASE)
        
        # Use finditer to match the regex
        matches = regex.finditer(document['10-K'])

        # Create the dataframe
        test_df = pd.DataFrame([(x.group(), x.start(), x.end()) for x in matches])
        
        if len(test_df.index) == 0:
            with open('error_' + output_filename, 'w', encoding='utf-8') as output:
                output.write('No Item matches found')
                return

        test_df.columns = ['item', 'start', 'end']
        test_df['item'] = test_df.item.str.lower()

        # Get rid of unnesesary charcters from the dataframe
        # test_df.replace('(?i)Part I, ||Part II, ','',regex=True,inplace=True)
        test_df.replace(r'&#160;|&#32;|&nbsp;',' ',regex=True,inplace=True)
        test_df.replace(r'\.','',regex=True,inplace=True)
        test_df.replace(r' |>|\(|\)|\n','',regex=True,inplace=True)

        # Form map of where the items are located
        pos_dat = test_df.sort_values('start', ascending=True)

        # Parsing validity checks, bypass this file if improper parse
        error_info = ''
        if 'item1a' not in pos_dat['item'].values:
            error_info = error_info + '1A '
        if 'item1b' not in pos_dat['item'].values and 'item2' not in pos_dat['item'].values:
            error_info = error_info + '1B/2 '
        if 'item7' not in pos_dat['item'].values:
            error_info = error_info + '7 '
        if 'item8' not in pos_dat['item'].values and 'item9' not in pos_dat['item'].values and 'item9a' not in pos_dat['item'].values and 'item9b' not in pos_dat['item'].values:
            error_info = error_info + '8/9/9a/9b '
            
        if error_info != '':
            error_info = error_info + 'not found\n'
            with open('error_' + output_filename, 'w', encoding='utf-8') as output:
                output.write(error_info)
                output.write(pos_dat.to_string())
            return

        # Combine duplicate rows to handle submissions with more than one page
        last_item = ''
        for index, row in pos_dat.iterrows():
            if row['item'] == last_item:
                pos_dat.drop(index, inplace=True)
            else:
                last_item = row['item']

        # Set item as the dataframe index
        pos_dat.set_index('item', inplace=True)

        # If more than 2 item1a rows, get rid of extra
        if len(pos_dat.loc['item1a']) > 2:  # df.shape[0]
            pos_dat = pos_dat.loc[pos_dat.start < pos_dat.loc['item1a']['start'][2]]

        # Set ending address of the section, and add a length column
        pos_dat['end'] = np.append(pos_dat.iloc[1:,0].values, [ 0 ])
        pos_dat['length'] = pos_dat['end'] - pos_dat['start']

        # Drop the shorter of each set of rows
        pos_dat = pos_dat.sort_values('length', ascending=True)
        pos_dat = pos_dat.loc[~pos_dat.index.duplicated(keep='last')]
        pos_dat = pos_dat.sort_values('start', ascending=True)

        # Use Beautiful Soup to extract text from the raw data 
        aggregate_text = SECTION_MARKER + extract_raw(document['10-K'], pos_dat, 'item1a')
        aggregate_text = aggregate_text + SECTION_MARKER + extract_raw(document['10-K'], pos_dat, 'item7')

        # If Item 7A is not present, assume issuer has lumped 7 and 7A together
        if 'item7a' in pos_dat.index:
            aggregate_text = aggregate_text + SECTION_MARKER + extract_raw(document['10-K'], pos_dat, 'item7a')

        with open(output_filename, 'w', encoding='utf-8') as output:
            output.write(aggregate_text)
    else:
        # Process 10Q
        print("Processing 10Q")


def clean_all_filings():
    """Clean all filings in sec-filings directory"""
    print("cleaning...")
    
    project_dir = directory.get_project_dir()
    rootDir = os.path.join(project_dir, 'sec-filings-downloaded')
    for dirName, subdirList, fileList in os.walk(rootDir):
        os.chdir(dirName)
        print('Found directory: %s' % dirName)
        for fName in fileList:
            # Skip if already cleaned or not a txt file
            if fName.startswith('cleaned') or fName.startswith('error') or not fName.endswith('txt') or glob.glob('*cleaned_' + fName): 
                continue
            
            if '10-K' in dirName:
                filing_type = '10-K'
            else:
                filing_type = '10-Q'

            print('***Cleaning: {}***'.format(fName))
            clean_filing(input_filename=fName, filing_type=filing_type, output_filename='cleaned_' + str(fName))
            print('{} filing cleaned'.format(fName))

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

# rename_10_Q_filings()

# move_10k_10q_to_folder()



