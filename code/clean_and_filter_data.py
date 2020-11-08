import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import os
import glob
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
        data = re.sub(pattern="(?s)(?i)<TABLE((?!Item).)*?</TABLE>", repl='', string=data)
        data = re.sub(pattern="(?s)(?i)</?(FONT|SPAN).*?>", repl='', string=data)

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
        document['10-K'] = re.sub(r'>(\s|&#32;|&#160;|Part I, |Part II, )*?(I?TEM)(?:<.*?>)?(\s|&#32;|&#160;|&nbsp;)+(<.*?>)?(1|2|7|8|9)(?:\&#160;)?\(?(A|B)?\)?(\.{0,1})', '>item \\5\\6\\7', document['10-K'], 0, re.IGNORECASE)
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

        # Drop duplicates Rather than drop all but the last item, we should implement the following logic:
        # If there are duplicates of a given item, 
        #   a) remove the first entry if size < 100 (probably the index entry)
        #   b) remove all duplicates except the first item. This will aggregate all items of a given type
        pos_dat = test_df.sort_values('start', ascending=True).drop_duplicates(subset=['item'], keep='last')

        # Set item as the dataframe index
        pos_dat.set_index('item', inplace=True)

        # Parsin validity checks, bypass this file if improper parse
        error_info = ''
        if 'item1a' not in pos_dat.index:
            error_info = error_info + '1A '
        if 'item1b' not in pos_dat.index and 'item2' not in pos_dat.index:
            error_info = error_info + '1B/2 '
        if 'item7' not in pos_dat.index:
            error_info = error_info + '7 '
        if 'item7a' not in pos_dat.index:
            error_info = error_info + '7A '
        if 'item8' not in pos_dat.index and 'item9' not in pos_dat.index and 'item9a' not in pos_dat.index and 'item9b' not in pos_dat.index:
            error_info = error_info + '8/9/9a/9b '
            
        if error_info != '':
            error_info = error_info + 'not found\n'
            with open('error_' + output_filename, 'w', encoding='utf-8') as output:
                output.write(error_info)
                output.write(pos_dat.to_string())
            return

        # Get Item 1a
        try:
            item_1a_raw = document['10-K'][pos_dat['start'].loc['item1a']+1:pos_dat['start'].loc['item1b']]
        except:
            item_1a_raw = document['10-K'][pos_dat['start'].loc['item1a']+1:pos_dat['start'].loc['item2']]

        # Get Item 7
        try:
            item_7_raw = document['10-K'][pos_dat['start'].loc['item7']+1:pos_dat['start'].loc['item7a']]
            # Get Item 7a
            item_7a_raw = document['10-K'][pos_dat['start'].loc['item7a']+1:pos_dat['start'].loc['item8']]
        except:
            # No Item 7a seen
            item_7_raw = document['10-K'][pos_dat['start'].loc['item7']+1:pos_dat['start'].loc['item8']]

        ### First convert the raw text we have to exrtacted to BeautifulSoup object 
        item_1a_content = BeautifulSoup(item_1a_raw, 'lxml')
        item_7_content = BeautifulSoup(item_7_raw, 'lxml')
        item_7a_content = BeautifulSoup(item_7a_raw, 'lxml')

        item_1a_text = re.sub(r'\s+\d+\s+Table of Contents', ' ', item_1a_content.get_text(' '), flags=re.IGNORECASE)
        item_1a_text = re.sub(r'\n\s*\d*\s*\n', '', item_1a_text)
        item_7_text =  re.sub(r'\s+\d+\s+Table of Contents', ' ', item_7_content.get_text(' '), flags=re.IGNORECASE)
        item_7_text = re.sub(r'\n\s*\d*\s*\n', '', item_7_text)
        item_7a_text = re.sub(r'\s+\d+\s+Table of Contents', ' ', item_7a_content.get_text(' '), flags=re.IGNORECASE)
        item_7a_text = re.sub(r'\n\s*\d*\s*\n', '', item_7a_text)

        # Tag each section for later analysis
        aggregate_text = 'Â°' + item_1a_text + 'Â°' + item_7_text + 'Â°' + item_7a_text

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



