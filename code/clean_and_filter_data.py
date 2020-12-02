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
import html

# Global variables. Make these settable from the command line.
SECTION_MARKER = 'Â°'
MAX_SEQ_ERRORS = 1  # Set to a really high value to disable sequence error logging
USE_EDGAR_FILENAME = False  # If true, use names of files downloaded using sec-utils
CLEAN_10K = True   # Clean 10-K filings
CLEAN_10Q = True    # Clean 10-Q filings
OVERWRITE_EXISTING = False  # If True, overwrite existing cleaned files, else skip
EDGAR_PATH = ''     # Will contain full path to the EDGAR website document being parsed
COMPANY_SCAN_LIST = ['']  # List of company name strings to limit parse, e.g., ['ABBOTT', 'AMERICAN FINANCIAL']
COMPANY_SCAN_CONTINUE = True    # If True, continue scanning when done with first company in list

# List of items_10K found in filings, in order of appearance.
items_10K = [
    'item1',    #0
    'item1a',   #1
    'item1b',   #2
    'item2',    #3
    'item3',    #4
    'item3a',   #5
    'item4',    #6
    'item4a',   #7
    'item5',    #8
    'item5a',   #9
    'item6',    #10
    'item7',    #11
    'item7a',   #12
    'item8',    #13
    'item8a',   #14
    'item8b',   #15
    'item9',    #16
    'item9a',   #17
    'item9b',   #18
    'item10',   #19
    'item11',   #20
    'item12',   #21
    'item12a',  #22
    'item12b',  #23
    'item13',   #24
    'item14',   #25
    'item15',   #26
    'item15a',  #27
    'item15b',  #28
    'item16'    #29
]

items_10QI = [
    'item1',   #0
    'item1a',  #1
    'item2',   #2
    'item3',   #3
    'item4',   #4
    'item5',   #5
    'item6'    #6
]
items_10QII = [
    'item21',  #0
    'item21a', #1
    'item22',  #2
    'item22a', #3
    'item23',  #4
    'item24',  #5
    'item24a', #6
    'item25',  #7
    'item26'   #8
]

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
    if pos_map['end'].loc[item] != 0:
        item_raw = in_document[pos_map['start'].loc[item]+1:pos_map['end'].loc[item]]
    else:
        item_raw = in_document[pos_map['start'].loc[item]+1:]

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

    # If less than 8% of the chars in the table are numbers, keep it, else delete
    if (numbers / divisor) < 0.1 or 'Item 1' in text: 
        return matchobj.group(0)
    else:
        return ''

# Function to compare returned sections
def len_no_tags(input_str):
    return(len(strip_tags(input_str)))

# Get rid of out-of-sequence rows in dataframe
def delete_out_of_seq(in_document, pos_dat, items_list, error_info):
    drop_rows = []
    error_count = 0
    for i in range(1, pos_dat.shape[0]):
        this_item = items_list.index( pos_dat.iloc[i]['item'] )
        if this_item == 0:
            continue
        prev_item = items_list.index( pos_dat.iloc[i-1]['item'] )
        if i+1 == pos_dat.shape[0]:
            next_item = len(items_list)+1
        else:
            next_item = items_list.index( pos_dat.iloc[i+1]['item'] )
            if next_item == 0 and (this_item > prev_item): 
                continue

        if (this_item <= prev_item) or ((next_item > prev_item) and (this_item > next_item)):
            error_count += 1
            error_info = error_info + f'Sequence err. Index: {i}, Prev: {items_list[prev_item]}, This: {items_list[this_item]}, Next: {"END" if next_item > len(items_list) else items_list[next_item]}\n' + \
                in_document[pos_dat.iloc[i]['start']-256: pos_dat.iloc[i]['start']+256 ].replace('\n', '') + '\n\n'
            pos_dat.at[i, 'item'] = pos_dat.iloc[i-1]['item']
            drop_rows.append(i)

    pos_dat.drop(drop_rows, inplace = True)
    return error_count
    
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

    # Extract EDGAR CIK and filename
    CIK = re.search(r'(?:CENTRAL INDEX KEY:\s+)(\d+)', data, re.IGNORECASE)[1]
    edgar_accession = re.search(r'(?:ACCESSION NUMBER:\s+)([\d-]+)', data, re.IGNORECASE)[1]
    edgar_filename = re.search(r'(?:<FILENAME>)(.+)\n', data, re.IGNORECASE)[1]
    EDGAR_PATH = f'https://www.sec.gov/Archives/edgar/data/{CIK}/{edgar_accession.replace("-", "")}/{edgar_filename}'

    if filing_type == '10-K':
        # Step 1. Remove all the encoded sections
        data = re.sub(r'<DOCUMENT>\n<TYPE>GRAPHIC.*?</DOCUMENT>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<DOCUMENT>\n<TYPE>ZIP.*?</DOCUMENT>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<DOCUMENT>\n<TYPE>EXCEL.*?</DOCUMENT>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<DOCUMENT>\n<TYPE>JSON.*?</DOCUMENT>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<DOCUMENT>\n<TYPE>PDF.*?</DOCUMENT>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<DOCUMENT>\n<TYPE>XML.*?</DOCUMENT>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<DOCUMENT>\n<TYPE>EX.*?</DOCUMENT>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<ix:header.*?</ix:header>', '', data, flags=re.S | re.A | re.I )
        #data = re.sub(r'<XBRL.*?</XBRL>', '', data, flags=re.S | re.A | re.IGNORECASE )

        # Delete certain HTML that may be embedded in words like ITEM
        data = re.sub(pattern="(?s)(?i)</?(FONT|SPAN|A|B|U|I).*?>", repl='', string=data)
        
        # Replace Unicode strings and special characters like nbsp
        # data = re.sub(pattern=r'(?s)(?i)(&#160;|&#32;|&nbsp;|&#xa0;)', repl=' ', string=data)
        # data = re.sub(pattern=r"(?s)(?i)(&#x2019;|&#8217;)", repl="'", string=data)
        # data = re.sub(pattern=r"(?s)(?i)(&#8211;|&#8212;)", repl="-", string=data)
        # data = re.sub(pattern=r"(?s)(?i)(&#8220;|&#8221;)", repl='"', string=data)
        # data = re.sub(pattern=r"(?s)(?i)&amp;", repl='&', string=data)
        # data = re.sub(pattern=r'&(.{2,6});', repl=' ', string=data)

        # data = ftfy.fix_text(data, fix_entities=True, max_decode_length=10**7)
        # repl_dict = {
        #     "&#160;" : " ",
        #     "&#32;" : " ",
        #     "&nbsp;" : " ",
        #     "&#xa0;" : " ",
        #     "&#x2019;" : "'",
        #     "&#8217;" : "'",
        #     "&#8211;" : "-",
        #     "&#8212;" : "-",
        #     "&#8220;" : '"',
        #     "&#8221;" : '"'          
        # }
        # data = saxutils.unescape(data, repl_dict)

        data = html.unescape(data)    # This function doesn't convert all the characters as needed, like xa0 and apostrophes
        data = data.replace('\xa0', ' ')
        data = data.replace('\u200b', ' ')
        data = data.replace("’", "'")
        data = data.replace('“', '"')
        data = data.replace('”', '"')

        # Intelligently remove tables. Some filers use tables as text alignment so keep the ones with < 10% numeric
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

        # STEP 3 : Apply REGEXes to find all Item sections
        document['10-K'] = re.sub(r'>\s*?(Part I+(?:\.|\||,|\s)*?)?(I?TEM)S?(?:<.*?>)?(\s)*(<.*?>)?(16|15|14|13|12|11|10|9|8|7|6|5|4|3|2|1|I)?(?:\s)?(?:\(?\.?(A|B)?\)?)?(\.|\s|<|\:)', '>item \\5\\6.\\7', document['10-K'], 0, re.IGNORECASE)
        regex = re.compile(r'>item\s(16|15|14|13|12|11|10|9|8|7|6|5|4|3|2|1|I)(A|B)?\.', re.IGNORECASE)
        
        # Use finditer to match the regex
        matches = regex.finditer(document['10-K'])

        # Create the dataframe
        test_df = pd.DataFrame([(x.group(), x.start(), x.end()) for x in matches])
        
        if len(test_df.index) == 0:
            with open('error_' + output_filename, 'w', encoding='utf-8') as output:
                output.write(EDGAR_PATH + '\nNo Item matches found in test_df')
                return

        test_df.columns = ['item', 'start', 'end']
        test_df['item'] = test_df.item.str.lower()

        # Get rid of unnesesary charcters from the dataframe
        # Replace all Unicode strings with a space
        test_df.replace(r'&(.{2,6});', ' ', regex=True,inplace=True)
        test_df.replace(r'\.','',regex=True,inplace=True)
        test_df.replace(r' |>|\(|\)|\n','',regex=True,inplace=True)
        
        # fix for 0000731012-16-000120
        test_df.replace(r'itemi','item1',regex=True,inplace=True)

        # Form map of where the items are located
        pos_dat = test_df.sort_values('start', ascending=True)

        # Parsing validity checks, bypass this file if improper parse
        error_info = EDGAR_PATH + '\n'
        if 'item1' not in pos_dat['item'].values:
            error_info = error_info + '1 '
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

        pos_dat = pos_dat.reset_index(drop=True)

        # Get rid of out-of-sequence rows
        drop_rows = []
        error_count = 0
        error_info = EDGAR_PATH + '\n' + pos_dat.to_string() + '\n' + '*' * 66 + '\n'
        for i in range(1, pos_dat.shape[0]):
            this_item = items_10K.index( pos_dat.iloc[i]['item'] )
            if this_item == 0:
                continue
            prev_item = items_10K.index( pos_dat.iloc[i-1]['item'] )
            if i+1 == pos_dat.shape[0]:
                next_item = len(items_10K)+1
            else:
                next_item = items_10K.index( pos_dat.iloc[i+1]['item'] )
                if next_item == 0 and (this_item > prev_item): 
                    continue
            if (this_item <= prev_item) or ((next_item > prev_item) and (this_item > next_item)):
                error_count += 1
                error_info = error_info + f'Sequence err. Index: {i}, Prev: {items_10K[prev_item]}, This: {items_10K[this_item]}, Next: {"END" if next_item > len(items_10K) else items_10K[next_item]}\n' + \
                    document['10-K'][pos_dat.iloc[i]['start']-256: pos_dat.iloc[i]['start']+256 ].replace('\n', '') + '\n\n'
                pos_dat.at[i, 'item'] = pos_dat.iloc[i-1]['item']
                drop_rows.append(i)

        pos_dat.drop(drop_rows, inplace = True)

        # Write sequnce fixes to output file. We can make this optional at some point.    
        if error_count > MAX_SEQ_ERRORS:
            with open('error_seq_' + output_filename, 'w', encoding='utf-8') as output:
                output.write(error_info)
                output.write('\n' + '*' * 66 + '\n' + pos_dat.to_string())


        # Set ending address of the section, and add a length column. To calculate length, strip HTML
        pos_dat['end'] = np.append(pos_dat.iloc[1:,1].values, [ 0 ])
        len_vals = []
        for index, row in pos_dat.iterrows():
            stripped_data = strip_tags(document['10-K'][row['start']:row['end']])
            len_vals.append(len(stripped_data))
        pos_dat.insert(3, 'length', len_vals)

        # Drop the shorter of each set of rows. 
        #   1. Sort by appearance in the filing
        #   2. Iterate through, saving each occurrence of item1.
        #   3. Upon finding item1, compare length to previous item1.
        #   4. If longer, delete all previous rows. If shorter, delete all following rown, inclusive.
        pos_dat = pos_dat.sort_values('start', ascending=True)
        prev_item1_len = 0
        for index, row in pos_dat.iterrows():
            if row['item'] == 'item1':
                if row['length'] > prev_item1_len:
                    rows_to_drop = pos_dat[pos_dat.index < index].index
                    pos_dat.drop(rows_to_drop, inplace=True)
                    prev_item1_len = row['length']
                else:
                    rows_to_drop = pos_dat[pos_dat.index >= index].index
                    pos_dat.drop(rows_to_drop, inplace=True)
                    break

        # Set item as the dataframe index
        pos_dat.set_index('item', inplace=True)

        # Use Beautiful Soup to extract text from the raw data 
        aggregate_text = ''
        for index, row in pos_dat.iterrows():
            aggregate_text = aggregate_text + SECTION_MARKER + extract_raw(document['10-K'], pos_dat, index)

        with open(output_filename, 'w', encoding='utf-8') as output:
            output.write(aggregate_text)
    else:
        # Process 10Q
        # Step 1. Remove all the encoded sections
        data = re.sub(r'<DOCUMENT>\n<TYPE>GRAPHIC.*?</DOCUMENT>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<DOCUMENT>\n<TYPE>ZIP.*?</DOCUMENT>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<DOCUMENT>\n<TYPE>EXCEL.*?</DOCUMENT>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<DOCUMENT>\n<TYPE>JSON.*?</DOCUMENT>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<DOCUMENT>\n<TYPE>PDF.*?</DOCUMENT>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<DOCUMENT>\n<TYPE>XML.*?</DOCUMENT>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<DOCUMENT>\n<TYPE>RENDERED XBRL.*?</DOCUMENT>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<DOCUMENT>\n<TYPE>EX.*?</DOCUMENT>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<ix:header.*?</ix:header>', '', data, flags=re.S | re.A | re.I )
        data = re.sub(r'<PDF.*?</PDF>', '', data, flags=re.S | re.A | re.I )
        #data = re.sub(r'<XBRL.*?</XBRL>', '', data, flags=re.S | re.A | re.IGNORECASE )

        # Delete certain HTML that may be embedded in words like ITEM
        data = re.sub(pattern="(?s)(?i)</?(FONT|SPAN|A|B|U|I).*?>", repl='', string=data)
        
        data = html.unescape(data)    # This function doesn't convert all the characters as needed, like xa0 and apostrophes
        data = data.replace('\xa0', ' ')
        data = data.replace('\u200b', ' ')
        data = data.replace("’", "'")
        data = data.replace('“', '"')
        data = data.replace('”', '"')

        # Intelligently remove tables. Some filers use tables as text alignment so keep the ones with < 10% numeric
        data = re.sub(r'<TABLE.*?</TABLE>', repl=tablerep, string=data, flags=re.S | re.A | re.IGNORECASE)

        # Change multiple whitespace to single whitespace
        data = re.sub(r'\s+', repl=' ', string=data, flags=re.S | re.A)

        # Extract text between PART I and PART II. Will probably get 2 matches
        part_list = re.findall(r'>\s*?PART (?:I|1)[^I](?:.*?FINANCIAL (?:INFORMATION|STATEMENTS))?(.*?)>\s*?PART II.*?OTHER INFORMATION', string=data, flags=re.S | re.A | re.I)
        if len(part_list) == 0:
            with open('error_' + output_filename, 'w', encoding='utf-8') as output:
                output.write(EDGAR_PATH + '\nCould not parse Part I')
                return
        dataI = max(part_list, key=len_no_tags)

        documentI = re.sub(r'>\s*?(?:ITEM)(?:<.*?>)?(?:\s)*(?:<.*?>)?(5|4|3|2|1|I)?(?:\s)?(?:\(?\.?(A|B)?\)?)?', '>item \\1\\2.', dataI, 0, re.IGNORECASE)
        documentI = documentI.replace('>item I', '>item 1')
        regex = re.compile(r'>item\s(5|4|3|2|1)(A|B)?\.', re.IGNORECASE)

        # Use finditer to match the regex
        matches = regex.finditer(documentI)

        # Create the dataframe
        dfI = pd.DataFrame([(x.group(), x.start(), x.end()) for x in matches])
        if len(dfI.index) == 0 :
            with open('error_NFI_' + output_filename, 'w', encoding='utf-8') as output:
                output.write(EDGAR_PATH + '\ndfI: No Item matches found')
                return

        # Extract text between PART II and end of document
        part_list = re.findall(r'>\s*?PART II.*?OTHER INFORMATION(.*?)(?=>\s*?PART (?:I|1)|$)', string=data, flags=re.S | re.A | re.I)
        if len(part_list) == 0:
            with open('error_' + output_filename, 'w', encoding='utf-8') as output:
                output.write(EDGAR_PATH + '\nCould not parse Part II')
                return
        dataII = max(part_list, key=len_no_tags)

        documentII = re.sub(r'>\s*?(?:ITEM)(?:<.*?>)?(?:\s)*(?:<.*?>)?(6|5|4|3|2|1|I)?(?:\s)?(?:\(?\.?(A|B)?\)?)?', '>item 2\\1\\2.', dataII, 0, re.IGNORECASE)
        documentII = documentII.replace('>item 2I', '>item 21')
        regex = re.compile(r'>item\s(26|25|24|23|22|21)(A|B)?\.', re.IGNORECASE)

        # Use finditer to match the regex
        matches = regex.finditer(documentII)

        # Create the dataframe
        dfII = pd.DataFrame([(x.group(), x.start(), x.end()) for x in matches])
        
        if len(dfII.index) == 0 :
            with open('error_NFII_' + output_filename, 'w', encoding='utf-8') as output:
                output.write(EDGAR_PATH + '\ndfII: No Item matches found')
                return

        dfI.columns = ['item', 'start', 'end']
        dfII.columns = ['item', 'start', 'end']
        dfI['item'] = dfI.item.str.lower()
        dfII['item'] = dfII.item.str.lower()

        # Get rid of unnessecary characters from the dataframe
        dfI.replace(r' |>|\(|\)|\.|\n','',regex=True,inplace=True)
        dfII.replace(r' |>|\(|\)|\.|\n','',regex=True,inplace=True)
        
        # Form map of where the items are located
        pos_datI = dfI.sort_values('start', ascending=True)
        pos_datII = dfII.sort_values('start', ascending=True)

        # Parsing validity checks, bypass this file if improper parse
        error_info = EDGAR_PATH + '\n'
        if 'item1' not in pos_datI['item'].values:
            error_info = error_info + '1 '
            error_info = error_info + 'not found\n'
            with open('error_' + output_filename, 'w', encoding='utf-8') as output:
                output.write(error_info)
                output.write(pos_datI.to_string())
            return

        if 'item26' not in pos_datII['item'].values:
            error_info = error_info + '21 '
            error_info = error_info + 'not found\n'
            with open('error_' + output_filename, 'w', encoding='utf-8') as output:
                output.write(error_info)
                output.write(pos_datII.to_string())
            return

        # Combine duplicate rows to handle submissions with more than one page
        last_item = ''
        for index, row in pos_datI.iterrows():
            if row['item'] == last_item:
                pos_datI.drop(index, inplace=True)
            else:
                last_item = row['item']
        pos_datI = pos_datI.reset_index(drop=True)

        last_item = ''
        for index, row in pos_datII.iterrows():
            if row['item'] == last_item:
                pos_datII.drop(index, inplace=True)
            else:
                last_item = row['item']
        pos_datII = pos_datII.reset_index(drop=True)

        error_info = EDGAR_PATH + '\n' + pos_datI.to_string() + '\n' + '*' * 66 + '\n'
        if delete_out_of_seq(documentI, pos_datI, items_10QI, error_info) > MAX_SEQ_ERRORS:
        # Write sequence fixes to output file. We can make this optional at some point.    
            with open('error_seq_' + output_filename, 'w', encoding='utf-8') as output:
                output.write(error_info)
                output.write('\n' + '*' * 66 + '\n' + pos_datI.to_string())

        error_info = EDGAR_PATH + '\n' + pos_datII.to_string() + '\n' + '*' * 66 + '\n'
        if delete_out_of_seq(documentII, pos_datII, items_10QII, error_info) > MAX_SEQ_ERRORS:
            with open('error_seq_' + output_filename, 'w', encoding='utf-8') as output:
                output.write(error_info)
                output.write('\n' + '*' * 66 + '\n' + pos_datII.to_string())

        # Set ending address of the section, and add a length column. To calculate length, strip HTML
        pos_datI['end'] = np.append(pos_datI.iloc[1:,1].values, [ 0 ])
        len_vals = []
        for index, row in pos_datI.iterrows():
            stripped_data = strip_tags(documentI[row['start']:row['end']])
            len_vals.append(len(stripped_data))
        pos_datI.insert(3, 'length', len_vals)

        pos_datII['end'] = np.append(pos_datII.iloc[1:,1].values, [ 0 ])
        len_vals = []
        for index, row in pos_datII.iterrows():
            stripped_data = strip_tags(documentII[row['start']:row['end']])
            len_vals.append(len(stripped_data))
        pos_datII.insert(3, 'length', len_vals)

        # Drop the shorter of each set of rows. 
        #   1. Sort by appearance in the filing
        #   2. Iterate through, saving each occurrence of item1.
        #   3. Upon finding item1, compare length to previous item1.
        #   4. If longer, delete all previous rows. If shorter, delete all following rown, inclusive.
        pos_datI = pos_datI.sort_values('start', ascending=True)
        prev_item1_len = 0
        for index, row in pos_datI.iterrows():
            if row['item'] == 'item1':
                if row['length'] > prev_item1_len:
                    rows_to_drop = pos_datI[pos_datI.index < index].index
                    pos_datI.drop(rows_to_drop, inplace=True)
                    prev_item1_len = row['length']
                else:
                    rows_to_drop = pos_datI[pos_datI.index >= index].index
                    pos_datI.drop(rows_to_drop, inplace=True)
                    break

        pos_datII = pos_datII.sort_values('start', ascending=True)
        prev_item1_len = 0
        for index, row in pos_datII.iterrows():
            if row['item'] == 'item1':
                if row['length'] > prev_item1_len:
                    rows_to_drop = pos_datII[pos_datII.index < index].index
                    pos_datII.drop(rows_to_drop, inplace=True)
                    prev_item1_len = row['length']
                else:
                    rows_to_drop = pos_datII[pos_datII.index >= index].index
                    pos_datII.drop(rows_to_drop, inplace=True)
                    break

        # Set item as the dataframe index
        pos_datI.set_index('item', inplace=True)
        pos_datII.set_index('item', inplace=True)

        # Use Beautiful Soup to extract text from the raw data 
        aggregate_text = ''
        for index, row in pos_datI.iterrows():
            aggregate_text = aggregate_text + SECTION_MARKER + extract_raw(documentI, pos_datI, index)

        for index, row in pos_datII.iterrows():
            try:
                aggregate_text = aggregate_text + SECTION_MARKER + extract_raw(documentII, pos_datII, index)
            except:
                error_info = EDGAR_PATH + '\n'
                with open('error_' + output_filename, 'w', encoding='utf-8') as output:
                    output.write(EDGAR_PATH + '\nError in pos_datII:\n' + pos_datII.to_string())
                return


        with open(output_filename, 'w', encoding='utf-8') as output:
            output.write(aggregate_text)

 
def clean_all_filings():
    """Clean all filings in sec-filings directory"""
    print("cleaning...")
    
    project_dir = directory.get_project_dir()

    if USE_EDGAR_FILENAME:      # Use sec-utils download directory structure /10-K/year/quarter
        rootDir = os.path.join(project_dir, 'sec-filings-downloaded')
        for dirName, subdirList, fileList in os.walk(rootDir):
            os.chdir(dirName)
            print('Found directory: %s' % dirName)
            #if '2020' not in dirName:
            #    print('Skipped')
            #    continue

            for fName in fileList:
                # Skip if already cleaned or not a txt file
                if 'cleaned' in fName or not fName.endswith('txt'): 
                    continue
                
                if '10-K' in dirName:
                    filing_type = '10-K'
                else:
                    filing_type = '10-Q'
                    continue

                print('***Cleaning: {}***'.format(fName))
                clean_filing(input_filename=fName, filing_type=filing_type, output_filename='cleaned_' + str(fName))
                print('{} filing cleaned'.format(fName))
    else:
        company_list = os.listdir(os.path.join(project_dir, 'sec-filings-downloaded'))  

        keep_going = False
        for company in company_list:
            # DEBUGGING PURPOSES *************************
            if not any(x in company for x in COMPANY_SCAN_LIST) and not keep_going:
                continue
            else:
                if COMPANY_SCAN_LIST:
                    keep_going = True

            company_dir = os.path.join(project_dir, 'sec-filings-downloaded', company)
            os.chdir(company_dir) # abs path to each company directory

            print('***Cleaning: {}***'.format(company))
            for file in os.listdir():  # iterate through all files in the respective company directory
                
                # cleaning files
                if 'cleaned' in file: 
                    continue

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

#rename_10_Q_filings()

#move_10k_10q_to_folder()



