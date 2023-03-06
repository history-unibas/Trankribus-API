#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""
Script to search for selected special characters in Transkribus document pages
and replace them by other characters. A selected collection was considered.
"""


import re
from connectTranskribus import *


def contains_character(char, search_string):
    # Test if a character occurs in string given

    match = re.search(char, search_string)
    if match:
        return True
    else:
        return False


def replace_string(page_xml, str_search, str_replace):
    # Replaces strings within Unicode areas of a page xml

    str_match = re.findall(r'<Unicode>.*%s.*</Unicode>' % str_search, page_xml)
    n_replaced = 0
    for match in str_match:
        match_replaced = re.sub(str_search, str_replace, match)
        page_xml, n = re.subn(match, match_replaced, page_xml)
        n_replaced += n
    return page_xml, n_replaced


if __name__ == "__main__":   
    # Define logging environment
    log_file = 'replaceCharacters.log'
    print(f'Consider the logfile {log_file} for information about the run.')
    logging.basicConfig(filename=log_file, format='%(asctime)s   %(levelname)s   %(message)s',
                        level=logging.INFO, encoding='utf-8')
    logging.info('Script started.')
    
    # Login to Transkribus
    user = input('Transkribus user:')
    password = input('Transkribus password:')  
    sid = get_sid(user, password)
    
    # Consider for each page the latest version
    page_version = 0

    # Set collection id of HGB_TRAINING
    colid = 163061

    # Define which characters should be replaced
    char_replace = {'ȶ': 't', 'ƒ': 'f', 'Ħ': 'H', 'Ŋ': 'No', 'ȴ': 'l'}

    docs = list_documents(sid, colid)

    # Iterate over all documents of hgb_training
    for row in docs:
        # Excluding documents beginning with title TRAINING_VALIDATION_SET_HGB
        if re.match(r'^TRAINING_VALIDATION_SET_HGB', row['title']):
            logging.info(f"No changes will be done on document {row['title']}.")
            continue

        logging.info(f"Query pages of document {row['title']} ...")

        doc_content = get_document_content(colid, row['docId'], sid)

        # Iterate over every document page
        for page_nr in range(1, row['nrOfPages'] + 1):
            has_changed = False
            
            xml_url = get_page_xml_url(doc_content, page_nr, page_version)
            page_xml = get_page_xml(xml_url, sid)

            # Iterate over special characters
            for char in char_replace:

                # Check if page xml contains the character
                if contains_character(char, page_xml):
                    # Replace the character
                    page_xml, n_replaced = replace_string(page_xml, char, char_replace[char])
                    logging.info(f"Character {char} is {n_replaced} times included on page number {page_nr} of document {row['title']}. Page xml: {xml_url}")
                    has_changed = True
            
            if has_changed:
                # Upload edited page xml to Transkribus
                post_success = post_page_xml(page_xml, colid, row['docId'], page_nr, sid, comment='Special characters replaced.')
                if post_success:
                    logging.info(f"The edited page xml of page number {page_nr} of document {row['title']} was uploaded to Transkribus.")
                else:
                    logging.error(f"The edited page xml of page number {page_nr} of document {row['title']} can't be uploaded to Transkribus.")
                    raise

    logging.info('Script finished.')
