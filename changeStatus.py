#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""
Change the status of the latest version of selected Transkribus pages given the following parameters:
- collection title,
- document title,
- page number.
"""


import pandas as pd
from connectTranskribus import *


if __name__ == "__main__":
    # Define logging environment
    log_file = 'changeStatus.log'
    print(f'Consider the logfile {log_file} for information about the run.')
    logging.basicConfig(filename=log_file, format='%(asctime)s   %(levelname)s   %(message)s',
                        level=logging.INFO, encoding='utf-8')
    logging.info('Script started.')


    ##
    # Preprocessing for use case "Identification of 'Brandlagerbücher'."
    ##
    
    # Read cases of interest
    edit_pages = pd.read_csv('./Brandlagerbuecher.csv')

    # Derive collection title from document title
    edit_pages['col_title'] = edit_pages.apply(lambda row: row['doc_title'][:-4], axis=1)

    
    ##
    # Set parameters
    ##

    # Comment transcript version
    comment = 'Page identified as "Brandlagerbuch". Status set to DONE.'

    # New status
    status='DONE'

    # Set directory for data export
    data_dir = './changeStatus.csv'


    ##
    # Determine the necessary variables for updating the status
    ##

    # Login to Transkribus
    user = input('Transkribus user:')
    password = input('Transkribus password:')  
    sid = get_sid(user, password)

    # Get collection id
    lut_coll = pd.DataFrame()
    lut_coll['colname'] = pd.unique(edit_pages['col_title'])
    lut_coll['colid'] = lut_coll.apply(lambda row: get_colid(row['colname'], sid), axis=1)
    edit_pages['colid'] = edit_pages.apply(lambda row: lut_coll[lut_coll['colname'] == row['col_title']]['colid'].iloc[0], axis=1)

    # Create a lookup table for affected documents to get their document ids
    lut_doc = pd.DataFrame(columns=['docid', 'tsid'])
    lut_doc['docname'] = pd.unique(edit_pages['doc_title'])
    lut_doc['colname'] = lut_doc.apply(lambda row: row['docname'][:-4], axis=1)

    # Iterate over affected collections
    for index, row in lut_coll.iterrows():
        docs = pd.DataFrame(list_documents(sid, row['colid']))
        lut_docs = lut_doc[lut_doc['colname'] == row['colname']]

        # Iterate over affected documents
        for i, r in lut_docs.iterrows():
            docname = r['docname']
            docid = docs[docs['title'] == docname]['docId'].iloc[0]

            # Save the document id in lookup table
            lut_doc.at[i, 'docid'] = docid

            # Receive the document content for getting the transkript id
            doc_content = get_document_content(row['colid'], docid, sid)
            pages = edit_pages[edit_pages['doc_title'] == docname] 

            # Iterate over affected pages
            for j, t in pages.iterrows():
                # Store the transcript id of the latest version (first entry in list)
                try:
                    edit_pages.at[j, 'tsid'] = doc_content['pageList']['pages'][t['page_nr'] - 1]['tsList']['transcripts'][0]['tsId']
                except:
                    logging.warning(f"Page not found. Document title: {t['doc_title']}, page number: {t['page_nr']}. Filename may be wrong.")

    # Get document id
    edit_pages['docid'] = edit_pages.apply(lambda row: lut_doc[lut_doc['docname'] == row['doc_title']]['docid'].iloc[0], axis=1)

    # Save dataframe
    edit_pages.to_csv(data_dir, index=False, header=True)
    logging.info(f"Dataframe created from pages to be modified and written to {data_dir}.")


    ##
    # Adapt selected transkript status 
    ##

    for index, row in edit_pages.iterrows():
        logging.info(f"Updating status of document {row['doc_title']}, page number {row['page_nr']}...")
        try:
            update_page_status(row['colid'], row['docid'], row['page_nr'], int(row['tsid']), status, sid, comment)
        except:
            logging.warning(f"Status not updated. Document title: {row['doc_title']}, page number: {row['page_nr']}.")

    logging.info('Script finished.')
