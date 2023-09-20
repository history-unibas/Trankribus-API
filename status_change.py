""" Change the status of Transkribus pages.

Change the status of the latest version of selected Transkribus pages given
the following parameters per Transkribus page available in a csv file:
- collection title (colname),
- document title (title),
- page number (pagenr).
"""


import logging
import pandas as pd
from connect_transkribus import (get_sid, get_colid, list_documents,
                                 get_document_content, update_page_status)


# Set directory of csv file containing the Trankribus pages to be changed.
PAGE_CHANGE_DIR = './reichspfennigverzeichnis.csv'

# Set directory of logfile and data export.
LOGFILE_DIR = './status_change.log'
DATA_OUTPUT_DIR = './status_change.csv'

# Set new Transkribus status for selected pages.
STATUS = 'DONE'

# Define Trankribus comment for new page version.
COMMENT = ('Page identified as part of the "Reichspfennigverzeichnis". '
           'Status set to DONE.')


def main():
    # Define logging environment.
    print(f'Consider the logfile {LOGFILE_DIR} for information about the run.')
    logging.basicConfig(filename=LOGFILE_DIR,
                        format='%(asctime)s   %(levelname)s   %(message)s',
                        level=logging.INFO,
                        encoding='utf-8'
                        )
    logging.info('Script started.')

    # Login to Transkribus.
    user = input('Transkribus user:')
    password = input('Transkribus password:')
    sid = get_sid(user, password)

    # Read pages of interest.
    edit_pages = pd.read_csv(PAGE_CHANGE_DIR)

    # Determine the necessary variables for updating the status.
    # Get collection id.
    lut_coll = pd.DataFrame()
    lut_coll['colname'] = pd.unique(edit_pages['colname'])
    lut_coll['colid'] = lut_coll.apply(
        lambda row: get_colid(col_name=row['colname'], sid=sid), axis=1)
    edit_pages['colid'] = edit_pages.apply(
        lambda row: lut_coll[
            lut_coll['colname'] == row['colname']
            ]['colid'].iloc[0], axis=1)

    # Create a lookup table for affected documents to get their document ids.
    lut_doc = pd.DataFrame(columns=['docid', 'tsid'])
    lut_doc['docname'] = pd.unique(edit_pages['title'])
    lut_doc['colname'] = lut_doc.apply(
        lambda row: edit_pages[edit_pages['title'] == row['docname']]
        ['colname'].iloc[0], axis=1)

    # Iterate over affected collections.
    for row in lut_coll.iterrows():
        docs = pd.DataFrame(list_documents(sid=sid, colid=row[1]['colid']))
        lut_docs = lut_doc[lut_doc['colname'] == row[1]['colname']]

        # Iterate over affected documents.
        for i, r in lut_docs.iterrows():
            docname = r['docname']
            docid = docs[docs['title'] == docname]['docId'].iloc[0]

            # Save the document id in lookup table.
            lut_doc.at[i, 'docid'] = docid

            # Receive the document content for getting the transkript id.
            doc_content = get_document_content(colid=row[1]['colid'],
                                               docid=docid, sid=sid)
            pages = edit_pages[edit_pages['title'] == docname]

            # Iterate over affected pages.
            for j, t in pages.iterrows():
                # Store the transcript id of the latest version (first entry
                # in list).
                try:
                    edit_pages.at[j, 'tsid'] = (
                        doc_content['pageList']['pages'][t['pagenr'] - 1]
                        ['tsList']['transcripts'][0]['tsId'])
                except Exception as e:
                    logging.warning(
                        f"Page not found. Document title: {t['title']}, "
                        f"page number: {t['pagenr']}. Error message: "
                        f'{str(e)}')

    # Get document id.
    edit_pages['docid'] = edit_pages.apply(
        lambda row: lut_doc[lut_doc['docname'] == row['title']]
        ['docid'].iloc[0], axis=1)

    # Export the dataframe.
    edit_pages.to_csv(DATA_OUTPUT_DIR, index=False, header=True)
    logging.info('Dataframe created from pages to be modified and written to '
                 f'{DATA_OUTPUT_DIR}.')

    # Adapt status of selected Transkribus pages.
    for row in edit_pages.iterrows():
        logging.info(
            f"Updating status of document {row[1]['title']}, page number "
            f"{row[1]['pagenr']}...")
        try:
            update_page_status(colid=row[1]['colid'],
                               docid=row[1]['docid'],
                               pagenr=row[1]['pagenr'],
                               transcriptid=int(row[1]['tsid']),
                               status=STATUS,
                               sid=sid,
                               comment=COMMENT)
        except Exception as e:
            logging.warning(
                f"Status not updated. Document title: {row[1]['title']}, "
                f"page number: {row[1]['pagenr']}, error message: {str(e)}")

    logging.info('Script finished.')


if __name__ == "__main__":
    main()
