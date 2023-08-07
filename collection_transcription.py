"""Execute specific layout analysis and text recognition on Transkribus.

With this module, the following steps are carried out on the Transkribus
platform for documents in different collections:
1. P2PaLA: Text regions are recognised.
2. Line finder: Text lines per text region are recognised.
3. Text recognition (HTR): Text per text line is recognised.

Each step is based on an existing model. The following functions are available:
- Collections not to be considered can be excluded (COLL_DROP).
- Only a set of documents can be considered (DOC_FILTER_DIR).
- Pages can be excluded based on status or page number (PAGE_DROP_STATUS,
PAGE_DROP_NR).
- Subsequent pages of an excluded page can be skipped
(PAGE_DROP_STATUS_FOLLOWING).

Note: Errors can occur in Transkribus jobs with FINISHED status. In the
Transkribus Expert Client, errors that occur in the jobs are documented in the
Error column. In this script, such errors that may have occurred are not
detected.
"""


import logging
import pandas as pd
import csv
import time

from connect_transkribus import (get_sid, list_collections,
                                 list_documents, get_document_content,
                                 run_layout_analysis, run_text_recognition)


# Set directory of logfile.
LOGFILE_DIR = './collection_transcription.log'

# List of collection ids that are dropped within this process.
COLL_DROP = [169494, 163061, 170320]

# CSV file, which contains per line a docId of documents to be filtered.
DOC_FILTER_DIR = './document_filter.csv'

# List of page status (of latest page version) that are dropped within this
# process.
PAGE_DROP_STATUS = ['DONE']

# Define if subsequent pages of a page with a status defined in
# PAGE_DROP_STATUS within the same Transkribus document should be dropped.
PAGE_DROP_STATUS_FOLLOWING = True

# List of page numbers that are dropped within this process.
PAGE_DROP_NR = [1, 2]

# Set the parameters for P2PaLA.
P2PALA_ID = 57774
P2PALA_NAME = 'HGB_M3'
MIN_AREA = 0.01
RECTIFY_REGIONS = 'true'
ENRICH_EXISTING_TRANSCRIPTIONS = 'false'
LABEL_REGIONS = 'false'
LABEL_LINES = 'false'
LABEL_WORDS = 'false'
KEEP_EXISTING_REGIONS = 'false'

# Set the parameters for line finder layout analysis.
LINEFINDER_ID = 53419
LINEFINDER_NAME = 'HGB_Baseline_M1'
MIN_PATH_LENGTH = 40
BIN_THRESH = 55
SEP_THRESH = 125
MAX_DIST_FRACTION = 0.01
CLUSTERING_METHOD = 'legacy'
CLUSTERING_LEGACY_TYPE = 'default'
CLUSTER_DIST_FRACTION = 1.0
SCALE = 1.0
LINE_OVERLAP_FRACTION = 0.1

# Set the parameter for text recognition.
HTR_ID = 52861  # HGB_FT_M5.2
DO_WORD_SEG = 'false'


def main():
    # Define the logging environment.
    print(f'Consider the logfile {LOGFILE_DIR} for information about the run.')
    logging.basicConfig(filename=LOGFILE_DIR,
                        format='%(asctime)s   %(levelname)s   %(message)s',
                        level=logging.INFO,
                        encoding='utf-8')
    logging.info('Script started.')

    # Define which models are applied.
    do_p2pala = input('Do you want to apply P2PaLA (text region recognition)? '
                      )
    if do_p2pala.lower() in ('true', 'yes', 'y', '1'):
        do_p2pala = True
    elif do_p2pala.lower() in ('false', 'no', 'n', '0'):
        do_p2pala = False
    else:
        logging.error(f'Your answer is not True or False: {do_p2pala}.')
        raise
    logging.info(f'P2PaLA will be applied: {do_p2pala}.')
    do_linefinder = input('Do you want to apply Linefinder (text line '
                          'recognition)? ')
    if do_linefinder.lower() in ('true', 'yes', 'y', '1'):
        do_linefinder = True
    elif do_linefinder.lower() in ('false', 'no', 'n', '0'):
        do_linefinder = False
    else:
        logging.error(f'Your answer is not True or False: {do_linefinder}.')
        raise
    logging.info(f'Linefinder will be applied: {do_linefinder}.')
    do_htr = input('Do you want to apply HTR (text recognition)? ')
    if do_htr.lower() in ('true', 'yes', 'y', '1'):
        do_htr = True
    elif do_htr.lower() in ('false', 'no', 'n', '0'):
        do_htr = False
    else:
        logging.error(f'Your answer is not True or False: {do_htr}.')
        raise
    logging.info(f'HTR will be applied: {do_htr}.')

    # Login to Transkribus.
    user = input('Transkribus user:')
    password = input('Transkribus password:')
    sid = get_sid(user, password)

    # Define all collections to be processed.
    coll_raw = pd.DataFrame(list_collections(sid))
    coll = coll_raw[~coll_raw['colId'].isin(COLL_DROP)]

    # Load document ids to be processed.
    with open(DOC_FILTER_DIR, 'r') as csvfile:
        doc_filter = [int(row[0]) for row in csv.reader(csvfile)]

    for row in coll.iterrows():
        logging.info(f"Processing collection {row[1]['colName']}...")

        # Define all documents to be processed.
        docs_raw = list_documents(sid, row[1]['colId'])
        docs = [d for d in docs_raw if d['docId'] in doc_filter]

        for doc in docs:
            start_time = time.time()

            # Generate a dictionary of pages to process.
            pages = get_document_content(row[1]['colId'],
                                         doc['docId'],
                                         sid)['pageList']
            page_nr_selected = {}
            drop_following_pages = False
            for page in pages['pages']:
                if drop_following_pages:
                    break
                elif page['pageNr'] in PAGE_DROP_NR:
                    continue
                elif (page['tsList']['transcripts'][0]['status'] in
                      PAGE_DROP_STATUS):
                    if PAGE_DROP_STATUS_FOLLOWING:
                        drop_following_pages = True
                    continue
                else:
                    page_nr_selected[page['pageNr']] = page['pageId']

            # Omit layout analysis and text recognition if there are no pages
            # to consider.
            if not page_nr_selected:
                continue

            # Generate xml string of page ids.
            pageid_str = ''
            for p in list(page_nr_selected.values()):
                pageid_str += f'<pages><pageId>{p}</pageId></pages>'

            if do_p2pala:
                # Generate xml for post request for P2PaLA job.
                p2pala_xml = '<?xml version="1.0" encoding="UTF-8" '\
                    'standalone="yes"?>'\
                    f"<jobParameters><docList><docs><docId>{doc['docId']}"\
                    f'</docId><pageList>{pageid_str}</pageList></docs>'\
                    '</docList><params>'\
                    f'<entry><key>modelId</key><value>{P2PALA_ID}</value>'\
                    '</entry>'\
                    f'<entry><key>modelName</key><value>{P2PALA_NAME}</value>'\
                    '</entry>'\
                    f'<entry><key>--min_area</key><value>{MIN_AREA}</value>'\
                    '</entry>'\
                    '<entry><key>--rectify_regions</key><value>'\
                    f'{RECTIFY_REGIONS}</value></entry>'\
                    '<entry><key>enrichExistingTranscriptions</key>'\
                    f'<value>{ENRICH_EXISTING_TRANSCRIPTIONS}</value></entry>'\
                    f'<entry><key>labelRegions</key><value>{LABEL_REGIONS}'\
                    '</value></entry>'\
                    f'<entry><key>labelLines</key><value>{LABEL_LINES}'\
                    '</value></entry>'\
                    f'<entry><key>labelWords</key><value>{LABEL_WORDS}'\
                    '</value></entry>'\
                    '<entry><key>keepExistingRegions</key><value>'\
                    f'{KEEP_EXISTING_REGIONS}</value></entry></params>'\
                    '</jobParameters>'

                # Start a P2PaLA job.
                run_layout_analysis(
                    xml=p2pala_xml,
                    colid=row[1]['colId'],
                    sid=sid,
                    do_block_seg='true',
                    job_impl='P2PaLAJob'
                    )

            if do_linefinder:
                # Generate xml for post request for line finder job.
                linefinder_xml = '<?xml version="1.0" encoding="UTF-8" '\
                    'standalone="yes"?>'\
                    f"<jobParameters><docList><docs><docId>{doc['docId']}"\
                    f'</docId><pageList>{pageid_str}</pageList></docs>'\
                    '</docList><params>'\
                    f'<entry><key>modelId</key><value>{LINEFINDER_ID}</value>'\
                    '</entry>'\
                    f'<entry><key>modelName</key><value>{LINEFINDER_NAME}'\
                    '</value></entry>'\
                    '<entry><key>pars.min_path_length</key><value>'\
                    f'{MIN_PATH_LENGTH}</value></entry>'\
                    f'<entry><key>pars.bin_thresh</key><value>{BIN_THRESH}'\
                    '</value></entry><entry><key>pars.sep_thresh</key><value>'\
                    f'{SEP_THRESH}</value></entry>'\
                    '<entry><key>pars.max_dist_fraction</key><value>'\
                    f'{MAX_DIST_FRACTION}</value></entry>'\
                    '<entry><key>pars.clustering_method</key><value>'\
                    f'{CLUSTERING_METHOD}</value></entry>'\
                    '<entry><key>pars.clustering_legacy_type</key><value>'\
                    f'{CLUSTERING_LEGACY_TYPE}</value></entry>'\
                    '<entry><key>pars.cluster_dist_fraction</key><value>'\
                    f'{CLUSTER_DIST_FRACTION}</value></entry>'\
                    f'<entry><key>pars.scale</key><value>{SCALE}</value>'\
                    '</entry>'\
                    '<entry><key>pars.line_overlap_fraction</key><value>'\
                    f'{LINE_OVERLAP_FRACTION}</value></entry>'\
                    '</params></jobParameters>'

                # Start a line finder job.
                run_layout_analysis(
                    xml=linefinder_xml,
                    colid=row[1]['colId'],
                    sid=sid
                    )

            if do_htr:
                # Create a string of selected pages for HTR request.
                pages_str = ','.join(
                    [str(key) for key in page_nr_selected.keys()]
                    )

                # Start a text recognition job.
                run_text_recognition(
                    colid=row[1]['colId'],
                    docid=doc['docId'],
                    pages=pages_str,
                    model_id=HTR_ID,
                    sid=sid,
                    do_word_seg=DO_WORD_SEG
                    )

            logging.info(f"Time to process document {doc['title']}: "
                         f'{round(time.time() - start_time, 2)}s. '
                         f'Number of pages processed: {len(page_nr_selected)}.'
                         )

    logging.info('Script finished.')


if __name__ == "__main__":
    main()
