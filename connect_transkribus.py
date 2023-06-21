#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""
Script to interact with the API of the Transkribus platform.
"""


import requests
import xml.etree.ElementTree as et
import time
import logging
import pandas as pd
import re


def get_sid(usr, pw):
    # Login to the API of transkribus and return the session id

    r = requests.post("https://transkribus.eu/TrpServer/rest/auth/login", data={"user": usr, "pw": pw})
    if r.status_code == requests.codes.ok:
        login_data = et.fromstring(r.text)
        return login_data.find("sessionId").text
    else:
        logging.error(f'Login failed: {r}')
        raise


def list_collections(sid):
    # Get information of all collections available for the account

    r = requests.get("https://transkribus.eu/TrpServer/rest/collections/list?JSESSIONID={}".format(sid))
    if r.status_code == requests.codes.ok:
        return r.json()
    else:
        logging.error(f'SessionID invalid? {r}')
        raise


def get_colid(col_name, sid):
    '''Given the name of one collection and session id,
    the function returns the corresponding collection id, if available.'''

    # Get available collections
    coll = pd.DataFrame(list_collections(sid))

    try:
        # Determine collection id of interest
        return coll[coll['colName'] == col_name]['colId'].iloc[0]
    except:
        # Collection with name given not found
        logging.error(f'No collection of name {col_name} found.')
        raise


def list_documents(sid, colid):
    # Get information of all documents of one collection

    r = requests.get("https://transkribus.eu/TrpServer/rest/collections/{}/list?JSESSIONID={}".format(colid, sid))
    if r.status_code == requests.codes.ok:
        return r.json()
    else:
        logging.error(f'SessionID or collectionID invalid? {r}')
        raise


def get_document_content(colid, docid, sid):
    # Get content of a specific document

    r = requests.get("https://transkribus.eu/TrpServer/rest/collections/{}/{}/fulldoc?JSESSIONID={}".format(colid,
                                                                                                            docid,
                                                                                                            sid))
    if r.status_code == requests.codes.ok:
        return r.json()
    else:
        logging.error(f'documentID or collectionID invalid? {r}')
        raise


def get_page_xml_url(doc_content, page_nr, page_version):
    # Given the document content derived by get_document_content(),
    # extracts the page xml url of a selected document page version

    return doc_content['pageList']['pages'][page_nr - 1]['tsList']['transcripts'][page_version]['url']


def get_page_xml(urlxml, sid, retry=False):
    # Get the page xml of a given document page

    try:
        r = requests.get(urlxml)
        r.encoding = 'utf-8'
        if r.status_code == requests.codes.ok:
            return r.text
        elif r.status_code == 500:
            # Internal Server Error: try a second time
            time.sleep(60)
            return get_page_xml(urlxml, sid, retry=True)
        else:
            logging.error(f'url invalid? {r}')
            return None
    except requests.ConnectionError as err:
        # Retry once if the connection went lost
        if not retry:
            time.sleep(60)
            return get_page_xml(urlxml, sid, retry=True)
        else:
            logging.error(f'Connection error: {err}')
            raise


def post_page_xml(page_xml, colid, docid, page_nr, sid, comment, status=''):
    # Update a page xml of a given document page (API method postPageTranscript)
    # If variable status is an empty string, the status on Transkribus will not change.

    r = requests.post(f'https://transkribus.eu/TrpServer/rest/collections/{colid}/{docid}/{page_nr}/text?JSESSIONID={sid}',
                      data=page_xml.encode('utf8'), params={'note': comment, 'status': status}
                      )
    if r.status_code == requests.codes.ok:
        return True
    else:
        logging.error(f'documentID or collectionID invalid? {r}')
        raise


def update_page_status(colid, docid, pagenr, transcriptid, status, sid, comment='Status changed.'):
    '''Updates a transcript status of a specific page using the Transkribus API method updatePageStatus.'''

    r = requests.post(f'https://transkribus.eu/TrpServer/rest/collections/{colid}/{docid}/{pagenr}/{transcriptid}/status?JSESSIONID={sid}',
                    params={'note': comment, 'status': status}
                    )
    if r.status_code == requests.codes.ok:
        return True
    else:
        logging.error(f'collectionID, documentID, pageNr or transcriptId invalid? {r}')
        raise


def get_job_status(jobid, sid):
    """Query the status of a job.

    Args:
        jobid (int): Id of a Transkribus job.
        sid (str): Session id to Transkribus server.

    Returns:
        str: Status of the job.

    Raises:
        Job status cannot be retrieved.
    """
    r = requests.get(f'https://transkribus.eu/TrpServer/rest/jobs/{jobid}'
                     f'?JSESSIONID={sid}')
    if r.status_code == requests.codes.ok:
        return re.search(r'"state":"[A-Z]+"', r.text).group()[9:-1]
    else:
        logging.error(f'Job status cannot be retrieved: {r}')
        raise


def run_layout_analysis(
        colid,
        docid,
        pageid,
        model_id,
        model_name,
        sid,
        min_area=0.01,
        rectify_regions='false',
        enrich_existing_transcriptions='true',
        label_regions='false',
        label_lines='false',
        label_words='false',
        keep_existing_regions='false',
        do_block_seg='false',
        do_line_seg='true',
        do_word_seg='false',
        do_polygon_to_baseline='false',
        do_baseline_to_polygon='false',
        job_impl='TranskribusLaJob'):
    """Run a layout analysis on Transkribus platform.
    This function start a layout analysis for selected pages within a document
    using the Transkribus API. If the job created is completed, the function
    returns the request text.

    Args:
        colid (int): Id of collection.
        docid (int): Id of document.
        pageid (list of int): Ids of pages to process.
        model_id (int): Id of layout analysis model.
        model_name (str): Name of layout analysis model.
        sid (str): Session id to Transkribus platform.
        min_area (float): Minimal area for new layouts (regions).
        rectify_regions (str): Should regions be rectified?
        enrich_existing_transcriptions (str): Should existing
            transcriptions be enriched?
        label_regions (str): Should regions be labelled?
        label_lines (str): Should lines be labelled?
        label_words (str): Should words be labelled?
        keep_existing_regions (str): Sould existing regions be kept?
        do_block_seg (str): Should block segmentation be done?
        do_line_seg (str): Should line segmentation be done?
        do_word_seg (str): Should word segmentation be done?
        do_polygon_to_baseline (str): Should polygon to baseline be done?
        do_baseline_to_polygon (str): Should baseline to polygon be done?
        job_impl (str): Name of layout analysis method.

    Returns:
        str: Text of request return.

    Raises:
        Request status code is not OK.
    """

    # Generate a string which concatenates all page ids in xml form.
    pageid_str = ''
    for p in pageid:
        pageid_str += f'<pages><pageId>{p}</pageId></pages>'

    # Start the layout analysis.
    xml = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'\
          f'<jobParameters><docList><docs><docId>{docid}</docId>'\
          f'<pageList>{pageid_str}</pageList></docs></docList><params>'\
          f'<entry><key>modelId</key><value>{model_id}</value></entry>'\
          f'<entry><key>modelName</key><value>{model_name}</value></entry>'\
          f'<entry><key>--min_area</key><value>{min_area}</value></entry>'\
          f'<entry><key>--rectify_regions</key><value>{rectify_regions}'\
          '</value></entry>'\
          '<entry><key>enrichExistingTranscriptions</key>'\
          f'<value>{enrich_existing_transcriptions}</value></entry>'\
          f'<entry><key>labelRegions</key><value>{label_regions}</value>'\
          '</entry>'\
          f'<entry><key>labelLines</key><value>{label_lines}</value></entry>'\
          f'<entry><key>labelWords</key><value>{label_words}</value></entry>'\
          '<entry><key>keepExistingRegions</key><value>'\
          f'{keep_existing_regions}</value></entry></params></jobParameters>'
    headers = {'Content-Type': 'application/xml', 'Accept': 'application/json'}
    r = requests.post('https://transkribus.eu/TrpServer/rest/LA',
                      headers=headers,
                      data=xml.encode('utf8'),
                      params={'JSESSIONID': sid,
                              'collId': colid,
                              'doBlockSeg': do_block_seg,
                              'doLineSeg': do_line_seg,
                              'doWordSeg': do_word_seg,
                              'doPolygonToBaseline': do_polygon_to_baseline,
                              'doBaselineToPolygon': do_baseline_to_polygon,
                              'jobImpl': job_impl})
    if r.status_code != requests.codes.ok:
        logging.error(f'Layout analysis execution failed: {r}')
        raise

    # Wait until the job is completed.
    jobid = int(re.search(r'"jobId":"[0-9]+"', r.text).group()[9:-1])
    while True:
        job_status = get_job_status(jobid, sid)
        if job_status == 'FINISHED':
            break
        time.sleep(10)

    return r.text
