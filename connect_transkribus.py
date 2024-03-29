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


def get_page_xml(urlxml, sid, n_retry=60):
    # Get the page xml of a given document page

    try:
        r = requests.get(urlxml)
        r.encoding = 'utf-8'
        if r.status_code == requests.codes.ok:
            return r.text
        elif r.status_code == 500:
            # Internal Server Error: try again.
            if n_retry > 0:
                n_retry -= 1
                time.sleep(60)
                return get_page_xml(urlxml, sid, n_retry)
            else:
                logging.error(f'url invalid? {r}')
                raise
        else:
            if n_retry > 0:
                n_retry -= 1
                time.sleep(60)
                return get_page_xml(urlxml, sid, n_retry)
            else:
                logging.error(f'url invalid? {r}')
                raise
    except requests.ConnectionError as err:
        # Retry if the connection went lost.
        if n_retry > 0:
            n_retry -= 1
            time.sleep(60)
            return get_page_xml(urlxml, sid, n_retry)
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


def get_job_status(jobid: int, sid: str, n_retry: int = 60):
    """Query the status of a job.

    Args:
        jobid (int): Id of a Transkribus job.
        sid (str): Session id to Transkribus server.
        n_retry (int): Number of maximal retries.

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
        if n_retry > 0:
            n_retry -= 1
            time.sleep(60)
            return get_job_status(jobid, sid, n_retry)
        else:
            logging.error(f'Job status cannot be retrieved: {r}, {r.text}')
            raise


def run_layout_analysis(
        xml,
        colid,
        sid,
        do_block_seg='false',
        do_line_seg='true',
        do_word_seg='false',
        job_impl='TranskribusLaJob',
        do_create_job_batch='false'):
    """Run a layout analysis on Transkribus platform.
    This function start a layout analysis for selected pages within a document
    using the Transkribus API. If the job created is completed, the function
    returns.
    The structure and content of the xml can be obtained using the Transkribus 
    Expert Client:
    - Start the Transkribus Expert Client via command line.
    - Execute a desired layout analysis with the Transkribus Expert Client.
    - Search the command line for the corresponding POST request.

    Args:
        xml(str): Xml containing the job parameters.
        colid (int): Id of collection.
        sid (str): Session id to Transkribus platform.
        do_block_seg (str): Should block segmentation be done?
        do_line_seg (str): Should line segmentation be done?
        do_word_seg (str): Should word segmentation be done?
        job_impl (str): Name of layout analysis method.
        do_create_job_batch (str): Should a job batch be created?

    Returns:
        None.

    Raises:
        Request status code is not OK.
    """

    # Start the layout analysis.
    headers = {'Content-Type': 'application/xml', 'Accept': 'application/json'}
    r = requests.post('https://transkribus.eu/TrpServer/rest/LA',
                      headers=headers,
                      data=xml.encode('utf8'),
                      params={'JSESSIONID': sid,
                              'collId': colid,
                              'doBlockSeg': do_block_seg,
                              'doLineSeg': do_line_seg,
                              'doWordSeg': do_word_seg,
                              'jobImpl': job_impl,
                              'doCreateJobBatch': do_create_job_batch})
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


def run_text_recognition(colid, docid, pages,
                         model_id,
                         sid,
                         language_model='trainDataLanguageModel',
                         do_line_polygon_simplification='true',
                         keep_original_line_polygons='false',
                         write_kws_index='false',
                         n_best=1,
                         use_existing_line_polygons='false',
                         batch_size=10,
                         clear_lines='true',
                         do_word_seg='true',
                         do_not_delete_work_dir='false',
                         b2p_backend='Legacy'):
    """Run a text recognition job on the Transkribus platform.
    This function start a text recognition for selected pages within a document
    using the Transkribus API. If the job created is completed, the function
    returns.

    Args:
        colid (int): Id of collection.
        docid (int): Id of document.
        pages (str): String of page numbers to consider.
        sid (str): Session id to Transkribus platform.
        language_model (str): Language dictionary of the model.
        do_line_polygon_simplification (str): Sould line polygon
            simplification be done?
        keep_original_line_polygons (str): Sould original line polygons be
            kept?
        write_kws_index (str): Sould kws index be written?
        n_best (int): Number of best.
        use_existing_line_polygons (str): Sould existing line polygons be used?
        batch_size (int): Size of the batch.
        clear_lines (str): Sould the lines be cleared?
        do_word_seg (str): Sould word segmentation be done?
        do_not_delete_work_dir (str): Sould the working directory not be
            deleted?
        b2p_backend (str): B2p backend.

    Returns:
        None.

    Raises:
        Request status code is not OK.
    """
    params = {'JSESSIONID': sid,
              'languageModel': language_model,
              'id': docid,
              'pages': pages,
              'doLinePolygonSimplification': do_line_polygon_simplification,
              'keepOriginalLinePolygons': keep_original_line_polygons,
              'writeKwsIndex': write_kws_index,
              'nBest': n_best,
              'useExistingLinePolygons': use_existing_line_polygons,
              'batchSize': batch_size,
              'clearLines': clear_lines,
              'doWordSeg': do_word_seg,
              'doNotDeleteWorkDir': do_not_delete_work_dir,
              'b2pBackend': b2p_backend
              }
    r = requests.post(f'https://transkribus.eu/TrpServer/rest/pylaia/{colid}/'
                      f'{model_id}/recognition',
                      params=params
                      )

    if r.status_code != requests.codes.ok:
        logging.error(f'Text recognition execution failed: {r}, {r.text}')
        raise

    # Wait until the job is completed.
    jobid = int(r.text)
    while True:
        job_status = get_job_status(jobid, sid)
        if job_status == 'FINISHED':
            break
        time.sleep(10)


def remove_transcript(colid, docid, pagenr, tskey, sid):
    """Delete one selected transcript (page version) on Transkribus.
    Args:
        colid (int): Id of collection.
        docid (int): Id of document.
        pagenr (int): Page number.
        tskey (str): Key of the transcript to be deleted.
        sid (str): Session id to Transkribus platform.

    Returns:
        None.

    Raises:
        Request status code is not OK.
    """
    params = {'JSESSIONID': sid, 'key': tskey}
    r = requests.post('https://transkribus.eu/TrpServer/rest/collections/'
                      f'{colid}/{docid}/{pagenr}/delete',
                      params=params
                      )
    if r.status_code != requests.codes.ok:
        logging.error(f'Deleting of transcript failed: {r}')
        raise


def download_pagexml(url, path, n_retry=60):
    """Download a pagexml file.

    Args:
        url (str): Url to a page xml file.
        path (str): Target filepath to store the page xml file.
        n_retry (int): Number of retries by request error.

    Returns:
        None.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()

        with open(path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
    except:
        if n_retry > 0:
            n_retry -= 1
            time.sleep(60)
            download_pagexml(url, path, n_retry)
        else:
            response.raise_for_status()
