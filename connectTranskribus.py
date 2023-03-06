#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""
Script to interact with the API of the Transkribus platform.
"""


import requests
import xml.etree.ElementTree as et
import time
import logging


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
