"""Download the latest pagexml of all pages over several collections.

With this module, Transkribus pagexml can be exported directly to a target
directory "DEST_DIR". Only the most recent pagexml is exported for each
Transkribus page.
- Collections with the prefix COLNAME_PREFIX are saved in the target directory
according to the following structure:
[collection_name]/[document_name]/[pagexml_filename]
- Collections in the COLNAME_TRAINING list are saved in the target directory
according to the following pattern:
[collection_name]/[document_name]/[document_name]_[page_number]/[pagexml_filename]

Special case collection 'HGB_Training':
- PageXML from documents with the prefix 'TRAINING_VALIDATION_SET' are not
exported.
- If available, the latest pageXML version with status 'GT' (ground truth) is
exported instead of the latest version with any status.
"""


import pandas as pd
from datetime import datetime
import os


from connect_transkribus import (get_sid, list_collections, list_documents,
                                 get_document_content, download_pagexml)


# Define which collections are to be processed.
COLNAME_PREFIX = 'HGB_1_'
COLNAME_TRAINING = ['HGB_Training', 'HGB_Experimentell']

# Define target directory for pageXMLs.
DEST_DIR = ('/mnt/research-storage/Projekt_HGB/HGB_pageXML_'
            + datetime.now().strftime('%Y%m%d'))


def main():
    # Login to Transkribus.
    user = input('Transkribus user:')
    password = input('Transkribus password:')
    sid = get_sid(user, password)

    # Read all available collections.
    coll = pd.DataFrame(list_collections(sid))

    # Save pagexmls of all pages within collections of interest.
    for col in coll.iterrows():
        if (col[1]['colName'] not in COLNAME_TRAINING
            and not col[1]['colName'].startswith(COLNAME_PREFIX)):
            continue
        docs = list_documents(sid, col[1]['colId'])
        for doc in docs:
            if (col[1]['colName'] == 'HGB_Training'
                and doc['title'].startswith('TRAINING_VALIDATION_SET')):
                # Skip selected documents within collection HGB_Training.
                continue
            pages = get_document_content(col[1]['colId'], doc['docId'], sid)

            # Create destination folder.
            dest_path = f"{DEST_DIR}/{col[1]['colName']}/{doc['title']}"
            if not os.path.exists(dest_path):
                os.makedirs(dest_path)

            for page in pages['pageList']['pages']:
                # Determine the latest transcript.
                timestamp_latest = datetime.min
                timestamp_latest_gt = datetime.min
                index_latest = None
                index_latest_gt = None
                index = -1
                for transcript in page['tsList']['transcripts']:
                    index += 1
                    timestamp = datetime.fromtimestamp(
                        transcript['timestamp']/1000
                        )
                    timestamp_latest = max(timestamp_latest, timestamp)
                    if timestamp_latest == timestamp:
                        index_latest = index

                    # For HGB_Training, determine the latest transcript basded
                    # also by the status.
                    if col[1]['colName'] == 'HGB_Training':
                        if transcript['status'] == 'GT':
                            timestamp_latest_gt = max(timestamp_latest_gt,
                                                      timestamp
                                                      )
                            if timestamp_latest_gt == timestamp:
                                index_latest_gt = index

                if (index_latest_gt is not None
                    and index_latest != index_latest_gt):
                    index_latest = index_latest_gt

                # Download pagexml of latest transcript.
                url_latest = page['tsList']['transcripts'][index_latest]['url']
                filename_latest = page['tsList']['transcripts'][index_latest][
                    'fileName']
                if col[1]['colName'] in COLNAME_TRAINING:
                    folder = f"{doc['title']}_{str(page['pageNr']).zfill(3)}"
                    if not os.path.exists(f'{dest_path}/{folder}'):
                        os.makedirs(f'{dest_path}/{folder}')
                    path = f'{dest_path}/{folder}/{filename_latest}'
                else:
                    path = f'{dest_path}/{filename_latest}'
                download_pagexml(url_latest, path)


if __name__ == "__main__":
    main()
