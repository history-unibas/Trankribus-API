"""
For a given Transkribus document, the script calculates the charactor error rate (CER) and word error rate (WER) comparing two Transkribus transcript versions.
If provided, only selected Transkribus text region types are considered. Furthermore, only reference transcript versionen of given status may be considered.

In particular, the following CER and WER score are derived:
- global CER and WER over all text regions in consideration
- CER and WER per type of text regions
- CER and WER per non-empty Transkribus page
- CER and WER per text region

More information about calculating CER or WER respectively:
https://huggingface.co/spaces/evaluate-metric/cer
https://huggingface.co/spaces/evaluate-metric/wer
"""


from datetime import datetime
import re
import pandas as pd
import numpy as np
from evaluate import load
from csv import writer
import matplotlib.pyplot as plt
import seaborn as sns
import xml.etree.ElementTree as et
import logging
from connectTranskribus import get_page_xml, get_sid, get_document_content


def get_page_version_index(transcripts, version_keyword):
    '''Given the Transkribus list of transcripts and a keyword of the page version, returns the corresponding transcripts index.'''

    if version_keyword == 'latest':
        return 0
    else:
        index = 0
        for transcript in transcripts:
            try:
                match = re.search(version_keyword, transcript['toolName'])
            except:
                match = None
            if match:
                return index
            index += 1
        return None


def get_textregions(url, sid, textregion_types=[]):
    '''Given the url to a Transkribus page xml, the functions extracts the id, type (if availabel) and the textlines of textregions.
    If the attribute textregion_types is provided in additional, only those text region types will be returned.'''

    page_xml = et.fromstring(get_page_xml(url, sid))

    textregions = []
    # Iterate over text regions
    for textregion in page_xml.iter('{http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15}TextRegion'):
        # Find all unicode tag childs
        unicode = textregion.findall('.//{http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15}Unicode')

        # Get custom parameter
        custom = textregion.get('custom')

        # Extract type of text region
        try:
            type = re.search(r'type:[a-z]+;', custom).group()[5:-1]
        except:
            type = None

        # Filter textregions
        if textregion_types == []:
            # If no filter is provided, all textregions will be processed.
            pass
        elif type not in textregion_types:
            # If filter is given, all not filtered textregion types will be scipped.
            continue

        # Get text region id
        id = textregion.get('id')

        # Extract all text lines
        textline = [item.text for item in unicode[:-1]]
        if not textline:
            # Skip empty textregions
            continue

        # Add textregion to list
        textregions.append([id, type, textline])

    return textregions


def calculate_metric(predictions, references, metric, is_valid=True):
    '''Calculating a metric given a list or predictions and references of the same length. Tested for cer and wer.'''

    # Handling not valid features
    if not is_valid:
        return np.nan

    # Calculating the value
    evaluate_metric = load(metric)
    return evaluate_metric.compute(predictions=predictions,
                                   references=references)


if __name__ == "__main__":

    ##
    # Set parameters
    ##

    # Set output directory
    output_dir = '.'

    # Define which textregion types should be considered. If parameter is an empty list, all textregion types will be analyzed.
    # textregion_types = ['header', 'paragraph']
    textregion_types = []

    # Set collection id
    colid = 163061  # HGB_Training

    # Set document id
    docid = 1369528  # HGB_Training_4

    # Set reference version of the Transkribus page ('latest' or part of Transkribus parameter toolName)
    reference_version = 'latest'

    # Only consider a reference version of a specific status. Set the value to None if you do not want to filter by status.
    filter_status = ['GT']

    # Set prediction version of the Transkribus page ('latest' or part of Transkribus parameter toolName)
    prediction_version = 'Model: 50719'

    # Set bin width of histogram
    hist_binwidth = 0.01

    # Define logging environment
    datetime_started = datetime.now()
    log_file = output_dir + '/validateHtrModel.log'
    print(f'Consider the logfile {log_file} for information about the run.')
    logging.basicConfig(filename=log_file, format='%(asctime)s   %(levelname)s   %(message)s',
                        level=logging.INFO, encoding='utf-8')
    logging.info('Script started.')

    if filter_status:
        logging.warning(f"Only pages with status {filter_status} will be considered.")

    ##
    # Create dataframe of text regions of interest
    ##

    # Login to Transkribus
    user = input('Transkribus user:')
    password = input('Transkribus password:')
    sid = get_sid(user, password)

    # Get document metadata
    doc = get_document_content(colid, docid, sid)

    # Iterate over every document page
    textregions = pd.DataFrame(columns=['colid', 'docid', 'pageid', 'pagenr',
                                        'tsid_reference', 'tsid_prediction',
                                        'url_reference', 'url_prediction',
                                        'textregionid', 'type',
                                        'text_reference', 'text_prediction',
                                        'is_valid', 'warning_message'])
    for page_nr in range(1, doc['md']['nrOfPages'] + 1):
        # Get page parameters
        page = doc['pageList']['pages'][page_nr -1]

        # Initialize new entry
        new_entry = {'colid': colid, 'docid': docid, 'pageid': page['pageId'],
                     'pagenr': page_nr, 'is_valid': True}

        # Determine reference version of page
        transcripts = page['tsList']['transcripts']
        reference_index = get_page_version_index(transcripts,
                                                 reference_version)
        if reference_index is None:
            logging.warning(f"No reference transcript version found for tsId = {transcripts[reference_index]['tsId']}. This page will be excluded from the calculations.")
            new_entry['warning_message'] = 'No reference transcript version found.'
            new_entry['is_valid'] = False     
            textregions = pd.concat([textregions, pd.Series(new_entry).to_frame().T], ignore_index=True)
            continue
        reference_transcript = transcripts[reference_index]
        if filter_status:
            # Exclude page, when status is not in filter_status
            if reference_transcript['status'] not in filter_status:
                continue
        new_entry['tsid_reference'] = reference_transcript['tsId']
        new_entry['url_reference'] = reference_transcript['url']

        # Determine prediction version of page
        prediction_index = get_page_version_index(transcripts,
                                                  prediction_version)
        if prediction_index is None:
            logging.warning(f"No prediction transcript version found for tsId = {transcripts[prediction_index]['tsId']}. This page will be excluded from the calculations.")
            new_entry['warning_message'] = 'No prediction transcript version found.'
            new_entry['is_valid'] = False
            textregions = pd.concat([textregions, pd.Series(new_entry).to_frame().T], ignore_index=True)
            continue
        prediction_transcript = transcripts[prediction_index]
        new_entry['tsid_prediction'] = prediction_transcript['tsId']
        new_entry['url_prediction'] = prediction_transcript['url']

        # Exclude case, when page version of reference is equal to prediction
        if reference_index == prediction_index:
            logging.warning(f"The reference and prediction transcript version found are the same: index = {reference_index}, tsId = {transcripts[reference_index]['tsId']}. This page will be excluded from the calculations.")
            new_entry['warning_message'] = 'Reference and prediction transcript are the same.'
            new_entry['is_valid'] = False

        # Get text regions of reference version of transcript
        reference_textregions = get_textregions(reference_transcript['url'], sid, textregion_types)
        if not reference_textregions:
            logging.warning(f"No non-empty textregions found for reference transcript. tsId = {transcripts[reference_index]['tsId']}. This page will be excluded from the calculations.")
            new_entry['warning_message'] = 'No non-empty textregions (of selected types) found for reference transcript.'
            new_entry['is_valid'] = False
            textregions = pd.concat([textregions, pd.Series(new_entry).to_frame().T], ignore_index=True)
            continue

        # Get text regions of prediction version of transcript
        prediction_textregions = get_textregions(prediction_transcript['url'], sid, textregion_types)
        if not prediction_textregions:
            logging.warning(f"No non-empty textregions found for prediction transcript. tsId = {transcripts[prediction_index]['tsId']}. This page will be excluded from the calculations.")
            new_entry['warning_message'] = 'No non-empty textregions (of selected types) found for prediction transcript.'
            new_entry['is_valid'] = False
            textregions = pd.concat([textregions, pd.Series(new_entry).to_frame().T], ignore_index=True)
            continue

        # Iterate over text regions of reference transcript
        for reference_tr in reference_textregions:
            tr_id = reference_tr[0]
            new_entry['textregionid'] = tr_id
            new_entry['type'] = reference_tr[1]
            new_entry['text_reference'] = reference_tr[2]
            new_entry['warning_message'] = None
            new_entry['is_valid'] = True

            # Check if text region is in prediction transcript available
            prediction_ids = [row[0] for row in prediction_textregions]
            if tr_id in prediction_ids:
                new_entry['text_prediction'] = prediction_textregions[prediction_ids.index(tr_id)][-1]
            else:
                logging.warning(f"No prediction transcript textregion found for textregion id {tr_id}: tsId = {transcripts[prediction_index]['tsId']}. This textregion will be excluded from the calculations.")
                new_entry['warning_message'] = 'No prediction transcript found for this textregion.'
                new_entry['is_valid'] = False
                new_entry['text_prediction'] = np.nan
                textregions = pd.concat([textregions, pd.Series(new_entry).to_frame().T], ignore_index=True)
                continue

            # Handling reference and perdiction of different length
            if len(new_entry['text_reference']) != len(new_entry['text_prediction']):
                logging.warning(f"Prediction transcript of textregion {tr_id} do not have the same number of lines than reference transcript. tsId (prediction transcript) = {transcripts[prediction_index]['tsId']}. This textregion will be excluded from the calculations.")
                new_entry['warning_message'] = 'Prediction and reference transcript do not have same length.'
                new_entry['is_valid'] = False
                textregions = pd.concat([textregions, pd.Series(new_entry).to_frame().T], ignore_index=True)
                continue

            # Add new entry
            textregions = pd.concat([textregions, pd.Series(new_entry).to_frame().T], ignore_index=True)

    logging.info(f'{len(textregions)} text regions of interest read.')

    if textregions.empty:
        logging.error('No textregion processed. No metric can be calculated.')
        raise

    ##
    # Calculate character error rate (CER) and word error rate (WER)
    ##

    # Calculate the global CER and WER over all text regions in consideration
    texts_reference = []
    texts_prediction = []
    for index, row in textregions.iterrows():
        if row['is_valid']:
            texts_reference.extend(row['text_reference'])
            texts_prediction.extend(row['text_prediction'])
    logging.info(f"Global CER: {round(calculate_metric(predictions=texts_prediction, references=texts_reference, metric='cer'), 3)}")
    logging.info(f"Global WER: {round(calculate_metric(predictions=texts_prediction, references=texts_reference, metric='wer'), 3)}")

    # Calculate the CER and WER per type of text regions
    textregions_groups = textregions.groupby('type')
    for group_name, df_group in textregions_groups:
        texts_reference = []
        texts_prediction = []
        for index, row in df_group.iterrows():
            if row['is_valid']:
                texts_reference.extend(row['text_reference'])
                texts_prediction.extend(row['text_prediction'])
        logging.info(f"CER for textregion type {group_name}: {round(calculate_metric(predictions=texts_prediction, references=texts_reference, metric='cer'), 3)}")
        logging.info(f"WER for textregion type {group_name}: {round(calculate_metric(predictions=texts_prediction, references=texts_reference, metric='wer'), 3)}")

    # Calculate the CER and WER for each Transkribus page
    textregions_groups = textregions.groupby('pageid')
    cer_pages = {}
    wer_pages = {}
    for group_name, df_group in textregions_groups:
        texts_reference = []
        texts_prediction = []
        for index, row in df_group.iterrows():
            if row['is_valid']:
                texts_reference.extend(row['text_reference'])
                texts_prediction.extend(row['text_prediction'])

        # Exclude empty pages
        if texts_prediction == [] or texts_prediction == []:
            continue

        cer_pages[group_name] = round(calculate_metric(predictions=texts_prediction, references=texts_reference, metric='cer'), 3)
        wer_pages[group_name] = round(calculate_metric(predictions=texts_prediction, references=texts_reference, metric='wer'), 3)
    logging.info('CER and WER calcuated per Transkribus page.')

    # Plot histograms for scores per Transkribus page with kernel density estimate
    cer_hist_dir = output_dir + '/cer_per_page.png'
    sns.histplot(list(cer_pages.values()), binwidth=hist_binwidth, kde=True)
    plt.title('CER per page')
    plt.savefig(cer_hist_dir)
    plt.clf()
    logging.info(f'Histogram for CER per page created: {cer_hist_dir}.')

    wer_hist_dir = output_dir + '/wer_per_page.png'
    sns.histplot(list(wer_pages.values()), binwidth=hist_binwidth, kde=True)
    plt.title('WER per page')
    plt.savefig(wer_hist_dir)
    logging.info(f'Histogram for WER per page created: {wer_hist_dir}.')

    # Calculate CER and WER for each text region
    textregions['cer'] = textregions.apply(lambda row: calculate_metric(predictions=row['text_prediction'], references=row['text_reference'],
                                                                        metric='cer', is_valid=row['is_valid']), axis=1)
    textregions['wer'] = textregions.apply(lambda row: calculate_metric(predictions=row['text_prediction'], references=row['text_reference'],
                                                                        metric='wer', is_valid=row['is_valid']), axis=1)
    logging.info('CER and WER calcuated per text region.')

    ##
    # Export results
    ##

    textregions_dir = output_dir + '/textregions.csv'
    textregions.to_csv(textregions_dir, index=False)
    logging.info(f'Table of text regions written: {textregions_dir}.')

    cer_pages_dir = output_dir + '/cer_pages.csv'
    with open(cer_pages_dir, 'w') as file:
        w = writer(file, delimiter=',', lineterminator='\n')
        w.writerow(['pageid', 'cer'])
        w.writerows(cer_pages.items())
    logging.info(f'CER per page written: {cer_pages_dir}.')

    wer_pages_dir = output_dir + '/wer_pages.csv'
    with open(wer_pages_dir, 'w') as file:
        w = writer(file, delimiter=',', lineterminator='\n')
        w.writerow(['pageid', 'wer'])
        w.writerows(wer_pages.items())
    logging.info(f'WER per page written: {wer_pages_dir}.')

    logging.info('Script finished.')
