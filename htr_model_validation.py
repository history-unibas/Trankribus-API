""" Validate a HTR model applied on the Transkribus platform.

For a given Transkribus document, the script calculates the
charactor error rate (CER) and word error rate (WER) comparing
two Transkribus transcript versions.
If provided, only selected Transkribus text region types are
considered. Furthermore, only reference transcript versionen
of given status may be considered.

In particular, the following CER and WER score are derived:
- global CER and WER over all text regions in consideration
- CER and WER per type of text regions
- CER and WER per non-empty Transkribus page
- CER and WER per text region

More information about calculating CER or WER respectively:
https://huggingface.co/spaces/evaluate-metric/cer
https://huggingface.co/spaces/evaluate-metric/wer
"""


import re
import pandas as pd
import numpy as np
from evaluate import load
from csv import writer
import matplotlib.pyplot as plt
import seaborn as sns
import xml.etree.ElementTree as et
import logging

from connect_transkribus import get_page_xml, get_sid, get_document_content


# Set output directory.
OUTPUT_DIR = '.'

# Define which textregion types should be considered. If parameter is an
# empty list, all textregion types will be analyzed.
# Example: TEXTREGION_TYPES = ['header', 'paragraph']
TEXTREGION_TYPES = ['header', 'paragraph']

# Set collection id of collection to be considered.
COLID = 163061  # HGB_Training

# Set document id of document to be considered.
DOCID = 1401077  # HGB_Training_10

# Define a keyword for reference version of the Transkribus page.
# Possibilities:
# - 'latest': the latest page version will be considered as reference.
# - <keyword_toolname>: a keyword string corresponding to part of the
# Transkribus toolName. The latest page version containing this keyword
# will be considered as reference.
# - <status>: a Transkribus status. The latest page version with this
# status will be considered as reference.
REFERENCE_VERSION = 'latest'

# Only consider a reference version of a specific status. Set the value to
# None if you do not want to filter by status.
# Example: FILTER_STATUS = ['GT']
FILTER_STATUS = ['GT']

# Define a keyword for prediction version of the Transkribus page.
# The possibilities are the same than for the variable REFERENCE_VERSION.
PREDICTION_VERSION = 'Model: 50719'

# Set the bin width of histogram.
HIST_BINWIDTH = 0.01


def get_page_version_index(transcripts, version_keyword):
    """Get the index of a Transkribus page version based on a keyword.
    Given the Transkribus list of transcripts and a keyword of the
    page version, returns the corresponding transcript index.

    Args:
        transcripts (list): The list containing Transkribus transcripts.
        version_keyword (str): A keyword of a page version.

    Returns:
        int or None: Index of transkript version in case of keyword matching.

    Raises:
        TypeError: If the input is not a number.
    """

    if not isinstance(version_keyword, str):
        raise TypeError("version_keyword must be a string.")
    if version_keyword == 'latest':
        return 0
    else:
        index = 0
        for transcript in transcripts:
            match = None
            if 'toolName' in transcript:
                match = re.search(version_keyword, transcript['toolName'])
            if match or transcript['status'] == version_keyword:
                return index
            index += 1
        return None


def get_textregions(url, sid, textregion_types=[]):
    """ Get all text regions of a Transkribus page.
    Given the url to a Transkribus page xml, the functions extracts the id,
    type (if available) and the textlines of textregions.
    If the attribute textregion_types is provided in additional,
    only those text region types will be returned.

    Args:
        url (str): Url of a Transkribus page xml.
        sid (str): Session id of a Transkribus API session.
        textregion_types (list): List of Strings to filter textregion types.

    Returns:
        list: List containing id, type and textlines of all non-empty
        textregions.
    """

    page_xml = et.fromstring(get_page_xml(url, sid))
    textregions = []

    # Iterate over the text regions.
    for textregion in page_xml.iter('{http://schema.primaresearch.org/PAGE'
                                    '/gts/pagecontent/2013-07-15}TextRegion'):
        # Find all unicode tag childs.
        unicode = textregion.findall('.//{http://schema.primaresearch.org/PAGE'
                                     '/gts/pagecontent/2013-07-15}Unicode')

        # Get the custom parameter.
        custom = textregion.get('custom')

        # Extract the type of text region.
        match = re.search(r'type:[a-z]+;', custom)
        if match:
            type = match.group()[5:-1]
        else:
            type = None

        # Filter requested textregions.
        if not textregion_types:
            # If no filter is provided, all textregions will be processed.
            pass
        elif type not in textregion_types:
            # If filter is given, all not filtered textregion types will be
            # skipped.
            continue

        # Get the text region id.
        id = textregion.get('id')

        # Extract all text lines. Skrip the last item corresponding to text of
        # whole text region.
        textline = [item.text for item in unicode[:-1]]
        if not textline:
            # Skip empty textregions.
            continue

        # Add received textregion to list.
        textregions.append([id, type, textline])

    return textregions


def calculate_metric(predictions, references, metric, is_valid=True):
    """Calculating the metric given two lists of the same length.

    Args:
        predictions (list): List containing prediction textlines.
        references (list): List containing reference textlines.
        metric (str): Name of metric to be applied.
        is_valid (boolean): Filter if the textlines are valid.

    Returns:
        float: Calculated metric value.
    """

    # Handling not valid features.
    if not is_valid:
        return np.nan

    # Calculating the value.
    evaluate_metric = load(metric)
    return evaluate_metric.compute(predictions=predictions,
                                   references=references)


def main():
    # Define the logging environment.
    log_file = OUTPUT_DIR + '/htr_model_validation.log'
    print(f'Consider the logfile {log_file} for information about the run.')
    logging.basicConfig(filename=log_file,
                        format='%(asctime)s   %(levelname)s   %(message)s',
                        level=logging.INFO,
                        encoding='utf-8')
    logging.info('Script started.')
    if FILTER_STATUS:
        logging.warning(f'Only pages with status {FILTER_STATUS} will be '
                        'considered.')

    ##
    # Create a dataframe of text regions of interest.
    ##

    # Login to Transkribus.
    user = input('Transkribus user:')
    password = input('Transkribus password:')
    sid = get_sid(user, password)

    # Get document metadata.
    doc = get_document_content(COLID, DOCID, sid)

    # Iterate over every document page.
    textregions = pd.DataFrame(columns=['colid', 'docid', 'pageid', 'pagenr',
                                        'tsid_reference', 'tsid_prediction',
                                        'url_reference', 'url_prediction',
                                        'textregionid', 'type',
                                        'text_reference', 'text_prediction',
                                        'is_valid', 'warning_message'])
    for page_nr in range(1, doc['md']['nrOfPages'] + 1):
        # Get page parameters.
        page = doc['pageList']['pages'][page_nr - 1]

        # Initialize new entry.
        new_entry = {'colid': COLID, 'docid': DOCID, 'pageid': page['pageId'],
                     'pagenr': page_nr, 'is_valid': True}

        # Determine reference version of page.
        transcripts = page['tsList']['transcripts']
        reference_index = get_page_version_index(transcripts,
                                                 REFERENCE_VERSION)
        if reference_index is None:
            logging.warning('No reference transcript version found for '
                            f"tsId = {transcripts[reference_index]['tsId']}. "
                            'This page will be excluded from the calculations.'
                            )
            new_entry['warning_message'] = 'No reference transcript version '\
                'found.'
            new_entry['is_valid'] = False
            textregions = pd.concat([textregions,
                                     pd.Series(new_entry).to_frame().T
                                     ], ignore_index=True)
            continue
        reference_transcript = transcripts[reference_index]
        if FILTER_STATUS:
            # Exclude page, when status is not in FILTER_STATUS.
            if reference_transcript['status'] not in FILTER_STATUS:
                continue
        new_entry['tsid_reference'] = reference_transcript['tsId']
        new_entry['url_reference'] = reference_transcript['url']

        # Determine prediction version of page.
        prediction_index = get_page_version_index(transcripts,
                                                  PREDICTION_VERSION)
        if prediction_index is None:
            logging.warning('No prediction transcript version found for '
                            f"tsId = {transcripts[prediction_index]['tsId']}. "
                            'This page will be excluded from the calculations.'
                            )
            new_entry['warning_message'] = 'No prediction transcript version '\
                                           'found.'
            new_entry['is_valid'] = False
            textregions = pd.concat([textregions,
                                     pd.Series(new_entry).to_frame().T
                                     ], ignore_index=True)
            continue
        prediction_transcript = transcripts[prediction_index]
        new_entry['tsid_prediction'] = prediction_transcript['tsId']
        new_entry['url_prediction'] = prediction_transcript['url']

        # Exclude case, when page version of reference is equal to prediction.
        if reference_index == prediction_index:
            logging.warning('The reference and prediction transcript version '
                            f"found are the same: index = {reference_index}, "
                            f"tsId = {transcripts[reference_index]['tsId']}. "
                            'This page will be excluded from the calculations.'
                            )
            new_entry['warning_message'] = 'Reference and prediction '\
                                           'transcript are the same.'
            new_entry['is_valid'] = False

        # Get text regions of reference version of transcript.
        reference_textregions = get_textregions(reference_transcript['url'],
                                                sid, TEXTREGION_TYPES)
        if not reference_textregions:
            logging.warning('No non-empty textregions found for reference '
                            'transcript. '
                            f"tsId = {transcripts[reference_index]['tsId']}. "
                            'This page will be excluded from the calculations.'
                            )
            new_entry['warning_message'] = 'No non-empty textregions '\
                                           '(of selected types) found for '\
                                           'reference transcript.'
            new_entry['is_valid'] = False
            textregions = pd.concat([textregions,
                                     pd.Series(new_entry).to_frame().T
                                     ], ignore_index=True)
            continue

        # Get text regions of prediction version of transcript.
        prediction_textregions = get_textregions(prediction_transcript['url'],
                                                 sid, TEXTREGION_TYPES)
        if not prediction_textregions:
            logging.warning('No non-empty textregions found for prediction '
                            'transcript. '
                            f"tsId = {transcripts[prediction_index]['tsId']}. "
                            'This page will be excluded from the calculations.'
                            )
            new_entry['warning_message'] = 'No non-empty textregions '\
                                           '(of selected types) found for '\
                                           'prediction transcript.'
            new_entry['is_valid'] = False
            textregions = pd.concat([textregions,
                                     pd.Series(new_entry).to_frame().T
                                     ], ignore_index=True)
            continue

        # Iterate over text regions of reference transcript.
        for reference_tr in reference_textregions:
            tr_id = reference_tr[0]
            new_entry['textregionid'] = tr_id
            new_entry['type'] = reference_tr[1]
            new_entry['text_reference'] = reference_tr[2]
            new_entry['warning_message'] = None
            new_entry['is_valid'] = True

            # Check if text region is in prediction transcript available.
            prediction_ids = [row[0] for row in prediction_textregions]
            if tr_id in prediction_ids:
                new_entry['text_prediction'] = prediction_textregions[
                    prediction_ids.index(tr_id)][-1]
            else:
                logging.warning('No prediction transcript textregion found '
                                f"for textregion id {tr_id}: tsId = "
                                f"{transcripts[prediction_index]['tsId']}. "
                                'This textregion will be excluded from the '
                                'calculations.')
                new_entry['warning_message'] = 'No prediction transcript '\
                                               'found for this textregion.'
                new_entry['is_valid'] = False
                new_entry['text_prediction'] = np.nan
                textregions = pd.concat([textregions,
                                         pd.Series(new_entry).to_frame().T
                                         ], ignore_index=True)
                continue

            # Handling reference and perdiction of different length.
            if len(new_entry['text_reference']) != len(new_entry[
                    'text_prediction']):
                logging.warning(f"Prediction transcript of textregion {tr_id} "
                                'do not have the same number of lines than '
                                'reference transcript. '
                                'tsId (prediction transcript) = '
                                f"{transcripts[prediction_index]['tsId']}. "
                                'This textregion will be excluded from the '
                                'calculations.')
                new_entry['warning_message'] = 'Prediction and reference '\
                                               'transcript do not have same '\
                                               'length.'
                new_entry['is_valid'] = False
                textregions = pd.concat([textregions,
                                         pd.Series(new_entry).to_frame().T
                                         ], ignore_index=True)
                continue

            # Add new entry
            textregions = pd.concat([textregions,
                                     pd.Series(new_entry).to_frame().T
                                     ], ignore_index=True)

    logging.info(f'{len(textregions)} text regions of interest read.')

    if textregions.empty:
        logging.error('No textregion processed. No metric can be calculated.')
        raise

    ##
    # Calculate character error rate (CER) and word error rate (WER).
    ##

    # Calculate the global CER and WER over all text regions in consideration.
    texts_reference = []
    texts_prediction = []
    for index, row in textregions.iterrows():
        if row['is_valid']:
            texts_reference.extend(row['text_reference'])
            texts_prediction.extend(row['text_prediction'])
    global_cer = round(calculate_metric(predictions=texts_prediction,
                                        references=texts_reference,
                                        metric='cer'), 3)
    global_wer = round(calculate_metric(predictions=texts_prediction,
                                        references=texts_reference,
                                        metric='wer'), 3)
    logging.info(f"Global CER: {global_cer}")
    logging.info(f"Global WER: {global_wer}")

    # Calculate the CER and WER per type of text regions.
    textregions_groups = textregions.groupby('type')
    for group_name, df_group in textregions_groups:
        texts_reference = []
        texts_prediction = []
        for index, row in df_group.iterrows():
            if row['is_valid']:
                texts_reference.extend(row['text_reference'])
                texts_prediction.extend(row['text_prediction'])
        tr_type_cer = round(calculate_metric(predictions=texts_prediction,
                                             references=texts_reference,
                                             metric='cer'), 3)
        tr_type_wer = round(calculate_metric(predictions=texts_prediction,
                                             references=texts_reference,
                                             metric='wer'), 3)
        logging.info(f"CER for textregion type {group_name}: {tr_type_cer}")
        logging.info(f"WER for textregion type {group_name}: {tr_type_wer}")

    # Calculate the CER and WER for each Transkribus page.
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

        # Exclude empty pages.
        if texts_prediction == [] or texts_prediction == []:
            continue

        cer_pages[group_name] = round(calculate_metric(
            predictions=texts_prediction,
            references=texts_reference,
            metric='cer'), 3)
        wer_pages[group_name] = round(calculate_metric(
            predictions=texts_prediction,
            references=texts_reference,
            metric='wer'), 3)
    logging.info('CER and WER calcuated per Transkribus page.')

    # Plot histograms for scores per Transkribus page with kernel density
    # estimate.
    cer_hist_dir = OUTPUT_DIR + '/cer_per_page.png'
    sns.histplot(list(cer_pages.values()), binwidth=HIST_BINWIDTH, kde=True)
    plt.title('CER per page')
    plt.savefig(cer_hist_dir)
    plt.clf()
    logging.info(f'Histogram for CER per page created: {cer_hist_dir}.')

    wer_hist_dir = OUTPUT_DIR + '/wer_per_page.png'
    sns.histplot(list(wer_pages.values()), binwidth=HIST_BINWIDTH, kde=True)
    plt.title('WER per page')
    plt.savefig(wer_hist_dir)
    logging.info(f'Histogram for WER per page created: {wer_hist_dir}.')

    # Calculate CER and WER for each text region.
    textregions['cer'] = textregions.apply(
        lambda row: calculate_metric(predictions=row['text_prediction'],
                                     references=row['text_reference'],
                                     metric='cer',
                                     is_valid=row['is_valid']
                                     ), axis=1)
    textregions['wer'] = textregions.apply(
        lambda row: calculate_metric(predictions=row['text_prediction'],
                                     references=row['text_reference'],
                                     metric='wer',
                                     is_valid=row['is_valid']
                                     ), axis=1)
    logging.info('CER and WER calcuated per text region.')

    ##
    # Export the results.
    ##

    textregions_dir = OUTPUT_DIR + '/textregions.csv'
    textregions.to_csv(textregions_dir, index=False)
    logging.info(f'Table of text regions written: {textregions_dir}.')

    cer_pages_dir = OUTPUT_DIR + '/cer_pages.csv'
    with open(cer_pages_dir, 'w') as file:
        w = writer(file, delimiter=',', lineterminator='\n')
        w.writerow(['pageid', 'cer'])
        w.writerows(cer_pages.items())
    logging.info(f'CER per page written: {cer_pages_dir}.')

    wer_pages_dir = OUTPUT_DIR + '/wer_pages.csv'
    with open(wer_pages_dir, 'w') as file:
        w = writer(file, delimiter=',', lineterminator='\n')
        w.writerow(['pageid', 'wer'])
        w.writerows(wer_pages.items())
    logging.info(f'WER per page written: {wer_pages_dir}.')

    logging.info('Script finished.')


if __name__ == "__main__":
    main()
