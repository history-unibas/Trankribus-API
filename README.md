# Trankribus-API
This repository contains Python scripts for interacting with the Transkribus platform using their REST API.

## Requirements
- Python 3.10 or newer (only on Python 3.10 tested)
- Packages: see requirements.txt

## Notes
- These scripts were developed as part of the following research project: https://dg.philhist.unibas.ch/de/bereiche/mittelalter/forschung/oekonomien-des-raums/
- Information about the Transkribus platform can be found at https://readcoop.eu/transkribus/.
- Details of the API can be found here:
    - Documentation: https://readcoop.eu/transkribus/docu/rest-api/
    - Wadl-Interface: https://transkribus.eu/TrpServer/Swadl/wadl.html
    - Full description: https://transkribus.eu/TrpServer/rest/application.wadl

## connectTranskribus.py
The script contains functions to interact with the REST API of Transkribus. Parts of this script are based on the github repository https://github.com/raykyn/TranskribusTagger.

## replaceCharacters.py
This script searches in a fixed Transkribus collection selected special characters within transcribed text and replaces them with defined characters. Some documents within the collection where excluded.

## changeStatus.py
This script can be used to change the status of the last version of several selected Transkribus pages. These pages can be located in different collections. In our use case, we have set selected pages to a specific status to exclude these pages from transcription. 

## Contact
For questions please contact jonas.aeby@unibas.ch.