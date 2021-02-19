# Modified from
# * https://github.com/elmeriniemela/bachelor/blob/master/clean_sec_files.py
# * https://github.com/elmeriniemela/bachelor/blob/master/constants.py
from bs4 import BeautifulSoup
import re

"""
    Predefined SEC form strings
    Feel free to add your own lists
    ND-SRAF / McDonald : 201606
"""

f_10K = ['10-K', '10-K405', '10KSB', '10-KSB', '10KSB40']
f_10KA = ['10-K/A', '10-K405/A', '10KSB/A', '10-KSB/A', '10KSB40/A']
f_10KT = ['10-KT', '10KT405', '10-KT/A', '10KT405/A']
f_10Q = ['10-Q', '10QSB', '10-QSB']
f_10QA = ['10-Q/A', '10QSB/A', '10-QSB/A']
f_10QT = ['10-QT', '10-QT/A']
# List of all 10-X related forms
f_10X = f_10K + f_10KA + f_10KT + f_10Q + f_10QA + f_10QT
f_10K_X = f_10K + f_10KA + f_10KT
f_10Q_X = f_10Q + f_10QA + f_10QT
# Regulation A+ related forms
f_1X = ['1-A', '1-A/A', '1-K', '1-SA', '1-U', '1-Z']

PARM_FORMS = f_10X  # or, for example, PARM_FORMS = ['8-K', '8-K/A']

RE_SEC_HEADER = re.compile(r"<(IMS-HEADER|SEC-HEADER)>[\w\W]*?</(IMS-HEADER|SEC-HEADER)>", re.MULTILINE)

FILE_STATS = """\
<FileStats>

    <OriginalChars>{original_chars}</OriginalChars>

    <CurrentChars>{current_chars}</CurrentChars>

    <PercentKept>{percent_kept}</PercentKept>

</FileStats>

"""

GOOD_DOCUMENT_RE_LIST = [re.compile("<TYPE>{}</TYPE>".format(t), flags=re.IGNORECASE) for t in PARM_FORMS]

RE_DOCUMENT = re.compile(r'<DOCUMENT[ >][\w\W]*?</DOCUMENT>', flags=re.IGNORECASE | re.MULTILINE)

RE_XBRL = re.compile(r'<XBRL[ >][\w\W]*?</XBRL>', flags=re.IGNORECASE | re.MULTILINE)

RE_TABLE = re.compile(r'<TABLE[ >][\w\W]*?</TABLE>', flags=re.IGNORECASE | re.MULTILINE)

RE_MARKUP = re.compile(r'<[\w\W]*?>', flags=re.IGNORECASE | re.MULTILINE)

RE_PRIVACY_START = re.compile(r'-----BEGIN PRIVACY-ENHANCED MESSAGE-----', flags=re.IGNORECASE)

RE_PRIVACY_END = re.compile(r'-----END PRIVACY-ENHANCED MESSAGE-----', flags=re.IGNORECASE)


RE_EXCESS_WHITESPACE = re.compile(r'\s{3,}')

RE_PDF = re.compile(r'<FILENAME>.*?\.pdf')

BAD_DOCUMENT_RE_LIST = [RE_PDF]


def parse_text(raw_data):
    # Close type tag
    only_text = re.sub(r"<TYPE>(\S+)", r"<TYPE>\1</TYPE>", raw_data)

    # Save SEC header
    header = ''
    headermatch =  RE_SEC_HEADER.search(only_text)
    if headermatch:
        header = headermatch.group()
        only_text = only_text.replace(header, '')


    # Remove all non 10-K documents like exhibitions xml files, json files, graphic files etc..
    all_documets = RE_DOCUMENT.findall(only_text)
    kept_documents = [d for d in all_documets if any(RE.search(d) for RE in GOOD_DOCUMENT_RE_LIST)]
    kept_documents = [d for d in kept_documents if not any(RE.search(d) for RE in BAD_DOCUMENT_RE_LIST)]


    only_text = '\n'.join(kept_documents)

    # Remove all XBRL – all characters between <XBRL …> … </XBRL> are deleted.
    only_text = RE_XBRL.sub(" ", only_text)

    # Remove all tables
    only_text = RE_TABLE.sub(" ", only_text)

    only_text = RE_PRIVACY_START.sub(" ", only_text)
    only_text = RE_PRIVACY_END.sub(" ", only_text)

    # Remove all html tags
    only_text = RE_MARKUP.sub(" ", only_text)

    only_text = RE_EXCESS_WHITESPACE.sub(" ", only_text)

    # Remove all html entities
    only_text = BeautifulSoup(only_text, 'lxml').get_text(' ')

    # Remove non ascii chars
    only_text = ''.join(i if ord(i) < 128 else ' ' for i in only_text)

    original_chars = len(raw_data)
    current_chars = len(only_text)
    percent_kept = (current_chars / original_chars) * 100
    print("{:.2f} % and {}/{} documents kept.".format(percent_kept, len(kept_documents), len(all_documets)))

    return only_text
