
import codecs
import csv
import os.path

import requests


def get_download_dir():
    dirname = os.path.join(
        os.path.dirname(__file__),
        'downloads')

    if not os.path.exists(dirname):
        os.mkdir(dirname)

    return dirname


def download(url, save_to_file=False):
    if save_to_file:
        download_dirname = get_download_dir()
        fname_part = url.split('/')[-1]
        save_path = os.path.join(download_dirname, fname_part)

        if os.path.exists(save_path):
            os.remove(save_path)

        return __download_to_file(url, save_path)

    else:
        return __download_to_mem(url)


def iter_csv(path):
    with codecs.open(path, mode='r', encoding='utf8') as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            yield row


def __download_to_mem(url):
    res = requests.get(url)
    res.raise_for_status()
    return res.content.decode('utf8')


def __download_to_file(url, save_path):
    res = requests.get(url, stream=True)
    res.raise_for_status()

    with open(save_path, mode='wb') as fh:
        for chunk in res.iter_content(chunk_size=1024):
            if chunk:
                fh.write(chunk)

    return save_path
