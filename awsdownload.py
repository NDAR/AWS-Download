# pip install git+https://github.com/NDAR/nda_aws_token_generator.git#egg=nda-aws-token-generator&subdirectory=python
# cd ~/nda_aws_token_generator/python/
# sudo python setup.py install

from __future__ import with_statement
from __future__ import absolute_import
import sys

IS_PY2 = sys.version_info < (3, 0)

if IS_PY2:
    from Queue import Queue
else:
    from queue import Queue

import argparse
import os
from getpass import getpass
from nda_aws_token_generator import *
import csv
from threading import Thread
import boto3
import botocore
import datetime
import requests
import xml.etree.ElementTree as ET
from boto3.s3.transfer import S3Transfer
import multiprocessing

class Worker(Thread):
    """ Thread executing tasks from a given tasks queue """
    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            func, args, kargs = self.tasks.get()
            try:
                func(*args, **kargs)
            except Exception as e:
                # An exception happened in this thread
                print(e)
            finally:
                # Mark this task as done, whether an exception happened or not
                self.tasks.task_done()


class ThreadPool:
    """ Pool of threads consuming tasks from a queue """
    def __init__(self, num_threads):
        self.tasks = Queue(num_threads)
        for _ in range(num_threads):
            Worker(self.tasks)

    def add_task(self, func, *args, **kargs):
        """ Add a task to the queue """
        self.tasks.put((func, args, kargs))

    def map(self, func, args_list):
        """ Add a list of tasks to the queue """
        for args in args_list:
            self.add_task(func, args)

    def wait_completion(self):
        """ Wait for completion of all the tasks in the queue """
        self.tasks.join()

class Download:

    def __init__(self, directory):
        if args.username:
            self.username = args.username[0]
        else:
            self.username = input('Enter your NIMH Data Archives username:')
        if args.password:
            self.password = args.password[0]
        else:
            self.password = getpass('Enter your NIMH Data Archives password:')

        self.url = 'https://ndar.nih.gov/DataManager/dataManager'
        self.directory = directory
        self.download_queue = Queue()
        self.path_list = set()

        self.access_key = None
        self.secret_key =  None
        self.session = None


    def useDataManager(self):
        """ Download package files (not associated files) """

        payload = ('<?xml version="1.0" ?>\n' +
                   '<S:Envelope xmlns:S="http://schemas.xmlsoap.org/soap/envelope/">\n' +
                   '<S:Body> <ns3:QueryPackageFileElement\n' +
                   'xmlns:ns4="http://dataManagerService"\n' +
                   'xmlns:ns3="http://gov/nih/ndar/ws/datamanager/server/bean/jaxb"\n' +
                   'xmlns:ns2="http://dataManager/transfer/model">\n' +
                   '<packageId>' + self.package + '</packageId>\n' +
                   '<associated>true</associated>\n' +
                   '</ns3:QueryPackageFileElement>\n' +
                   '</S:Body>\n' +
                   '</S:Envelope>')

        headers = {
            'Content-Type': "text/xml"
        }

        r = requests.request("POST", self.url, data=payload, headers=headers)

        root = ET.fromstring(r.text)
        packageFiles = root.findall(".//queryPackageFiles")
        for element in packageFiles:
            associated = element.findall(".//isAssociated")
            path = element.findall(".//path")
            for a in associated:
                if a.text == 'false':
                    for p in path:
                        file = 's3:/' + p.text
                        self.path_list.add(file)

        print('Downloading package files for package {}.'.format(self.package))


    def searchForDataStructure(self):
        """ Download associated files listed in data structures """
        all_paths = self.path_list
        self.path_list = set()

        for path in all_paths:
            if 'Package_{}'.format(self.package) in path:
                file = path.split('/')[-1]
                shortName = file.split('.')[0]
                try:
                    ddr = requests.request("GET", "https://ndar.nih.gov/api/datadictionary/v2/datastructure/{}".format(shortName))
                    ddr.raise_for_status()
                    dataStructureFile = path.split('gpop/')[1]
                    dataStructureFile = os.path.join(self.directory, dataStructureFile)
                    self.dataStructure = dataStructureFile
                    self.useDataStructure()

                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 404:
                        continue


    def useDataStructure(self):
        with open(self.dataStructure) as tsv_file:
            tsv = csv.reader(tsv_file, delimiter="\t")
            for row in tsv:
                for element in row:
                    if element.startswith('s3://'):
                        self.path_list.add(element)

    def get_links(self):
        if args.packageNumber:
            self.package = args.paths[0]
            self.useDataManager()


        elif args.datastructure:
            self.dataStructure = args.paths[0]
            self.useDataStructure()

        elif args.txt:
            with open(args.paths[0]) as tsv_file:
                tsv = csv.reader(tsv_file, delimiter="\t")
                for row in tsv:
                    self.path_list.add(row[0])

        else:
            self.path_list = args.paths

    def check_time(self):
        now_time = datetime.datetime.now()
        if now_time >= self.refresh_time:
            self.get_tokens()

    def get_tokens(self):
        start_time = datetime.datetime.now()
        generator = NDATokenGenerator(self.url)
        self.token = generator.generate_token(self.username, self.password)
        self.refresh_time = start_time + datetime.timedelta(hours=23, minutes=55)

        self.access_key = self.token.access_key
        self.secret_key = self.token.secret_key
        self.session = self.token.session

    def download_path(self, path):
        filename = path.split('/')
        self.filename = filename[3:]
        self.key = '/'.join(self.filename)
        self.bucket = filename[2]
        self.newdir = filename[3:-1]
        self.newdir = '/'.join(self.newdir)
        self.newdir = os.path.join(self.directory, self.newdir)
        local_filename = os.path.join(self.directory, self.key)

        downloaded = False

        # check previous downloads
        if args.resume:
            prev_directory = args.resume[0]
            prev_local_filename = os.path.join(prev_directory, self.key)
            if os.path.isfile(prev_local_filename):
                # print(prev_local_filename, 'is already downloaded.')
                downloaded = True

        if not downloaded:
            try:
                os.makedirs(self.newdir)
            except OSError as e:
                pass

            # check tokens
            self.check_time()

            session = boto3.session.Session(self.access_key, self.secret_key, self.session)
            s3client = session.client('s3')
            s3transfer = S3Transfer(s3client)

            try:
                s3transfer.download_file(self.bucket, self.key, local_filename)
                print('downloaded: ', path)
            except botocore.exceptions.ClientError as e:
                # If a client error is thrown, then check that it was a 404 error.
                # If it was a 404 error, then the bucket does not exist.
                error_code = int(e.response['Error']['Code'])
                if error_code == 404:
                    print('This path is incorrect:', path, 'Please try again.\n')
                    pass
                if error_code == 403:
                    print('This is a private bucket. Please contact NDAR for help:', path, '\n')
                    pass

def parse_args():
    parser = argparse.ArgumentParser(
        description='This application allows you to enter a list of aws S3 paths and will download the files to your local drive '
                    'in your home folder. Alternatively, you may enter an NDAR data structure file, and the client will download '
                    'all associated files from S3 listed in the text file.',
        usage='%(prog)s <S3_path_list>')

    parser.add_argument('paths', metavar='<S3_path_list>', type=str, nargs='+', action='store',
                        help='Will download all S3 files to your local drive')

    parser.add_argument('-n', '--packageNumber', action='store_true',
                        help='Flags to download all S3 files in package.')

    parser.add_argument('-t', '--txt', action='store_true',
                        help='Flags that a text file has been entered from where to download S3 files.')

    parser.add_argument('-s', '--datastructure', action='store_true',
                        help='Flags that a  data structure text file has been entered from where to download S3 files.')

    parser.add_argument('-r', '--resume', metavar='<arg>', type=str, nargs=1, action='store',
                        help='Flags to restart a download process. If you already have some files downloaded, you must enter the directory where they are saved.')

    parser.add_argument('-u', '--username', metavar='<arg>', type=str, nargs=1, action='store',
                        help='NDA username')

    parser.add_argument('-p', '--password', metavar='<arg>', type=str, nargs=1, action='store',
                        help='NDA password')

    parser.add_argument('-d', '--directory', metavar='<arg>', type=str, nargs=1, action='store',
                        help='Enter an alternate full directory path where you would like your files to be saved.')

    args = parser.parse_args()

    return args


if __name__ == "__main__":

    args = parse_args()

    if args.directory:
        dir = args.directory[0]
    else:
        dir = os.path.join(os.path.expanduser('~'), 'AWS_downloads')

    s3Download = Download(dir)
    s3Download.get_links()
    s3Download.get_tokens()

    # Function to be executed in a thread
    def download(path):
        s3Download.download_path(path)

    # Instantiate a thread pool with i worker threads
    i = multiprocessing.cpu_count()
    if i > 1:
        i -= 1
    pool = ThreadPool(i)

    # Add the jobs in bulk to the thread pool
    pool.map(download, s3Download.path_list)
    pool.wait_completion()