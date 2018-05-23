# pip install git+https://github.com/NDAR/nda_aws_token_generator.git#egg=nda-aws-token-generator&subdirectory=python
# cd ~/nda_aws_token_generator/python/
# sudo python setup.py install

from __future__ import with_statement
from __future__ import absolute_import
import sys

if sys.version_info[0] < 3:
	import Queue as queue
	input = raw_input
else:
	import queue
import argparse
import os
from getpass import getpass
from nda_aws_token_generator import *
import csv
import threading
import multiprocessing
import boto3
import botocore
import datetime
import requests
import xml.etree.ElementTree as ET


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
		self.download_queue = queue.Queue()
		self.path_list = set()


	def useDataManager(self):
		# download package files (not associated files)
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
		# download associated files listed in data structures
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


	def queuing(self):
		cpu_num = multiprocessing.cpu_count()
		if cpu_num > 1:
			cpu_num -= 1
		for x in range(cpu_num):
			worker = Download.DownloadTask(self)
			worker.daemon = True
			worker.start()
		for path in self.path_list:
			self.download_queue.put(path)
		self.download_queue.join()
		print('Finished downloading all files.')

	class DownloadTask(threading.Thread):
		def __init__(self, Download):
			threading.Thread.__init__(self)
			self.download = Download
			self.download_queue = Download.download_queue
			self.url = Download.url
			self.username = Download.username
			self.password = Download.password
			self.directory = Download.directory
			self.access_key = Download.token.access_key
			self.secret_key = Download.token.secret_key
			self.session = Download.token.session


		def run(self):
			while True:
				path = self.download_queue.get()

				filename = path.split('/')
				self.filename = filename[3:]
				self.key = '/'.join(self.filename)
				self.bucket = filename[2]
				self.newdir = filename[3:-1]
				self.newdir = '/'.join(self.newdir)
				self.newdir = os.path.join(self.directory, self.newdir)
				self.local_filename = os.path.join(self.directory, self.key)

				downloaded = False

                # check previous downloads
				if args.resume:
					prev_directory = args.resume[0]
					prev_local_filename = os.path.join(prev_directory, self.key)
					if os.path.isfile(prev_local_filename):
						#print(prev_local_filename, 'is already downloaded.')
						downloaded = True

				if not downloaded:
					if not os.path.exists(self.newdir):
						os.makedirs(self.newdir)

					# check tokens
					self.download.check_time()

					session = boto3.session.Session(self.access_key,
					                                self.secret_key,
					                                self.session)
					s3client = session.client('s3')


					try:
						s3client.download_file(self.bucket, self.key, self.local_filename)
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
				self.download_queue.task_done()


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
	s3Download.queuing()
	if args.packageNumber:
		print('\nDownloading associated files.\n')
		s3Download.searchForDataStructure()
		s3Download.get_tokens()
		s3Download.queuing()
