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

	def get_links(self):

		if args.datastructure:
			with open(args.paths[0]) as tsv_file:
				tsv = csv.reader(tsv_file, delimiter="\t")
				header = next(tsv)

				if args.filters:
					print(args.filters)
					filter = args.filters[0]
					filter = filter.split(',')
					column = filter[0]
					value = filter[1]
					column_index = header.index(column)
					image_file = header.index('image_file')
					for row in tsv:
						if row[column_index] == value:
							self.path_list.add(row[image_file])
				else:
					image_file = header.index('image_file')
					for row in tsv:
						self.path_list.add(row[image_file])
		elif args.txt:
			with open(args.paths[0]) as tsv_file:
				tsv = csv.reader(tsv_file, delimiter="\t")
				for row in tsv:
					self.path_list.add(row[0])

		else:
			self.path_list = args.paths

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

		def run(self):
			while True:
				generator = NDATokenGenerator(self.url)
				token = generator.generate_token(self.username, self.password)

				path = self.download_queue.get()

				filename = path.split('/')
				self.filename = filename[3:]
				self.key = '/'.join(self.filename)
				self.bucket = filename[2]
				self.newdir = filename[3:-1]
				self.newdir = '/'.join(self.newdir)
				self.newdir = os.path.join(self.directory, self.newdir)
				self.local_filename = os.path.join(self.directory, self.key)

				if not os.path.exists(self.newdir):
					os.makedirs(self.newdir)

				session = boto3.session.Session(token.access_key,
				                                token.secret_key,
				                                token.session)
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
						print('This is a private bucket. Please contact NDAR for help.\n')
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

	parser.add_argument('-u', '--username', metavar='<arg>', type=str, nargs=1, action='store',
	                    help='NDA username')

	parser.add_argument('-p', '--password', metavar='<arg>', type=str, nargs=1, action='store',
	                    help='NDA password')

	parser.add_argument('-t', '--txt', action='store_true',
	                    help='Flags that a text file has been entered from where to download S3 files.')

	parser.add_argument('-s', '--datastructure', action='store_true',
	                    help='Flags that a  data structure text file has been entered from where to download S3 files.')

	parser.add_argument('-f', '--filters', metavar='<filter_list>', type=str, nargs='+', action='store',
	                    help='Enter the column name you want to filter by and the value of interest, separated by a comma. '
	                         'EX: image_description,fMRI. Can only apply one filter as of now.')

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
	s3Download.queuing()
