#!/usr/bin/env python
#
# Copyright (c) 2015 Marcel Moolenaar
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
# OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
# IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
# NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
# THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

import argparse
import azure
import azure.storage
import logging
import os
import socket
import sys
import threading
import time
import Queue


MAX_SIZE = 4 * 1048576
PAGE_SIZE = 512

default_account = os.getenv('AZURE_STORAGE_ACCOUNT')
default_key = os.getenv('AZURE_STORAGE_KEY')

logging.basicConfig(level=logging.INFO)

ap = argparse.ArgumentParser(description="Aruze blob uploader")
ap.add_argument('files', metavar='file', type=str, nargs='+',
                help='file to upload as a blob')
ap.add_argument('--account', dest='account', default=default_account,
                help='storage account name')
ap.add_argument('--key', dest='key', default=default_key,
                help='storage account access key')
ap.add_argument('--container', dest='container', default=None,
                help='storage container to upload to')
ap.add_argument('--blob-type', dest='blob_type', default='page',
                help='the type of blob to create (page or block)')
ap.add_argument('--threads', dest='threads', type=int, default=8,
                help='the number of concurrent requests [1..64]')

args = ap.parse_args()
if not args.account or not args.key:
    logging.error('Missing --account and/or --key information')
    sys.exit(1)

if args.container is None:
    logging.error('Missing container name')
    sys.exit(1)

if args.blob_type not in ['page', 'block']:
    logging.error('%s is not a valid blob type' % (args.blob_type))
    sys.exit(1)

if args.threads < 1 or args.threads > 64:
    logging.error('%s is not a valid thread argument' % (args.threads))
    sys.exit(1)

bs = azure.storage.BlobService(account_name=args.account, account_key=args.key)

try:
    bs.create_container(args.container, None, None, False)
except socket.gaierror as e:
    # invalid account
    logging.error('unable to create container %s' % (args.container))
    sys.exit(2)
except TypeError:
    # key too short
    logging.error('invalid access key')
    sys.exit(2)
except azure.WindowsAzureError:
    # invalid (wrong) key
    logging.error('invalid access key')
    sys.exit(2)

queue = Queue.Queue(args.threads * 2)
done = False


def request_handler():
    global done
    thr = threading.currentThread()
    while not done:
        try:
            (bs, cntnr, file, data, range, type) = queue.get(timeout=2)
        except Queue.Empty:
            continue
        logging.info("%s: %s" % (thr.name, range))
        attempts = 0
        success = False
        while not success and attempts < 5:
            attempts += 1
            try:
                bs.put_page(cntnr, file, data, x_ms_range=range,
                            x_ms_page_write=type)
                success = True
            except Exception:
                pass
        if success:
            if attempts > 1:
                logging.warning("%s: %s attempts" % (thr.name, attempts))
            queue.task_done()
        else:
            logging.error("%s: FAILED %s" % (thr.name, range))
            # XXX this terminates the prohgram, it doesn't stop the upload
            # of just this file
            done = True


for i in xrange(args.threads):
    thr = threading.Thread(target=request_handler)
    thr.setDaemon(True)
    thr.start()


def page_write(bs, container, file, data, offset, size, type):
    if type != "update":
        return 0
    logging.info("%s: offset=%lu, length=%lu" % (file, offset, size))
    end = offset + size - 1
    range = 'bytes=%lu-%lu' % (offset, end)
    queue.put((bs, container, file, data, range, type))
    return size


def page_upload(blobsvc, container, file):
    try:
        f = open(file, 'rb')
    except IOError as e:
        logging.error('error opening %s: %s (errno=%d)' %
                      (file, e.strerror, e.errno))
        return (False, 0, 0)

    f.seek(0, os.SEEK_END)
    filesize = f.tell()
    if filesize % PAGE_SIZE:
        logging.error('%s is not a multiple of the page size (= %d bytes)' %
                      (file, PAGE_SIZE))
        f.close()
        return (False, filesize, 0)

    logging.info('Uploading %s' % (file))

    blobsvc.put_blob(container, file, '', 'PageBlob',
                     x_ms_blob_content_length=filesize)

    offset = 0
    chunk_type = None
    chunk_start = 0
    chunk_size = 0
    chunk_data = None
    uploaded = 0
    while offset < filesize:
        f.seek(offset, os.SEEK_SET)
        data = f.read(PAGE_SIZE)
        if data == bytearray(PAGE_SIZE):
            type = "clear"
        else:
            type = "update"

        if chunk_type != type:
            uploaded += page_write(blobsvc, container, file, chunk_data,
                                   chunk_start, chunk_size, chunk_type)
            chunk_type = type
            chunk_start = offset
            chunk_size = 0
            chunk_data = b''

        chunk_size += PAGE_SIZE
        if type == "update":
            chunk_data += data

        if chunk_size == MAX_SIZE:
            uploaded += page_write(blobsvc, container, file, chunk_data,
                                   chunk_start, chunk_size, chunk_type)
            chunk_type = None

        offset += PAGE_SIZE

    uploaded += page_write(blobsvc, container, file, chunk_data, chunk_start,
                           chunk_size, chunk_type)
    logging.info('%s: waiting for upload to complete' % (file))
    queue.join()
    return (True, filesize, uploaded)


if args.blob_type == 'page':
    total_uploaded = 0
    start_time = time.time()
    for file in args.files:
        file_start_time = time.time()
        (status, filesize, uploaded) = page_upload(bs, args.container, file)
        file_end_time = time.time()
        # XXX show file stats
        total_uploaded += uploaded
    end_time = time.time()
    # XXX show total stats
else:
    logging.error('block blobs cannot be uploaded by us yet')
    sys.exit(1)

done = True
