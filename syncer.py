#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import argparse
import datetime
import time
from pytz import reference
# Import Minio library.
from minio import Minio
from minio.error import ResponseError


def traverseDir(dir):
    files = []
    for name in os.listdir(dir):
        path = os.path.join(dir, name)
        if os.path.isfile(path):
            files += [path]
        else:
            files += traverseDir(path)
    return files


def getSetObjects(objects):
    setObjects = set([])
    for object in objects:
        setObjects.add(object.object_name.encode('utf-8'))
    return setObjects


def createParser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--s3')
    parser.add_argument('--accesskey')
    parser.add_argument('--secretkey')
    parser.add_argument('--dir')
    parser.add_argument('--bucket')
    return parser

if __name__ == '__main__':
    parser = createParser()
    namespace = parser.parse_args(sys.argv[1:])
    bucket = namespace.bucket
    dir = namespace.dir + '/'
    timeZone = reference.LocalTimezone()
    updateDeltaTime = datetime.timedelta(seconds=1)

    # Initialize minioClient with an endpoint and access/secret keys.
    minioClient = Minio(namespace.s3,
                        access_key=namespace.accesskey,
                        secret_key=namespace.secretkey,
                        secure=False)

    # create bucket if is not exists
    try:
        if not minioClient.bucket_exists(bucket):
                minioClient.make_bucket(bucket, location="us-east-1")
    except ResponseError as err:
        print(err)

    # load all files from machine to server
    filesOld = set(traverseDir(dir))
    for file in filesOld:
        minioClient.fput_object(bucket, file[len(dir)::], file)

    # load all objects from server to machine
    objects = list(minioClient.list_objects(bucket, prefix='', recursive=True))
    for object in objects:
        objectName = object.object_name.encode('utf-8')
        minioClient.fget_object(bucket, objectName, dir + objectName)
    objectsOld = getSetObjects(objects)

    # synchronize files
    while True:
        time.sleep(2)
        # get all files in directory in this moment
        filesNow = set(traverseDir(dir))
        # get all objects in bucket in this moment
        objects = list(minioClient.list_objects(bucket, recursive=True))
        objectsNow = getSetObjects(objects)

        # check files for updates
        for object in objects:
            objectName = object.object_name.encode('utf-8')
            if dir + objectName in filesNow:
                fileName = dir + objectName
                # get fileLastModified Time
                fileTime = os.path.getatime(fileName)
                fileLastModified = datetime.datetime.fromtimestamp(fileTime)
                fileLastModified = fileLastModified.replace(tzinfo=timeZone)
                # get fileLastModified Time
                objectLastModified = object.last_modified
                if fileLastModified - objectLastModified > updateDeltaTime:
                    minioClient.fput_object(bucket, objectName, fileName)
                elif objectLastModified - fileLastModified > updateDeltaTime:
                    minioClient.fget_object(bucket, objectName, fileName)

        # check changes objects in bucket
        for newObject in objectsNow - objectsOld:
            if not dir + newObject in filesNow:
                minioClient.fget_object(bucket, newObject, dir + newObject)
        for deleteObject in objectsOld - objectsNow:
            if dir + deleteObject in filesNow:
                os.remove(dir + deleteObject)
        objectsOld = objectsNow

        # check changes files in folder
        for newFile in filesNow - filesOld:
            if not newFile[len(dir)::] in objectsNow:
                minioClient.fput_object(bucket, newFile[len(dir)::], newFile)
        for deleteFile in filesOld - filesNow:
            if deleteFile[len(dir)::] in objectsNow:
                minioClient.remove_object(bucket, deleteFile[len(dir)::])
        filesOld = filesNow
