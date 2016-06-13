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


def getNewObjects(objectsOld, objectsNow):
    rezult = list([])
    for objectNow in objectsNow:
        for objectOld in objectsOld:
            if objectNow["name"] == objectOld["name"]:
                break
        else:
            rezult.append(objectNow)
    return rezult


def getDeleteObjects(objectsOld, objectsNow):
    rezult = list([])
    for objectOld in objectsOld:
        for objectNow in objectsNow:
            if objectNow["name"] == objectOld["name"]:
                break
        else:
            rezult.append(objectOld)
    return rezult


def getNewFiles(filesOld, filesNow):
    rezult = list([])
    for fileNow in filesNow:
        for fileOld in filesOld:
            if fileNow["name"] == fileOld["name"]:
                break
        else:
            rezult.append(fileNow)
    return rezult


def getDeleteFiles(filesOld, filesNow):
    rezult = list([])
    for fileOld in filesOld:
        for fileNow in filesNow:
            if fileNow["name"] == fileOld["name"]:
                break
        else:
            rezult.append(fileOld)
    return rezult


def getStableObjects(objectsOld, objectsNow):
    rezult = list([])
    for objectOld in objectsOld:
        for objectNow in objectsNow:
            if objectNow["name"] == objectOld["name"]:
                rezult.append({"name": objectNow["name"],
                               "timeNow": objectNow["time"],
                               "timeOld": objectOld["time"]})
    return rezult


def getStableFiles(filesOld, filesNow):
    rezult = list([])
    for fileOld in filesOld:
        for fileNow in filesNow:
            if fileNow["name"] == fileOld["name"]:
                rezult.append({"name": fileNow["name"],
                               "timeNow": fileNow["time"],
                               "timeOld": fileOld["time"]})
    return rezult


def traverseDir(dir, workDir):
    files = list([])
    for name in os.listdir(dir):
        path = os.path.join(dir, name)
        if os.path.isfile(path):
            fileTime = os.path.getatime(path)
            files.append({"name": path[len(workDir)::], "time": fileTime})
        else:
            files += traverseDir(path, workDir)
    return files


def getObjects(minioClient, bucket):
    rezult = list([])
    objects = minioClient.list_objects(bucket, prefix='', recursive=True)
    for object in objects:
        objectName = object.object_name.encode('utf-8')
        objectTime = object.last_modified
        objectTime = objectTime.replace(tzinfo=timeZone)
        objectTime = objectTime.replace(microsecond=0)
        rezult.append({"name": objectName, "time": objectTime})
    return rezult


def createBucket(minioClient, bucket):
    try:
        if not minioClient.bucket_exists(bucket):
                minioClient.make_bucket(bucket, location="us-east-1")
    except ResponseError as err:
        print(err)


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
    bucketName = namespace.bucket
    dir = namespace.dir + '/'
    timeZone = reference.LocalTimezone()

    # Initialize minioClient with an endpoint and access/secret keys.
    minioClient = Minio(namespace.s3,
                        access_key=namespace.accesskey,
                        secret_key=namespace.secretkey,
                        secure=False)

    # create bucket if is not exists
    createBucket(minioClient, bucketName)

    objectsOld = getObjects(minioClient, bucketName)
    filesOld = traverseDir(dir, dir)

    # load all objects from server to machine
    for object in objectsOld:
        objectName = object["name"]
        minioClient.fget_object(bucketName, objectName, dir + objectName)

    # delete files in machine wich is not found in the server
    for file in filesOld:
        for object in objectsOld:
            if file["name"] == object["name"]:
                break
        else:
            os.remove(dir + file["name"])

    time.sleep(1)

    objectsOld = getObjects(minioClient, bucketName)
    filesOld = traverseDir(dir, dir)

    errorPutsFiles = list([])
    errorUpdatesFiles = list([])

    # synchronize files
    while True:
        # get all files in directory in this moment
        filesNow = traverseDir(dir, dir)
        # get all objects in bucket in this moment
        objectsNow = getObjects(minioClient, bucketName)

        newObjects = getNewObjects(objectsOld, objectsNow)
        deleteObjects = getDeleteObjects(objectsOld, objectsNow)
        newFiles = getNewFiles(filesOld, filesNow) + errorPutsFiles
        deleteFiles = getDeleteFiles(filesOld, filesNow)
        stableFiles = getStableFiles(filesOld, filesNow) + errorUpdatesFiles
        stableObjects = getStableObjects(objectsOld, objectsNow)
        errorPutsFiles = list([])
        errorUpdatesFiles = list([])

        # загрузка новых объектов с сервера
        for object in newObjects:
            path = object["name"]
            # загружаем новый файл с сервера
            minioClient.fget_object(bucketName, path, dir + path)
            # добавляем в список текущих файлов
            fileTime = os.path.getatime(dir+path)
            filesNow.append({"name": path, "time": fileTime})

        # удаление файлов на машине
        for object in deleteObjects:
            path = object["name"]
            # удаляем файла на машине
            os.remove(dir + path)
            # удаляем файл из списка текущих файлов
            for file in filesNow:
                if file["name"] == path:
                    filesNow.remove(file)

        # загрузка на сервер новых файлов
        for file in newFiles:
            path = file["name"]
            # загрузка файла на сервер
            try:
                minioClient.fput_object(bucketName, path, dir + path)
            except BaseException as err:
                errorPutsFiles.append(file)
                pass
            else:
                # добавляем в список текущих объектов
                objectTime = datetime.datetime.fromtimestamp(
                    minioClient.stat_object(bucketName, path).last_modified)
                objectTime = objectTime.replace(tzinfo=timeZone)
                objectsNow.append({"name": path, "time": objectTime})

        # удаление файлов с сервера
        for file in deleteFiles:
            path = file["name"]
            # удаляем объект на сервере
            minioClient.remove_object(bucketName, path)
            # удаляем объект из списка текущих объектов
            for object in objectsNow:
                if object["name"] == path:
                    objectsNow.remove(object)

        # обновление файла на сервере
        for file in stableFiles:
            if int(file["timeNow"]) / 10 > int(file["timeOld"]) / 10:
                path = file["name"]
                # отправляем новый объект на сервер
                try:
                    minioClient.fput_object(bucketName, path, dir + path)
                except BaseException as err:
                    # если неудачно то добавляем в список повторных
                    errorUpdatesFiles.append(file)
                    pass
                else:
                    # удаляем файл из списка текущих объектов
                    for object in objectsNow:
                        if object["name"] == path:
                            objectsNow.remove(object)

                    for object in objectsOld:
                        if object["name"] == path:
                            objectsOld.remove(object)
                    # добавляем в список текущих объектов
                    objectTime = datetime.datetime.fromtimestamp(
                        minioClient.stat_object(bucketName, path).last_modified)
                    objectTime = objectTime.replace(tzinfo=timeZone)
                    objectsNow.append({"name": path, "time": objectTime})
                    objectsOld.append({"name": path, "time": objectTime})

        # обновление файла на машине
        for object in stableObjects:
            if object["timeNow"] > object["timeOld"]:
                path = object["name"]
                # отправляем новый объект на сервер
                minioClient.fget_object(bucketName, path, dir + path)
                # удаляем файл из списка текущих файлов
                for file in filesNow:
                    if file["name"] == path:
                        filesNow.remove(file)
                for file in filesOld:
                    if file["name"] == path:
                        filesOld.remove(file)
                # добавляем в список текущих файлов
                fileTime = os.path.getatime(dir+path)
                filesNow.append({"name": path, "time": fileTime})
                filesOld.append({"name": path, "time": fileTime})

        filesOld = filesNow
        objectsOld = objectsNow

        time.sleep(1)
