# folder-sync-demon
Демон синхронизирующий содержимое директорий на разных машинах с помощью S3

использование:
./syncer.py --s3 127.0.0.1:9000 --accesskey=ACCESS_KEY --secretkey=SECRET_KEY --dir a --bucket test &
./syncer.py --s3 127.0.0.1:9000 --accesskey=ACCESS_KEY --secretkey=SECRET_KEY --dir b --bucket test &
* синхронизирует 2 дирректории

Параметры
--s3 - задаёт адрес сервера s3
--accesskey - ключ доступа
--secretkey - 
--dir - директория которую нужно синхронизировать
--bucket - бакет который используется для обмена файлами между директориями
