# coding: utf-8
import os
import sys
from time import time

from utils import get_logger, wait
from test import Test, TestError


_config = {
    "variant": "gateway",
    "primary_server": "10.10.152.72",
    "secondary_server": "10.10.152.100",
    "login": "user1",
    "password": "12345678",
    "data_dir": "data",
    "results_dir": "results",
    "new_objects_dir": "data/put_new_objects",
    "new_versions_dir": "data/put_new_versions",
    "first_timeout": 90,
    "max_timeout": 60,
    "period": 10,
    "download_files_timeout": 10,
    "delete_objects_on_error": True,
    "loglevel": "INFO_OK"
}


# Preliminaries
os.chdir(os.path.dirname(os.path.realpath(__file__)))
if not os.path.isdir(_config['results_dir']):
    os.mkdir(_config['results_dir'])
logger = get_logger('colorlog', _config['loglevel'])


# Action
test = Test(_config, logger)

# Специальный случай: если загруженные объекты не были удалены после предыдущего запуска скрипта
if len(sys.argv) > 1 and sys.argv[1].startswith('delete'):
    logger.info(u'Удаляю объекты с серверов по именам. Имена беру из xml-файлов в каталоге %s' %
                _config['new_objects_dir'])
    test.delete_objects_by_names()
    sys.exit(0)

try:
    time0 = time()
    logger.info(u'Выполняю предварительную проверку...')
    test.precheck()

    # Помещение новых объектов
    logger.info(u'Загружаю из каталога %s на сервер %s новые объекты.' %
                (_config['new_objects_dir'], _config['primary_server']))
    test.put_objects_from_directory(_config['new_objects_dir'])
    logger.info(u'Запускаю задачу "ВыгрузкаФайлов"')
    test.offload_files()
    logger.info(u'Жду, пока объекты пройдут через шлюз (%d секунд)' % _config['first_timeout'])
    wait(_config['first_timeout'])
    test.track_replication(_config['first_timeout'])

    # Помещение новых версий тех же объектов
    logger.info(u'Загружаю из каталога %s на сервер %s новые версии тех же объектов.' %
                (_config['new_versions_dir'], _config['primary_server']))
    test.backup_and_clear_uploaded_objects()
    test.put_objects_from_directory(_config['new_versions_dir'])
    logger.info(u'Запускаю задачу "ВыгрузкаФайлов"')
    test.offload_files()
    logger.info(u'Жду, пока объекты пройдут через шлюз (%d секунд)' % _config['first_timeout'])
    wait(_config['first_timeout'])
    test.track_replication(_config['first_timeout'])

    # Изменение метаданных
    logger.info(u'Меняем метаданные всех объектов на сервере %s' % _config['primary_server'])
    test.change_metadata()
    logger.info(u'Запускаю задачу "ВыгрузкаФайлов"')
    test.offload_files()
    logger.info(u'Жду, пока объекты пройдут через шлюз (%d секунд)' % _config['first_timeout'])
    wait(_config['first_timeout'])
    test.track_changing_metadata(_config['first_timeout'])

    # Удаление объектов
    logger.info(u'Удаляем все загруженные объекты')
    test.delete_uploaded_objects()
    logger.info(u'Запускаю задачу "ВыгрузкаФайлов"')
    test.offload_files()
    logger.info(u'Жду, пока объекты пройдут через шлюз (%d секунд)' % _config['first_timeout'])
    wait(_config['first_timeout'])
    test.track_deletion(_config['first_timeout'])

except TestError, e:
    logger.critical(u'Ошибка теста')
    if _config['delete_objects_on_error']:
        logger.info(u'Удаляю все загруженные объекты')
        test.delete_uploaded_objects(error=True)
    else:
        logger.warn(u'Загруженные объекты НЕ будут удалены')
    logger.critical(u'Тест провален')
    if e.message:
        logger.critical(e.message)

except Exception:
    logger.critical(u'Произошла непредвиденная ошибка!')
    if _config['delete_objects_on_error']:
        logger.info(u'Удаляю все загруженные объекты')
        test.delete_uploaded_objects(error=True)
    else:
        logger.warn(u'Загруженные объекты НЕ будут удалены, потому что delete_objects_on_error=False')
    raise

else:
    time1 = time()
    logger.info_ok(u'\nТест успешно пройден! Время прогона: %d сек.' % int(time1-time0))
