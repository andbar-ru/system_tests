# coding: utf-8
import os
import sys
from time import time

from utils import get_logger, wait
from test import Test, TestError


_config = {
    "variant": "correcting_replication",
    "primary_server": "10.10.152.85",
    "secondary_server": "10.10.152.86",
    "replicant_name": "secondarybnd",
    "replicationTimeOut": 3600,
    "replicationPeriod": 10,
    "login": "user1",
    "password": "12345678",
    "data_dir": "data",
    "results_dir": "results",
    "new_objects_dir": "data/put_new_objects",
    "new_versions_dir": "data/put_new_versions",
    "assure_timeout": 60,
    "first_timeout": 30,
    "max_timeout": 30,
    "period": 10,
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

    logger.info(u'Тупо ждём перед проверкой отключённости потоковой репликации (%d секунд)' %
                _config['assure_timeout'])
    wait(_config['assure_timeout'])
    logger.info(u'Проверяю, что потоковая репликация на сервере %s выключена' %
                _config['primary_server'])
    test.assure_stream_replication_is_disabled()

    logger.info(u'Запускаю корректирующую репликацию')
    test.run_correcting_replication(_config['replicant_name'])
    logger.info(u'Жду, пока случится репликация (%d секунд)' % _config['first_timeout'])
    wait(_config['first_timeout'])
    logger.info(u'Периодически проверяю, среплицировались ли новые объекты')
    test.track_replication(_config['first_timeout'])

    # Помещение новых версий тех же объектов
    logger.info(u'Загружаю из каталога %s на вышестоящий сервер новые версии тех же объектов.'
                % _config['new_versions_dir'])
    test.backup_and_clear_uploaded_objects()
    test.put_objects_from_directory(_config['new_versions_dir'])
    logger.info(u'Запускаю корректирующую репликацию')
    test.run_correcting_replication(_config['replicant_name'])
    logger.info(u'Жду, пока случится репликация (%d секунд)' % _config['first_timeout'])
    wait(_config['first_timeout'])
    logger.info(u'Периодически проверяю, среплицировались ли новые версии')
    test.track_replication(_config['first_timeout'])

    # # Изменение метаданных
    # logger.info(u'Меняем метаданные всех объектов...')
    # test.change_metadata()
    # logger.info(u'Запускаю корректирующую репликацию')
    # test.run_correcting_replication(_config['replicant_name'])
    # logger.info(u'Жду, пока случится репликация (%d секунд)' % _config['first_timeout'])
    # wait(_config['first_timeout'])
    # test.track_changing_metadata(_config['first_timeout'])

    # # Удаление объектов
    # logger.info(u'Удаляем все загруженные объекты')
    # test.delete_uploaded_objects()
    # logger.info(u'Запускаю корректирующую репликацию')
    # test.run_correcting_replication(_config['replicant_name'])
    # logger.info(u'Жду, пока случится репликация (%d секунд)' % _config['first_timeout'])
    # wait(_config['first_timeout'])
    # test.track_deletion(_config['first_timeout'])

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
