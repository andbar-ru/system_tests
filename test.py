# coding: utf-8
import os
import json
from lxml import etree
from zipfile import ZipFile
from glob import glob
from time import time
from copy import deepcopy

from utils import UnicodeException, wait
from session import Session


class TestError(UnicodeException):
    """Если во время объединённого теста возникли ошибка,
    после которой продолжать тест не имеет смысла до устранения.
    """
    pass


class Test(object):
    def __init__(self, config, logger):
        """В self.config хранятся все данные конфигурации. Достаётся из файла config.json"""
        self.config = config
        self.logger = logger
        self.sessions = {}
        self.add_session('primary', self.config['primary_server'])
        self.add_session('secondary', self.config['secondary_server'])
        self.uploaded_objects = {}  # key -> (id, version)
        self.unreplicated_objects_idvers = []  # заполняется одновременно с self.uploaded_objects
        self.replicated_objects = {}  # key -> (id, version)
        self.uploaded_objects_archives = []
        self.json_files = []

    @staticmethod
    def set_getcatalog_from(session, tree):
        """Обновляет свойство getcatalog_from у сессии session"""
        created = tree.get('Created')
        session.set_getcatalog_from(created)

    def add_session(self, name, server):
        self.sessions[name] = Session(server, self.config['login'], self.config['password'])

    def handle_error_response(self, response):
        msg = u'(код %s: %s)\n' % (response.status_code, response.reason)
        try:
            results = etree.fromstring(response.content).findall('.//result')
            for result in results:
                msg += result.get('result_message')
            if response.status_code == 403:
                msg += (u"Возможно, неправильная роль пользователя на userver'е, у которого "
                        u" справляется cdserver.'\n")
        except etree.XMLSyntaxError, e:
            with open('last_error_response', 'wb') as f:
                f.write(response.content)
            msg += u'Не удалось распарсить ответ сервера. Ответ сохранён в файле last_error_response.'
        self.logger.error(msg)
        raise TestError

    def precheck(self):
        """Набор проверок, которые нужно провести перед тестовыми операциями"""
        self.check_md_classifiers_match()
        self.check_names_not_exist_on_servers()
        self.logger.info_ok('OK')

    def check_md_classifiers_match(self):
        """Проверить, что версии классификаторов метаданных на серверах совпадают"""
        primary_md_classifier_version = self.get_md_classifier_version('primary')
        secondary_md_classifier_version = self.get_md_classifier_version('secondary')
        if primary_md_classifier_version != secondary_md_classifier_version:
            raise TestError(u'Версии классификаторов метаданных на серверах не совпадают')

    def get_names_from_pairs(self, pairs):
        """Вернуть словарь {name:class} из пар (zipfile,xmlfile)"""
        names = {}
        for zipfile, xmlfile in pairs:
            tree = etree.parse(xmlfile)
            chart = tree.find('.//chart')
            name = chart.get('Name')
            class_ = chart.get('Class')
            names[name] = class_
        return names

    def check_names_not_exist_on_servers(self):
        pairs = self.get_pairs(self.config['new_objects_dir'])
        names = self.get_names_from_pairs(pairs)

        names_existing = {'primary':set(), 'secondary':set()}
        for session_name in ('primary', 'secondary'):
            session = self.sessions[session_name]
            response = session.get_catalog()
            if response.status_code != 200:
                self.handle_error_response(response)
            tree = self.get_tree_from_xml_from_zip_from_response(response, 'GetCatalog.zip', 'WF.CLL')
            self.set_getcatalog_from(session, tree)
            for name, class_ in names.iteritems():
                if tree.find('.//CHART[@Name="%s"][@Class="%s"]' % (name, class_)) is not None:
                    names_existing[session_name].add(name)

        if names_existing['primary'] or names_existing['secondary']:
            msg = u'Объекты, предназначенные для загрузки, уже есть на сервере:\n'
            if names_existing['primary']:
                msg += 'primary: %s\n' % names_existing['primary']
            if names_existing['secondary']:
                msg += 'secondary: %s\n' % names_existing['secondary']
            raise TestError(msg)

    def add_uploaded_object(self, id, version, xmlfile):
        """С объектами в self.uploaded_objects мы потом сравниваем содержимое каталога(ов)."""
        obj = {}
        tree = etree.parse(xmlfile)
        chart = tree.find('.//chart')
        obj['Class'] = chart.get('Class')
        obj['Name'] = chart.get('Name')
        obj['Type'] = chart.get('Type')
        attributes = tree.findall('.//Attribute')
        for attribute in attributes:
            obj[attribute.get('name')] = attribute.get('value')
        self.uploaded_objects[(id, version)] = obj

    def save_uploaded_objects_to_file(self, filename):
        """Сохранить информацию о загруженных объектах в файл в формате json.
        Этот файл будет полезен, если потребуется удалить загруженные объекты.
        """
        uploaded_objects_1 = {}
        for idver, obj in self.uploaded_objects.iteritems():
            key = '%s-%s' % (idver[0], idver[1])
            uploaded_objects_1[key] = obj
        with open(filename, 'w') as f:
            json.dump(uploaded_objects_1, f, encoding='utf-8', indent=2)
        self.json_files.append(filename)

    def backup_and_clear_uploaded_objects(self):
        """Переносит в архив предыдущие загруженные объекты, чтобы в конце их удалить с сервера,
        и зачищает self.uploaded_objects, self.replicated_objects и
        self.unreplicated_objects_idvers, перед новым циклом загрузки-репликации.
        """
        self.uploaded_objects_archives.append(deepcopy(self.uploaded_objects))
        self.uploaded_objects = {}
        self.unreplicated_objects_idvers = []
        self.replicated_objects = {}

    def get_pairs(self, directory):
        """Получить пары (zip-файл, xml-файл) из каталога directory.
        Для каждого найденного в directory zip-файла должен быть одноимённый xml-файл.
        """
        pairs = []
        for zipfile in glob('%s/*.zip' % directory):
            xmlfile = os.path.splitext(zipfile)[0] + '.xml'
            if not os.path.isfile(xmlfile):
                raise TestError(u'Для файла %s не найден соответствующий xml-файл %s' %
                                (zipfile, xmlfile))
            pairs.append((zipfile, xmlfile))
        return pairs

    def put_objects_from_directory(self, directory):
        """Отгрузить все объекты из заданного каталога на вышестоящий сервер. """
        pairs = self.get_pairs(directory)
        for zipfile, xmlfile in pairs:
            name = os.path.splitext(os.path.basename(zipfile))[0]
            response = self.sessions['primary'].upload_object(zipfile, xmlfile)
            if response.status_code == 200:
                try:
                    tree = etree.fromstring(response.content)
                    obj = tree.find('.//object')
                    id = obj.get('objectId')
                    version = obj.get('version')
                    self.logger.info_ok(u'Объект %s отправлен (id=%s, version=%s).' %
                                        (name, id, version))
                    self.add_uploaded_object(id, version, xmlfile)
                    self.unreplicated_objects_idvers = self.uploaded_objects.keys()
                except etree.XMLSyntaxError, e:
                    with open('%s/upload_object_error' % self.config['results_dir'], 'wb') as f:
                        f.write(response.content)
                    self.logger.error(u'Не удалось распарсить ответ сервера. Ответ сохранён в файле '
                                 u'%s/upload_object_error' % self.config['results_dir'])
                    raise TestError
            else:
                self.handle_error_response(response)

    def check_jsonrpc_response(self, response, method):
        if response.status_code != 200:
            self.handle_error_response(response)
        result = json.loads(response.content)
        if result['success'] is not True:
            msg = u'Результат jsonrpc-запроса %s != true\n' % method
            msg += u'msg: %s' % result['msg']
            raise TestError(msg)
        return result

    def offload_files(self):
        """Запустить задачу "ВыгрузкаФайлов" (url /adminpanel/ikp/) на первом сервере"""
        method = 'admin.run_script'
        script_id = 16  # из таблицы adminpanel_ikpscript
        response = self.sessions['primary'].run_jsonrpc(method=method, data={'script_id':script_id})
        self.check_jsonrpc_response(response, method)

    def download_files(self):
        """Запустить задачу "ЗагрузкаФайловИзДиректории" (url /adminpanel/ikp/) на втором сервере"""
        method = 'admin.run_script'
        script_id = 17  # из таблицы adminpanel_ikpscript
        response = self.sessions['secondary'].run_jsonrpc(method=method, data={'script_id':script_id})
        self.check_jsonrpc_response(response, method)

    def get_replicant(self, name):
        """Найти и вернуть репликанта с именем name. Если его нет, то ошибка."""
        method = 'admin.get_list_db'
        response = self.sessions['primary'].run_jsonrpc(method=method)
        result = self.check_jsonrpc_response(response, method)
        replicants = result['rows']
        for replicant in replicants:
            if replicant['username'] == name:
                return replicant
        else:
            raise TestError(u'Не найден репликант с именем %s' % name)

    def run_correcting_replication(self, name):
        """Запуск корректирующей репликации
        name -- имя пользователя с ролью SUPPLY_CENTER. Имя должно согласовываться с
        self.config['secondary_server'] в /etc/hosts на self.config['primary_server']
        """
        replicant = self.get_replicant(name)
        method = 'admin.start_replications'
        response = self.sessions['primary'].run_jsonrpc(
                method=method,
                data={'replicantId':replicant['replicantId'],
                      'replicantDB':replicant['username'],
                      'replicationTimeOut':self.config['replicationTimeOut'],
                      'replicationPeriod':self.config['replicationPeriod']}
                )
        self.check_jsonrpc_response(response, method)

    def get_md_classifier_version(self, session_name='primary'):
        """Получить версию загруженного классификатора метаданных"""
        session = self.sessions[session_name]
        if hasattr(session, 'md_classifier_version'):
            return session.md_classifier_version
        method = 'admin.md_classifier_version'
        response = session.run_jsonrpc(method=method)
        result = self.check_jsonrpc_response(response, method)
        md_classifier_version = result['md_version']
        session.md_classifier_version = md_classifier_version
        return md_classifier_version

    def change_metadata(self):
        """На вышестоящем сервере у всех загруженных объектов:
        - меняем атрибут c201 (scale) на 987654;
        - добавляем к атрибуту c122 (name) " штрих"
        """
        md_classifier_version = self.get_md_classifier_version()
        session = self.sessions['primary']
        self.logger.debug(u'Меняем атрибут scale на 987654 и добавляем к атрибуту c122 " штрих".')
        self.logger.debug(u'Для изменения метаданных нужно скачать каталог с вышестоящего сервера.')
        response = session.get_catalog()
        if response.status_code != 200:
            self.handle_error_response(response)
        catalog_tree = self.get_tree_from_xml_from_zip_from_response(response, 'GetCatalog.zip', 'WF.CLL')
        self.set_getcatalog_from(session, catalog_tree)
        for (id, version), obj in self.uploaded_objects.iteritems():
            chart = catalog_tree.find('.//CHART[@ID="%s"]' % id)
            metadata = {}
            for attr, value in chart.attrib.iteritems():
                if attr.startswith('c'):
                    metadata[attr] = value
            metadata['c122'] += u' штрих'
            metadata['c201'] = 987654
            updated = float(chart.attrib['Updated'])
            try:
                tags = metadata['c234'].split('|')
            except KeyError:
                tags = []
            params = (
                self.sessions['primary'].id,
                md_classifier_version,
                [{'id':id, 'updated':updated}],  # chart_ids
                metadata,  # md_item
                []  #tags
            )
            response = self.sessions['primary'].run_xmlrpc('set_chart_metadata', params)
            # response.status_code здесь ни о чём не говорит (он всегда 200)
            try:
                tree = etree.fromstring(response.content)
                success = tree.xpath('.//name[text()="success"]/following-sibling::value/boolean')[0].text
                if success == '0':
                    msg = tree.xpath('.//name[text()="msg"]/following-sibling::value/string')[0].text
                    raise TestError(msg)
                self.logger.info_ok(u'Изменены метаданные объекта с id=%s' % id)
            except etree.XMLSyntaxError, e:
                with open('%s/change_metadata_error' % self.config['results_dir'], 'wb') as f:
                    f.write(response.content)
                self.logger.error(u'Не удалось распарсить ответ сервера. Ответ сохранён в файле '
                             u'%s/change_metadata_error' % self.config['results_dir'])
                raise TestError

    def track_changing_metadata(self, time_dec):
        """Проверка, что изменились метаданные на нижестоящем сервере.
        Изменения можно посмотреть в методе self.change_metadata().
        time_dec - убавка от текущего времени (если перед запуском метода был wait)
        """
        time0 = time() - time_dec
        self.unreplicated_objects_idvers = self.uploaded_objects.keys()
        seconds_left = self.config['max_timeout']
        session = self.sessions['secondary']
        while seconds_left > 0:
            if self.config['variant'] == 'gateway':
                self.download_files()
                self.logger.info(u'Таймаут после выполнения задачи "ЗагрузкаФайловИзДиректории": '
                                 u'%d секунд' % self.config['download_files_timeout'])
                wait(self.config['download_files_timeout'])
            response = session.get_catalog()
            if response.status_code != 200:
                self.handle_error_response(response)
            tree = self.get_tree_from_xml_from_zip_from_response(response, 'GetCatalog.zip', 'WF.CLL')
            self.set_getcatalog_from(session, tree)
            for idver in self.unreplicated_objects_idvers:
                id, version = idver
                chart = tree.find('.//CHART[@ID="%s"]' % id)
                if chart is None:
                    raise TestError(u'Куда-то внезапно с нижестоящего сервера исчез объект с id=%s' % id)
                c122 = chart.get('c122')
                c201 = chart.get('c201')
                if c122.endswith(u'штрих') and c201 == '987654':
                    self.logger.info_ok(u'Среплицировались метаданные объекта с id=%s (%d сек.)' %
                                        (id, int(time()-time0)))
                    seconds_left = self.config['max_timeout']  # восстанавливаем оставшееся время
                    self.unreplicated_objects_idvers.remove(idver)

            # всё ли среплицировалось
            if len(self.unreplicated_objects_idvers) == 0:
                self.logger.info_ok(u'Метаданные всех объектов среплицировались!')
                break
            else:
                self.logger.warn(u'Метаданные не всех объектов среплицировались: %s' %
                            self.unreplicated_objects_idvers)
                self.logger.info(u'Cледующая попытка через %d секунд' % self.config['period'])
                wait(self.config['period'])
                seconds_left -= self.config['period']
                continue

        # Истекло время max_timeout
        else:
            self.logger.error(u'%d секунд без репликации новых объектов' % self.config['max_timeout'])
            self.logger.error(u'Не среплицировались объекты с (id, версией): %s' %
                         self.unreplicated_objects_idvers)
            raise TestError

        # Все объекты среплицировались, проверяем соответствие метаданных
        success = self.compare_uploaded_and_replicated_objects()
        if success is True:
            self.logger.info_ok(u'Метаданные загруженных и реплицированных объектов совпадают.')
        else:
            raise TestError(u'Метаданные загруженных и реплицированных объектов не совпадают.')

    def get_all_idvers(self):
        """Возвращает все idver'ы когда-либо загруженных объектов."""
        idvers = []
        for idver in self.uploaded_objects.iterkeys():
            idvers.append(idver)
        for archive in self.uploaded_objects_archives:
            for idver in archive.iterkeys():
                idvers.append(idver)
        return idvers

    def delete_uploaded_objects(self, error=False):
        """Удалить загруженные объекты с вышестоящего сервера.
        error - вызов функции вследствие возбуждения исключения
        """
        idvers = self.get_all_idvers()
        if not idvers:
            return

        self.logger.info(u'Удаляю загруженные объекты с сервера %s' % self.config['primary_server'])
        for id, version in idvers:
            response = self.sessions['primary'].delete_object(id, version)
            if response.status_code == 200:
                self.logger.info_ok(u'Объект (id=%s, version=%s) удалён.' % (id, version))
            else:
                self.handle_error_response(response)


        if error is True:
            if self.config['variant'] in ('correcting_replication', 'gateway') and self.replicated_objects.keys():
                self.logger.info(u'Удаляю реплицированные объекты с сервера %s' %
                                    self.config['secondary_server'])
                for id, version in self.replicated_objects.keys():
                    response = self.sessions['secondary'].delete_object(id, version)
                    if response.status_code == 200:
                        self.logger.info_ok(u'Объект (id=%s, version=%s) удалён.' % (id, version))
                    else:
                        self.handle_error_response(response)

    def delete_objects_by_names(self):
        """Удалить объекты с серверов (все версии), используя атрибуты "Name" и "Class"."""
        self.logger.info(u'Удаляю с серверов объекты по именам...')
        pairs = self.get_pairs(self.config['new_objects_dir'])
        names = self.get_names_from_pairs(pairs)
        for session_name in ('primary', 'secondary'):
            session = self.sessions[session_name]
            response = session.get_catalog()
            if response.status_code != 200:
                self.handle_error_response(response)
            tree = self.get_tree_from_xml_from_zip_from_response(response, 'GetCatalog.zip', 'WF.CLL')
            self.set_getcatalog_from(session, tree)
            for name, class_ in names.iteritems():
                chart = tree.find('.//CHART[@Name="%s"][@Class="%s"]' % (name, class_))
                if chart is not None:
                    id = chart.get('ID')
                    version = chart.get('Issue')
                    if chart is not None:
                        # Запрашиваю все версии
                        response1 = session.get_archive_catalog(id)
                        if response1.status_code != 200:
                            self.handle_error_response(response1)
                        tree1 = self.get_tree_from_xml_from_zip_from_response(response1,
                                'GetArchiveCatalog.zip', 'catalog.xml')
                        charts1 = tree1.findall('.//CHART')
                        if charts1:  # архивные версии имеются
                            for chart1 in charts1:
                                name1 = chart1.get('Name')
                                class_1 = chart1.get('Class')
                                id1 = chart1.get('ID')
                                version1 = chart1.get('Issue')
                                if name1 != name or class_1 != class_ or id1 != id:
                                    raise TestError(u'В архивном каталоге для объекта %s не совпал '
                                            u'какой-то из атрибутов: Name, Class, ID' % name)
                                # Удаление архивных версий
                                response2 = session.delete_object(id1, version1)
                                if response2.status_code != 200:
                                    self.handle_error_response(response2)
                                self.logger.info_ok(u'Объект %s (id=%s, version=%s) с сервера %s удалён.' %
                                                    (name1, id1, version1, session.server))
                        # Удаление последней версии
                        response1 = session.delete_object(id, version)
                        if response1.status_code != 200:
                            self.handle_error_response(response1)
                        self.logger.info_ok(u'Объект %s (id=%s, version=%s) с сервера %s удалён.' %
                                            (name, id, version, session.server))

    def get_tree_from_xml_from_zip_from_response(self, response, zipfilename, xmlfilename):
        resfile = '%s/%s' % (self.config['results_dir'], zipfilename)
        with open(resfile, 'wb') as F:
            F.write(response.content)
        self.logger.debug(u'Записан архив %s' % resfile)
        zf = ZipFile(resfile)
        if xmlfilename not in zf.namelist():
            raise TestError(u'Почему-то в архиве каталога нет файла %s' % xmlfilename)
        xmlfile = zf.read(xmlfilename)
        tree = etree.fromstring(xmlfile)
        return tree

    def track_replication(self, time_dec):
        """Отслеживаем изменения на нижестоящем сервере
        time_dec - убавка от текущего времени (если перед запуском метода был wait)
        """
        time0 = time() - time_dec
        seconds_left = self.config['max_timeout']
        session = self.sessions['secondary']
        while seconds_left > 0:
            if self.config['variant'] == 'gateway':
                self.download_files()
                self.logger.info(u'Таймаут после выполнения задачи "ЗагрузкаФайловИзДиректории": '
                                 u'%d секунд' % self.config['download_files_timeout'])
                wait(self.config['download_files_timeout'])
            response = session.get_catalog()
            if response.status_code != 200:
                self.handle_error_response(response)
            tree = self.get_tree_from_xml_from_zip_from_response(response, 'GetCatalog.zip', 'WF.CLL')
            self.set_getcatalog_from(session, tree)
            for idver in self.unreplicated_objects_idvers:
                id, version = idver
                chart = tree.find('.//CHART[@ID="%s"][@Issue="%s"]' % (id, version))
                if chart is not None:
                    self.logger.info_ok(u'Среплицировался объект с id=%s и версией %s (%d сек.)' %
                                        (id, version, int(time()-time0)))
                    seconds_left = self.config['max_timeout']  # восстанавливаем оставшееся время
                    self.unreplicated_objects_idvers.remove(idver)
                    self.replicated_objects[idver] = chart.attrib

            # всё ли среплицировалось
            if len(self.unreplicated_objects_idvers) == 0:
                self.logger.info_ok(u'Все объекты среплицировались!')
                break
            else:
                self.logger.warn(u'Не все объекты среплицировались: %s' % self.unreplicated_objects_idvers)
                self.logger.info(u'Cледующая попытка через %d секунд' % self.config['period'])
                wait(self.config['period'])
                seconds_left -= self.config['period']
                continue

        # Истекло время max_timeout
        else:
            self.logger.error(u'%d секунд без репликации новых объектов' % self.config['max_timeout'])
            self.logger.error(u'Не среплицировались объекты с (id, версией): %s' %
                              self.unreplicated_objects_idvers)
            if len(self.replicated_objects) == 0:
                msg = (u'Возможно, недостаточно прав у пользователя с ролью "Банк данных" '
                       u'на сервере %s' % self.config['primary_server'])
            else:
                msg = ''
            raise TestError(msg)

        # Все объекты среплицировались, проверяем соответствие метаданных
        success = self.compare_uploaded_and_replicated_objects()
        if success is True:
            self.logger.info_ok(u'Метаданные загруженных и реплицированных объектов совпадают.')
        else:
            raise TestError(u'Метаданные загруженных и реплицированных объектов не совпадают.')

    def assure_stream_replication_is_disabled(self):
        """Если что-то среплицируется, то вызовется исключение."""
        session = self.sessions['secondary']
        response = session.get_catalog()
        if response.status_code != 200:
            self.handle_error_response(response)
        tree = self.get_tree_from_xml_from_zip_from_response(response, 'GetCatalog.zip', 'WF.CLL')
        self.set_getcatalog_from(session, tree)
        for idver in self.unreplicated_objects_idvers:
            id, version = idver
            chart = tree.find('.//CHART[@ID="%s"][@Issue="%s"]' % (id, version))
            if chart is not None:
                raise TestError(u'Потоковая репликация не выключена!')
        self.logger.info_ok('OK')

    def track_deletion(self, time_dec):
        """Отслеживаем удаление объектов на нижестоящем сервере.
        time_dec - убавка от текущего времени (если перед запуском метода был wait)
        """
        time0 = time() - time_dec
        idvers = self.get_all_idvers()
        seconds_left = self.config['max_timeout']
        session = self.sessions['secondary']
        while seconds_left > 0:
            if self.config['variant'] == 'gateway':
                self.download_files()
                self.logger.info(u'Таймаут после выполнения задачи "ЗагрузкаФайловИзДиректории": '
                                 u'%d секунд' % self.config['download_files_timeout'])
                wait(self.config['download_files_timeout'])
            response = session.get_catalog()
            if response.status_code != 200:
                self.handle_error_response(response)
            tree = self.get_tree_from_xml_from_zip_from_response(response, 'GetCatalog.zip', 'WF.CLL')
            self.set_getcatalog_from(session, tree)
            for idver in idvers:
                id, version = idver
                chart = tree.find('.//CHART[@ID="%s"][@Issue="%s"]' % (id, version))
                if chart is None:
                    self.logger.info_ok(u'Удалился объект с id=%s и версией %s (%d сек.)' %
                                        (id, version, int(time()-time0)))
                    seconds_left = self.config['max_timeout']  # восстанавливаем оставшееся время
                    idvers.remove(idver)

            # все ли удаления среплицировались
            if len(idvers) == 0:
                self.logger.info_ok(u'Все объекты удалились!')
                break
            else:
                self.logger.warn(u'Не все объекты удалились: %s' % idvers)
                self.logger.info(u'Cледующая попытка через %d секунд' % self.config['period'])
                wait(self.config['period'])
                seconds_left -= self.config['period']
                continue

        # Истекло время max_timeout
        else:
            self.logger.error(u'%d секунд без удаления неудалившихся объектов' %
                         self.config['max_timeout'])
            self.logger.error(u'Не удалились объекты с (id, версией): %s' %
                         self.unreplicated_objects_idvers)
            raise TestError

    def compare_uploaded_and_replicated_objects(self):
        assert(set(self.uploaded_objects.keys()) == set(self.replicated_objects.keys()))
        success = True
        for idver in self.uploaded_objects.iterkeys():
            id, version = idver
            for attr in self.uploaded_objects[idver].iterkeys():
                if attr not in self.replicated_objects[idver]:
                    success = False
                    self.logger.warn(u'Не найден атрибут %s у объекта с id=%s и версией %s' %
                                (attr, id, version))
                elif self.uploaded_objects[idver][attr] != self.replicated_objects[idver][attr]:
                    success = False
                    self.logger.warn(u'Не совпадают атрибуты %s (%s != %s) у объекта с id=%s и версией %s' %
                                (attr, self.uploaded_objects[idver][attr],
                                 self.replicated_objects[idver][attr], id, version))
        return success

