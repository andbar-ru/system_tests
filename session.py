# coding: utf-8
from uuid import uuid4
import xmlrpclib

import requests


class Session(object):
    """В этом объекте хранятся все данные соединения с конкретным сервером"""
    def __init__(self, server, login, password):
        self.server = server
        self.base_url = 'http://%s' % server
        self.api_url = '%s/api/easo' % self.base_url
        self.webapi_url = '%s/webapi' % self.base_url
        self.jsonrpc_url = '%s/jsonrpc' % self.base_url
        self.session = requests.Session()
        self.session.get('http://%s/login' % server)
        self.csrftoken = self.session.cookies['csrftoken']
        self.headers = {'X-CSRFToken':self.csrftoken}
        self.headers_xml = self.headers.copy()
        self.headers_xml['Content-Type'] = 'text/xml'
        self.session.post('http://%s/login' % server, headers=self.headers,
                          data={'login':login, 'password':password})
        self.id = self.session.cookies['sessionid']
        self.getcatalog_from = 0
        # self.md_classifier_version (test.get_md_classifier_version)

    @staticmethod
    def gen_getarchivecatalog_xml(id):
        return """<?xml version='1.0' encoding='utf-8'?>
                  <request>
                    <header parcel_id='{parcel_id}'/>
                    <objects>
                      <object object_id='{object_id}'/>
                    </objects>
                  </request>
               """.format(parcel_id=uuid4().hex, object_id=id)

    def gen_getcatalog_xml(self):
        return """<?xml version='1.0' encoding='utf-8'?>
                  <request>
                    <header parcel_id="{parcel_id}"/>
                    <getCatalog from="{getcatalog_from}"/>
                  </request>
               """.format(parcel_id=uuid4().hex, getcatalog_from=self.getcatalog_from)

    def get_catalog(self):
        response = self.session.post('%s/GetCatalog' % self.api_url,
                headers=self.headers_xml, data=self.gen_getcatalog_xml())
        return response

    def get_archive_catalog(self, id):
        response = self.session.post('%s/GetArchiveCatalog' % self.api_url,
                headers=self.headers_xml, data=self.gen_getarchivecatalog_xml(id))
        return response

    def upload_object(self, zipfile, xmlfile):
        response = self.session.post('%s/PutObject' % self.api_url,
                                     headers=self.headers,
                                     data={'object_attrs': open(xmlfile).read()},
                                     files={'object_file': open(zipfile, 'rb')})
        return response

    def run_jsonrpc(self, method, data={}):
        return self.session.post('%s/%s' % (self.jsonrpc_url, method), data=data)

    def run_xmlrpc(self, methodname, params):
        xml = xmlrpclib.dumps(params, methodname=methodname, encoding='utf-8')
        response = self.session.post(self.webapi_url, data=xml)
        return response

    @staticmethod
    def gen_deleteobject_xml(id, version):
        return """<?xml version='1.0' encoding='utf-8'?>
                  <request>
                    <header parcel_id='{parcel_id}'/>
                    <deleteVersion objectId='{id}' versionNumber='{version}'/>
                  </request>
               """.format(parcel_id=uuid4().hex, id=id, version=int(float(version)))

    def delete_object(self, id, version):
        response = self.session.post('%s/DeleteObjects' % self.api_url,
                                     headers=self.headers_xml,
                                     data=self.gen_deleteobject_xml(id, version))
        return response

    def set_getcatalog_from(self, value):
        """Вызывается из методов класса Test"""
        if self.getcatalog_from == 0:
            self.getcatalog_from = value

