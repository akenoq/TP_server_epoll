from time import gmtime, strftime

import resp_src as const
import debugger


class RespBuilder:
    def __init__(self, code=const.STATUS['NOT_FOUND'], content_length=0, content_type='', body=b''):
        self.code = code
        self.content_length = content_length
        self.content_type = content_type
        self.body = body

    def build(self):
        if self.code is const.STATUS['OK']:
            resp = self.resp_success()
            debugger.log('RESP OK\n')
        else:
            resp = self.resp_fail()
            debugger.log('RESP not OK\n')
        return resp

    @staticmethod
    def http_datenow_formating(format_conf):
        return strftime(format_conf, gmtime())

    def resp_success(self):
        resp = 'HTTP/' + const.HTTP_VERSION + ' ' + self.code + '\r\n' + \
               'Server: ' + const.SERVER_NAME + '\r\n' + \
               'Date: ' + self.http_datenow_formating(const.HTTP_DATE_FORMAT) + '\r\n' + \
               'Connection: Close\r\n' + \
               'Content-Length: ' + str(self.content_length) + '\r\n' + \
               'Content-Type: ' + self.content_type + '\r\n\r\n'
        return resp.encode() + self.body

    def resp_fail(self):
        resp = 'HTTP/' + const.HTTP_VERSION + ' ' + self.code + '\r\n' + \
               'Server: ' + const.SERVER_NAME + '\r\n' + \
               'Date: ' + self.http_datenow_formating(const.HTTP_DATE_FORMAT) + '\r\n' + \
               'Connection: Close\r\n\r\n'
        return resp.encode() + self.body
