import os
from urllib.parse import unquote
import debugger

import resp_src
from resp_builder import RespBuilder


class ReqParser:
    def __init__(self, data):
        debugger.log("parsing request...")
        self.method, self.path = self.get_method_path(data)
        debugger.log("method = {}".format(self.method))
        debugger.log("path = {}".format(self.path))

    @staticmethod
    def get_method_path(data):
        method, url = data.split(' ')[0:2]
        path = unquote(url.split('?')[0])
        return method, path


def cont_type(path):
    f_type = path.split('.')[-1]
    if f_type in resp_src.TYPES:
        return resp_src.TYPES[f_type]
    return ''


def handle(req, resp, root_dir):
    # debug_print("\n_HANDLE")

    host_path = os.path.normpath(root_dir + req.path)
    if os.path.commonprefix([host_path, root_dir]) != root_dir:
        return

    host_path_index = os.path.join(host_path, resp_src.INDEX_PAGE)
    if os.path.isfile(host_path_index):
        host_path = host_path_index
    elif os.path.exists(host_path):
        resp.code = resp_src.STATUS['FORBIDDEN']
    try:
        f = open(host_path, 'rb')
        content = f.read()
        f.close()
        resp.content_length = len(content)
        if req.method == "HEAD":
            content = b''
        resp.body = content
        resp.content_type = cont_type(host_path)
        resp.code = resp_src.STATUS['OK']
    except IOError as e:
        # pass
        print("Error: with file " + e.filename)


def process(rdir, data):
    data = data.decode('utf-8')
    req = ReqParser(data)
    resp = RespBuilder()
    if req.method in resp_src.ALLOW_METHOD:
        handle(req, resp, rdir)
    else:
        resp.code = resp_src.STATUS['NOT_ALLOWED']
    return resp.build()
