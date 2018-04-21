import os
import sys

import argparse

import debugger

import server

config_path = '/etc/httpd.conf'

config = {
    'cpu': os.cpu_count(),
    'rdir': '/var/www/html',
    'listeners': 128,
    'port': 5933
}


def config_parse():
    try:
        conf_file = open(config_path, 'r')
        k = 0
        for line in conf_file:
            str_line = line.strip('\n')
            k += 1
            if len(str(str_line)) > 0:
                conf_line = str(str_line).split(' ')
                if conf_line[0] == 'cpu_limit':
                    config['cpu'] = int(conf_line[1])
                elif conf_line[0] == 'document_root':
                    config['rdir'] = conf_line[1]
                elif conf_line[0] == 'port':
                    config['port'] = int(conf_line[1])
                else:
                    print("Warning: config unknown {}th line\n".format(k))
        conf_file.close()
    except IOError as e:
        print("Warning: cannot read config {}, using default configuration".format(e.filename))


def parse_keys():
    global config
    config_parse()
    parser = argparse.ArgumentParser()
    parser.add_argument("--cpu", "-c",
                        type=int,
                        default=config.get('cpu'),
                        help="'cpu' set the number of CPU")
    parser.add_argument("--rdir", "-r",
                        type=str,
                        default=config.get('rdir'),
                        help="'rdir' set the root dir")
    parser.add_argument("--port", "-p",
                        type=int,
                        default=config.get('port'),
                        help="'port' set the server port")
    args = parser.parse_args()
    return args


def start():
    args = parse_keys()

    if not os.path.exists(args.rdir):
        print("Error: rootdir not found")
        sys.exit()

    # debugger.log(args)

    server.run(args.cpu, args.rdir, config['listeners'], args.port)


if __name__ == '__main__':
    start()
