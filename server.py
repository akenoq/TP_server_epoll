import os
import socket

import debugger

import service

import select  # epoll() O(1) available on Linux 2.5+


def child_proc(sock, rdir):
    print("worker PID={}".format(os.getpid()))
    buf_size = 1024

    epoll = select.epoll()
    epoll.register(sock.fileno(), select.EPOLLIN)  # когда приходит соединение, with Eventmask = EPOLLIN Available for read
    # ev.events = EPOLLIN | EPOLLPRI | EPOLLERR | EPOLLHUP;
    # ev.data.fd = client_sock;
    # int res = epoll_ctl(epfd, EPOLL_CTL_ADD, client_sock, &ev)
    try:
        connections = {}
        requests = {}
        responses = {}
        while 1:
            events = epoll.poll()
            debugger.log("PID = {} EVENTS = {} ".format(os.getpid(), events))
            for (fileno, eventmask) in events:
                if fileno == sock.fileno():
                    try:
                        (cl_conn, cl_addr) = sock.accept()

                        cl_conn.setblocking(0)
                        cl_fd = cl_conn.fileno()

                        epoll.register(cl_conn.fileno(), select.EPOLLIN)

                        connections[cl_fd] = cl_conn
                        requests[cl_fd] = b''
                        responses[cl_fd] = b''

                        debugger.log(str((cl_conn, cl_addr)))
                    except BlockingIOError:
                        pass

                elif eventmask & select.EPOLLIN:
                    debugger.log("EVENT IN = {} FD = {}".format(eventmask, fileno))
                    cl_conn = connections[fileno]
                    requests[fileno] = cl_conn.recv(buf_size)

                    if len(requests[fileno].strip()) == 0:
                        epoll.unregister(fileno)
                        cl_conn.close()
                        del connections[fileno]
                        del requests[fileno]
                        del responses[fileno]
                        debugger.log("CLOSED ********************>")
                        continue
                    epoll.modify(fileno, select.EPOLLOUT)
                    responses[fileno] = service.process(rdir, requests[fileno])

                elif eventmask & select.EPOLLOUT:
                    debugger.log("EVENT OUT = {} FD = {}".format(eventmask, fileno))

                    cl_conn = connections[fileno]
                    resp = responses[fileno]

                    cl_conn.sendall(resp)

                    epoll.unregister(fileno)
                    connections[fileno].close()

                    del connections[fileno]
                    del requests[fileno]
                    del responses[fileno]

                elif eventmask & select.EPOLLHUP:
                    epoll.unregister(fileno)
                    connections[fileno].close()
                    del connections[fileno]
                    del requests[fileno]
                    del responses[fileno]
    finally:
        epoll.unregister((sock.fileno()))
        epoll.close()
        sock.close()


def run(cpu_num, rdir, listeners, port):
    sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('0.0.0.0.', port))
    sock.listen(listeners)

    sock.setblocking(0)

    print("Server started on port {}".format(port))
    print("PID={}".format(os.getpid()))

    workers = []
    for i in range(cpu_num):
        new_pid = os.fork()
        if new_pid == 0:
            child_proc(sock, rdir)
            # break
        else:
            workers.append(new_pid)

    sock.close()

    for child_pid in workers:
        os.waitpid(child_pid, 0)
