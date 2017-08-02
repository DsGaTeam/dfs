"""Storage server."""


import argparse
import pickle
import logging
import os
import shutil
import socket
import threading
from common import MessageTypes


logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(asctime)s:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
BUFFER_SIZE = 1024
STORAGE_PREFIX = os.path.join('storage')


class SimpleSocket(object):
    def __init__(self, server_address):
        self.__server_address = server_address
        self.__server_thread = threading.Thread(target=self.__start_server)

    def start_server(self):
        self.__server_thread.start()

    def __start_server(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('', self.__server_address[1]))
        sock.listen(1)
        print('Started server on \'' + str(self.__server_address) + '\'')
        logging.info('Started server on \'' + str(self.__server_address) + '\'')

        while True:
            conn, address = sock.accept()
            received_data = conn.recv(BUFFER_SIZE)
            self.receive(conn, received_data, address)
            if conn is not None:
                conn.close()

    def receive(self, conn, received_data, client_address):
        pass

    def send(self, conn, server_address, data):
        try:
            conn.sendto(data, server_address)
        finally:
            conn.close()


class Node(SimpleSocket):
    def __init__(self, storage_address):
        super(Node, self).__init__(storage_address)

    def send_msg(self, conn, address, msg_type, name, data=None):
        logging.info('Send to host ' + str(address) + ' message ' + str(msg_type))
        self.send(conn, address, pickle.dumps((msg_type, name, data)))

    def receive(self, conn, received_data, client_address):
        msg = pickle.loads(received_data)
        msg_type = msg[0]
        path = msg[1]
        if len(msg) == 3:
            obj = msg[2]
        else:
            obj = None
        logging.info('Received from host ' + str(client_address) + ' message ' + str(msg_type))
        try:
            if msg_type == MessageTypes.READ:
                self.do_read(conn, path, client_address)
            elif msg_type == MessageTypes.WRITE_STORAGE:
                self.do_write(conn, path, obj, client_address)
            elif msg_type == MessageTypes.DELETE:
                self.do_delete(conn, path, client_address)
            elif msg_type == MessageTypes.RM:
                self.do_rm(conn, path, client_address)
            else:
                self.send_msg(conn, client_address, 'INVALID_REQ')
                print('Invalid request: ' + str(msg_type))
                logging.error('Invalid request: ' + str(msg_type))
        except Exception as e:
            print(e)
            logging.error(e)

    def do_read(self, conn, path, address):
        local_path = os.path.join(STORAGE_PREFIX, *path.split('/'))
        if not os.path.isfile(local_path):
            self.send_msg(conn, address, MessageTypes.READ_ANSWER, path, 'NO FILE!')
            return
        with open(local_path) as fd:
            data = fd.read()
        self.send_msg(conn, address, MessageTypes.READ_ANSWER, path, data)

    def do_write(self, conn, path, obj, address):
        local_path = os.path.join(STORAGE_PREFIX, *path.split('/'))
        local_dir = os.path.split(local_path)[0]
        os.makedirs(local_dir, exist_ok=True)
        # noinspection PyBroadException
        try:
            with open(local_path, 'w') as fd:
                fd.write(obj)
            self.send_msg(conn, address, MessageTypes.WRITE_STORAGE_ANSWER, path, True)
        except Exception:
            self.send_msg(conn, address, MessageTypes.WRITE_STORAGE_ANSWER, path, False)

    def do_delete(self, conn, path, address):
        local_path = os.path.join(STORAGE_PREFIX, *path.split('/'))
        if os.path.isfile(local_path):
            os.remove(local_path)
            self.send_msg(conn, address, MessageTypes.DELETE_ANSWER, path, True)
        else:
            self.send_msg(conn, address, MessageTypes.DELETE_ANSWER, path, False)

    def do_rm(self, conn, path, address):
        local_path = os.path.join(STORAGE_PREFIX, *path.split('/'))
        if os.path.isdir(local_path):
            os.rmdir(local_path)
            self.send_msg(conn, address, MessageTypes.RM_ANSWER, path, True)
        else:
            self.send_msg(conn, address, MessageTypes.RM_ANSWER, path, False)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('-n', help='Hostname of this storage node in format <hostname>:<port>', nargs=1)
    args = ap.parse_args()
    if args.n is not None and len(args.n) == 1:
        name, port = args.n[0].split(':')
        localhost = (str(name), int(port))
        print(localhost)
    else:
        localhost = ('localhost', 9001)
    global STORAGE_PREFIX
    STORAGE_PREFIX = os.path.join('storage', str(localhost[1]))
    if os.path.isdir(STORAGE_PREFIX):
        shutil.rmtree(STORAGE_PREFIX)
    os.makedirs(STORAGE_PREFIX)
    node = Node(storage_address=localhost)
    node.start_server()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
