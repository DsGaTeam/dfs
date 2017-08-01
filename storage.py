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
        sock.bind(self.__server_address)
        sock.listen(1)
        print('Started server on \'' + str(self.__server_address) + '\'')
        logging.info('Started server on \'' + str(self.__server_address) + '\'')

        while True:
            conn, address = sock.accept()
            received_data, client_address = sock.recvfrom(BUFFER_SIZE)
            self.receive(received_data, client_address)
            conn.close()

    def receive(self, received_data, client_address):
        pass

    def send(self, server_address, data):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.sendto(data, server_address)
        finally:
            sock.close()


class Node(SimpleSocket):
    def __init__(self, storage_id, storage_addresses, naming_address):
        super(Node, self).__init__(storage_addresses[storage_id])
        self.storage_id = storage_id
        self.storage_addresses = storage_addresses
        self.naming_address = naming_address

    def send_msg(self, address, msg_type, data=None):
        logging.info('Send to host %s message %s', address, msg_type)
        self.send(address, pickle.dumps((msg_type, data)))

    def receive(self, received_data, client_address):
        (msg_type, path, obj) = pickle.loads(received_data)
        logging.INFO('Received from host %s message %s', client_address, msg_type)
        if msg_type == MessageTypes.READ:
            self.do_read(path, client_address)
        elif msg_type == MessageTypes.WRITE_STORAGE:
            self.do_write(path, obj, client_address)
        elif msg_type == MessageTypes.DELETE:
            self.do_delete(path, client_address)
        else:
            self.send_msg(client_address, 'INVALID_REQ')

    def do_read(self, path, address):
        local_path = os.path.join(STORAGE_PREFIX, *path.split('/'))
        if not os.path.isfile(local_path):
            self.send_msg(address, MessageTypes.READ_ANSWER, 'NO FILE!')
            return
        with open(local_path) as fd:
            data = fd.read()
        self.send_msg(address, MessageTypes.READ_ANSWER, data)

    def do_write(self, path, obj, address):
        local_path = os.path.join(STORAGE_PREFIX, *path.split('/'))
        local_dir = os.path.split(local_path)[0]
        os.makedirs(local_dir, exist_ok=True)
        # noinspection PyBroadException
        try:
            with open(local_path, 'w') as fd:
                fd.write(obj)
            self.send_msg(address, MessageTypes.WRITE_STORAGE_ANSWER, True)
        except Exception:
            self.send_msg(address, MessageTypes.WRITE_STORAGE_ANSWER, False)

    def do_delete(self, path, address):
        local_path = os.path.join(STORAGE_PREFIX, *path.split('/'))
        if os.path.isfile(local_path):
            os.remove(local_path)
            self.send_msg(address, MessageTypes.DELETE_ANSWER, True)
        else:
            self.send_msg(address, MessageTypes.DELETE_ANSWER, False)

    def do_rm(self, path, address):
        local_path = os.path.join(STORAGE_PREFIX, *path.split('/'))
        if os.path.isdir(local_path):
            os.rmdir(local_path)
            self.send_msg(address, MessageTypes.RM_ANSWER, True)
        else:
            self.send_msg(address, MessageTypes.RM_ANSWER, False)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('id', help='ID of this storage node', type=int)
    args = ap.parse_args()
    if os.path.isdir(STORAGE_PREFIX):
        shutil.rmtree(STORAGE_PREFIX)
    os.makedirs(STORAGE_PREFIX)
    node = Node(
        storage_id=args.id-1,
        storage_addresses=[('localhost', 9001), ('localhost', 9002)],
        naming_address=('localhost', 9000),
    )
    node.start_server()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
