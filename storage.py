"""Storage server."""


import argparse
import cPickle
import logging
import os
import socket
import sys
import threading


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
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(('', self.__server_address[1]))

        while True:
            received_data, client_address = sock.recvfrom(BUFFER_SIZE)
            self.receive(received_data, client_address)

    def receive(self, received_data, client_address):
        pass

    def send(self, server_address, data):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            size = sys.getsizeof(data)
            sock.sendto(data, server_address)
        finally:
            sock.close()

    def send_to_sock(self, sock, server_address, data):
        size = sys.getsizeof(data)
        sock.sendto(data, server_address)


class Node(SimpleSocket):
    def __init__(self, storage_id, storage_addresses, naming_address):
        super(Node, self).__init__(storage_addresses[storage_id])
        self.storage_id = storage_id
        self.storage_addresses = storage_addresses
        self.naming_address = naming_address

    def send_msg(self, address, type, data=None):
        logging.info('Send to host %s message %s', address, type)
        self.send(address, cPickle.dumps((type, data)))

    def receive(self, received_data, client_address):
        (msg_type, path, obj) = cPickle.loads(received_data)
        logging.INFO('Received from host %s message %s', client_address, msg_type)
        if msg_type == 'READ':
            self.do_read(path, client_address)
        elif msg_type == 'WRITE':
            self.do_write(path, obj, client_address, replicate=True)
        elif msg_type == 'REPLICATE':
            self.do_write(path, obj, client_address, replicate=False)
        elif msg_type == 'DELETE':
            self.do_delete(path, client_address)
        elif msg_type == 'INFO':
            self.do_info(path, client_address)
        else:
            self.send_msg(client_address, 'INVALID_REQ')

    def do_read(self, path, address):
        localpath = os.path.join(STORAGE_PREFIX, *path.split('/'))
        if not os.path.isfile(localpath):
            self.send_msg(address, 'NOFILE')
            return
        with open(localpath) as fd:
            data = fd.read()
        self.send_msg(address, 'READ', data)

    def do_write(self, path, obj, address, replicate):
        localpath = os.path.join(STORAGE_PREFIX, *path.split('/'))
        localdir = os.path.split(localpath)[0]
        os.makedirs(localdir, exist_ok=True)
        with open(localpath, 'w') as fd:
            fd.write(obj)
        if not replicate:
            return
        for peer_id, peer_address in enumerate(self.storage_addresses):
            if peer_id == self.storage_id:
                continue
            self.send_msg(peer_address, 'REPLICATE', obj)

    def do_delete(self, path, address):
        localpath = os.path.join(STORAGE_PREFIX, *path.split('/'))
        if not os.path.isfile(localpath):
            self.send_msg(address, 'NOFILE')
            return
        os.remove(localpath)
        # Recursively remove empty directories.
        # while localpath != STORAGE_PREFIX:
        #     prefix, suffix = os.path.split(localpath)
        #     if len(os.listdir(prefix)) != 0:
        #         break
        #     os.rmdir(prefix)
        #     localpath = prefix
        self.send_msg(address, 'DELETE')

    def do_info(self, path, address):
        localpath = os.path.join(STORAGE_PREFIX, *path.split('/'))
        if not os.path.isfile(localpath):
            self.send_msg(address, 'NOFILE')
            return
        size = os.path.getsize(localpath)
        self.send_msg(address, 'INFO', size)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('id', help='ID of this storage node', type=int)
    args = ap.parse_args()
    os.makedirs(STORAGE_PREFIX)
    node = Node(
        storage_id=args.id-1,
        storage_addresses=[('localhost', 9001), ('localhost', 9002)],
        naming_address=('localhost', 9000),
    )
    logging.info('Starting storage server')
    node.start_server()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
