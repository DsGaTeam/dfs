import logging
import pickle
import socket
import sys
import threading
from pprint import pprint

from common import MessageTypes


BUFFER_SIZE = 1024
NAMING_SERVER_ADDRESS = ('localhost', 9000)


# BUSINESS LOGIC SERVICE FUNCTIONS

def open_socket(address):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(1)
    sock.connect(address)
    return sock


def ensure_msg_validity(msg, expected_type, expected_length):
    if msg[0] != expected_type:
        raise TypeError('Incorrect message type! Expected ' + expected_type + ' but got instead ' + msg[0])
    if len(msg) != expected_length:
        raise ValueError('Incorrect size of response tuple! Expected ' + expected_length + ' but received ' + len(msg))


def pack_message(msg_type, path, obj=None):
    if obj is not None:
        result = pickle.dumps((msg_type, path, obj))
    else:
        result = pickle.dumps((msg_type, path))
    return result


def unpack_message(obj):
    return pickle.loads(obj)


# BUSINESS LOGIC

def one_step_operation(server_address, msg_type, response_type, path):
    sock = None
    try:
        sock = open_socket(server_address)

        msg_bytes = pack_message(msg_type, path)
        sock.send(msg_bytes)

        msg_bytes = sock.recv(BUFFER_SIZE)
        msg = unpack_message(msg_bytes)

        ensure_msg_validity(msg, response_type, 3)
        return msg[2]
    finally:
        if sock is not None:
            sock.close()


def read(server_address, file_name):
    sock = None
    try:
        sock = open_socket(server_address)

        msg_read_bytes = pack_message(MessageTypes.READ, file_name)
        sock.send(msg_read_bytes)

        msg_bytes = sock.recv(BUFFER_SIZE)
        msg = unpack_message(msg_bytes)
        ensure_msg_validity(msg, MessageTypes.READ_ANSWER, 3)
    finally:
        if sock is not None:
            sock.close()

    for storage in msg[2]:
        sock = open_socket(storage)

        try:
            sock.send(msg_read_bytes)

            msg_bytes = sock.recv(BUFFER_SIZE)
            msg = unpack_message(msg_bytes)
            ensure_msg_validity(msg, MessageTypes.READ_ANSWER, 3)

            return msg[2]
        except socket.timeout:
            logging.error('Timeout was reached with storage ' + storage)
        finally:
            sock.close()


def write(server_address, file_name, string):
    pass


def info(server_address, file_name):
    one_step_operation(server_address, MessageTypes.INFO, MessageTypes.INFO_ANSWER, file_name)


def delete(server_address, file_name):
    pass


def cd(server_address, path):
    one_step_operation(server_address, MessageTypes.CD, MessageTypes.CD_ANSWER, path)


def ls(server_address, path):
    one_step_operation(server_address, MessageTypes.LS, MessageTypes.LS_ANSWER, path)


def mk(server_address, path):
    return one_step_operation(server_address, MessageTypes.MK, MessageTypes.MK_ANSWER, path)


def rm(server_address, path):
    pass


# COMMAND LINE SERVICE FUNCTIONS

def ensure_amount_of_params(params, amount):
    if len(params) != amount:
        raise ValueError('Incorrect amount of command\'s parameters! Expected ' + amount + ' but received ' + len(params))


# COMMAND LINE INTERFACE

cmd = ''
while cmd != 'exit':
    cmd = sys.stdin.readline().strip()

    try:
        params = cmd.split(' ')
        command = params[0]

        if command == 'read':
            pass
        elif command == 'write':
            pass
        elif command == 'info':
            pass
        elif command == 'delete':
            pass
        elif command == 'cd':
            pass
        elif command == 'ls':
            ensure_amount_of_params(params, 2)
            res = ls(NAMING_SERVER_ADDRESS, params[1])
        elif command == 'mk':
            ensure_amount_of_params(params, 2)
            res = mk(NAMING_SERVER_ADDRESS, params[1])
            pprint (res)
            if res:
                output = 'Folder \'' + params[1] + '\' was successfully created.'
            else:
                output = 'Folder \'' + params[1] + '\' was not created!'
            print(output)
            logging.info(output)
        elif command == 'rm':
            pass
        else:
            print('Unrecognized command \'' + command + '\'!')

#    except Exception as e:
#        logging.error(e)
    finally:
        pass
