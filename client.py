import logging
import pickle
import socket
import sys

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
        raise TypeError('Incorrect message type! Expected ' + str(expected_type) + ' but got instead ' + str(msg[0]))
    if len(msg) != expected_length:
        raise ValueError('Incorrect size of response tuple! Expected '
                         + str(expected_length) + ' but received ' + str(len(msg)))


def pack_message(msg_type, path, obj=None):
    if obj is not None:
        result = pickle.dumps((msg_type, path, obj))
    else:
        result = pickle.dumps((msg_type, path))
    return result


def unpack_message(obj):
    return pickle.loads(obj)


def get_file_size(file_name):
    import os
    stat_info = os.stat(file_name)
    return stat_info.st_size


def read_file(file_name):
    result = ''
    file = open(file_name)
    try:
        for line in file:
            result += line
    finally:
        if file is not None:
            file.close()
    return result


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
            logging.error('Timeout was reached with storage ' + str(storage))
        finally:
            sock.close()
    return 'Error! Unable to read file. Timeout was reached while trying to read from storage.'


def write(server_address, local_file_name, dfs_file_name):
    file_size = get_file_size(local_file_name)
    file_content = read_file(local_file_name)

    sock = None
    try:
        sock = open_socket(server_address)

        msg_bytes = pack_message(MessageTypes.WRITE_NAMING, dfs_file_name, file_size)
        sock.send(msg_bytes)

        msg_bytes = sock.recv(BUFFER_SIZE)
        msg = unpack_message(msg_bytes)
        ensure_msg_validity(msg, MessageTypes.WRITE_NAMING_ANSWER, 4)
    finally:
        if sock is not None:
            sock.close()

    if not msg[2]:
        return 'Unable to write file!'

    success = []
    for storage in msg[3]:
        sock = open_socket(storage)

        try:
            msg_bytes = pack_message(MessageTypes.WRITE_STORAGE, dfs_file_name, file_content)
            sock.send(msg_bytes)

            msg_bytes = sock.recv(BUFFER_SIZE)
            msg = unpack_message(msg_bytes)
            ensure_msg_validity(msg, MessageTypes.WRITE_STORAGE_ANSWER, 3)

            if msg[2]:
                success.append(True)
            else:
                success.append(False)
        except socket.timeout:
            logging.error('Timeout was reached with storage ' + str(storage))
        finally:
            sock.close()

    s = success.count(True)
    l = len(success)

    sock = open_socket(server_address)
    try:
        msg_bytes = pack_message(MessageTypes.WRITE_NAMING_CONFIRMATION, dfs_file_name, s)
        sock.send(msg_bytes)
    finally:
        if sock is not None:
            sock.close()

    return 'Successfully wrote to ' + str(s) + ' out of ' + str(l) + ' storage.'


def info(server_address, file_name):
    return one_step_operation(server_address, MessageTypes.INFO, MessageTypes.INFO_ANSWER, file_name)


def delete(server_address, file_name):
    return one_step_operation(server_address, MessageTypes.DELETE, MessageTypes.DELETE_ANSWER, file_name)


def cd(server_address, path):
    return one_step_operation(server_address, MessageTypes.CD, MessageTypes.CD_ANSWER, path)


def ls(server_address, path):
    return one_step_operation(server_address, MessageTypes.LS, MessageTypes.LS_ANSWER, path)


def mk(server_address, path):
    return one_step_operation(server_address, MessageTypes.MK, MessageTypes.MK_ANSWER, path)


def rm(server_address, path):
    return one_step_operation(server_address, MessageTypes.RM, MessageTypes.RM_ANSWER, path)


# COMMAND LINE SERVICE FUNCTIONS

def ensure_amount_of_params(params, amount):
    if len(params) != amount:
        raise ValueError('Incorrect amount of command\'s parameters! Expected '
                         + str(amount) + ' but received ' + str(len(params)))


# COMMAND LINE INTERFACE

cmd = ''
while cmd != 'exit':
    cmd = sys.stdin.readline().strip()

    try:
        params = cmd.split(' ')
        command = params[0]

        if command == 'read':
            ensure_amount_of_params(params, 2)
            res = read(NAMING_SERVER_ADDRESS, params[1])
            print(res)
            logging.info(res)

        elif command == 'write':
            ensure_amount_of_params(params, 3)
            res = write(NAMING_SERVER_ADDRESS, params[1], params[2])
            print(res)
            logging.info(res)

        elif command == 'info':
            ensure_amount_of_params(params, 2)
            res = info(NAMING_SERVER_ADDRESS, params[1])
            print(res)
            logging.info(res)

        elif command == 'delete':
            ensure_amount_of_params(params, 2)
            res = mk(NAMING_SERVER_ADDRESS, params[1])
            if res:
                output = 'File \'' + params[1] + '\' was successfully removed.'
            else:
                output = 'File \'' + params[1] + '\' was not removed!'
            print(output)
            logging.info(output)

        elif command == 'cd':
            ensure_amount_of_params(params, 2)
            res = cd(NAMING_SERVER_ADDRESS, params[1])
            if cd:
                output = 'Changed folder to: \'' + params[1] + '\''
            else:
                output = 'Unable to change folder to: \'' + params[1] + '\''
            print(output)
            logging.info(output)

        elif command == 'ls':
            ensure_amount_of_params(params, 2)
            res = ls(NAMING_SERVER_ADDRESS, params[1])
            print('Content of the folder \'' + params[1] + '\'')
            logging.info('Content of the folder \'' + params[1] + '\'')
            for line in res:
                print(line)
                logging.info(line)

        elif command == 'mk':
            ensure_amount_of_params(params, 2)
            res = mk(NAMING_SERVER_ADDRESS, params[1])
            if res:
                output = 'Folder \'' + params[1] + '\' was successfully created.'
            else:
                output = 'Folder \'' + params[1] + '\' was not created!'
            print(output)
            logging.info(output)

        elif command == 'rm':
            ensure_amount_of_params(params, 2)
            res = rm(NAMING_SERVER_ADDRESS, params[1])
            if res:
                output = 'Successfully removed folder \'' + params[1] + '\''
            else:
                output = 'Unable to remove folder \'' + params[1] + '\''
            print(output)
            logging.info(output)

        else:
            print('Unrecognized command \'' + command + '\'!')
    except Exception as e:
        logging.error(e)
