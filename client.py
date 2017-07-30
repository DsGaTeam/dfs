import logging
import pickle
import socket
import sys
import threading
from common import MessageTypes


def send(address, data):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.sendto(data, address)
    finally:
        sock.close()


def read(server_address, file_name):
    pass


def write(server_address, file_name, string):
    pass


def info(server_address, file_name):
    pass


def delete(server_address, file_name):
    pass


def cd(server_address, path):
    pass


def ls(server_address, path):
    pass


def mk(server_address, path):
    pass


def rm(server_address, path):
    pass


cmd = ''
while cmd != 'exit':
    cmd = sys.stdin.readline().strip()
    print(cmd)
