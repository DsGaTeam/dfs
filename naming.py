import logging
import pickle
import socket
import sys
import threading
import MySQLdb
from pprint import pprint
from common import MessageTypes

def rm(name):
    dirs = name.split("/")
    dropdir = dirs.pop()
    parent_id = 0
    for dir in dirs:
        if dir == '':
            continue
        cursor = db.cursor()

        try:
            cursor.execute("SELECT id FROM `files` WHERE name=%s AND parent_id=%s AND is_folder=1", (dir,parent_id))
            file_obj = cursor.fetchone()
            if file_obj:
                print (dir + " found with id = " + str(file_obj[0]))
                parent_id = file_obj[0]
            else:
                print (dir + " not found")
                return False

        except (MySQLdb.Error, MySQLdb.Warning) as e:
            print(e)
            return False

    try:
        cursor.execute(
            "DELETE FROM files WHERE `name`=%s AND `parent_id`= %s AND is_folder=1",
            (dropdir,parent_id))
        db.commit()
        return True

    except (MySQLdb.Error, MySQLdb.Warning) as e:
        print(e)
        return False


def mk(name):
    print "mk " + name
    dirs = name.split("/")
    parent_id = 0

    for dir in dirs:
        if dir == '':
            continue
        cursor = db.cursor()
        is_folder = True
        size = 0

        try:
            cursor.execute("SELECT id FROM `files` WHERE name=%s AND parent_id=%s AND is_folder=1", (dir,parent_id))
            file_obj = cursor.fetchone()
            if file_obj:
                print (dir + " found with id = " + str(file_obj[0]))
                parent_id = file_obj[0]
            else:
                print (dir + " not found and will be created")
                try:
                    cursor.execute(
                        "INSERT INTO files (`name`, `parent_id`, `is_folder`, `size`) VALUES (%s, %s, %s, %s)",
                        (dir, parent_id, is_folder, size))
                    parent_id = cursor.lastrowid
                    db.commit()
                except (MySQLdb.Error, MySQLdb.Warning) as e:
                    print(e)

        except (MySQLdb.Error, MySQLdb.Warning) as e:
            print(e)
            return False

    return True


def write(name, size):
    print "write " + name
    dirs = name.split("/")
    filename = dirs.pop()
    print "filename is " + filename + " with size " + str(size)
    parent_id = 0

    cursor = db.cursor()

    for dir in dirs:
        if dir == '':
            continue
        try:
            cursor.execute("SELECT id FROM `files` WHERE name=%s AND parent_id=%s AND is_folder=1", (dir,parent_id))
            file_obj = cursor.fetchone()
            if file_obj:
                print (dir + " found with id = " + str(file_obj[0]))
                parent_id = file_obj[0]
            else:
                print (dir + " not found")
                return False

        except (MySQLdb.Error, MySQLdb.Warning) as e:
            print(e)
            return False

    try:
        cursor.execute(
            "INSERT INTO files (`name`, `parent_id`, `is_folder`, `size`) VALUES (%s, %s, %s, %s)",
            (filename, parent_id, 0, -size))
        db.commit()
        return True

    except (MySQLdb.Error, MySQLdb.Warning) as e:
        print(e)
        return False

def server():
    sock = socket.socket()
    sock.bind(('', 9000))
    sock.listen(1)
    conn, addr = sock.accept()

    print 'connected:', addr

    while True:
        data = conn.recv(1024)
        if not data:
            break
        # REQUEST
        msg = pickle.loads(data)
        msg_type = msg[0]
        msg_param = msg[1]
        print "Received message " + msg_type
        pprint (msg)

        # RESPONSE
        if msg_type == MessageTypes.MK:
            result = mk(msg_param)
            r_msg = (MessageTypes.MK_ANSWER, msg_param, result)

        print "Response message"
        pprint (r_msg)

        response_msg = pickle.dumps(r_msg)

        conn.send(response_msg)

    conn.close()


db = MySQLdb.connect(host="localhost", user="root", passwd="qwerty", db="dfs", charset='utf8')
print("Naming server connected to DB")

#res = mk("/home/testuser/somedirA")
#print str(res)

#res = write("/home/testuser/somedir/somefile4.txt", 123456)
#print str(res)

#res = rm("/home/testuser/somedir")
#print str(res)

server()

db.close()