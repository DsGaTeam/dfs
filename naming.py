import logging
import pickle
import socket
import sys
import threading
import MySQLdb
from pprint import pprint
from common import MessageTypes

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


db = MySQLdb.connect(host="localhost", user="root", passwd="qwerty", db="dfs", charset='utf8')
print("Naming server connected to DB")

res = mk("/home/testuser/somedir")
print str(res)

res = write("/home/testuser/somedir/somefile4.txt", 123456)
print str(res)

db.close()