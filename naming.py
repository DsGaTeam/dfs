import logging
import pickle
import socket
import sys
import threading
import MySQLdb
from pprint import pprint
from common import MessageTypes

DEFAULT_PORT = 9000

def cd(name):
    print("cd " + name)
    dirs = name.split("/")
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

    return True


def read(name):
    print("read " + name)
    dirs = name.split("/")
    filename = dirs.pop()
    print("filename is " + filename)
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
                return False, []

        except (MySQLdb.Error, MySQLdb.Warning) as e:
            print(e)
            return False, []

    try:
        cursor.execute("SELECT id FROM `files` WHERE name=%s AND parent_id=%s AND is_folder=0", (filename, parent_id))

        file_obj = cursor.fetchone()
        if file_obj:
            file_id = file_obj[0]
            print ("File found with id = " + str(file_id))

            # dictionary of storages
            storages_dict = get_storages_dict()
            storages = []

            cursor = db.cursor()
            cursor.execute("SELECT storage_id FROM file_storage WHERE file_id = %s and %s", (file_id, "1"))

            for storage in cursor:
                if storage:
                    print ("Found storage during read:")
                    pprint(storage)
                    storage_id = int(storage[0])
                    storages.append( ( storages_dict[storage_id][1],  9000 + storage_id ) )

            return storages
        else:
            print ("File not found")
            return False, []

    except (MySQLdb.Error, MySQLdb.Warning) as e:
        print(e)
        return False, []


def check_folder_empty(name):
    print("check folder empty " + name)
    dirs = name.split("/")
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
        cursor.execute("SELECT id FROM `files` WHERE parent_id=%s ORDER BY %s", (parent_id,"name"))

        if cursor.rowcount==0:
            return True
        else:
            print (name + " not empty")
            return False

    except (MySQLdb.Error, MySQLdb.Warning) as e:
        print(e)
        return False


def rm(name):
    print("rm " + name)
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

    cursor = db.cursor()
    try:
        cursor.execute(
            "DELETE FROM files WHERE `name`=%s AND `parent_id`= %s AND is_folder=1",
            (dropdir,parent_id))
        deleted_row_count = cursor.rowcount
        db.commit()
        if deleted_row_count > 0:
            print (dropdir + " was deleted")
            return True
        else:
            return False

    except (MySQLdb.Error, MySQLdb.Warning) as e:
        print(e)
        return False


def mk(name):
    print("mk " + name)
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


def write(name, size, confirm):
    print("write " + name)
    dirs = name.split("/")
    filename = dirs.pop()
    print("filename is " + filename + " with size " + str(size))
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
                return False, 0

        except (MySQLdb.Error, MySQLdb.Warning) as e:
            print(e)
            return False, 0

    if confirm:

        try:
            cursor.execute(
                "UPDATE files SET size=size*(-1) WHERE name=%s AND parent_id=%s AND size < 0",
                (filename, parent_id))
            db.commit()
            return True, 0

        except (MySQLdb.Error, MySQLdb.Warning) as e:
            print(e)
            return False, 0

    else:

        insert_id = 0

        try:
            cursor.execute(
                "INSERT INTO files (`name`, `parent_id`, `is_folder`, `size`) VALUES (%s, %s, %s, %s)",
                (filename, parent_id, 0, -size))
            insert_id = cursor.lastrowid
            db.commit()
            return True, insert_id

        except (MySQLdb.Error, MySQLdb.Warning) as e:
            print(e)
            return False, 0


def ls(name):
    print("ls " + name)
    dirs = name.split("/")
    parent_id = 0
    not_found = "not found"

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
                print (dir + " " + not_found)
                return False, [not_found]

        except (MySQLdb.Error, MySQLdb.Warning) as e:
            print(e)
            return False, [not_found]

    cursor = db.cursor()
    res = []
    try:
        cursor.execute("SELECT name,is_folder FROM `files` WHERE parent_id=" + str(parent_id) + " ORDER BY is_folder DESC")

        if cursor.rowcount==0:
            return False, [not_found]

        for file_obj in cursor:
            if file_obj:
                if int(file_obj[1])!=1:
                    resf = "-"
                else:
                    resf = "d"

                resf = resf + " " + str(file_obj[0])

                print (resf)

                res.append( resf )

    except (MySQLdb.Error, MySQLdb.Warning) as e:
        print(e)
        return False, [not_found]

    return True, res


def info(name):
    print("info " + name)
    dirs = name.split("/")
    target = dirs.pop()
    print("target is " + target)
    parent_id = 0
    not_found = "cannot find target"
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
                return not_found

        except (MySQLdb.Error, MySQLdb.Warning) as e:
            print(e)
            return not_found

    try:
        cursor.execute("SELECT id, name, is_folder, size FROM `files` WHERE name=%s AND parent_id=%s", (target, parent_id))
        file_obj = cursor.fetchone()
        if file_obj:
            print (str(file_obj[1]) + " found with id = " + str(file_obj[0]))
            res = "name:" + str(file_obj[1]) + " size:" + str(file_obj[3]) + " is_folder: " + str(file_obj[2])
            return res
        else:
            print (dir + " not found")
            return not_found


    except (MySQLdb.Error, MySQLdb.Warning) as e:
        print(e)
        return not_found


def get_storages():
    storages = []
    
    cursor = db.cursor()
    cursor.execute("SELECT id, url, free_space FROM storage ORDER BY id ASC, free_space DESC")

    for storage in cursor:
        if storage:
            print ("Found storage:")
            pprint (storage)
            storages.append(storage)

    return storages


def get_storages_dict():
    storages = {}

    cursor = db.cursor()
    cursor.execute("SELECT id, url, free_space FROM storage ORDER BY id ASC, free_space DESC")

    for storage in cursor:
        if storage:
            print ("Found storage:")
            storages[storage[0]]=storage

    return storages


def add_file_to_storages(file_id, file_size, storages):
    for cur_storage in storages:
        print ("Adding to storage = " + str(cur_storage))
        cursor = db.cursor()
        try:
            cursor.execute(
                "INSERT INTO file_storage (`file_id`, `storage_id`) VALUES (%s, %s)",
                (file_id, cur_storage))

            file_storage_id = cursor.lastrowid

            db.commit()

            cursor.execute(
                "UPDATE storage SET free_space = free_space - %s WHERE id = %s AND free_space >= %s",
                (file_size, cur_storage, file_size))
            db.commit()


        except (MySQLdb.Error, MySQLdb.Warning) as e:
            print(e)
            return False

    return True


def server():
    sock = socket.socket()
    sock.bind(('', DEFAULT_PORT))
    sock.listen(1)

    while True:
        conn, addr = sock.accept()

        print('connected:', addr)

        while True:
            data = conn.recv(1024)
            if not data:
                break
            # REQUEST
            msg = pickle.loads(data)
            msg_type = msg[0]
            msg_param = msg[1]
            print("Received message " + msg_type)
            pprint (msg)

            # RESPONSE
            if msg_type == MessageTypes.CD:
                result = cd(msg_param)
                r_msg = (MessageTypes.CD_ANSWER, msg_param, result)


            if msg_type == MessageTypes.MK:
                result = mk(msg_param)
                r_msg = (MessageTypes.MK_ANSWER, msg_param, result)

            if msg_type == MessageTypes.RM:
                result = check_folder_empty(msg_param)
                if result==True:
                    result = rm(msg_param)
                r_msg = (MessageTypes.RM_ANSWER, msg_param, result)

            if msg_type == MessageTypes.LS:
                result, ls_array = ls(msg_param)
                r_msg = (MessageTypes.LS_ANSWER, msg_param, ls_array)

            if msg_type == MessageTypes.INFO:
                result = info(msg_param)
                r_msg = (MessageTypes.INFO_ANSWER, msg_param, result)

            if msg_type == MessageTypes.READ:
                result = read(msg_param)
                r_msg = (MessageTypes.READ_ANSWER, msg_param, result)

            if msg_type == MessageTypes.WRITE_NAMING:
                file_size = int(msg[2])
                result, new_file_id = write(msg_param, file_size, False)

                storages_list = get_storages()

                storages = []
                storage_ids = []

                out_of_space = False

                for storage in storages_list:
                    storage_port = DEFAULT_PORT + int(storage[0])  # id
                    storage_host = str(storage[1])                 # url
                    if storage[2]<file_size:                       # free storage
                        out_of_space = True
                    STORAGE = (storage_host, storage_port)
                    storages.append(STORAGE)
                    storage_ids.append(int(storage[0]))
                    if len(storages)>=2:
                        break

                print ("new file id = " + str(new_file_id))
                pprint (storage_ids)
                if new_file_id>0 or out_of_space:
                    add_file_to_storages(new_file_id, file_size, storage_ids)
                else:
                    print ("Invalid file ID during write operation")
                    result = False

                r_msg = (MessageTypes.WRITE_NAMING_ANSWER, msg_param, result, storages)

            if msg_type == MessageTypes.WRITE_NAMING_CONFIRMATION:
                file_name = msg_param
                print ("Confirm write:" + file_name)
                result = write(file_name, 0, True)

            print("Response message")
            pprint (r_msg)

            response_msg = pickle.dumps(r_msg)

            conn.send(response_msg)



        conn.close()


db = MySQLdb.connect(host="localhost", user="root", passwd="qwerty", db="dfs", charset='utf8')
print("Naming server connected to DB")

#res = mk("/home/testuser/somedirA")
#print str(res)

#res = write("/home/somefile567.txt", 123)
#print str(res)

#res = rm("/home/testuser/somedir")
#print str(res)

server()

#res = ls("/home/")
#pprint (res)


db.close()