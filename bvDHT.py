import socket
from sys import argv
from typing import *
import threading
import hashlib

FingerTable = {
    "self": None, "prev": None, "next": None, "finger1": None, "finger2": None, "finger3": None, "finger4": None
}

# just a dictionary
hashTable = {}

selfPort = 54321
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind(('', selfPort))
sock.listen(5)
maxHash = 2 ** 160 - 1

# ----------------------Helper Functions--------------------------
def get_line(conn: socket):
    msg = b''
    while True:
        ch = conn.recv(1)
        msg += ch
        if ch == b'\n' or len(ch) == 0:
            break
    return msg.decode().strip()


def recvall(conn, msgLength):
    msg = b''
    while len(msg) < msgLength:
        retVal = conn.recv(msgLength - len(msg))
        msg += retVal
        if len(retVal) == 0:
            break
    return msg

def get_HashIndex(addr):
    b_addrStr = ("%s:%d" % addr).encode()
    return int.from_bytes(hashlib.sha1(b_addrStr).digest(), byteorder="big")


def getHashKey(data: str):
    return int.from_bytes(hashlib.sha1(data.encode()).digest(), byteorder="big")

def get_LocalIP():
    ''' my old code that didn't work had some help from stack overflow & GfG
    machine_name = socket.gethostname()
    local_IP = socket.gethostbyname(machine_name)
    return local_IP  '''
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.connect(("8.8.8.8", 80))  # Google DNS doesn't need to be reachable
        return s.getsockname()[0]

def print_help():
    print("valid commands:")
    print("insert <key> <value>")
    print("get <key>")
    print("remove <key>")
    print("contains <key>")
    print("disconnect")
    print("help")

#actuall use the functions to get our own address
self_IP = get_LocalIP()
self_Conn = (self_IP, selfPort)
self_Location = get_HashIndex(self_Conn)

#---------------------------finger table logic------------------------------
def findClosest(hashedKey: int):
    closest = FingerTable["self"]
    for key, value in FingerTable.items():
        if not value:
            continue
        # go through all the if statements trying to find the closest key without going past
        if hashedKey >= value[1] and hashedKey >= closest[1]:
            closest = value if value[1] > closest[1] else closest
        elif hashedKey >= value[1] and hashedKey < closest[1]:
            closest = value
        elif hashedKey < value[1] and hashedKey < closest[1]:
            if (maxHash - value[1]) + hashedKey < (maxHash - closest[1]) + hashedKey:
                closest = value
    return closest[0]



# this should just be a boolean function reutrning true or false (is this person the owner of this space? isOwner? get it)
def isOwner(hashedKey: int):
    selfFinger = FingerTable["self"]
    nextFinger = FingerTable["next"]

    # if we are the only one in the fingertable then return true
    if nextFinger is None:
        return True

    # Check if hashkey is after (greater then self)
    if selfFinger[1] <= hashedKey < nextFinger[1]:
        return True

    # If we are the last node (i.e., the range wraps around)
    if selfFinger[1] > nextFinger[1] and (hashedKey >= selfFinger[1] or hashedKey < nextFinger[1]):
        return True

    return False


def update_prev_finger(peer_Addr):
    # boolean function that returns true upon updating the fingerTable[prev]
    FingerTable["prev"] = (peer_Addr, get_HashIndex(peer_Addr))
    return True

def updateFingers():
    for i in range(4):
        hashedKey = int(self_Location + (maxHash / 5) * (i + 1))
        if hashedKey > maxHash:
            hashedKey -= maxHash
        peer = locate(hashedKey)
        location_p = get_HashIndex(peer)
        FingerTable[f"finger{i}"] = (peer, location_p)

# -------------------- Sending commands Functionality ------------------------

def insert(key, data):
    print('inside insert')
    hashKey = getHashKey(key)
    ack = None
    while ack != '1':
        # Get peer we think owns data
        peer = locate(hashKey)
        print(f"peer: {peer}")

        # Connect to peer
        peerConn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peerConn.connect(peer)

        # Send the peer the command and the key
        peerConn.sendall('INSERT\n'.encode())
        peerConn.sendall(f'{hashKey}\n'.encode())

        # Get acknowledgement of ownership of space
        ack = get_line(peerConn)
        if ack != '1':
            peerConn.close()

    # Send length of data followed by data
    print('outside while loop')
    peerConn.sendall((str(len(data)) + '\n').encode())
    peerConn.sendall(data.encode())

    # ack of the successful insert (hopefully of course)
    ack = get_line(peerConn)
    peerConn.close()
    if ack != '1':
        print("Error: issue inserting data")
        return False
    return True

def locate(data, *args):
    # Get the hashed key of data (if its a int it's fine
    if isinstance(data, int):
        hashedKey = data
    else:
        #if data is a string then call gethashkey to hash it
        hashedKey = getHashKey(data)

    # if we own the data no need to look through the finger table
    if isOwner(hashedKey):
        return self_Conn

    # Get closest finger
    if len(args) == 1:
        closest = args[0]
    else:
        closest = findClosest(hashedKey)

    # Connect to the peer we just found
    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn.connect(closest)
    conn.sendall("LOCATE\n".encode())
    conn.sendall((str(hashedKey) + '\n').encode())

    # Receive Peer Address
    ip, port = get_line(conn).split(':')
    port = int(port)
    conn.close()

    # if you just asked the person who is next closest
    if closest[0] == ip and closest[1] == port:
        return (ip, port)
    # Otherwise try again with the new connection info
    else:
        return locate((ip, port), hashedKey)

def remove(key):
    hashKey = getHashKey(key)
    ack = None
    while ack != '1':
        # Get peer we think owns data
        peer = locate(hashKey)

        # Connect to peer
        peerConn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peerConn.connect(peer)
        peerConn.sendall('REMOVE\n'.encode())
        peerConn.sendall((f"{hashKey}\n").encode())

        ack = get_line(peerConn)
        if ack != '1':
            peerConn.close()

    # Get ack of successful removal
    ack = get_line(peerConn)
    peerConn.close()
    if ack == '1':
        print("Successfully removed!")
        return True
    else:
        print(f"Error removing {key}")
        return False

def get(key):
    hashedKey = getHashKey(key)
    ack = None
    while ack != '1':
        # Get peer we think owns data
        peer = locate(hashedKey)

        # Connect to peer
        peerConn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peerConn.connect(peer)

        # Send the connect protocol information
        peerConn.sendall(b'GET\n')
        peerConn.sendall((str(hashedKey) + '\n').encode())

        # Get acknowledgement of ownership of space
        ack = get_line(peerConn)
        if ack != '1':
            peerConn.close()

    # Get length of data and data of that length
    dataSize = int(get_line(peerConn))
    if dataSize == 0:
        print("No data found...")
        data = ''
    else:
        data = recvall(peerConn, dataSize).decode()
        print(data)
    peerConn.close()
    return data

def connect(peerIP, peerPort):
    # place ourself in the finger table
    FingerTable["self"] = (self_Conn, self_Location)

    self_address = "%s:%d" % self_Conn
    ack = None
    while ack == '0':
        # find the closest peer to you to place yourself in the ring so to speak
        peer = locate(self_address, (peerIP, peerPort))

        # connect to the decided peer and send our location (and the command)
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.connect(peer)
        conn.sendall('CONNECT\n'.encode())
        conn.sendall(f"{self_Location}\n".encode())

        # acknowledge for the space owner
        ack = get_line(conn)
        if ack != '1':
            conn.close()

    # Receive data
    numEntries = int(get_line(conn))
    for i in range(numEntries):
        key = int(get_line(conn))
        len_item = int(get_line(conn))
        item = recvall(conn, len_item).decode()
        hashTable[key] = item

    # Get address of the next peer
    ip, port = get_line(conn).split(':')
    port = int(port)
    FingerTable["next"] = ((ip, port), get_HashIndex((ip, port)))
    conn.sendall((self_address + "\n").encode())
    conn.close()

    #handle updating all our fingers
    update_prev_finger(peer)
    update_prev((ip, port))
    updateFingers()

def disconnect():
    print("Disconnecting from DHT...")

    # Transfer data to your prev person in the finger table
    prev = FingerTable["prev"]
    nextPeer = FingerTable["next"]

    # If you are lonely and the only person currently in the DHT
    if prev[1] == FingerTable["self"][1]:
        return

    ack = None
    while ack == '0': # while you still have a negative acknowledgment
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.connect(prev[0])
        conn.sendall('DISCONNECT\n'.encode())

        # Send next finger address as {IP}:{PORT}
        conn.sendall((f"{nextPeer[0][0]}:{nextPeer[0][1]}\n").encode())

        # Send the length of our hash table, how many entries we have
        hashTable_len = len(hashTable)
        conn.sendall(f'{hashTable_len}\n'.encode())

        # Send hashed key, length of the data, and the data itself
        for key, data in hashTable.items():
            conn.sendall(f'{key}\n'.encode())
            data_len = len(data) # get the length of the data
            conn.sendall(f'{data_len}\n'.encode())
            conn.sendall(data.encode())

        # take in th eacknowledgment of a successful transfer
        ack = get_line(conn)
        conn.close() # we just reopen the socket et the beginneing of the loop anyway
    update_prev(nextPeer[0], prev[0])
    print("Disconnect command was successful YIPPEE")

def contains(key):
    hashKey = getHashKey(key)
    ack = None # default initialization

    while ack == '0': # while you have a negative acknowledgement try and locate/connect to peers
        # Get peer we think owns data
        peer = locate(hashKey)

        # Connect to the peer
        peerConn = socket(socket.AF_INET, socket.SOCK_STREAM)
        peerConn.connect(peer)
        peerConn.sendall('CONTAINS\n'.encod())
        peerConn.sendall((f"{hashKey}\n").encode())

        # Get acknowledgement of ownership of space
        ack = get_line(peerConn)
        if ack != '1':
            peerConn.close()

    ack = get_line(peerConn) # acknowledgement if the data actually got there
    peerConn.close()
    if ack == '0':
        print("Fales")
        return False
    else:

        return True

def update_prev(nextAddr, prevAddr = self_Conn): # prevAddr is default to our own connection (self conn is our IP and port)
    try:
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.connect(nextAddr)

        conn.sendall("UPDATE_PREV\n".encode())
        conn.sendall((f"{prevAddr[0]}:{prevAddr[1]}\n").encode())

        ack = get_line(conn)
        if ack == '0':
            print("Update Prev command was unsuccessful")
            conn.close()
            return False
        else:
            print("Update Prev command was successful, can i get 5 big booms? (thats good)")
            conn.close()
            return True
    except Exception as e:
        print(f"Error: {e}")
        return False
    except KeyboardInterrupt:
        print("Exiting update_prev function hold on a minute plz")

#------------------------- Recieving Funcitonality ---------------------------------

def locate_recv(conn):
    # grab the hash key
    hashKey = int(get_line(conn))

    #Get closest peer
    closest = findClosest(hashKey)
    closest_string = f"{closest[0]}:{closest[1]}\n"

    # actually send the thing
    conn.sendall(closest_string.encode())

def get_recv(conn):
    hashKey = int(get_line(conn))
    # Send ack of ownership of space
    if isOwner(hashKey) == False:
        conn.sendall(b'0\n')
        conn.close()
        return
    conn.sendall(b'1\n')

    # Send len of data followed by data. 0 if not found
    if hashKey in hashTable:
        conn.sendall((str(len(hashTable[hashKey])) + '\n').encode())
        conn.sendall(hashTable[hashKey].encode())
    else:
        conn.sendall('0\n'.encode())
    conn.close()

def insert_recv(conn):
    hashKey = int(get_line(conn))
    # make sure we arent the owner of the space
    if isOwner(hashKey) == False:
        conn.sendall('0\n'.encode())
        conn.close()
        return
    conn.sendall('1\n'.encode())

    # Grab data
    len_data = int(get_line(conn))
    data = recvall(conn, len_data)
    data = data.decode()

    # put the data in our hashTable and tell our peer everythings fine
    hashTable[hashKey] = data
    conn.sendall('1\n'.encode())
    conn.close()

def remove_recv(conn):
    hashKey = int(get_line(conn))

    # Send ack of ownership of space
    if not isOwner(hashKey):
        conn.sendall('0\n'.encode())
        conn.close()
        return
    conn.sendall('1\n'.encode())
    # Remove data and send 1 if successful, 0 if not
    try:
        if hashKey in hashTable:
            hashTable.pop(hashKey)
        conn.sendall('1\n'.encode())
    except Exception as e:
        print(f"Error: {e}")
        conn.sendall(b'0\n')
    except KeyboardInterrupt:
        print("stopped inside of remove_recv()")
    finally:
        conn.close()

def contains_recv(conn):
    hashKey = int(get_line(conn))
    # if isOwner is false close the connection and send the negative acknowledgment
    if isOwner(hashKey) == False:
        conn.sendall('0\n'.encode())
        conn.close()
        return
    conn.sendall('1\n'.encode())

    # send negative acknowledgement if the key isn't in your hashTable
    if hashKey not in hashTable:
        conn.sendall('0\n'.encode())
    else:
        conn.sendall('1\n'.encode())
    conn.close()


def connect_recv(conn):
    try:
        hashKey = int(get_line(conn))

        if isOwner(hashKey) == True: # if you have reached the owner of the hashkey
            conn.sendall('1\n'.encode())

            numfiles = len(hashTable)
            conn.sendall(f"{numfiles}\n".encode())
            # for every key value pair in our hashtable send the key, len(value), and value over
            for key, value in hashTable:
                len_value = len(value)
                conn.sendall(f"{key}\n".encode())
                conn.sendall(f"{len_value}\n".encode())
                conn.sendall(value.encode())

            if FingerTable["next"]:
                nextConn = FingerTable["next"][0]
                conn.sendall((f"{nextConn[0]}:{nextConn[1]}\n").encode())
            else:
                nextConn = FingerTable["self"][0]
                conn.sendall((f"{nextConn[0]}:{nextConn[1]}\n").encode())

            ip, port = get_line(conn).split(':')
            port = int(port)
            FingerTable["next"] = ((ip, port), get_HashIndex((ip, port)))
        else:
            conn.sendall(b'0\n')
            conn.close()
            return
    except Exception as e:
        print(f"Error found at connect_recv: {e}")
    except KeyboardInterrupt:
        print("stopped inside of connect_recv()")

def disconnect_recv(conn):
    # Get ip and port of new next Finger
    try:
        ip, port = get_line(conn).split(':')
        port = int(port)

        # Update Fingers
        FingerTable["next"] = ((ip, port), get_HashIndex((ip, port)))

        # Get num entries
        n = int(get_line(conn))

        # for the number of entries get the key, len, and data
        for i in range(n):
            key = int(get_line(conn))
            len_Data = int(get_line(conn))
            data = recvall(conn, len_Data).decode()
            hashTable[key] = data

        # Send ACK of successful transfer
        conn.sendall(b'1\n')
    except Exception as e:
        print(e)

def update_prev_recv(conn):
    try:
        ip, port = get_line(conn).split(':')
        update = update_prev_finger((ip, int(port)))

        if update:
            conn.sendall(b'1\n')
        else:
            conn.sendall(b'0\n')
    except Exception as e:
        print(f"Error: {e}")
        conn.sendall(b'0\n')

def handle_recieving(conn, addr):
    try:
        command = get_line(conn)

        #this is the
        if command == "CONNECT":
            print(f"recieved command: {command} from {addr}")
            connect_recv(conn)
        elif command == "CONTAINS":
            print(f"recieved command: {command} from {addr}")
            contains_recv(conn)
        elif command == "LOCATE":
            print(f"recieved command: {command} from {addr}")
            locate_recv(conn)
        elif command == "GET":
            print(f"recieved command: {command} from {addr}")
            get_recv(conn)
        elif command == "INSERT":
            print(f"recieved command: {command} from {addr}")
            insert_recv(conn)
        elif command == "REMOVE":
            print(f"recieved command: {command} from {addr}")
            remove_recv(conn)
        elif command == "DISCONNECT":
            print(f"recieved command: {command} from {addr}")
            disconnect_recv(conn)
        elif command == "UPDATE_PREV":
            print(f"recieved command: {command} from {addr}")
            update_prev_recv(conn)

    except Exception as e:
        print(f"Error within handling_recv(): {e}")
    except KeyboardInterrupt:
        print("stopped inside of handle_recieving()")
    finally:
        conn.close()

def start_FingerTable():
    print("Startign up the hashTable, setting the fingerTable[self] and [next] to self")

    FingerTable["self"] = (self_Conn, self_Location)
    FingerTable["next"] = (self_Conn, self_Location)

def accept_loop(): # legit only creates a thread theres no other purpose
    print('in accept loop')
    while True:
        threading.Thread(target=handle_recieving, args=(*sock.accept(),), daemon=True).start()

print("Starting up the Hash Table")
print(f"Your IP Address: {self_IP} Your Port: {selfPort}")

try:
    if len(argv) == 3:
        peerIP = argv[1]
        peerPort = int(argv[2])
        connect(peerIP, peerPort)
    elif len(argv) == 1:
        start_FingerTable()
        print('here in elif statement')
    else:
        print("Wrong number of arguments")
        print("correct syntax: Python3 bv-DHT.py <ip> <port> or python3 bv-DHT.py if you want to start the DHT")
    threading.Thread(target=accept_loop, args=(), daemon=True).start()

    print("get input maybe")
    while True:
        #get input from the user
        msg = input('Enter a Command>')
        if " " in msg:
            command, data = msg.split(' ', 1)
        else:
            command = msg
            data = ''

        #start the menu to send out commands to your peers
        if command == "get":
            get(data)
        elif command == "locate":
            locate(data)
        elif command == "insert":
            key, value = data.split(' ', 1) #split at the first space only
            print("inside of the insert menu elif")
            insert(key, value)
        elif command == "remove":
            remove(data)
        elif command == "contains":
            contains(data)
        elif command == "disconnect":
            disconnect()
            exit(0)
        elif command == "help":
            print_help()
        else:
            print(f"soooo {command} is not an actual command")
            print('use the <help> command if you are stuck (i know i forget all of them -the programmers')
except KeyboardInterrupt:
    print("\n trying to shut the thing down")
    sock.close()
