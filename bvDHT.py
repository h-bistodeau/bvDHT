import hashlib
from sys import argv
from socket import *
import threading
from time import sleep

# Application is launched through argv. Lack of arguments indicates this will be the server. If there are arguments, then we are
# connecting to a peer.


# Backmans getHashIndex function------
# Returns an integer index into the hash-space for a node Address
#  - addr is of the form ("ipAddress or hostname", portNumber)
#    where the first item is a string and the second is an integer
def getHashIndex(addr):
    b_addrStr = ("%s:%d" % addr).encode()
    return int.from_bytes(hashlib.sha1(b_addrStr).digest(), byteorder="big")

# Don't need a class for the finger table, just a dictionary. Key is whatever it is, value should be the peer address.
def fingerTableSetup(self, startup=True):
    table = {}
    if startup:
        for i in range(1, 5):
            table[f"finger{i}"] = self
        table["next"] = self
        table["prev"] = self
    else:
        for i in range(1, 5):
            # this will have to iterate through who's in the data space.
            table[f"finger{i}"] = "Test"
    table["self"] = self

    return table


def getLine(conn):
    msg = b''
    while True:
        ch = conn.recv(1)
        msg += ch
        if ch == b'\n' or len(ch) == 0:
            break
    return msg.decode()


# The all-powerful hash table for this project.
Valid_commands = ["LOCATE", "CONNECT", "DISCONNECT", "CONTAINS", "GET", "INSERT", "REMOVE", "UPDATE_PREV"]
running = True

hashTable = {}

# Our space in the system is determined by our distance between us and next.
fingertable = {}

# Commands

def get(peer, key):
    peer.send(("GET\n").encode())
    peer.send((f"{key}\n").encode())
    ack = getLine(peer)
    length = int(getLine(peer))
    data = getLine(peer)
    if ack == "0":
        return "NULL"
    return data


def insert(peer, key, value):
    peer.send(("INSERT\n").encode())
    peer.send((f"{key}\n").encode())
    ack = getLine(peer)
    if ack == "0":
        return False
    peer.send((f"{len(key)}\n").encode())
    peer.send((f"{key}\n").encode())
    peer.send((f"{value}\n").encode())
    ack = getLine(peer)
    if ack == "0":
        return False
    return True


def remove(peer, key):
    peer.send(("REMOVE\n"))
    peer.send((f"{key}\n").encode())
    ack = getLine(peer)
    if ack == "0":
        return False
    ack = getLine(peer)
    if ack == "0":
        return False
    return True


def contains(peer, key):
    peer.send(("CONTAINS\n"))
    peer.send((f"{key}\n").encode())
    ack = getLine(peer)
    if ack == "0":
        return False
    ack = getLine(peer)
    if ack == "0":
        return False
    return True


def locate(peer, key):
    peer.send(("LOCATE\n").encode())
    peer.send((f'{key}\n').encode())
    address = getLine(peer)
    return address


def connect(peer, address_key, finger_table):
    peer.send(("CONNECT\n").encode())
    peer.send((f'{address_key}\n').encode())
    num_entries = getLine(peer)
    while num_entries != 0:
        key = getLine(peer)
        length = int(getLine(peer))
        value = getLine(peer)
        hashTable[key] = value
        num_entries -= 1
    next_peer = getLine(peer)
    finger_table["next"] = next_peer
    update_prev(peer, next_peer)
    peer.send((f"{finger_table['self']}\n").encode())


def disconnect(peer, address_key):
    peer.send(("DISCONNECT\n").encode())
    peer.send((f'{address_key}\n').encode())
    num_files = len(hashTable)
    peer.send((f'{num_files}\n').encode())
    for key in hashTable:
        peer.send((f"{key}\n").encode())
        peer.send((f"{len(hashTable[key])}").encode())
        peer.send((f"{hashTable[key]}").encode())
    ack = getLine(peer)
    if ack == "0":
        return False
    return True


def update_prev(next, self_key):
    next.send(("UPDATE_PREV\n").encode())
    next.send((f"{self_key}\n").encode())
    ack = getLine(next)
    if ack == '0':
        return False
    return True


# Helper functions
def handle_messages(socket):
    global running

    # while we are still connected to the DHT
    while running:
        try:
            # recieve a message/command
            str_msg = getLine(socket).strip()

            # disconnect if you haven't recieved one
            if not str_msg:
                print("Disconnected")
                break
            if str_msg in Valid_commands:
                if str_msg == "LOCATE":
                    print("recieved locate command...")
                    key = getLine(socket)

                    #call your own locate in order to send your closest peer
                    address = locate(socket, key)
                    socket.send((f"{address}\n").encode())

                elif str_msg == "CONTAINS":
                    print("recieved contains command...")
                    key = getLine(socket)
                    # if you own the space or that key ack(1) if not ack(0)
                    if key in hashTable.keys():
                        socket.send((f"1\n").encode())

                        # if you have entry ack(1) if not ack(0)
                        if hashTable[key] is not None:
                            socket.send((f"1\n").encode())
                        else:
                            socket.send((f"0\n").encode())
                    #if its not even in the space send both negative acknowledgments
                    else:
                        socket.send((f"0\n").encode())
                        socket.send((f"0\n").encode())

                elif str_msg == "GET":
                    print("recieved get command...")
                    key = getLine(socket)

                    # acknowledge ownership of the hashed space
                    if key in hashTable.keys():
                        socket.send((f"1\n").encode())
                    else:
                        socket.send((f"0\n").encode())
                    # try to get the length of the value at the key, but it might be zero throwing an error.
                    try:
                        length = len(hashTable[key])
                    except KeyError:
                        length = 0

                    socket.send((f"{length}\n").encode())
                    socket.send((f"{hashTable[key]}").encode())

                elif str_msg == "INSERT":
                    print("recieved insert command...")
                    key = getLine(socket)

                    # ack if the key is owned by you
                    if key in hashTable.keys():
                        socket.send((f"1\n").encode())

                        try:
                            # try to recieve all the proper data to insert and acknowledge
                            len_key = getLine(socket)
                            key = getLine(socket)
                            data = socket.recv(len_key.decode())
                            hashTable[key] = data
                            socket.send((f"1\n").encode())
                        except KeyError:
                            socket.send((f"0\n").encode())
                    else:
                        socket.send((f"0\n").encode())

                elif str_msg == "REMOVE":
                    print("recieved remove command...")
                    key = getLine(socket)

                    #if the key is within our table (we own it) thn remove it
                    if key in hashTable.keys():
                        hashTable.pop(key)
                        socket.send((f"1\n").encode())
                    else:
                        socket.send((f"0\n").encode())

                elif str_msg == "UPDATE_PREV":
                    print("received update_prev command...")
                    #try to recieve the peer's key and set it within your fingertable
                    try:
                        key = getLine(socket)
                        fingertable["prev"] = key
                        socket.send((f"1\n").encode())
                    except KeyError:
                        socket.send((f"0\n").encode())
            else:
                print("recieved unknown command, or data is being recieved elsewhere")
        except Exception as e:
            print('Error', e)
            print("Disconnected")
            running = False
            break



def con(connection):
    connection.send(("Connection established\n").encode())
    msg = getLine(connection)
    print(msg)


def recv(conn):
    msg = getLine(conn)
    print(msg)
    sleep(4)
    conn.send(("Finished work\n").encode())
    return


def handle_input():
    while True:
        i = input("Enter a string: ")
        print("This is your string: " + i)
        print("This is the string hashed: " + str(int.from_bytes(hashlib.sha1(i.encode()).digest(), byteorder='big')))


def socket_listener(sock):
    while True:
        try:
            conn, addr = sock.accept()
            print("Connected by", addr)
            threading.Thread(target=con, args=(conn,), daemon=True).start()
        except socket.timeout:
            continue


if __name__ == "__main__":
    if len(argv) == 1:
        sock = socket(AF_INET, SOCK_STREAM)
        sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        sock.bind(('', 8008))
        sock.listen(2)

        print("Server is listening on port 8008...")

        # Start socket listener thread
        threading.Thread(target=socket_listener, args=(sock,), daemon=True).start()

        # Start input thread
        threading.Thread(target=handle_input, daemon=True).start()

        # Keep main thread alive
        try:
            while True:
                sleep(1)
        except KeyboardInterrupt:
            print("\nKeyboard interrupt received. Shutting down server.")
            sock.close()


    # Client mode
    elif len(argv) == 3:
        ip = argv[1]
        port = int(argv[2])
        sock = socket(AF_INET, SOCK_STREAM)

        try:
            sock.connect((ip, port))
            print(f"Connected to {ip}:{port}")
            # You can later replace this with DHT join logic or messaging
            recv(sock)

        except Exception as e:
            print(f"Failed to connect to {ip}:{port} - {e}")


    else:
        print("Usage:")
        print("  python file.py              # Start new DHT server")
        print("  python file.py <IP> <PORT>  # Join existing DHT at IP:PORT")
