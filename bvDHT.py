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

def send_get(peer, key):
    peer.send(("GET\n").encode())
    peer.send((f"{key}\n").encode())
    ack = getLine(peer)
    if ack == "0":
        return "NULL"
    length = int(getLine(peer))
    data = getLine(peer)

    return data


def send_insert(peer, key, value):
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


def send_remove(peer, key):
    peer.send(("REMOVE\n"))
    peer.send((f"{key}\n").encode())
    ack = getLine(peer)
    if ack == "0":
        return False
    ack = getLine(peer)
    if ack == "0":
        return False
    return True


def send_contains(peer, key):
    peer.send(("CONTAINS\n"))
    peer.send((f"{key}\n").encode())
    ack = getLine(peer)
    if ack == "0":
        return False
    ack = getLine(peer)
    if ack == "0":
        return False
    return True


def send_locate(peer, key):
    peer.send(("LOCATE\n").encode())
    peer.send((f'{key}\n').encode())
    address = getLine(peer)
    return address


def send_connect(peer, address_key, finger_table):
    peer.send(("CONNECT\n").encode())
    peer.send((f'{address_key}\n').encode())
    num_entries = int(getLine(peer))
    while num_entries != 0:
        key = getLine(peer)
        # length = int(getLine(peer))
        value = getLine(peer)
        hashTable[key] = value
        num_entries -= 1
    next_peer = getLine(peer)
    finger_table["next"] = next_peer
    send_update_prev(peer, next_peer)
    peer.send((f"{finger_table['self']}\n").encode())


def send_disconnect(peer, address_key):
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


def send_update_prev(next, self_key):
    next.send(("UPDATE_PREV\n").encode())
    next.send((f"{self_key}\n").encode())
    ack = getLine(next)
    if ack == '0':
        return False
    return True


def recv_get(conn):
    key = getLine(conn)

    # acknowledge ownership of the hashed space
    if key in hashTable.keys():
        conn.send((f"1\n").encode())
    else:
        conn.send((f"0\n").encode())
    # try to get the length of the value at the key, but it might be zero throwing an error.
    try:
        length = len(hashTable[key])
    except KeyError:
        length = 0

    conn.send((f"{length}\n").encode())
    conn.send((f"{hashTable[key]}").encode())


def recv_locate(conn):
    key = getLine(conn)
    # change this to iterate through the fingers with their keys
    valid_fingers = ['finger 1', 'finger 2', 'finger 3', 'finger 4']
    for finger in valid_fingers:
        if finger in hashTable.keys():
            f_addr = fingertable[finger]
            try:
                found_addr = send_locate(f_addr, key)
                conn.send((str(found_addr) + "\n").encode())
            except Exception as e:
                print("there was an error running recv_locate")
                print(e)

def recv_contains(conn):
    key = getLine(conn)
    # if you own the space or that key ack(1) if not ack(0)
    if key in hashTable.keys():
        conn.send((f"1\n").encode())

        # if you have entry ack(1) if not ack(0)
        if hashTable[key] is not None:
            conn.send((f"1\n").encode())
        else:
            conn.send((f"0\n").encode())
    # if its not even in the space send both negative acknowledgments
    else:
        conn.send((f"0\n").encode())
        conn.send((f"0\n").encode())

def recv_insert(conn):
    key = getLine(conn)

    # ack if the key is owned by you
    if key in hashTable.keys():
        conn.send((f"1\n").encode())
        try:
            # try to recieve all the proper data to insert and acknowledge
            len_key = getLine(conn)
            key = getLine(socket)
            data = conn.recv(len_key.decode())
            hashTable[key] = data
            conn.send((f"1\n").encode())
        except KeyError:
            print("there was an error running recv_insert")
            conn.send((f"0\n").encode())
    else:
        conn.send((f"0\n").encode())

def recv_remove(conn):
    key = getLine(conn)

    # if the key is within our table (we own it) thn remove it
    if key in hashTable.keys():
        hashTable.pop(key)
        conn.send((f"1\n").encode())
    else:
        print("not our owned space so we couldn't remove it")
        conn.send((f"0\n").encode())

def recv_connect(peer):
    key = getLine(peer)
    peer.send(("1\n").encode())
    # need to check which ones to send. More on that later...
    for entry in hashTable:
        # check if meets criterea here
        peer.send((str(entry) + "\n").encode())
        peer.send((str(len(hashTable[entry])) + "\n").encode())
        peer.send((str(hashTable[entry]) + "\n").encode())
    peer.send((str(fingertable["next"]) + "\n").encode())
    new_friend = getLine(peer)
    # add to finger table here... somehow.


def recv_disconnect(peer):
    next_key = getLine(peer)
    num_entries = int(getLine(peer))
    while num_entries != 0:
        key = getLine(peer)
        length = int(getLine(peer))
        data = getLine(peer)
        # Shove these into the hash table. working on that.
    updated = send_update_prev(fingertable["next"])
    if updated:
        peer.send(("1\n").encode())
        return
    peer.send(("0\n").encode())
    return


def recv_update_prev(conn):
    # try to recieve the peer's key and set it within your fingertable
    try:
        key = getLine(conn)
        fingertable["prev"] = key
        conn.send((f"1\n").encode())
    except KeyError:
        conn.send((f"0\n").encode())


# Helper functions
def handle_messages(socket):
    global running

    # while we are still connected to the DHT
    while running:
        try:
            # recieve a message/command
            print("Enter Command: ")
            str_msg = getLine(socket).strip()

            # disconnect if you haven't recieved one
            if not str_msg:
                print("Disconnected")
                break

            # from the list of available commands call the corresponding function
            if str_msg in Valid_commands:
                if str_msg == "LOCATE":
                    print("recieved locate command...")
                    recv_locate(socket)

                elif str_msg == "CONTAINS":
                    print("recieved contains command...")
                    recv_contains(socket)

                elif str_msg == "GET":
                    print("recieved get command...")
                    recv_get(socket)

                elif str_msg == "INSERT":
                    print("recieved insert command...")
                    recv_insert(socket)

                elif str_msg == "REMOVE":
                    print("recieved remove command...")
                    recv_remove(socket)

                elif str_msg == "UPDATE_PREV":
                    print("received update_prev command...")
                    recv_update_prev(socket)

                elif str_msg == "exit":
                    return
            else:
                print("recieved unknown command, or data is being recieved elsewhere")

        except Exception as e:
            print('Error', e)
            print("Disconnected")
            running = False
            break
def accept_loop():
    while True:
        conn, addr = server_sock.accept()
        print(f"Accepted connection from {addr}")
        threading.Thread(target=handle_messages, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    if len(argv) == 1:
        # Step 1: CONNECT to existing peer

        sock = socket(AF_INET, SOCK_STREAM)

        server_sock = socket(AF_INET, SOCK_STREAM)
        server_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        server_sock.bind(('', 8008))
        server_sock.listen(5)
        print("Now listening on port 8008 for new peers...")

        try:
            client_sock = socket(AF_INET, SOCK_STREAM)
            client_sock.connect(('', 8008))
            client_sock.send(("Connected to self\n").encode())

            # Optional: start handler for that connection
            threading.Thread(target=handle_messages, args=(client_sock,), daemon=True).start()

        except Exception as e:
            print(f"Failed to connect to DHT peer: {e}")
            exit(1)

        threading.Thread(target=accept_loop, daemon=True).start()

        try:
            while True:
                sleep(1)
        except KeyboardInterrupt:
            print("\nKeyboard interrupt received. Shutting down.")
            server_sock.close()

    elif len(argv) == 3:
        peer_ip = argv[1]
        peer_port = int(argv[2])

        # Step 1: CONNECT to existing peer
        try:
            client_sock = socket(AF_INET, SOCK_STREAM)
            client_sock.connect((peer_ip, peer_port))
            client_sock.send(("Connected to you\n").encode())
            print(f"Connected to DHT peer at {peer_ip}:{peer_port}")

            # Optional: start handler for that connection
            threading.Thread(target=handle_messages, args=(client_sock,), daemon=True).start()

        except Exception as e:
            print(f"Failed to connect to DHT peer: {e}")
            exit(1)

        # Step 2: Start SERVER to accept new peers
        server_sock = socket(AF_INET, SOCK_STREAM)
        server_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        server_sock.bind(('', 8008))  # or use a different port if needed
        server_sock.listen(2)
        print("Now listening on port 8008 for new peers...")


        def accept_loop():
            while True:
                conn, addr = server_sock.accept()
                print(f"Accepted connection from {addr}")
                threading.Thread(target=handle_messages, args=(conn,), daemon=True).start()


        threading.Thread(target=accept_loop, daemon=True).start()

        try:
            while True:
                sleep(1)
        except KeyboardInterrupt:
            print("\nKeyboard interrupt received. Shutting down.")
            server_sock.close()


    else:
        print("Usage:")
        print("  python file.py              # Start new DHT server")
        print("  python file.py <IP> <PORT>  # Join existing DHT at IP:PORT")
