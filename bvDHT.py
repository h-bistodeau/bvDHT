import hashlib
from sys import argv
from socket import *
import threading
from time import sleep

# Application is launched through argv. Lack of arguments indicates this will be the server. If there are arguments, then we are
# connecting to a peer.

# The all-powerful hash table for this project.
Valid_commands = ["LOCATE", "CONNECT", "DISCONNECT", "CONTAINS", "GET", "INSERT", "REMOVE", "UPDATE_PREV"]
running = True
hashTable = {}
fingertable = {}


# local functions

# determine if we have the key to begin with.chatgpt
def local_locate(key):
    if key in hashTable.keys():
        return '1'
    return '0'


# determine if we have ownership of the space the key is in.
# run through our fingers. get the closest one and return it.
def determine_ownership(key_hash):
    key = int(key_hash)
    self_id = getHashIndex(fingertable["self"])
    pred_id = getHashIndex(fingertable["prev"])

    # If key is between (pred_id, self_id]
    if pred_id < self_id:
        if pred_id < key <= self_id:
            return fingertable["self"]
    else:
        # Wrap-around case
        if key > pred_id or key <= self_id:
            return fingertable["self"]

    # Otherwise, not our responsibility — route to best finger
    closest = fingertable["finger1"]
    closest_dist = (key - getHashIndex(closest)) % (2 ** 160)

    for i in range(2, 5):  # finger2 to finger4
        finger = fingertable[f"finger{i}"]
        dist = (key - getHashIndex(finger)) % (2 ** 160)
        if 0 < dist < closest_dist:
            closest = finger
            closest_dist = dist

    return closest



# Don't need a class for the finger table, just a dictionary. Key is whatever it is, value should be the peer address.

# ------------------- Finger table functionality----------------------
def fingerTableSetup(self, startup=False, known_ip=None, known_port=None):
    table = {}
    table["self"] = self
    self_hash = getHashIndex(self)

    if startup:
        for i in range(1, 5):
            table[f"finger{i}"] = self
        table["next"] = self
        table["prev"] = self
    else:
        for i in range(1, 5):
            # Step 1: compute (self_hash + 2^(i-1)) % 2^160
            target_hash = (self_hash + 2**(i - 1)) % (2**160)

            # Step 2: ask known peer who owns that hash
            try:
                # Open a socket to a known peer (e.g., one passed into your node at startup)
                with socket(AF_INET, SOCK_STREAM) as s:
                    s.connect((known_ip, known_port))  # You need to pass these in
                    owner_addr = locate(s, False, str(target_hash))

                    # Step 3: Save address into fingertable
                    table[f"finger{i}"] = owner_addr.strip()
            except Exception as e:
                print(f"Failed to find owner for finger{i}: {e}")
                table[f"finger{i}"] = "None"

        # Optional: use finger1 as "next" if no other logic is available
        table["next"] = table["finger1"]
        table["prev"] = "None"  # Will be updated during connect()

    return table



def updateFingerTable(self_addr, known_ip, known_port):
    self_hash = getHashIndex(self_addr)

    for i in range(1, 5):  # If you're using 4 fingers
        target_hash = (self_hash + 2 ** (i - 1)) % (2 ** 160)

        try:
            with socket(AF_INET, SOCK_STREAM) as s:
                s.connect((known_ip, known_port))
                owner_addr = locate(s, False, str(target_hash))

                if owner_addr and owner_addr != "0":
                    fingertable[f"finger{i}"] = owner_addr.strip()
                else:
                    fingertable[f"finger{i}"] = "None"
        except Exception as e:
            print(f"[updateFingerTable] Failed to update finger{i}: {e}")
            fingertable[f"finger{i}"] = "None"

    # Optionally update next based on finger1
    fingertable["next"] = fingertable["finger1"]



def send_peerInfo():
    pass


def receive_peerInfo():
    pass


# --------------------Helper Functions-----------------------
# Returns an integer index into the hash-space for a node Address
#  - addr is of the form ("ipAddress or hostname", portNumber)
#    where the first item is a string and the second is an integer
def getHashIndex(addr):
    b_addrStr = ("%s:%d" % addr).encode()
    return int.from_bytes(hashlib.sha1(b_addrStr).digest(), byteorder="big")


def getLine(conn):
    msg = b''
    while True:
        ch = conn.recv(1)
        msg += ch
        if ch == b'\n' or len(ch) == 0:
            break
    return msg.decode()


def accept_loop():
    while True:
        conn, addr = server_sock.accept()
        print(f"Accepted connection from {addr}")
        threading.Thread(target=handle_messages, args=(conn,), daemon=True).start()


# --------------------------Actual Command Functionality-------------------------
def get(conn, recv, key=None):
    if recv == True:
        key = getLine(conn)
        # acknowledge ownership of the hashed space
        if key in hashTable.keys():
            conn.send((f"1\n").encode())
            value = hashTable[key]
            length = len[value]
            conn.send((f"{length}\n").encode())
            conn.send((f"{hashTable[key]}\n").encode())
        else:
            conn.send((f"0\n").encode())
    else:
        conn.send(("GET\n").encode())
        conn.send((f"{key}\n").encode())
        ack = getLine(conn)
        if ack == "0":
            return "NULL"
        length = int(getLine(conn))
        data = getLine(conn)
        return data


def locate(conn, recv, key=None):
    if recv == True:
        key = getLine(conn).strip()
        valid_fingers = ['finger1', 'finger2', 'finger3', 'finger4']

        for finger in valid_fingers:
            f_addr = fingertable[finger]
            if f_addr == "None":  
                continue
            ip, port = f_addr.split(':')
            try:
                with socket(AF_INET, SOCK_STREAM) as s:
                    s.connect((ip, int(port)))
                    addr_found = locate(s, False, key)
                    if addr_found and addr_found.strip() != "0":
                        conn.send((str(addr_found) + "\n").encode())
                        return
            except Exception as e:
                print(f"Failed to connect to finger {f_addr}: {e}")
                continue  # try next finger

        # fallback: none found
        conn.send(("0\n").encode())
    else:
        conn.send(("LOCATE\n").encode())
        conn.send((f"{key}\n").encode())
        address = getLine(conn)
        return address.strip()


def contains(conn, recv, key=None):
    if recv == True:
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
    else:
        conn.send(("CONTAINS\n"))
        conn.send((f"{key}\n").encode())
        ack = getLine(conn)
        if ack == "0":
            return False
        ack = getLine(conn)
        if ack == "0":
            return False
        return True


def insert(conn, recv, key=None):
    if recv == True:
        key = getLine(conn)
        # ack if the key is owned by you
        if key in hashTable.keys():
            conn.send((f"1\n").encode())
            try:
                # try to recieve all the proper data to insert and acknowledge
                len_key = int(getLine(conn))
                key = getLine(conn)
                data = conn.recv(len_key.decode())
                hashTable[key] = data
                conn.send((f"1\n").encode())
            except Exception as e:
                print(f"there was an error running insert: {e}")
                conn.send((f"0\n").encode())
        else:
            conn.send((f"0\n").encode())
    else:
        conn.send(("INSERT\n").encode())
        conn.send((f"{key}\n").encode())
        ack = getLine(conn)
        if ack == "0":
            return False
        conn.send((f"{len(key)}\n").encode())
        conn.send((f"{key}\n").encode())
        conn.send((f"{hashTable[key]}\n").encode())
        ack = getLine(conn)
        if ack == "0":
            return False
        return True


def remove(conn, recv, key=None):
    if recv == True:
        key = getLine(conn)
        # if the key is within our table (we own it) thn remove it
        if key in hashTable:
            hashTable.pop(key)
            conn.send((f"1\n").encode())
        else:
            print("not our owned space so we couldn't remove it")
            conn.send((f"0\n").encode())
    else:
        conn.send(("REMOVE\n"))
        conn.send((f"{key}\n").encode())
        ack = getLine(conn)
        if ack == "0":
            return False
        return True


def _connect(peer, recv, finger_table, address_key = None):
    if recv == True:
        address_key = getLine(peer)
        peer.send(("1\n").encode())
        # get the number of entries and send it to the peer
        num_entries = len(hashTable)
        peer.send((f"{num_entries}\n").encode())
        # for everything in the hashtable send the key and its corresponding value
        for key, value in hashTable.items():
            peer.send((f"{key}\n").encode())
            peer.send((f"{value}\n").encode())
        # give the peer our next
        peer.send((f"{finger_table['next']}\n").encode())
        # set the new buddy equal to our previous
        new_peer = getLine(peer)
        finger_table['prev'] = new_peer

    else:
        peer.send(("CONNECT\n").encode())
        peer.send((f'{address_key}\n').encode())
        num_entries = int(getLine(peer))
        while num_entries != 0:
            key = getLine(peer)
            value = getLine(peer)
            hashTable[key] = value
            num_entries -= 1
        next_peer = getLine(peer)
        finger_table["next"] = next_peer
        peer.send((f"{finger_table['self']}\n").encode())


def _disconnect(peer, recv, address_key=None):
    if recv == True:
        next_key = getLine(peer)
        num_entries = int(getLine(peer))
        while num_entries != 0:
            key = getLine(peer)
            length = int(getLine(peer))
            data = getLine(peer)
            hashTable[key] = data
            num_entries -= 1

        updated = update_prev(fingertable["next"], False, fingertable["self"])
        if updated:
            peer.send(("1\n").encode())
        else:
            peer.send(("0\n").encode())
        return
    else:
        peer.send(("DISCONNECT\n").encode())
        peer.send((f'{address_key}\n').encode())
        num_files = len(hashTable)
        peer.send((f'{num_files}\n').encode())
        for key in hashTable:
            peer.send((f"{key}\n").encode())
            peer.send((f"{len(hashTable[key])}").encode())
            peer.send((f"{hashTable[key]}\n").encode())
        ack = getLine(peer)
        if ack == "0":
            return False
        return True


def update_prev(conn, recv, self_key):
    if recv == True:
        # try to recieve the peer's key and set it within your fingertable
        try:
            key = getLine(conn)
            fingertable["prev"] = key
            conn.send((f"1\n").encode())
        except KeyError:
            conn.send((f"0\n").encode())
    else:
        next.send(("UPDATE_PREV\n").encode())
        next.send((f"{self_key}\n").encode())
        ack = getLine(next)
        if ack == '0':
            return False
        return True


# One of the main functionality pieces, it takes in a string and runs the corresponding function to whatever command was given
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
                    locate(socket, True)

                elif str_msg == "CONTAINS":
                    print("recieved contains command...")
                    contains(socket, True)

                elif str_msg == "GET":
                    print("recieved get command...")
                    get(socket, True)

                elif str_msg == "INSERT":
                    print("recieved insert command...")
                    insert(socket, True)

                elif str_msg == "REMOVE":
                    print("recieved remove command...")
                    remove(socket, True)

                elif str_msg == "UPDATE_PREV":
                    print("received update_prev command...")
                    update_prev(socket, True)

                elif str_msg == "exit":
                    return
            else:
                print("recieved unknown command, or data is being recieved elsewhere")

        except Exception as e:
            print('Error', e)
            print("Disconnected")
            running = False
            break


def handle_input():
    user_in = input("Enter Command > ").strip().upper()
    words = user_in.split(" ",1)
    command = words[0]
    if command not in Valid_commands:
        print("invalid command was entered please enter a correct one")
        print("Valid Commands are: DISCONNECT, LOCATE, INSERT, REMOVE, UPDATE_PREV")
        return
    if command == "DISCONNECT":
        print("Received disconnect command")
        ip, port = fingertable["prev"].decode().strip().split(":")
        with socket(AF_INET, SOCK_STREAM) as conn:
            conn.connect(ip, port)
            _disconnect(conn, False, fingertable["self"])
            return
    elif command == "LOCATE":
        if len(words) == 2:
            print("Received Locate command")
            locate(peer_socket, False, words[1]) # calls locate function on the sending side with the key givec
        else:
            print('invalid syntax. correct syntax is LOCATE <key>')
            return
    elif command == "INSERT":
        if len(words) == 2:
            print("Received Insert command")
            insert(peer_socket, False, words[1])
        else:
            print('invalid syntax. correct syntax is INSERT <key>')
            return
    elif command == "REMOVE":
        if len(words) == 2:
            print("Received Remove command")
            remove(peer_socket, False, words[1])
        else:
            print('invalid syntax. correct syntax is REMOVE <key>')
    elif command == "CONTAINS":
        if len(words) == 2:
            print("Received Contains command")
            contains(peer_socket, False, words[1])
        else:
            print('invalid syntax. correct syntax is CONTAINS <key>')
    elif command == "GET":
        if len(words) == 2:
            print("Received Get command")
            get(peer_socket, False, words[1])
        else:
            print('invalid syntax. correct syntax is GET <key>')
    elif command == "UPDATE_PREV":
        if len(words) == 2:
            print("Received Update Prev command")
            update_prev(peer_socket, False, words[1])
        else:
            print('invalid syntax. correct syntax is UPDATE_PREV <self_key>')

if __name__ == "__main__":
    global peer_socket

    if len(argv) == 1:
        server_sock = socket(AF_INET, SOCK_STREAM)
        server_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        server_sock.bind(('', 8008))
        server_sock.listen(5)

        self_addr = "127.0.0.1:8008"
        fingertable = fingerTableSetup(self_addr, startup=True)

        print("Now listening on port 8008 for new peers...")

        try:
            client_sock = socket(AF_INET, SOCK_STREAM)
            client_sock.connect(('127.0.0.1', 8008))
            peer_socket = client_sock
            threading.Thread(target=handle_messages, args=(client_sock,), daemon=True).start()
        except Exception as e:
            print(f"Failed to create self-connection: {e}")
            exit(1)

        threading.Thread(target=accept_loop, daemon=True).start()

        try:
            while running:
                handle_input()
        except KeyboardInterrupt:
            print("\nShutting down.")
            server_sock.close()

    elif len(argv) >= 3:
        peer_ip = argv[1]
        peer_port = int(argv[2])
        my_port = int(argv[3]) if len(argv) > 3 else 8008
        self_addr = f"127.0.0.1:{my_port}"

        try:
            client_sock = socket(AF_INET, SOCK_STREAM)
            client_sock.connect((peer_ip, peer_port))
            peer_socket = client_sock

            fingertable = fingerTableSetup(self_addr, startup=False, known_ip=peer_ip, known_port=peer_port)

            updateFingerTable(self_addr, peer_ip, peer_port)

            client_sock.send(("Connected to you\n").encode())
            threading.Thread(target=handle_messages, args=(client_sock,), daemon=True).start()
            print(f"Connected to peer at {peer_ip}:{peer_port}")
        except Exception as e:
            print(f"Failed to connect to DHT peer: {e}")
            exit(1)

        server_sock = socket(AF_INET, SOCK_STREAM)
        server_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        server_sock.bind(('', my_port))
        server_sock.listen(2)
        print(f"Now listening on port {my_port} for new peers...")
        threading.Thread(target=accept_loop, daemon=True).start()

        try:
            while running:
                handle_input()
        except KeyboardInterrupt:
            print("\nShutting down.")
            server_sock.close()

    else:
        print("Usage:")
        print("  python file.py                  # Start a new DHT")
        print("  python file.py <IP> <PORT>      # Join existing DHT at IP:PORT")
