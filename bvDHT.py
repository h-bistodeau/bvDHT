# This is just some preliminary work for the DHT based on what we came up with/what I remember from class.
from sys import argv
from socket import *
import threading

# Application is launched through argv. Lack of arguments indicates this will be the server. If there are arguments, then we are
# connecting to a peer. 

# Focusing on starting the server. 

# Don't need a class for the finger table, just a dictionary. Key is whatever it is, value should be the peer address.
def fingerTableSetup(self, startup = True):
    table = {}
    if startup:
        for i in range(1,5):
            table[f"finger{i}"] = self
        table["next"] = self
        table["prev"] = self
    else:
        for i in range(1,5):
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
Valid_commands = ["LOCATE", "CONNECT", "DISCONNECT", "CONTAINS","GET", "INSERT", "REMOVE", "UPDATE_PREV"]
running = True

hashTable = {}

# Our space in the system is determined by our distance between us and next.
fingertable = {}
# This is almost like a bag of holding, stuff can go in, but to pull it you you need to know what the value is. 

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


def insert(peer, key):
    peer.send(("INSERT\n").encode())
    peer.send((f"{key}\n").encode())
    ack = getLine(peer)
    if ack == "0":
        return False
    peer.send((f"{len(key)}\n").encode())
    peer.send((f"{key}\n").encode())
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
    peer.send((f"{finger_table["self"]}\n").encode())


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
            msg = getLine(socket)

            # disconnect if you haven't recieved one
            if not msg:
                print("Disconnected")
                break

            str_msg = msg.strip() # added .strip() just in case
            if str_msg in Valid_commands:
                if str_msg == "LOCATE":
                    print("recieved locate command.... Now running locate")

                elif str_msg == "CONTAINS":
                    print("recieved contains command.... Now running contains")

                elif str_msg == "GET":
                    print("recieved get command.... Now running get")

                elif str_msg == "INSERT":
                    print("recieved insert command.... Now running insert")

                elif str_msg == "REMOVE":
                    print("recieved remove command.... Now running remove")
                
                elif str_msg == "UPDATE_PREV":
                    print("received update_prev command... now running update_prev")

            else:
                print("recieved unknown command, or data is being recieved elsewhere")
        except Exception as e:
            print('Error', e)
            print("Disconnected")
            running = False
            break



if __name__ == "__main__":
    sock = socket(AF_INET, SOCK_STREAM)
    sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    # If the length of argv is one (just the program), then we launch as the initial connection
    if len(argv) == 1:
        # server_start()
        sock.bind(('', 8008))
        # decided to use single thread and deal with deadlock for now. 
        # If time allows, expand to multithreading.
        sock.listen(1)

        #connetion to peer is initiated
        conn, addr = sock.accept()
        print("Connection by", addr)

        self = sock.getsockname()[0]
        fingertable = fingerTableSetup(hash(self), True)
        print(fingertable)
 
        threading.Thread(target=handle_messages, args=(conn,), daemon=True).start()

    # Otherwise, we connect to a peer.
    elif len(argv) > 1:
        # connect_to_system()
        fingertable = fingerTableSetup(hash(argv[1]), False)
        print(fingertable)

        threading.Thread(target=handle_messages, args=(sock,), daemon=True).start()

        #super duper bare bones send commands loop (literally no cool features just the starting point)   
    while running:
        command = input("Enter command: ")
        if command not in Valid_commands:
            print("Invalid command")
            pass
        
        sock.send(command.encode())