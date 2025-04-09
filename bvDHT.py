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
            table[f"finger{i}"] = "Test"
    table["self"] = self

    return table


# The all-powerful hash table for this project. 
hashTable = {}

# This is almost like a bag of holding, stuff can go in, but to pull it you you need to know what the value is. 


if __name__ == "__main__":
    sock = socket(AF_INET, SOCK_STREAM)
    sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    # If the length of argv is one (just the program), then we launch as the initial connection
    if len(argv) == 1:
        # server_start()
        sock.bind(('', 8008))
        self = sock.getsockname()[0]
        hashTable = fingerTableSetup(hash(self), True)
        print(hashTable)
    # Otherwise, we connect to a peer.
    elif len(argv) > 1:
        # connect_to_system()
        hashTable = fingerTableSetup(hash(argv[1]), False)
        print(hashTable)