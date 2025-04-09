# This is just some preliminary work for the DHT based on what we came up with/what I remember from class.
from sys import argv
from socket import *
import threading

Valid_commands = ["locate", "connect", "disconnect", "contains","get", "insert", "remove", "updatePrev"]
hashTable = {} # The all-powerful hash table for this project.
running = True

# Application is launched through argv. Lack of arguments indicates this will be the server. If there are arguments, then we are
# connecting to a peer.

# Focusing on starting the server.

# Don't need a class for the finger table, just a dictionary. Key is whatever it is, value should be the peer address.
def fingerTableSetup(self_id, startup = True):
    table = {}
    if startup:
        for i in range(1,5):
            table[f"finger{i}"] = self_id
        table["next"] = self_id
        table["prev"] = self_id
    else:
        for i in range(1,5):
            table[f"finger{i}"] = "Test"
    table["self"] = self_id

    return table

def handle_messages(socket):
    global running

    # while we are still connected to the DHT
    while running:
        try:
            # recieve a message/command
            msg = socket.recv(1024)

            # disconnect if you haven't recieved one
            if not msg:
                print("Disconnected")
                break

            str_msg = msg.decode().strip() # added .strip() just in case
            if str_msg in Valid_commands:
                if str_msg == "locate":
                    print("recieved locate command.... Now running locate")

                elif str_msg == "contains":
                    print("recieved contains command.... Now running contains")

                elif str_msg == "get":
                    print("recieved get command.... Now running get")

                elif str_msg == "insert":
                    print("recieved insert command.... Now running insert")

                elif str_msg == "remove":
                    print("recieved remove command.... Now running remove")
                elif str_msg == "updatePrev":
                    print("recieved updatePrev command.... Now running updatePrev")

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
        # we are acting as the server in some regard and start everything up
        # server_start()
        sock.bind(('', 8008))
        sock.listen(1)

        #connetion to peer is initiated
        conn, addr = sock.accept()
        print("Connection by", addr)

        self_id = sock.getsockname()[0]
        hashTable = fingerTableSetup(hash(self_id), True)
        print(hashTable)

        #create thread to handle incoming msg/command
        active_sock = conn
        threading.Thread(target=handle_messages, args=(conn,), daemon=True).start()

    # Otherwise, we connect to a peer.
    else:
        # this implies that the server has already been started and we connect to a peer within the DHT
        # connect_to_system()
        peerIP = argv[1]
        sock.connect((peerIP, 8008))

        self_id = sock.getsockname()[0]
        hashTable = fingerTableSetup(hash(self_id), False)
        print(hashTable)

        active_sock = sock
        threading.Thread(target=handle_messages, args=(sock,), daemon=True).start()

    #super duper bare bones send commands loop (literally no cool features just the starting point)
    while running:
        command = input("Enter command: ")
        if command not in Valid_commands:
            print("Invalid command")
            continue

        try:
            #attempts to send command to the peer that was initialized prior to this
            active_sock.send(command.encode())

        except KeyboardInterrupt:
            print("shutting down the program")
            running = False

        except Exception as e:
            print("there was an error, thats probably on me")
