import socket
import threading
import sys
# Note: For a real package, you would import with '.command_executor', 
# but for a flat directory, the import might need adjustment.
from app.command_execution import handle_connection
import app.command_execution as ce

PING_COMMAND_RESP = b"*1\r\n$4\r\nPING\r\n"

def connect_to_master():
    """
    Called when the server is a replica. It connects to the master and performs 
    the first handshake step (PING).
    """
    master_host = ce.MASTER_HOST
    master_port = ce.MASTER_PORT

    if not master_host or not master_port:
        print("Replication Error: Master host or port not configured for replica.")
        return

    print(f"Replication: Connecting to master at {master_host}:{master_port}...")
    
    try:
        # Create a new socket for the replica-master connection
        master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_socket.connect((master_host, master_port))
        
        print(f"Replication: Connected to master. Sending PING...")
        
        # --- Handshake Step 1: Send PING ---
        master_socket.sendall(PING_COMMAND_RESP)
        print(f"Replication: Sent PING: {PING_COMMAND_RESP!r}")

        # The replica must now wait for the master's response (PONG).
        # We'll read the response in the next stage. For now, just send the PING.
        # Store the socket for later use in subsequent stages
        ce.MASTER_SOCKET = master_socket
        
    except Exception as e:
        print(f"Replication Error: Could not connect to master or send PING: {e}")
        # Note: Do not exit main thread here, as the server must still listen for client connections

def main():

    # --- ADDED: Argument Parsing for Port ---
    port = 6379 # Default Redis port
    args = sys.argv[1:]

    is_replica = False
    master_host = None
    master_port = None
    
    # Simple argument parsing loop
    i = 0
    while i < len(args):
        arg = args[i]
        
        if arg == "--port":
            # ... (Existing --port logic is fine)
            if i + 1 >= len(args):
                print("Server Error: Missing port number after --port.")
                return
            try:
                port = int(args[i + 1])
                i += 2
            except ValueError:
                print("Server Error: Port value is not an integer.")
                return
        
        elif arg == "--replicaof":
            # --- CORRECTION APPLIED HERE ---
            if i + 1 >= len(args):
                print("Server Error: Missing argument after --replicaof.")
                return
            
            # The value is expected to be a single string "host port"
            replicaof_value = args[i + 1]
            parts = replicaof_value.split()
            
            if len(parts) != 2:
                print("Server Error: --replicaof value must be 'host port'.")
                return

            try:
                master_host = parts[0]
                master_port = int(parts[1]) # Cast the port part to int
                is_replica = True
                i += 2 # Consume --replicaof and its single string value
            except ValueError:
                print("Server Error: Master port value is not an integer.")
                return
            # --- END CORRECTION ---
            
        elif arg == "--dir" or arg == "--dbfilename":
            # Consuming other flags
            if i + 1 >= len(args):
                print(f"Server Error: Missing value for {arg}.")
                return
            i += 2
            
        else:
            i += 1

    if is_replica:
        ce.SERVER_ROLE = "slave"
        ce.MASTER_HOST = master_host
        ce.MASTER_PORT = master_port

        connect_to_master()
    # ----------------------------------------

    try:
        # This tells the operating system to create a listening point at $\texttt{localhost}$ (your computer) on port $\texttt{6379}$ (the standard Redis port).

        
        server_socket = socket.create_server(("localhost", port), reuse_port=True)
        print(f"Server: Starting server on localhost:{port}...")
        print("Server: Listening for connections...")
    except OSError as e:
        print(f"Server Error: Could not start server: {e}")
        return

    # The server is now waiting patiently for a customer (client) to walk in.
    while True:
        try:
            # When you run $\texttt{redis-cli}$, the server blocks here until that client connects. Once connected, it gets a new, dedicated connection socket {connection} and the client's address (client address}.
            connection, client_address = server_socket.accept()

            # To handle multiple clients simultaneously, the server hands the connection off to a new thread
            threading.Thread(target=handle_connection, args=(connection, client_address)).start()
        except Exception as e:
            print(f"Server Error: Exception during connection acceptance or thread creation: {e}")
            break

if __name__ == "__main__":
    main()