import socket
import threading
import sys
# Note: For a real package, you would import with '.command_executor', 
# but for a flat directory, the import might need adjustment.
from app.command_execution import handle_connection
import app.command_execution as ce


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