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
            # Check for value presence and validity
            if i + 1 >= len(args):
                print("Server Error: Missing port number after --port.")
                return
            try:
                port = int(args[i + 1])
                i += 2 # Consume --port and port value
            except ValueError:
                print("Server Error: Port value is not an integer.")
                return
        
        elif arg == "--replicaof":
            # Check for host and port presence
            if i + 2 >= len(args):
                print("Server Error: Missing master host or port after --replicaof.")
                return
            try:
                master_host = args[i + 1]
                # The port is expected to be a string that can be cast to int
                master_port = int(args[i + 2])
                is_replica = True
                i += 3 # Consume --replicaof, master_host, master_port
            except ValueError:
                print("Server Error: Master port value is not an integer.")
                return
            
        elif arg == "--dir" or arg == "--dbfilename":
            # Consuming other flags that are handled by command_execution's module-level init
            if i + 1 >= len(args):
                print(f"Server Error: Missing value for {arg}.")
                return
            i += 2
            
        else:
            # Skip any unrecognized single argument or a value from a previous argument
            # This handles the complex case where arguments might be interleaved/unrecognized.
            # However, for the given set of flags, all arguments should be consumed by the above logic.
            # If we hit this, it's likely an error, but we'll increment by 1 to proceed defensively.
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