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
    i=0
    while i < len(args):
        if args[i] == "--port":
            try:
                # Use the value after --port
                master_host = args[i + 1]
                master_port = int(args[i + 2])
                is_replica = True
                i += 3
            except (IndexError, ValueError):
                print("Server Error: Invalid or missing port number after --port.")
                return
        # Add a placeholder for other args to be passed to command_execution
        elif args[i] == "--dir" or args[i] == "--dbfilename":
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