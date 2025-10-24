import socket
import threading
import sys
# Note: For a real package, you would import with '.command_executor', 
# but for a flat directory, the import might need adjustment.
from app.command_execution import handle_connection
import app.command_execution as ce

PING_COMMAND_RESP = b"*1\r\n$4\r\nPING\r\n"
REPLCONF_CAPA_PSYNC2 = b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
PSYNC_COMMAND_RESP = b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"

# In main.py, define this new function after connect_to_master or at the top level
def replica_command_listener(master_socket: socket.socket):
    """Listens on the master-replica connection for propagated commands."""
    while True:
        try:
            # Propagated commands are RESP arrays.
            # We pass the master_socket as the 'client' to handle_command.
            data = master_socket.recv(4096)
            if not data:
                print("Replication: Master closed connection.")
                break

            print(f"Replica: Received propagated data from master: {data!r}")
            
            # Use a buffer to handle concatenated commands
            buffer = data
            while buffer:
                # Use the updated parser which returns (parsed_command, bytes_consumed)
                parsed_command, bytes_consumed = ce.parsed_resp_array(buffer)
                
                if not parsed_command:
                    # Case 1: The RDB response. The command parser (for '*') will fail here.
                    # If data is not an array, it's the RDB payload (+FULLRESYNC, $LENGTH).
                    if buffer.startswith(b'+') or buffer.startswith(b'$'):
                        print("Replica: Ignoring master handshake response (RDB payload).")
                        buffer = b'' # Consume and discard the remaining buffer (RDB payload)
                        break # Exit inner while loop
                    
                    # Case 2: Incomplete command or other error.
                    print(f"Replica: Could not parse propagated command. Skipping remaining buffer: {buffer!r}")
                    break 

                command = parsed_command[0].upper()
                arguments = parsed_command[1:]
                
                print(f"Command: Parsed command: {command}, Arguments: {arguments}")
                
                # Delegate to handle_command. The logic inside handle_command must suppress the response.
                ce.handle_command(command, arguments, master_socket)

                # Move to the next command in the buffer
                buffer = buffer[bytes_consumed:]

        except Exception as e:
            print(f"Replication Listener Error: {e}")
            break
        
def read_simple_string_response(sock: socket.socket, expected: bytes):
    """
    Reads a response from the master and verifies it against the expected simple string.
    """
    response = sock.recv(1024) # Read a small buffer
    if not response or not response.startswith(b"+"):
        print(f"Replication Error: Master sent unexpected response: {response!r}")
        return False
    
    # Simple check for "+OK\r\n"
    if response.strip() == expected.strip():
        print(f"Replication: Received expected response: {response!r}")
        return True
    
    # If the response is larger than expected, or different
    print(f"Replication Error: Received response {response!r} did not match expected {expected!r}")
    return False

def connect_to_master(listening_port: int) -> socket.socket | None:
    """
    Called when the server is a replica. It connects to the master and performs 
    the first handshake step (PING).
    """
    master_host = ce.MASTER_HOST
    master_port = ce.MASTER_PORT
    master_socket = None

    if not master_host or not master_port:
        print("Replication Error: Master host or port not configured for replica.")
        return

    print(f"Replication: Connecting to master at {master_host}:{master_port}...")
    
    try:
        # Create a new socket for the replica-master connection
        master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_socket.connect((master_host, master_port))
        
        # ----------------------------------------------------
        # Handshake Step 1: PING
        # ----------------------------------------------------
        master_socket.sendall(PING_COMMAND_RESP)
        if not read_simple_string_response(master_socket, b"+PONG\r\n"): # PING expects +PONG
            return
        
        # ----------------------------------------------------
        # Handshake Step 2: REPLCONF listening-port <PORT>
        # ----------------------------------------------------
        port_str = str(listening_port)
        # *3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$LEN\r\n<PORT>\r\n
        replconf_listening_port = (
            b"*3\r\n" +
            b"$8\r\nREPLCONF\r\n" +
            b"$14\r\nlistening-port\r\n" +
            b"$" + str(len(port_str)).encode() + b"\r\n" +
            port_str.encode() + b"\r\n"
        )

        master_socket.sendall(replconf_listening_port)
        if not read_simple_string_response(master_socket, b"+OK\r\n"): # REPLCONF expects +OK
            return

        # ----------------------------------------------------
        # Handshake Step 3 (2nd REPLCONF): capa psync2
        # ----------------------------------------------------
        master_socket.sendall(REPLCONF_CAPA_PSYNC2)
        if not read_simple_string_response(master_socket, b"+OK\r\n"): # REPLCONF expects +OK
            return

        # ----------------------------------------------------
        # Handshake Step 4: PSYNC ? -1 (Ignore response for now)
        # ----------------------------------------------------
        print("Replication: Sending PSYNC ? -1...")
        master_socket.sendall(PSYNC_COMMAND_RESP)
        
        # We are instructed to ignore the master's response (+FULLRESYNC...) for this stage.
        # We'll handle reading the response in a later stage.

        print("Replication: Handshake steps 1, 2, & 3 complete (PSYNC sent).")
        
        # Store the socket for later use
        ce.MASTER_SOCKET = master_socket

        return master_socket
        
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

        master_socket = connect_to_master(port)

    # If connect_to_master succeeded, start the listener thread
    if master_socket:
        # Start listener thread (daemon=True ensures thread exits when main program exits)
        threading.Thread(target=replica_command_listener, args=(master_socket,), daemon=True).start()
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