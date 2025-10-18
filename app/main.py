import socket  # noqa: F401
import threading
import time

DATA_STORE = {}  # In-memory key-value store

# Example Input: data = b'*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n'
def parsed_resp_array(data: bytes) -> list[str]:
    if not data or not data.startswith(b"*"):
        # print("Parser: Data is empty or does not start with *") # Can be noisy
        return []
    
    try:

        # Find the first CRLF to get the number of elements
        crlf_index = data.find(b"\r\n")
        if crlf_index == -1:
            # print("Parser: Missing initial CRLF")
            return []
        
        # count_bytes is bytes between * and \r\n (b'2' for example)
        count_bytes = data[1:crlf_index]
        if not count_bytes:
             print("Parser Error: No element count found.")
             return []

        # decode to string and convert to int so now it is 2 for example
        num_elements_str = count_bytes.decode()
        num_elements = int(num_elements_str)

    except ValueError:
        print(f"Parser Error: Invalid element count value: {data[1:crlf_index]}")
        return []
    

    parsed_elements = []
    # Move index to the start of the first element (past the initial CRLF (\r\n))
    index = crlf_index + 2
    
    print(f"Parser: Expecting {num_elements} elements.")
    
    for i in range(num_elements):

        # Confirms data at index is b"$"
        if index >= len(data) or data[index: index + 1] != b"$":
            print(f"Parser Error: Element {i} not starting with $ at index {index}.")
            return []
        
        index += 1 # Skip $

        # This next finds next \r\n to get length of string. Find takes index as second arg to start searching from there. Returns index of \r\n
        crlf_index = data.find(b"\r\n", index)
        if crlf_index == -1:
            print(f"Parser Error: Element {i} missing length CRLF.")
            return []

        # length_bytes is bytes between $ and \r\n. This is '`4` for example'
        try:
            length_bytes = data[index:crlf_index]
            str_length = int(length_bytes.decode())
            print(f"Parser: Element {i} length is {str_length}.")
        except ValueError:
            print(f"Parser Error: Element {i} invalid length value: {length_bytes}")
            return []
        
        index = crlf_index + 2 # Skip length and \r\n

        # Extract value. This is b'ECHO' for example
        value_end_index = index + str_length
        if value_end_index + 2 > len(data): # +2 for trailing \r\n
            print(f"Parser Error: Element {i} incomplete data or missing trailing CRLF.")
            return []
        
        # Decode and append value
        value = data[index:value_end_index].decode()
        parsed_elements.append(value)
        print(f"Parser: Element {i} value: '{value}'")
        
        index = value_end_index + 2  # Skip value and \r\n
        
    return parsed_elements

# --------------------------------------------------------------------------------

def handle_connection(client: socket.socket, client_address):
    """
    This function is called for each new client connection.
    """
    print(f"Connection: New connection from {client_address}")
    # Get the client's address for logging
    with client: 
        while True:
            # The thread waits for the client to send a command.When you run {redis-cli ECHO hey}, the server receives the raw RESP bytes: data = b'*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n'
            data = client.recv(4096) 
            if not data:
                print(f"Connection: Client {client_address} closed connection.")
                break
                
            print(f"Received: Raw bytes from {client_address}: {data!r}")

            # The raw bytes are immediately sent to the parser to be translated into a usable Python list.
            parsed_command = parsed_resp_array(data) # Ex: ['PING'] or ['ECHO', 'Hello']
            
            if not parsed_command:
                # Handle case where parser fails but connection is still open
                print(f"Received: Could not parse command from {client_address}. Closing connection.")
                break

            command = parsed_command[0].upper()
            arguments = parsed_command[1:]
            
            print(f"Command: Parsed command: {command}, Arguments: {arguments}")

            if command == "PING":
                response = b"+PONG\r\n"
                client.sendall(response)
                print(f"Sent: PONG to {client_address}.")
            elif command == "ECHO":

                # msg_str is like 'Hey' and we must convert back to RESP bulk string. 
                msg_str = arguments[0]

                # encode back to bytes
                msg_bytes = msg_str.encode() 

                # grab length of msg_bytes and construct RESP bulk string
                length_bytes = str(len(msg_bytes)).encode()

                # b"$3\r\nhey\r\n"
                response = b"$" + length_bytes + b"\r\n" + msg_bytes + b"\r\n"
                
                client.sendall(response)
                print(f"Sent: ECHO response '{msg_str}' to {client_address}.")
            elif command == "SET":

                if len(arguments) < 2:
                    response = b"-ERR wrong number of arguments for 'set' command\r\n"
                    client.sendall(response)
                    print(f"Sent: SET argument error to {client_address}.")
                    continue # Go back to listening for more data
                
                key = arguments[0]
                value = arguments[1]

                expiry_time = None
                i = 2
                while i < len(arguments):
                    option = arguments[i].upper()

                    try:

                        if option == "EX":
                            expiry_time = int(arguments[i + 1]) * 1000  # Convert seconds to milliseconds
                            i += 2
                        elif option == "PX":
                            expiry_time = int(arguments[i + 1])
                            i += 2
                        else:
                            i += 1
                    except (IndexError, ValueError):
                        response = b"-ERR syntax error\r\n"
                        client.sendall(response)
                        print(f"Sent: SET syntax error to {client_address}.")
                        continue
                

                current_time = int(time.time() * 1000)

                expiry_timestamp = current_time + expiry_time if expiry_time is not None else None

                DATA_STORE[key] = {
                    "value": value,
                    "expiry": expiry_timestamp
                }
                response = b"+OK\r\n"
                client.sendall(response)
                print(f"Sent: OK to {client_address} for SET command.")
            elif command == "GET":
                if not arguments:
                    response = b"-ERR wrong number of arguments for 'get' command\r\n"
                    client.sendall(response)
                    print(f"Sent: GET argument error to {client_address}.")
                    continue
                key = arguments[0]
                data_entry = DATA_STORE.get(key)

                if data_entry is None:
                    response = b"$-1\r\n"  # RESP Null Bulk String
                else:
                    current_time = int(time.time() * 1000)
                    expiry = data_entry.get("expiry")
                    if expiry is not None and current_time >= expiry:
                        del DATA_STORE[key]
                        response = b"$-1\r\n"  # RESP Null Bulk String
                    else:
                        value_bytes = data_entry["value"].encode()
                        length_bytes = str(len(value_bytes)).encode()
                        response = b"$" + length_bytes + b"\r\n" + value_bytes + b"\r\n"
                client.sendall(response)
                print(f"Sent: GET response for key '{key}' to {client_address}.")

# --------------------------------------------------------------------------------

def main():

    try:
        # This tells the operating system to create a listening point at $\texttt{localhost}$ (your computer) on port $\texttt{6379}$ (the standard Redis port).
        server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
        print("Server: Starting server on localhost:6379...")
        print("Server: Listening for connections...")
    except OSError as e:
        print(f"Server Error: Could not start server: {e}")
        return

    # The server is now waiting patiently for a customer (client) to walk in.
    while True:
        try:
            # When you run $\texttt{redis-cli}$, the server blocks here until that client connects.Once connected, it gets a new, dedicated connection socket {connection} and the client's address (client address}.
            connection, client_address = server_socket.accept()

            # To handle multiple clients simultaneously, the server hands the connection off to a new thread
            threading.Thread(target=handle_connection, args=(connection, client_address)).start()
        except Exception as e:
            print(f"Server Error: Exception during connection acceptance or thread creation: {e}")
            break

if __name__ == "__main__":
    main()