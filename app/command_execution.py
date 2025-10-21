from ast import arguments
import socket
import threading
import time
from app.parser import parsed_resp_array
from app.datastore import BLOCKING_CLIENTS, BLOCKING_CLIENTS_LOCK, DATA_LOCK, DATA_STORE, cleanup_blocked_client, lrange_rtn, prepend_to_list, remove_elements_from_list, size_of_list, append_to_list, existing_list, get_data_entry, set_list, set_string

# --------------------------------------------------------------------------------

def handle_command(command: str, arguments: list, client: socket.socket) -> bool:
    """
    Executes a single command and sends the response.
    Returns True if the command was processed successfully, False otherwise (e.g., unknown command).
    """
    client_address = client.getpeername()

    if command == "PING":
        response = b"+PONG\r\n"
        client.sendall(response)
        print(f"Sent: PONG to {client_address}.")

    elif command == "ECHO":
        if not arguments:
            response = b"-ERR wrong number of arguments for 'echo' command\r\n"
            client.sendall(response)
            return True
        
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
            return True # Go back to listening for more data
        
        key = arguments[0]
        value = arguments[1]
        duration_ms = None
        
        # Option Parsing Loop
        i = 2
        while i < len(arguments):
            option = arguments[i].upper()
            
            if option in ("EX", "PX"):
                # Check if the duration argument exists
                if i + 1 >= len(arguments):
                    response = f"-ERR syntax error\r\n".encode()
                    client.sendall(response)
                    return True 

                try:
                    # Convert the duration argument (string) to an integer first
                    duration = int(arguments[i + 1])
                    
                    if option == "EX":
                        duration_ms = duration * 1000  # Convert seconds to milliseconds
                    elif option == "PX":
                        duration_ms = duration
                    
                    i += 2 # Skip the option and its value
                    break # Assuming only one EX/PX option
                
                except ValueError:
                    # Catch case where duration is not an integer
                    response = b"-ERR value is not an integer or out of range\r\n"
                    client.sendall(response)
                    return True 
            else:
                # Handle unrecognized option
                response = f"-ERR syntax error\r\n".encode()
                client.sendall(response)
                return True 
        
        current_time = int(time.time() * 1000)
        
        # Calculate the absolute expiration timestamp
        expiry_timestamp = current_time + duration_ms if duration_ms is not None else None

        # Use the data store function to set the value safely
        set_string(key, value, expiry_timestamp)
        
        response = b"+OK\r\n"
        client.sendall(response)
        print(f"Sent: OK to {client_address} for SET command. Expiry: {expiry_timestamp}")

    elif command == "GET":
        if not arguments:
            response = b"-ERR wrong number of arguments for 'get' command\r\n"
            client.sendall(response)
            print(f"Sent: GET argument error to {client_address}.")
            return True
            
        key = arguments[0]
        
        # Use the data store function to get the value with expiry check
        data_entry = get_data_entry(key)

        if data_entry is None:
            response = b"$-1\r\n"  # RESP Null Bulk String
        else:
            # Check for correct type (important: we only support string GET for now)
            if data_entry.get("type") != "string":
                 response = b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
            else:
                # Construct the Bulk String response
                value = data_entry["value"]
                value_bytes = value.encode()
                length_bytes = str(len(value_bytes)).encode()
                response = b"$" + length_bytes + b"\r\n" + value_bytes + b"\r\n"
            
        client.sendall(response)
        print(f"Sent: GET response for key '{key}' to {client_address}.")
    
    elif command == "RPUSH":
        # 1. Argument and Key setup
        if not arguments:
            # ... handle error ...
            return True
        
        list_key = arguments[0]
        elements = arguments[1:]

        # 2. Add all elements (Acquire/Release DATA_LOCK implicitly)
        # Assuming append_to_list, set_list handle locking internally
        if existing_list(list_key):
            for element in elements:
                append_to_list(list_key, element)
        else:
            set_list(list_key, elements, None)

        size_to_report = size_of_list(list_key) # Size after all RPUSHes

        # 3. Check and Serve the longest waiting BLPOP client
        blocked_client_condition = None

        with BLOCKING_CLIENTS_LOCK:
            # Use pop(0) for FIFO (longest waiting client)
            if list_key in BLOCKING_CLIENTS and BLOCKING_CLIENTS[list_key]:
                blocked_client_condition = BLOCKING_CLIENTS[list_key].pop(0)
                # Keep the key in BLOCKING_CLIENTS if there are still waiters

        if blocked_client_condition:
            
            # 3a. LPOP the element that was just pushed (Acquire/Release DATA_LOCK implicitly)
            popped_elements = remove_elements_from_list(list_key, 1) 
            
            # Update the size the RPUSH client will see
            size_to_report = size_of_list(list_key)
            
            if popped_elements:
                popped_element = popped_elements[0]
                
                # 3b. Construct and Send BLPOP response to the BLOCKED client
                key_resp = b"$" + str(len(list_key.encode())).encode() + b"\r\n" + list_key.encode() + b"\r\n"
                element_resp = b"$" + str(len(popped_element.encode())).encode() + b"\r\n" + popped_element.encode() + b"\r\n"
                blpop_response = b"*2\r\n" + key_resp + element_resp

                blocked_client_socket = blocked_client_condition.client_socket
                
                # Send immediately, then notify
                try:
                    blocked_client_socket.sendall(blpop_response)
                except Exception:
                    # Handle case where the blocked client disconnected
                    pass

                # 3c. Wake up the blocked thread (must be inside its own lock context)
                with blocked_client_condition:
                    blocked_client_condition.notify() 

        # 4. Final step: Send the RPUSH response (always sends the final size)
        response = b":{size}\r\n".replace(b"{size}", str(size_to_report).encode())
        client.sendall(response)
        
        return True
    elif command == "LRANGE":
        if not arguments or len(arguments) < 3:
            response = b"-ERR wrong number of arguments for 'lrange' command\r\n"
            client.sendall(response)
            print(f"Sent: LRANGE argument error to {client_address}.")
            return True

        list_key = arguments[0]
        start = int(arguments[1])
        end = int(arguments[2])

        list_elements = lrange_rtn(list_key, start, end)

        response_parts = []
        for element in list_elements: 
            element_bytes = element.encode()
            length_bytes = str(len(element_bytes)).encode()
            response_parts.append(b"$" + length_bytes + b"\r\n" + element_bytes + b"\r\n")

        response = b"*" + str(len(list_elements)).encode() + b"\r\n" + b"".join(response_parts)
        client.sendall(response)
        print(f"Sent: LRANGE response for key '{list_key}' to {client_address}.")

    elif command == "LPUSH":
        if not arguments:
            response = b"-ERR wrong number of arguments for 'lpush' command\r\n"
            client.sendall(response)
            print(f"Sent: LPUSH argument error to {client_address}.")
            return True
        
        list_key = arguments[0]
        elements = arguments[1:]

        size = 0

        if existing_list(list_key):
            for element in elements:
                prepend_to_list(list_key, element)
        else:
            set_list(list_key, elements, None)

        size = size_of_list(list_key)
        response = b":{size}\r\n".replace(b"{size}", str(size).encode())
        client.sendall(response)
        print(f"Sent: LPUSH response for key '{list_key}' to {client_address}.")
    
    elif command == "LLEN":
        if not arguments:
            response = b"-ERR wrong number of arguments for 'llen' command\r\n"
            client.sendall(response)
            print(f"Sent: LLEN argument error to {client_address}.")
            return True
        
        list_key = arguments[0]
        size = size_of_list(list_key)
        response = b":{size}\r\n".replace(b"{size}", str(size).encode())
        client.sendall(response)
        print(f"Sent: LLEN response for key '{list_key}' to {client_address}.")

    elif command == "LPOP":
        if not arguments:
            response = b"-ERR wrong number of arguments for 'lpop' command\r\n"
            client.sendall(response)
            print(f"Sent: LPOP argument error to {client_address}.")
            return True
        
        list_key = arguments[0]
        arguments = arguments[1:]

        if not existing_list(list_key):
            response = b"$-1\r\n"  # RESP Null Bulk String
            client.sendall(response)
            print(f"Sent: LPOP null response for non-existing list '{list_key}' to {client_address}.")
            return True

        if arguments == []:
            list_elements = remove_elements_from_list(list_key, 1)
        else:
            list_elements = remove_elements_from_list(list_key, int(arguments[0]))
        if list_elements is None:
            response = b"$-1\r\n"  # RESP Null Bulk String
            client.sendall(response)
            print(f"Sent: LPOP null response for empty list '{list_key}' to {client_address}.")
            return True

        response_parts = []
        for element in list_elements: 
            element_bytes = element.encode()
            length_bytes = str(len(element_bytes)).encode()
            response_parts.append(b"$" + length_bytes + b"\r\n" + element_bytes + b"\r\n")

        if len(response_parts) == 1:
            response = b"$" + str(len(list_elements[0].encode())).encode() + b"\r\n" + list_elements[0].encode() + b"\r\n"
        else:
            response = b"*" + str(len(list_elements)).encode() + b"\r\n" + b"".join(response_parts)
        
        
        client.sendall(response)

        print(f"Sent: LPOP response '{list_elements}' for list '{list_key}' to {client_address}.")

    elif command == "BLPOP":
        # 1. Argument and Key setup
        if len(arguments) != 2:
            # ... handle error ...
            return True
        
        list_key = arguments[0]
        try:
            # Redis timeout is in seconds, threading.Condition.wait() takes seconds
            timeout = int(arguments[1]) 
        except ValueError:
            # ... handle error for non-integer timeout ...
            return True
        
        # 2. Check for elements (Acquire/Release DATA_LOCK implicitly inside existing_list/size_of_list)
        if size_of_list(list_key) > 0:
            # If element exists, perform LPOP and return immediately.
            list_elements = remove_elements_from_list(list_key, 1)
            
            # Since size > 0, we should get one element. Construct and send RESP Array [key, element]
            if list_elements:
                popped_element = list_elements[0]
                
                # Construct RESP Array *2...
                key_resp = b"$" + str(len(list_key.encode())).encode() + b"\r\n" + list_key.encode() + b"\r\n"
                element_resp = b"$" + str(len(popped_element.encode())).encode() + b"\r\n" + popped_element.encode() + b"\r\n"
                response = b"*2\r\n" + key_resp + element_resp

                client.sendall(response)
                return True
            # Fallthrough to block if list was found but unexpectedly empty (unlikely after check)

        # 3. Blocking Logic (List is empty or non-existent)
        client_condition = threading.Condition()
        # Attach the client socket directly to the Condition object for retrieval by RPUSH
        client_condition.client_socket = client

        # Add client to the blocking registry (must be FIFO)
        with BLOCKING_CLIENTS_LOCK:
            BLOCKING_CLIENTS.setdefault(list_key, []).append(client_condition)

        # Wait for notification or timeout
        with client_condition:
            # wait() returns True if notified, False if timeout
            if timeout == 0:
                notified = client_condition.wait()  # Wait indefinitely
            else:
                notified = client_condition.wait(timeout)

        
        # 4. Post-Blocking Cleanup and Response
        if notified:
            # If True, the client was served by the RPUSH thread (response already sent by RPUSH)
            return True 
        else:
            # If False, timeout occurred. The client must be removed from the registry.
            with BLOCKING_CLIENTS_LOCK:
                # Safely remove, in case it was removed by RPUSH just before wait() returned False
                if client_condition in BLOCKING_CLIENTS.get(list_key, []):
                    BLOCKING_CLIENTS[list_key].remove(client_condition)
                    if not BLOCKING_CLIENTS[list_key]:
                        del BLOCKING_CLIENTS[list_key]
            
            # Send Null Array response
            response = b"*-1\r\n"
            client.sendall(response)
            return True
    

def handle_connection(client: socket.socket, client_address):
    """
    This function is called for each new client connection.
    It manages the connection lifecycle and command loop.
    """
    print(f"Connection: New connection from {client_address}")
    
    with client: 
        while True:
            # The thread waits for the client to send a command. When you run {redis-cli ECHO hey}, the server receives the raw RESP bytes: data = b'*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n'
            data = client.recv(4096) 
            if not data:
                print(f"Connection: Client {client_address} closed connection.")
                cleanup_blocked_client(client)
                break
                
            print(f"Received: Raw bytes from {client_address}: {data!r}")

            # The raw bytes are immediately sent to the parser to be translated into a usable Python list.
            parsed_command = parsed_resp_array(data)
            
            if not parsed_command:
                print(f"Received: Could not parse command from {client_address}. Closing connection.")
                break

            command = parsed_command[0].upper()
            arguments = parsed_command[1:]
            
            print(f"Command: Parsed command: {command}, Arguments: {arguments}")
            
            # Delegate command execution to the router
            handle_command(command, arguments, client)