from ast import arguments
import socket
import sys
import os
import threading
import time
import argparse
from xmlrpc import client
from app.parser import parsed_resp_array
from app.datastore import BLOCKING_CLIENTS, BLOCKING_CLIENTS_LOCK, BLOCKING_STREAMS, BLOCKING_STREAMS_LOCK, CHANNEL_SUBSCRIBERS, DATA_LOCK, DATA_STORE, SORTED_SETS, STREAMS, WAIT_CONDITION, WAIT_LOCK, _serialize_command_to_resp_array, add_to_sorted_set, cleanup_blocked_client, enqueue_client_command, get_client_queued_commands, get_sorted_set_range, get_sorted_set_rank, get_stream_max_id, get_zscore, increment_key_value, is_client_in_multi, is_client_subscribed, load_rdb_to_datastore, lrange_rtn, num_client_subscriptions, prepend_to_list, remove_elements_from_list, remove_from_sorted_set, set_client_in_multi, size_of_list, append_to_list, existing_list, get_data_entry, set_list, set_string, subscribe, unsubscribe, xadd, xrange, xread, REPLICA_ACK_OFFSETS

# --------------------------------------------------------------------------------

WRITE_COMMANDS = {"SET", "LPUSH", "RPUSH", "LPOP", "ZADD", "ZREM", "XADD", "INCR", "GEOADD"}

MIN_LON = -180.0
MAX_LON = 180.0
MIN_LAT = -85.05112878
MAX_LAT = 85.05112878

LATITUDE_RANGE = MAX_LAT - MIN_LAT
LONGITUDE_RANGE = MAX_LON - MIN_LON

def spread_int32_to_int64(v: int) -> int:
    """Spreads bits of a 32-bit integer to occupy even positions in a 64-bit integer."""
    v = v & 0xFFFFFFFF
    v = (v | (v << 16)) & 0x0000FFFF0000FFFF
    v = (v | (v << 8)) & 0x00FF00FF00FF00FF
    v = (v | (v << 4)) & 0x0F0F0F0F0F0F0F0F
    v = (v | (v << 2)) & 0x3333333333333333
    v = (v | (v << 1)) & 0x5555555555555555
    return v

def interleave(x: int, y: int) -> int:
    """Interleaves bits of two 32-bit integers to create a single 64-bit Morton code."""
    x_spread = spread_int32_to_int64(x)
    y_spread = spread_int32_to_int64(y)
    y_shifted = y_spread << 1
    return x_spread | y_shifted

def encode_geohash(latitude: float, longitude: float) -> int:
    """Encodes latitude and longitude into a single integer score using Morton encoding."""
    # 2^26
    power_26 = 1 << 26 
    
    # 1. Normalize to the range 0-2^26
    normalized_latitude = power_26 * (latitude - MIN_LAT) / LATITUDE_RANGE
    normalized_longitude = power_26 * (longitude - MIN_LON) / LONGITUDE_RANGE

    # 2. Truncate to integers
    lat_int = int(normalized_latitude)
    lon_int = int(normalized_longitude)

    # 3. Interleave bits
    return interleave(lat_int, lon_int)

def compact_int64_to_int32(v: int) -> int:
    """
    Compact a 64-bit integer with interleaved bits back to a 32-bit integer.
    """
    v = v & 0x5555555555555555
    v = (v | (v >> 1)) & 0x3333333333333333
    v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F
    v = (v | (v >> 4)) & 0x00FF00FF00FF00FF
    v = (v | (v >> 8)) & 0x0000FFFF0000FFFF
    v = (v | (v >> 16)) & 0x00000000FFFFFFFF
    return v

def convert_grid_numbers_to_coordinates(grid_latitude_number: int, grid_longitude_number: int) -> tuple[float, float]:
    """Converts grid numbers back to (longitude, latitude) coordinates (center of grid cell)."""
    # 2**26
    power_26 = 1 << 26 
    
    # Calculate the grid boundaries
    grid_latitude_min = MIN_LATITUDE + LATITUDE_RANGE * (grid_latitude_number / power_26)
    grid_latitude_max = MIN_LATITUDE + LATITUDE_RANGE * ((grid_latitude_number + 1) / power_26)
    grid_longitude_min = MIN_LONGITUDE + LONGITUDE_RANGE * (grid_longitude_number / power_26)
    grid_longitude_max = MIN_LONGITUDE + LONGITUDE_RANGE * ((grid_longitude_number + 1) / power_26)
    
    # Calculate the center point of the grid cell
    latitude = (grid_latitude_min + grid_latitude_max) / 2
    longitude = (grid_longitude_min + grid_longitude_max) / 2
    
    # GEOPOS returns Longitude then Latitude
    return (longitude, latitude) 

def decode_geohash_to_coords(geo_code: int) -> tuple[float, float]:
    """
    Decodes geo code (WGS84) to tuple of (longitude, latitude)
    """
    # Align bits of both latitude and longitude to take even-numbered position
    y = geo_code >> 1
    x = geo_code
    
    # Compact bits back to 32-bit ints
    grid_latitude_number = compact_int64_to_int32(x)
    grid_longitude_number = compact_int64_to_int32(y)
    
    return convert_grid_numbers_to_coordinates(grid_latitude_number, grid_longitude_number)

# Default Redis config
DIR = "."
DB_FILENAME = "dump.rdb"

SERVER_ROLE = "master" # Default role is master
MASTER_HOST = None
MASTER_PORT = None

MASTER_REPLID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb" # Hardcoded 40-char ID
MASTER_REPL_OFFSET = 0 # Starts at 0
REPLICA_REPL_OFFSET = 0
MASTER_SOCKET = None

REPLICA_SOCKETS = []

# Define the 59-byte empty RDB file content (hexadecimal)
# command_execution.py around line 40

# Define the 59-byte empty RDB file content (hexadecimal)
# CHANGE: Use the RDB HEX string provided in the sample answer
EMPTY_RDB_HEX = (
    "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
)
empty_rdb_bytes = bytes.fromhex(EMPTY_RDB_HEX) 
# RDB_FILE_SIZE will be determined by the actual length of the new hex string.
RDB_FILE_SIZE = len(empty_rdb_bytes)
RDB_HEADER = b"$" + str(RDB_FILE_SIZE).encode() + b"\r\n" # Dynamically create the header bytes
# Note: This is equivalent to b"$102\r\n" if the hex string is 102 bytes long.

# Parse args like --dir /path --dbfilename file.rdb
args = sys.argv[1:]
for i in range(0, len(args), 2):
    if args[i] == "--dir":
        DIR = args[i + 1]
    elif args[i] == "--dbfilename":
        DB_FILENAME = args[i + 1]

RDB_PATH = os.path.join(DIR, DB_FILENAME)

# Only load if file exists
if os.path.exists(RDB_PATH):
    DATA_STORE.update(load_rdb_to_datastore(RDB_PATH))
else:
    print(f"RDB file not found at {RDB_PATH}, starting with empty DATA_STORE.")

def _xread_serialize_response(stream_data: dict[str, list[dict]]) -> bytes:
    """Serializes the result of xread into a RESP array response."""
    if not stream_data:
        return b"*-1\r\n" 

    # Outer Array: Array of [key, [entry1, entry2, ...]]
    # *N\r\n
    outer_response_parts = []

    for key, entries in stream_data.items():
        # Array for [key, list of entries] -> *2\r\n
        key_resp = b"$" + str(len(key.encode())).encode() + b"\r\n" + key.encode() + b"\r\n"
        
        # Array for list of entries -> *M\r\n
        entries_array_parts = []
        for entry in entries:
            entry_id = entry["id"]
            fields = entry["fields"]

            # Array for [id, [field1, value1, field2, value2, ...]] -> *2\r\n
            id_resp = b"$" + str(len(entry_id.encode())).encode() + b"\r\n" + entry_id.encode() + b"\r\n"

            # Array for field/value pairs -> *2K\r\n
            fields_array_parts = []
            for field, value in fields.items():
                field_bytes = field.encode()
                value_bytes = value.encode()
                fields_array_parts.append(b"$" + str(len(field_bytes)).encode() + b"\r\n" + field_bytes + b"\r\n")
                fields_array_parts.append(b"$" + str(len(value_bytes)).encode() + b"\r\n" + value_bytes + b"\r\n")
            
            fields_array_resp = b"*" + str(len(fields) * 2).encode() + b"\r\n" + b"".join(fields_array_parts)

            # Combine [id, fields_array]
            entry_array_resp = b"*2\r\n" + id_resp + fields_array_resp
            entries_array_parts.append(entry_array_resp)
        
        # Combine all entries into the inner array
        entries_resp = b"*" + str(len(entries_array_parts)).encode() + b"\r\n" + b"".join(entries_array_parts)
        
        # Combine [key, entries_resp]
        key_entries_resp = b"*2\r\n" + key_resp + entries_resp
        outer_response_parts.append(key_entries_resp)

    # Final response: Array of [key, entries] arrays
    return b"*" + str(len(outer_response_parts)).encode() + b"\r\n" + b"".join(outer_response_parts)

def execute_single_command(command: str, arguments: list, client: socket.socket) -> bytes | bool:

    response = None
    """
    Executes a single command and sends the response.
    Returns True if the command was processed successfully, False otherwise (e.g., unknown command).
    """
    if is_client_subscribed(client):
        ALLOWED_COMMANDS_WHEN_SUBSCRIBED = {"SUBSCRIBE", "UNSUBSCRIBE", "PING", "QUIT", "PSUBSCRIBE", "PUNSUBSCRIBE"}
        if command not in ALLOWED_COMMANDS_WHEN_SUBSCRIBED:
            response = b"-ERR Can't execute '" + command.encode() + b"' when client is subscribed\r\n"
            return response
        

    if command == "PING":
        if is_client_subscribed(client):
            response_parts = []
            pong_bytes = "pong".encode()
            response_parts.append(b"$" + str(len(pong_bytes)).encode() + b"\r\n" + pong_bytes + b"\r\n")

            empty_bytes = "".encode()
            response_parts.append(b"$" + str(len(empty_bytes)).encode() + b"\r\n" + empty_bytes + b"\r\n")

            response = b"*" + str(len(response_parts)).encode() + b"\r\n" + b"".join(response_parts)
            # client.sendall(response
            return response
        else:
            response = b"+PONG\r\n"
            # client.sendall(response
            return response

    elif command == "REPLCONF": 
        # Check for REPLCONF GETACK * (Replica logic)
        if len(arguments) == 2 and arguments[0].upper() == "GETACK" and arguments[1] == "*":
            try:
                # REPLCONF ACK <offset> - use the replica's current offset
                global REPLICA_REPL_OFFSET # Access the global offset
                offset = REPLICA_REPL_OFFSET
                offset_str = str(offset)
                
                # Construct the RESP Array: *3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$LEN\r\n<OFFSET>\r\n
                response = (
                    b"*3\r\n" + # Array of 3 elements
                    b"$8\r\nREPLCONF\r\n" +
                    b"$3\r\nACK\r\n" +
                    b"$" + str(len(offset_str)).encode() + b"\r\n" +
                    offset_str.encode() + b"\r\n"
                )
                return response
            except Exception as e:
                print(f"Error building REPLCONF ACK response: {e}")
                # Return an error message to prevent unexpected silent failure
                return b"-ERR Internal error building ACK\r\n"
        
        # ADDED: Check for REPLCONF ACK <offset> (Master receives from replica)
        elif len(arguments) == 2 and arguments[0].upper() == "ACK":
            global REPLICA_ACK_OFFSETS
            
            try:
                replica_socket = client
                ack_offset = int(arguments[1])

                with WAIT_LOCK: # Acquire lock to update shared state
                    REPLICA_ACK_OFFSETS[replica_socket] = ack_offset
                    # Wake up any waiting threads (the one executing WAIT)
                    WAIT_CONDITION.notify_all() 

                return True
            except ValueError:
                return b"-ERR invalid offset value in ACK\r\n"
        
        # Handshake REPLCONF commands (listening-port <PORT> and capa psync2)
        response = b"+OK\r\n"
        return response
    
    elif command == "PSYNC": 

        # 2. Construct the FULLRESYNC response string
        fullresync_response_str = f"+FULLRESYNC {MASTER_REPLID} {MASTER_REPL_OFFSET}\r\n"
        fullresync_response_bytes = fullresync_response_str.encode()

        # 3. Use the new RDB hex content and convert to binary bytes
        # This is the RDB HEX string provided in the sample answer
        EMPTY_RDB_HEX = (
            "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
        )
        rdb_binary_contents = bytes.fromhex(EMPTY_RDB_HEX) 
        rdb_file_size = len(rdb_binary_contents)

        # 4. Construct the RDB file bulk response header and combine with contents
        # The format is $<length>\r\n<binary_contents>
        rdb_header = b"$" + str(rdb_file_size).encode() + b"\r\n"
        rdb_response_bytes = rdb_header + rdb_binary_contents

        global REPLICA_SOCKETS # <-- FIX 1: Use global to modify the variable
        REPLICA_SOCKETS.append(client)

        # 5. Return the two parts separately as a tuple
        response = fullresync_response_bytes + rdb_response_bytes
        return response
    
    elif command == "ECHO":
        if not arguments:
            response = b"-ERR wrong number of arguments for 'echo' command\r\n"
            # client.sendall(response
            return response
        
        # msg_str is like 'Hey' and we must convert back to RESP bulk string. 
        msg_str = arguments[0]

        # encode back to bytes
        msg_bytes = msg_str.encode() 

        # grab length of msg_bytes and construct RESP bulk string
        length_bytes = str(len(msg_bytes)).encode()

        # b"$3\r\nhey\r\n"
        response = b"$" + length_bytes + b"\r\n" + msg_bytes + b"\r\n"
        
        # client.sendall(response
        return response

    elif command == "SET":
        if len(arguments) < 2:
            response = b"-ERR wrong number of arguments for 'set' command\r\n"
            # client.sendall(response
            return response
        
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
                    # client.sendall(response
                    return response

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
                    # client.sendall(response
                    return response
            else:
                # Handle unrecognized option
                response = f"-ERR syntax error\r\n".encode()
                # client.sendall(response
                return response
        
        current_time = int(time.time() * 1000)
        
        # Calculate the absolute expiration timestamp
        expiry_timestamp = current_time + duration_ms if duration_ms is not None else None

        # Use the data store function to set the value safely
        set_string(key, value, expiry_timestamp)
        
        response = b"+OK\r\n"
        # client.sendall(response
        return response
    
    elif command == "GET":
        if not arguments:
            response = b"-ERR wrong number of arguments for 'get' command\r\n"
            # client.sendall(response
            return response
            
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
            
        # client.sendall(response
        return response
    
    elif command == "LRANGE":
        if not arguments or len(arguments) < 3:
            response = b"-ERR wrong number of arguments for 'lrange' command\r\n"
            # client.sendall(response
            return response

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
        # client.sendall(response
        return response

    elif command == "LPUSH":
        if not arguments:
            response = b"-ERR wrong number of arguments for 'lpush' command\r\n"
            # client.sendall(response
            return response
        
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
        # client.sendall(response
        return response
        
    elif command == "LLEN":
        if not arguments:
            response = b"-ERR wrong number of arguments for 'llen' command\r\n"
            # client.sendall(response
            return response
        
        list_key = arguments[0]
        size = size_of_list(list_key)
        response = b":{size}\r\n".replace(b"{size}", str(size).encode())
        # client.sendall(response
        return response

    elif command == "LPOP":
        if not arguments:
            response = b"-ERR wrong number of arguments for 'lpop' command\r\n"
            # client.sendall(response
            return response
        
        list_key = arguments[0]
        arguments = arguments[1:]

        if not existing_list(list_key):
            response = b"$-1\r\n"  # RESP Null Bulk String
            # client.sendall(response
            return response

        if arguments == []:
            list_elements = remove_elements_from_list(list_key, 1)
        else:
            list_elements = remove_elements_from_list(list_key, int(arguments[0]))
        if list_elements is None:
            response = b"$-1\r\n"  # RESP Null Bulk String
            # client.sendall(response
            return response

        response_parts = []
        for element in list_elements: 
            element_bytes = element.encode()
            length_bytes = str(len(element_bytes)).encode()
            response_parts.append(b"$" + length_bytes + b"\r\n" + element_bytes + b"\r\n")

        if len(response_parts) == 1:
            response = b"$" + str(len(list_elements[0].encode())).encode() + b"\r\n" + list_elements[0].encode() + b"\r\n"
        else:
            response = b"*" + str(len(list_elements)).encode() + b"\r\n" + b"".join(response_parts)
        
        
        # client.sendall(response
        return response

    elif command == "RPUSH":
        # 1. Argument and Key setup
        if not arguments:
            # No arguments -> ignore / error (your code returns True and keeps listening)
            return True
        
        list_key = arguments[0]
        elements = arguments[1:]

        # 2. Add all elements to the list (the helper functions handle DATA_LOCK internally)
        #    - If the key already holds a list, append each pushed element.
        #    - Otherwise create a new list key with the elements.
        #    This models Redis: RPUSH adds elements to the tail.
        if existing_list(list_key):
            for element in elements:
                append_to_list(list_key, element)
        else:
            set_list(list_key, elements, None)

        # IMPORTANT: compute the size *after insertion* and store it.
        # Redis's RPUSH returns the list length *after* the push operation,
        # even if the server immediately serves a blocked client afterwards.
        size_to_report = size_of_list(list_key)  # Size that must be returned to RPUSH caller

        # 3. Check if there are blocked clients waiting on this list
        #    We will wake up the longest-waiting client (FIFO). The structure is:
        #      BLOCKING_CLIENTS = { 'list_key': [cond1, cond2, ...], ... }
        #    Each entry is a threading.Condition used to notify the blocked thread.
        blocked_client_condition = None

        # Acquire the BLOCKING_CLIENTS_LOCK while we inspect / modify the shared dict.
        # This prevents races where multiple RPUSH/BLPOP threads change the waiters concurrently.
        with BLOCKING_CLIENTS_LOCK:
            # If there are waiters, pop the first one (FIFO: the longest-waiting client).
            if list_key in BLOCKING_CLIENTS and BLOCKING_CLIENTS[list_key]:
                blocked_client_condition = BLOCKING_CLIENTS[list_key].pop(0)
                # Note: we intentionally *don't* delete the list_key here even if empty;
                # your code deletes the dict key later when cleaning up waiters on timeout.
                # The critical property is FIFO ordering via pop(0).

        if blocked_client_condition:
            # 3a. When serving a blocked client, we must remove an element from the list.
            #     remove_elements_from_list pops from the head (LPOP semantics).
            #     This returns the element that will be sent to the blocked client.
            popped_elements = remove_elements_from_list(list_key, 1) 
            
            # (You already computed size_to_report before popping; do NOT recalc it here,
            #  since Redis returns the size *after insertion*, not after serving waiters.)

            if popped_elements:
                popped_element = popped_elements[0]
                
                # 3b. Build the RESP array that BLPOP expects:
                #     *2\r\n
                #     $<len(key)>\r\n<key>\r\n
                #     $<len(element)>\r\n<element>\r\n
                key_resp = b"$" + str(len(list_key.encode())).encode() + b"\r\n" + list_key.encode() + b"\r\n"
                element_resp = b"$" + str(len(popped_element.encode())).encode() + b"\r\n" + popped_element.encode() + b"\r\n"
                blpop_response = b"*2\r\n" + key_resp + element_resp

                blocked_client_socket = blocked_client_condition.client_socket
                
                # Send the BLPOP response directly to the blocked client's socket.
                # We do this *before* notify() so that when the blocked thread wakes it
                # can safely assume the response has already been sent (avoids a race).
                try:
                    blocked_client_socket.sendall(blpop_response)
                except Exception:
                    # If the blocked client disconnected between RPUSH discovering it and us sending,
                    # sendall will fail; we catch and ignore because we still need to notify the thread
                    # (or let its wait time out and the cleanup code remove it).
                    pass

                # 3c. Wake up the blocked thread by notifying its Condition.
                #      According to Condition semantics, notify() should be called while
                #      holding the Condition's own lock, so we enter the Condition context.
                #      The blocked thread is waiting on the same Condition and will be awakened.
                with blocked_client_condition:
                    blocked_client_condition.notify() 

        # 4. Final step: Send the RPUSH response (always the size immediately after insertion)
        #    This is the value clients expect (e.g., ":1\r\n")
        response = b":{size}\r\n".replace(b"{size}", str(size_to_report).encode())
        # client.sendall(response
        return response

    elif command == "BLPOP":
        # 1. Argument and Key setup
        if len(arguments) != 2:
            # Wrong number of args
            return True
        
        list_key = arguments[0]
        try:
            # Redis accepts fractional seconds for the timeout (e.g., 0.4).
            # threading.Condition.wait() accepts float seconds as well, so use float().
            timeout = float(arguments[1]) 
        except ValueError:
            # If parsing fails, send an error to the client (avoid silent failure).
            response = b"-ERR timeout is not a float\r\n"
            # client.sendall(response
            return response
        
        # 2. Fast path: if the list already has elements, pop and return immediately.
        #    This mirrors Redis: BLPOP behaves like LPOP when the list is non-empty.
        if size_of_list(list_key) > 0:
            list_elements = remove_elements_from_list(list_key, 1)
            
            if list_elements:
                popped_element = list_elements[0]
                
                # Construct the RESP array [key, popped_element] and send it.
                key_resp = b"$" + str(len(list_key.encode())).encode() + b"\r\n" + list_key.encode() + b"\r\n"
                element_resp = b"$" + str(len(popped_element.encode())).encode() + b"\r\n" + popped_element.encode() + b"\r\n"
                response = b"*2\r\n" + key_resp + element_resp

                # client.sendall(response
                return response
            # If remove_elements_from_list returns None unexpectedly, fall through to blocking.
            # (This is unlikely if size_of_list returned > 0, but handling it avoids crashes.)

        # 3. Blocking logic (list empty / non-existent)
        #    We create a Condition object that the current thread will wait on.
        client_condition = threading.Condition()
        # Store the client socket on the Condition so RPUSH can send the response
        # directly to the waiting client's socket when an element arrives.
        client_condition.client_socket = client

        # Register this Condition in BLOCKING_CLIENTS under the list_key.
        # Use BLOCKING_CLIENTS_LOCK to guard concurrent access to the shared dict.
        with BLOCKING_CLIENTS_LOCK:
            BLOCKING_CLIENTS.setdefault(list_key, []).append(client_condition)

        # Wait for notification or timeout.
        # Note: timeout==0 handled as "block indefinitely" (wait() without timeout).
        with client_condition:
            if timeout == 0:
                # Block forever until notify()
                notified = client_condition.wait()
            else:
                # Block up to `timeout` seconds; wait() returns True if notified, False if timed out
                notified = client_condition.wait(timeout)

        # 4. Post-block handling
        if notified:
            # If True, RPUSH already sent the BLPOP response to the socket, so there's
            # nothing more to do here. Just return True and continue listening for commands.
            return True 
        else:
            # Timeout occurred. We must remove this client from the BLOCKING_CLIENTS registry
            # because RPUSH may never visit it (or might have visited it but failed to notify).
            with BLOCKING_CLIENTS_LOCK:
                # Defensive: only remove if it's still present (RPUSH could have popped it)
                if client_condition in BLOCKING_CLIENTS.get(list_key, []):
                    BLOCKING_CLIENTS[list_key].remove(client_condition)
                    # If no more waiters, delete empty list to keep the dict tidy
                    if not BLOCKING_CLIENTS[list_key]:
                        del BLOCKING_CLIENTS[list_key]
            
            # Send Null Array response on timeout: Redis returns "*-1\r\n" for BLPOP timeout.
            response = b"*-1\r\n"
            # client.sendall(response
            return response

    elif command == "CONFIG":
        if len(arguments) != 2 or arguments[0].upper() != "GET":
            # Handle wrong arguments or non-GET subcommands
            response = b"-ERR wrong number of arguments for 'CONFIG GET' command\r\n"
            # client.sendall(response
            return response

        # 1. Extract the parameter name requested by the client
        param_name = arguments[1].lower()
        value = None

        if param_name == "dir":
            value = DIR
        elif param_name == "dbfilename":
            value = DB_FILENAME

        # 2. Handle unknown parameters
        if value is None:
            # Per Redis spec, CONFIG GET for an unknown param returns nil array or empty array.
            # A simple response of the parameter name and empty string is often used in clones.
            value = ""
            # We should still use the param_name for the first element
            
        
        # --- Correct RESP Serialization ---
        
        # 3. Encode strings
        param_bytes = param_name.encode('utf-8')
        value_bytes = value.encode('utf-8')
        
        # 4. Construct the RESP Array: *2 [param_name] [value]
        response = (
            # *2 (Array of 2 elements)
            b"*2\r\n" +
            # $len(param_name)
            b"$" + str(len(param_bytes)).encode('utf-8') + b"\r\n" + 
            # param_name
            param_bytes + b"\r\n" +
            # $len(value)
            b"$" + str(len(value_bytes)).encode('utf-8') + b"\r\n" +
            # value
            value_bytes + b"\r\n"
        )

        # client.sendall(response
        return response

    elif command == "KEYS":
        if len(arguments) != 1:
            response = b"-ERR wrong number of arguments for 'KEYS' command\r\n"
            # client.sendall(response
            return response
        
        pattern = arguments[0]
        
        # Simple pattern matching: only supports '*' wildcard
        with DATA_LOCK:
            matching_keys = []
            for key in DATA_STORE.keys():
                if pattern == "*" or pattern == key:
                    matching_keys.append(key)

        # Construct RESP Array response
        response_parts = []
        for key in matching_keys:
            key_bytes = key.encode()
            length_bytes = str(len(key_bytes)).encode()
            response_parts.append(b"$" + length_bytes + b"\r\n" + key_bytes + b"\r\n")

        response = b"*" + str(len(matching_keys)).encode() + b"\r\n" + b"".join(response_parts)
        # client.sendall(response
        return response

    elif command == "SUBSCRIBE":
        # Construct RESP Array response
        channel = arguments[0] if arguments else ""
        subscribe(client, channel)
        num_subscriptions = num_client_subscriptions(client)

        response_parts = []
        response_parts.append(b"$" + str(len("subscribe".encode())).encode() + b"\r\n" + b"subscribe" + b"\r\n")
        response_parts.append(b"$" + str(len(channel.encode())).encode() + b"\r\n" + channel.encode() + b"\r\n")
        response_parts.append(b":" + str(num_subscriptions).encode() + b"\r\n")  # Number of subscriptions

        response = b"*" + str(len(response_parts)).encode() + b"\r\n" + b"".join(response_parts)
        # client.sendall(response
        return response

    elif command == "PUBLISH":
        if len(arguments) != 2:
            response = b"-ERR wrong number of arguments for 'PUBLISH' command\r\n"
            # client.sendall(response
            return response
        
        channel = arguments[0]
        message = arguments[1]
        recipients = 0

        with BLOCKING_CLIENTS_LOCK:
            if channel in CHANNEL_SUBSCRIBERS:
                subscribers = CHANNEL_SUBSCRIBERS[channel]
                for subscriber in subscribers:
                    # Construct the message RESP Array
                    response_parts = []
                    response_parts.append(b"$" + str(len("message".encode())).encode() + b"\r\n" + b"message" + b"\r\n")
                    response_parts.append(b"$" + str(len(channel.encode())).encode() + b"\r\n" + channel.encode() + b"\r\n")
                    response_parts.append(b"$" + str(len(message.encode())).encode() + b"\r\n" + message.encode() + b"\r\n")

                    response = b"*" + str(len(response_parts)).encode() + b"\r\n" + b"".join(response_parts)
                    try:
                        subscriber.sendall(response)
                        recipients += 1
                    except Exception:
                        pass  # Ignore send errors for subscribers

        # Send number of recipients to publisher
        response = b":" + str(recipients).encode() + b"\r\n"
        # client.sendall(response
        return response

    elif command == "UNSUBSCRIBE":
        channel = arguments[0] if arguments else ""

        unsubscribe(client, channel)
        num_subscriptions = num_client_subscriptions(client)    

        response_parts = []
        response_parts.append(b"$" + str(len("unsubscribe".encode())).encode() + b"\r\n" + b"unsubscribe" + b"\r\n")
        response_parts.append(b"$" + str(len(channel.encode())).encode() + b"\r\n" + channel.encode() + b"\r\n")
        response_parts.append(b":" + str(num_subscriptions).encode() + b"\r\n")  # Number of subscriptions
        response = b"*" + str(len(response_parts)).encode() + b"\r\n" + b"".join(response_parts)
        # client.sendall(response
        return response

    elif command == "ZADD":
        if len(arguments) < 3:
            response = b"-ERR wrong number of arguments for 'zadd' command\r\n"
            # client.sendall(response
            return response
        
        set_key = arguments[0]
        
        if len(arguments) > 3:
             response = b"-ERR only single score/member pair supported in this stage\r\n"
             # client.sendall(response
             return response

        # Extract the single score and member pair
        score_str = arguments[1]
        member = arguments[2]

        try:
            # The helper handles the addition/update and returns the count of new members (1 or 0).
            num_new_elements = add_to_sorted_set(set_key, member, score_str)
        except Exception:
            # Catch exceptions from the helper (e.g., if score_str is not a number)
            response = b"-ERR value is not a valid float\r\n"
            # client.sendall(response
            return response

        # ZADD returns the number of *newly added* elements.
        # Encode as a RESP Integer (e.g., :1\r\n)
        response = b":" + str(num_new_elements).encode() + b"\r\n"
        # client.sendall(response
        return response

    elif command == "ZRANK":
        set_key = arguments[0] if len(arguments) > 0 else ""
        member = arguments[1] if len(arguments) > 1 else ""

        rank = get_sorted_set_rank(set_key, member)
        if rank is None:
            response = b"$-1\r\n"  # RESP Null Bulk String
        else:
            response = b":" + str(rank).encode() + b"\r\n"
        
        # client.sendall(response
        return response

    elif command == "ZRANGE":
        if len(arguments) < 3:
            response = b"-ERR wrong number of arguments for 'ZRANGE' command\r\n"
            # client.sendall(response
            return response
        
        set_key = arguments[0]
        try:
            start = int(arguments[1])
            end = int(arguments[2])
        except ValueError:
            response = b"-ERR start or end is not an integer\r\n"
            # client.sendall(response
            return response


        list_of_members = get_sorted_set_range(set_key, start, end)

        response_parts = []
        for member in list_of_members:
            member_bytes = member.encode() if isinstance(member, str) else bytes(member)
            response_parts.append(b"$" + str(len(member_bytes)).encode() + b"\r\n" + member_bytes + b"\r\n")
        response = b"*" + str(len(list_of_members)).encode() + b"\r\n" + b"".join(response_parts)
        # client.sendall(response
        return response

    elif command == "ZCARD":
        if len(arguments) < 1:
            response = b"-ERR wrong number of arguments for 'ZCARD' command\r\n"
            # client.sendall(response
            return response
        
        set_key = arguments[0]
        
        with DATA_LOCK:
            if set_key in SORTED_SETS:
                cardinality = len(SORTED_SETS[set_key])
            else:
                cardinality = 0

        response = b":" + str(cardinality).encode() + b"\r\n"
        # client.sendall(response
        return response

    elif command == "ZSCORE":
        if len(arguments) < 2:
            response = b"-ERR wrong number of arguments for 'ZSCORE' command\r\n"
            # client.sendall(response
            return response
        
        set_key = arguments[0]
        member = arguments[1]

        score = get_zscore(set_key, member)

        if score is None:
            response = b"$-1\r\n"  # RESP Null Bulk String
        else:
            score_str = str(score)
            score_bytes = score_str.encode()
            length_bytes = str(len(score_bytes)).encode()
            response = b"$" + length_bytes + b"\r\n" + score_bytes + b"\r\n"

        # client.sendall(response
        return response
    
    elif command == "ZREM":
        if len(arguments) < 2:
            response = b"-ERR wrong number of arguments for 'ZREM' command\r\n"
            # client.sendall(response
            return response
        
        set_key = arguments[0]
        members = arguments[1]

        removed_count = remove_from_sorted_set(set_key, members)

        response = b":" + str(removed_count).encode() + b"\r\n"
        # client.sendall(response
        return response
    
    elif command == "TYPE":
        if len(arguments) < 1:
            response = b"-ERR wrong number of arguments for 'TYPE' command\r\n"
            # client.sendall(response
            return response
        
        key = arguments[0]

        data_entry = get_data_entry(key)

        if data_entry is None:
            type_str = "none"
        else:
            type_str = data_entry.get("type", "none")

        type_bytes = type_str.encode()
        length_bytes = str(len(type_bytes)).encode()
        response = b"$" + length_bytes + b"\r\n" + type_bytes + b"\r\n"

        # client.sendall(response
        return response
    
    elif command == "XADD":
        # XADD requires at least: key, id, field, value (4 arguments), and even number of field/value pairs

        if len(arguments) < 4 or (len(arguments) - 2) % 2 != 0:
            response = b"-ERR wrong number of arguments for 'XADD' command\r\n"
            # client.sendall(response
            return response
        
        key = arguments[0]
        entry_id = arguments[1]
        fields = {}
        for i in range(2, len(arguments) - 1, 2):
            fields[arguments[i]] = arguments[i + 1]

        new_entry_id_or_error = xadd(key, entry_id, fields)

        i# Check if xadd returned an error (RESP errors start with '-')
        if new_entry_id_or_error.startswith(b'-'):
            response = new_entry_id_or_error
            # client.sendall(response
            return response
        else:
            # Success: new_entry_id_or_error is the raw ID bytes (e.g. b"1-0").
            # Format as a RESP Bulk String. Fixed the incorrect .encode() call on a bytes object.
            raw_id_bytes = new_entry_id_or_error
            blocked_client_condition = None
            new_entry = None

            with BLOCKING_STREAMS_LOCK:
                if key in BLOCKING_STREAMS and BLOCKING_STREAMS[key]:
                    blocked_client_condition = BLOCKING_STREAMS[key].pop(0)

            if blocked_client_condition:
            # Get the single new entry that was just added (it's the last one)
                with DATA_LOCK: # Acquire lock to safely access STREAMS
                    if key in STREAMS and STREAMS[key]:
                        new_entry = STREAMS[key][-1]
                
                if new_entry:
                    # Prepare the data structure for serialization (single entry for a single stream)
                    stream_data_to_send = {key: [new_entry]}
                    xread_block_response = _xread_serialize_response(stream_data_to_send)

                    blocked_client_socket = blocked_client_condition.client_socket
                    
                    # Send the XREAD BLOCK response directly to the blocked client's socket.
                    try:
                        blocked_client_socket.sendall(xread_block_response)
                    except Exception:
                        pass # Ignore send errors

                    # Wake up the blocked thread by notifying its Condition.
                    with blocked_client_condition:
                        blocked_client_condition.notify()

            length_bytes = str(len(raw_id_bytes)).encode()
            response = b"$" + length_bytes + b"\r\n" + raw_id_bytes + b"\r\n"
            # client.sendall(response
            return response

    elif command == "XRANGE":
        if len(arguments) < 3:
            response = b"-ERR wrong number of arguments for 'XRANGE' command\r\n"
            # client.sendall(response
            return response
        
        key = arguments[0]
        start_id = arguments[1]
        end_id = arguments[2]

        entries = xrange(key, start_id, end_id)

        response_parts = []
        for entry in entries:
            entry_id = entry["id"]
            fields = entry["fields"]

            # Construct RESP Array for each entry: [entry_id, [field1, value1, field2, value2, ...]]
            entry_parts = []
            entry_id_bytes = entry_id.encode()
            entry_parts.append(b"$" + str(len(entry_id_bytes)).encode() + b"\r\n" + entry_id_bytes + b"\r\n")

            # Now construct the inner array of fields and values
            field_value_parts = []
            for field, value in fields.items():
                field_bytes = field.encode()
                value_bytes = value.encode()
                field_value_parts.append(b"$" + str(len(field_bytes)).encode() + b"\r\n" + field_bytes + b"\r\n")
                field_value_parts.append(b"$" + str(len(value_bytes)).encode() + b"\r\n" + value_bytes + b"\r\n")

            # Combine field/value parts into an array
            field_value_array = b"*" + str(len(field_value_parts)).encode() + b"\r\n" + b"".join(field_value_parts)
            entry_parts.append(field_value_array)

            # Combine entry parts into an array
            entry_array = b"*" + str(len(entry_parts)).encode() + b"\r\n" + b"".join(entry_parts)
            response_parts.append(entry_array)
        response = b"*" + str(len(response_parts)).encode() + b"\r\n" + b"".join(response_parts)
        # client.sendall(response
        return response

    elif command == "XREAD":
        # Format: XREAD [BLOCK <ms>] STREAMS key1 key2 ... id1 id2 ...
        
        # 1. Parse optional BLOCK argument
        arguments_start_index = 0
        timeout_ms = None
        
        if len(arguments) >= 3 and arguments[0].upper() == "BLOCK":
            try:
                # Timeout is in milliseconds, convert to seconds for threading.wait
                timeout_ms = int(arguments[1]) 
                arguments_start_index = 2
            except ValueError:
                response = b"-ERR timeout is not an integer\r\n"
                # client.sendall(response
                return response
        
        # 2. Check for STREAMS keyword and argument count
        if len(arguments) < arguments_start_index + 3 or arguments[arguments_start_index].upper() != "STREAMS":
            response = b"-ERR wrong number of arguments or missing STREAMS keyword for 'XREAD' command\r\n"
            # client.sendall(response
            return response

        # 3. Find the split point between keys and IDs
        streams_keyword_index = arguments_start_index
        args_after_streams = arguments[streams_keyword_index + 1:]
        num_args_after_streams = len(args_after_streams)
        
        if num_args_after_streams % 2 != 0:
            response = b"-ERR unaligned key/id pairs for 'XREAD' command\r\n"
            # client.sendall(response
            return response

        num_keys = num_args_after_streams // 2
        
        keys_start_index = 0
        keys = args_after_streams[keys_start_index : keys_start_index + num_keys]
        ids_start_index = keys_start_index + num_keys
        ids = args_after_streams[ids_start_index:]

        resolved_ids = []
        for key, last_id in zip(keys, ids):
            if last_id == "$":
                resolved_ids.append(get_stream_max_id(key))
            else:
                resolved_ids.append(last_id)
        

        # 4. Main XREAD logic loop (synchronous part - fast path)
        stream_data = xread(keys, resolved_ids)
        
        if stream_data:
            # Non-blocking path: Data is available. Serialize and send immediately.
            response = _xread_serialize_response(stream_data)
            # client.sendall(response
            return response
        
        # 5. Blocking path
        if timeout_ms is not None:
            # We are blocking: list of entries is empty.

            if timeout_ms == 0:
                # BLOCK 0 means block indefinitely.
                timeout = None
            else:
                # Convert ms to seconds.
                timeout = timeout_ms / 1000.0
            
            # Since only one key/id pair is supported in this stage, enforce it for blocking
            if len(keys) != 1:
                response = b"-ERR only single key blocking supported in this stage\r\n"
                # client.sendall(response
                return response
            
            key_to_block = keys[0]

            # Create and register the condition
            client_condition = threading.Condition()
            client_condition.client_socket = client
            client_condition.key = key_to_block 

            with BLOCKING_STREAMS_LOCK:
                BLOCKING_STREAMS.setdefault(key_to_block, []).append(client_condition)

            # Wait for notification or timeout
            notified = False
            with client_condition:
                if timeout is None:
                    notified = client_condition.wait()
                else:
                    notified = client_condition.wait(timeout)

            # 6. Post-block handling
            if notified:
                # If True, XADD already sent the response.
                return None 
            else:
                # Timeout occurred. Clean up the blocking registration.
                with BLOCKING_STREAMS_LOCK:
                    if client_condition in BLOCKING_STREAMS.get(key_to_block, []):
                        BLOCKING_STREAMS[key_to_block].remove(client_condition)
                        if not BLOCKING_STREAMS[key_to_block]:
                            del BLOCKING_STREAMS[key_to_block]
                
                # Send Null Array response on timeout: Redis returns "*-1\r\n"
                response = b"*-1\r\n"
                # client.sendall(response
                return response

        # 7. Non-blocking path (no data, no BLOCK keyword) - returns Null Array
        response = b"*0\r\n" 
        # client.sendall(response
        return response

    elif command == "INCR":
        if len(arguments) != 1:
            response = b"-ERR wrong number of arguments for 'incr' command\r\n"
            # client.sendall(response
            return response

        key = arguments[0]
        
        # Call the atomic helper function
        new_value, error_message = increment_key_value(key)
        
        if error_message:
            # Handle error from the helper (WRONGTYPE or not an integer/overflow)
            # client.sendall(error_message.encode())
            return error_message.encode()
        else:
            # Success: new_value is an integer. Return RESP Integer.
            response = b":" + str(new_value).encode() + b"\r\n"
            # client.sendall(response
            return response

    elif command == "MULTI":

        if is_client_in_multi(client):
            response = b"-ERR MULTI calls can not be nested\r\n"
            # client.sendall(response
            return response
        
        # Set the client's state to "in transaction"
        set_client_in_multi(client, True)
        
        response = b"+OK\r\n"
        # client.sendall(response
        return response

    elif command == "EXEC":
        if is_client_in_multi(client):

            queued_commands = get_client_queued_commands(client)
            set_client_in_multi(client, False)

            if not queued_commands:
                # The required response for an empty transaction is an empty RESP Array.
                response = b"*0\r\n"
                # client.sendall(response
                return response
            
            # 4. Execute all queued commands and collect responses
            response_parts = []
            for cmd, args in queued_commands:
                # Recursively call execute_single_command for each queued command
                # The execution should not cause nested queuing, as the multi flag is now False
                # and the recursive call won't re-trigger the main handle_command's checks.
                try:
                    # We pass the client socket for execution (e.g., SET/INCR needs it)
                    cmd_response = execute_single_command(cmd, args, client)
                    
                    # EXEC only returns the actual response, never a connection close signal
                    if cmd == "QUIT":
                        cmd_response = b"+OK\r\n" # We don't actually close the connection yet
                    
                    # Check for blocking/transaction control commands that might return False/True signals
                    if isinstance(cmd_response, bool):
                        # This should not happen if the refactoring is correct, but defensively use a generic error
                        cmd_response = b"-ERR Internal execution error\r\n" 

                except Exception:
                    # This catches errors during the execution of a queued command (e.g., wrong type)
                    cmd_response = b"-ERR EXEC-failed during command execution\r\n" 
                
                response_parts.append(cmd_response)

            # 5. Assemble the final RESP Array
            final_response = b"*" + str(len(response_parts)).encode() + b"\r\n" + b"".join(response_parts)
            
            return final_response
        else:
            response = b"-ERR EXEC without MULTI\r\n"
            # client.sendall(response
            return response
    
    elif command == "DISCARD":
        if is_client_in_multi(client):
            response = b"+OK\r\n"
            set_client_in_multi(client, False)
            # client.sendall(response
            return response
        else:
            response = b"-ERR DISCARD without MULTI\r\n"
            # client.sendall(response
            return response
        
    elif command == "INFO": # <--- ADDED INFO COMMAND
        if len(arguments) == 0:
            # INFO without arguments should return all sections, 
            # but for this stage, we'll only respond with the replication section if no argument is provided.
            section = "replication"
        elif len(arguments) == 1:
            section = arguments[0].lower()
        else:
            response = b"-ERR wrong number of arguments for 'INFO' command\r\n"
            return response

        # Only support 'replication' section for this stage
        if section == "replication":
            # Use the global SERVER_ROLE
            info_content = f"role:{SERVER_ROLE}\r\n"

            # Add master_replid and master_repl_offset only if we are the master
            if SERVER_ROLE == "master":
                # Use the global hardcoded values
                info_content += f"master_replid:{MASTER_REPLID}\r\n"
                info_content += f"master_repl_offset:{MASTER_REPL_OFFSET}\r\n"
            
            # Encode the string as a RESP Bulk String
            info_bytes = info_content.encode()
            length_bytes = str(len(info_bytes)).encode()
            
            # Format: $length\r\ncontent\r\n
            response = b"$" + length_bytes + b"\r\n" + info_bytes + b"\r\n"
            
            return response

        else:
            # For unsupported sections, return an empty bulk string (or whatever 
            # the specific server behavior is, but an empty one is often safe for unimplemented)
            # A simpler approach is to return a bulk string containing only the section header.
            info_content = f"#{section.capitalize()}\r\n"
            info_bytes = info_content.encode()
            length_bytes = str(len(info_bytes)).encode()
            response = b"$" + length_bytes + b"\r\n" + info_bytes + b"\r\n"
            return response
        
    elif command == "WAIT":
        if len(arguments) != 2:
            response = b"-ERR wrong number of arguments for 'WAIT' command\r\n"
            return response

        try:
            num_replicas_required = int(arguments[0])
            timeout_ms = int(arguments[1])
        except ValueError:
            response = b"-ERR numreplicas or timeout is not an integer\r\n"
            return response


        target_offset = MASTER_REPL_OFFSET
        timeout_s = timeout_ms / 1000.0
        start_time = time.time()

        # Optimization: If target is 0, required replicas is 0, or no replicas are connected, return immediately.
        if target_offset == 0 or num_replicas_required == 0 or not REPLICA_SOCKETS:
            num_connected = len(REPLICA_SOCKETS)
            return b":" + str(num_connected).encode() + b"\r\n"

        # The master must send GETACK to all replicas to get their current offset
        getack_command = b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
        
        # 1. Initial send of GETACK to ALL replicas (Poll phase)
        replicas_to_remove = []
        for replica_socket in list(REPLICA_SOCKETS):
            try:
                replica_socket.sendall(getack_command)
            except Exception:
                # Mark failed sockets for removal
                replicas_to_remove.append(replica_socket)

        # Clean up dead replicas
        for dead_socket in replicas_to_remove:
            if dead_socket in REPLICA_SOCKETS:
                 REPLICA_SOCKETS.remove(dead_socket)
                 REPLICA_ACK_OFFSETS.pop(dead_socket, None) # Also remove from ACK tracking


        final_acknowledged_count = 0

        with WAIT_LOCK:
            
            # 2. Polling and waiting loop
            while True:
                
                # Check for timeout first
                timeout_remaining = timeout_s - (time.time() - start_time)
                if timeout_remaining <= 0:
                    # Timeout expired
                    break

                # Check current acknowledged count
                acknowledged_count = 0
                for replica_socket in REPLICA_SOCKETS:
                    # Use a default of 0 if replica hasn't ACKed yet
                    ack_offset = REPLICA_ACK_OFFSETS.get(replica_socket, 0) 
                    if ack_offset >= target_offset:
                        acknowledged_count += 1
                
                # Check completion condition
                if acknowledged_count >= num_replicas_required:
                    final_acknowledged_count = acknowledged_count
                    break

                # Wait for notification or remaining timeout
                WAIT_CONDITION.wait(timeout_remaining)
            
            # If the loop finished due to timeout or early completion, calculate the final count
            # Use the already calculated final_acknowledged_count if it met the requirement.
            # If it broke due to timeout, we must check the last known counts.
            if final_acknowledged_count == 0:
                for replica_socket in REPLICA_SOCKETS:
                    ack_offset = REPLICA_ACK_OFFSETS.get(replica_socket, 0)
                    if ack_offset >= target_offset:
                        final_acknowledged_count += 1
                    
        # Return the final count as a RESP Integer
        response = b":" + str(final_acknowledged_count).encode() + b"\r\n"
        return response
    
    elif command == "GEOADD":
        # GEOADD <key> <longitude> <latitude> <member>
        if len(arguments) < 4:
            response = b"-ERR wrong number of arguments for 'GEOADD' command\r\n"
            return response
        
        key = arguments[0]
        longitude_str = arguments[1]
        latitude_str = arguments[2]
        member = arguments[3]
        
        # 1. Validate coordinates
        try:
            longitude = float(longitude_str)
            latitude = float(latitude_str)
        except ValueError:
            error_msg = b"-ERR value is not a valid float\r\n"
            return error_msg

        # 2. Check Longitude range [-180, 180]
        if not (MIN_LON <= longitude <= MAX_LON):
            error_msg = f"-ERR invalid longitude,latitude pair {longitude:.6f},{latitude:.6f}\r\n".encode()
            return error_msg

        # 3. Check Latitude range [-85.05112878, 85.05112878]
        if not (MIN_LAT <= latitude <= MAX_LAT):
            error_msg = f"-ERR invalid longitude,latitude pair {longitude:.6f},{latitude:.6f}\r\n".encode()
            return error_msg
            
        # 4. Persistence: Calculate geohash score and add to sorted set
        score = encode_geohash(latitude, longitude)
        score_str = str(score)
        
        # add_to_sorted_set returns 1 if a new element was added, or 0 if an existing member was updated.
        num_new_elements = add_to_sorted_set(key, member, score_str)
        
        # 5. Return the count as a RESP Integer
        response = b":" + str(num_new_elements).encode() + b"\r\n"
        return response
   
    elif command == "GEOPOS":
        if len(arguments) < 2:
            response = b"-ERR wrong number of arguments for 'GEOPOS' command\r\n"
            return response
        
        key = arguments[0]
        members = arguments[1:]
        
        final_response_parts = []
        
        for member in members:
            # 1. Retrieve the score (stored as float in Redis)
            score_float = get_zscore(key, member) 
            
            if score_float is None:
                # Member or key does not exist: Null Array (*-1\r\n)
                final_response_parts.append(b"*-1\r\n")
                continue
                
            # 2. Convert score (float) to integer for bitwise decoding
            score_int = int(score_float)
            
            # 3. Decode the geohash score
            try:
                # Returns (longitude, latitude)
                longitude, latitude = decode_geohash_to_coords(score_int)
            except Exception:
                # Should not happen with valid GeoHash scores, but defensively handle error
                final_response_parts.append(b"*-1\r\n")
                continue

            # 4. Format coordinates as RESP Bulk Strings (with high precision)
            lon_str = f"{longitude:.15f}"
            lat_str = f"{latitude:.15f}"
            
            # Format as Bulk Strings
            lon_bytes = lon_str.encode()
            lat_bytes = lat_str.encode()
            lon_resp = b"$" + str(len(lon_bytes)).encode() + b"\r\n" + lon_bytes + b"\r\n"
            lat_resp = b"$" + str(len(lat_bytes)).encode() + b"\r\n" + lat_bytes + b"\r\n"
            
            # Final response for an existing member: *2\r\n<lon_resp><lat_resp>
            member_resp = b"*2\r\n" + lon_resp + lat_resp
            final_response_parts.append(member_resp)

        # 5. Wrap all individual responses in the final RESP array
        response = b"*" + str(len(final_response_parts)).encode() + b"\r\n" + b"".join(final_response_parts)
        return response
    elif command == "QUIT":
        response = b"+OK\r\n"
        # client.sendall(response
        return response

    return b"-ERR unknown command '" + command.encode() + b"'\r\n"

def handle_command(command: str, arguments: list, client: socket.socket) -> bool:
    
    client_address = client.getpeername()

    # 1. TRANSACTION QUEUEING CHECK
    if is_client_in_multi(client):
        # Commands that must be executed immediately, even inside MULTI: MULTI, EXEC, DISCARD
        TRANSACTION_CONTROL_COMMANDS = {"EXEC", "MULTI", "DISCARD"} 
        
        if command not in TRANSACTION_CONTROL_COMMANDS:
            # Queue the command and respond with +QUEUED\r\n
            enqueue_client_command(client, command, arguments)
            response = b"+QUEUED\r\n"
            client.sendall(response)
            print(f"Sent: QUEUED response for command '{command}' to {client_address}.")
            return True # Signal that the command was handled (queued)
        
    # 2. COMMAND EXECUTION
    response_or_signal = execute_single_command(command, arguments, client)

    # 3. PROPAGATION LOGIC (MASTER ROLE)
    is_write_command = command in WRITE_COMMANDS
    global REPLICA_SOCKETS 
    is_master_with_replicas = SERVER_ROLE == "master" and REPLICA_SOCKETS
    
    if is_master_with_replicas and is_write_command:
        # Propagate only if the command executed successfully (returned bytes, not an error)
        if isinstance(response_or_signal, bytes) and not response_or_signal.startswith(b'-'):
            
            # Reconstruct the raw RESP array
            resp_array_to_send = _serialize_command_to_resp_array(command, arguments)
            command_byte_size = len(resp_array_to_send)

            # Iterate and send to ALL replicas
            for replica_socket in list(REPLICA_SOCKETS): 
                try:
                    replica_socket.sendall(resp_array_to_send)
                    print(f"Propagation: Sent command '{command}' to replica {replica_socket.getpeername()}.") 
                except Exception as e:
                    print(f"Propagation Error: Could not send command to replica {replica_socket.getpeername()}: {e}. Removing dead replica.")
                    try:
                        REPLICA_SOCKETS.remove(replica_socket)
                    except ValueError:
                        pass
            global MASTER_REPL_OFFSET
            MASTER_REPL_OFFSET += command_byte_size

    # 4. SEND THE RESPONSE (CONSOLIDATED LOGIC)
    
    # 4a. Check for internal signals (None means response was sent by another thread, e.g., XREAD BLOCK)
    if response_or_signal is None:
        print(f"Execution signal: Command '{command}' successfully processed (response sent by another thread or not required).")
        return True

    # 4b. Handle response only if it's a bytes object (a valid RESP response)
    if isinstance(response_or_signal, bytes):
        global MASTER_SOCKET
        
        # --- RESPONSE SUPPRESSION CHECK (REPLICA ROLE) ---
        # If we are a slave AND the command came on the master's replication connection, suppress the response.
        if SERVER_ROLE == "slave" and client == MASTER_SOCKET:
            # EXCEPTION: REPLCONF GETACK is the *only* command that requires a response back to the master.
            is_replconf_getack = (
                command == "REPLCONF" and 
                len(arguments) >= 2 and 
                arguments[0].upper() == "GETACK"
            )

            if is_replconf_getack:
                print(f"Replica: Executing REPLCONF GETACK and sending ACK back to master.")
                # Fall through to the response sending logic below
            else:
                print(f"Replica: Executed propagated command '{command}' silently.")
                return True # Suppressed successfully, DO NOT send response.

        # --- REGULAR CLIENT RESPONSE ---
        client_address = client.getpeername()
        client.sendall(response_or_signal)
        
        # Special case handling for PSYNC response (Master role)
        if command == "PSYNC":
            print(f"Sent: FULLRESYNC + RDB file for command '{command}' to {client_address}. Waiting 10ms...")
            time.sleep(0.05)
        
        # Log success and return True
        print(f"Sent: Response for command '{command}' to {client_address}.")
        return True
    
    # 4c. Final return for commands that succeeded but didn't produce a bytes response
    if response_or_signal is not False: 
        return True
            
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
            parsed_command, _ = parsed_resp_array(data)
            
            if not parsed_command:
                print(f"Received: Could not parse command from {client_address}. Closing connection.")
                break

            command = parsed_command[0].upper()
            arguments = parsed_command[1:]
            
            print(f"Command: Parsed command: {command}, Arguments: {arguments}")
            
            # Delegate command execution to the router
            handle_command(command, arguments, client)