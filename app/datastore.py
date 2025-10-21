import time
import threading

# The Lock ensures that only one thread can modify the store at a time,
# preventing data corruption (race conditions) when multiple clients run SET simultaneously.
DATA_LOCK = threading.Lock()

BLOCKING_CLIENTS_LOCK = threading.Lock()
BLOCKING_CLIENTS = {}

# The central storage. Keys map to a dictionary containing value, type, and expiry metadata.
# Example: {'mykey': {'type': 'string', 'value': 'myvalue', 'expiry': 1731671220000}}
DATA_STORE = {}

def get_data_entry(key: str) -> dict | None:
    """
    Retrieves a key, checks for expiration, and performs lazy deletion if expired.
    Returns the valid data entry dictionary or None if the key is missing/expired.
    """
    with DATA_LOCK:
        data_entry = DATA_STORE.get(key)

        if data_entry is None:
            # Key does not exist
                return None

        if data_entry is None:
            return data_entry
        
        expiry = data_entry.get("expiry")
        current_time_ms = int(time.time() * 1000)

        # Check for expiration
        if expiry is not None and current_time_ms >= expiry:
            # Key has expired; delete it
            del DATA_STORE[key]
            return None
        
        return data_entry

def set_string(key: str, value: str, expiry_timestamp: int | None):
    """
    Sets a key to a string value with optional expiration.
    """
    with DATA_LOCK:
        DATA_STORE[key] = {
            "type": "string",
            "value": value,
            "expiry": expiry_timestamp
        }

def set_list(key: str, elements: list[str], expiry_timestamp: int | None):
    """
    Sets a key to a list of strings with optional expiration.
    """
    with DATA_LOCK:
        DATA_STORE[key] = {
            "type": "list",
            "value": elements,
            "expiry": expiry_timestamp
        }

def existing_list(key: str) -> bool:
    """
    Checks if a list exists by key, without retrieving it.
    """
    with DATA_LOCK:
        data_entry = DATA_STORE.get(key)
        if data_entry is None:
            return False
        return data_entry.get("type") == "list"

def append_to_list(key: str, element: str):
    """
    Appends an element to an existing list at the given key.
    Assumes the list already exists.
    """
    with DATA_LOCK:
        data_entry = DATA_STORE.get(key)
        if data_entry and data_entry.get("type") == "list":
            data_entry["value"].append(element)

def size_of_list(key: str) -> int:
    """
    Returns the size of the list stored at key, or 0 if the key does not exist or is not a list.
    """
    with DATA_LOCK:
        data_entry = DATA_STORE.get(key)
        if data_entry and data_entry.get("type") == "list":
            return len(data_entry["value"])
        return 0

def lrange_rtn(key: str, start: int, end: int) -> list[str]:
    """
    Returns a sublist from the list stored at key, from start to end indices (inclusive).
    If the key does not exist or is not a list, returns an empty list.
    """
    with DATA_LOCK:
        data_entry = DATA_STORE.get(key)
        if data_entry and data_entry.get("type") == "list":
            list = data_entry["value"]
            if start < 0:
                start = start + len(list)
            if end < 0:
                end = end + len(list)
            if start > end or start >= len(list):
                return []
            if end >= len(list):
                return list[start:]
            
            start = max(0, start)
            return list[start:end + 1]
        return []

def prepend_to_list(key: str, element: str):
    """
    Prepends an element to an existing list at the given key.
    Assumes the list already exists.
    """
    with DATA_LOCK:
        data_entry = DATA_STORE.get(key)
        if data_entry and data_entry.get("type") == "list":
            data_entry["value"].insert(0, element)

def remove_elements_from_list(key: str, count: int) -> list[str] | None: 
    """
    Removes and returns the first elements from the list at the given key.
    Returns None if the list is empty or the key does not exist/is not a list.
    """
    with DATA_LOCK:
        data_entry = DATA_STORE.get(key)
        if data_entry and data_entry.get("type") == "list":
            if data_entry["value"]:
                return [data_entry["value"].pop(0) for _ in range(count)]
            
            if not data_entry["value"]:
                del DATA_STORE[key]
                return None

    return None

def cleanup_blocked_client(client):
    with BLOCKING_CLIENTS_LOCK:
        for key, waiters in list(BLOCKING_CLIENTS.items()):
            BLOCKING_CLIENTS[key] = [
                cond for cond in waiters if getattr(cond, "client_socket", None) != client
            ]
            if not BLOCKING_CLIENTS[key]:
                del BLOCKING_CLIENTS[key]

def read_key_from_rdb(rdb_path, target_key):
    with open(rdb_path, "rb") as f:
        # 1. Read header (magic + 4-byte version). Do not consume the rest of the file.
        magic = f.read(5)
        if magic != b"REDIS":
            raise Exception("Unsupported RDB file: missing 'REDIS' magic")
        version = f.read(4)
        if not version or len(version) < 4:
            raise Exception("Unsupported RDB version")
        # optionally consume a single newline after the version
        maybe_nl = f.read(1)
        if maybe_nl not in (b"\n", b"\r", b""):
            f.seek(-1, 1)

        # 2. Skip metadata sections
        while True:
            byte = f.read(1)
            if byte == b'\xFA':  # metadata start
                # Read metadata key and value
                meta_key = read_string(f)
                meta_val = read_string(f)
            else:
                # Not metadata, rewind one byte and break
                f.seek(-1, 1)
                break

        # 3. Read database sections
        while True:
            byte = f.read(1)
            if byte == b'\xFE':  # start of DB section
                db_index = read_length(f)  # size-encoded DB index

                # Read key-value pairs
                while True:
                    type_byte = f.read(1)
                    if type_byte == b'\xFF':  # end of RDB
                        return None

                    # Optional expiry
                    if type_byte in (b'\xFC', b'\xFD'):
                        expiry = read_expiry(f, type_byte)
                        type_byte = f.read(1)  # actual value type

                    value_type = type_byte  # 1 byte
                    key = read_string(f)

                    value = read_value(f, value_type)

                    # 4. Check if this is the key we want
                    if key == target_key:
                        return value

                    # else, continue to next key-value
            elif byte == b'\xFF':  # EOF
                break
            else:
                raise Exception("Unexpected byte in RDB")
    return None


# Helper to read a string (size-encoded)
# datastore.py (modified read_string)

def read_string(f):
    length_or_encoding_byte = read_length(f)
    
    # Check if the length is actually an encoding byte (prefix 0b11)
    if (length_or_encoding_byte >> 6) == 0b11:
        # It's an encoded string (C0-C3), delegate to read_encoded_string
        return read_encoded_string(f, length_or_encoding_byte) # <<< Pass the encoding byte
    
    # Regular string: the result is the length
    length = length_or_encoding_byte
    data = f.read(length)
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return data # Return raw bytes if not valid UTF-8

# Helper to read a size-encoded length
def read_length(f):
    first_byte = f.read(1)[0]
    prefix = first_byte >> 6  # first 2 bits

    if prefix == 0b00:
        # small length
        return first_byte & 0x3F
    elif prefix == 0b01:
        # 14-bit length
        second_byte = f.read(1)[0]
        return ((first_byte & 0x3F) << 8) | second_byte
    elif prefix == 0b10:
        # 32-bit length
        return int.from_bytes(f.read(4), "big")
    else:
        # special string encoding (C0–C3)
        return first_byte

# Helper to read a value depending on its type
def read_value(f, value_type):
    if value_type == b'\x00':  # string
        return read_string(f)
    # other types like lists/hashes could be added later
    return None

# Helper to read expiry timestamps
def read_expiry(f, type_byte):
    if type_byte == b'\xFC':  # ms
        return int.from_bytes(f.read(8), "little")
    elif type_byte == b'\xFD':  # sec
        return int.from_bytes(f.read(4), "little")

def read_encoded_string(f, first_byte):
    encoding_type = first_byte & 0x3F  # last 6 bits
    if encoding_type == 0x00:  # C0 = 8-bit int
        val = int.from_bytes(f.read(1), "big")
        return str(val)
    elif encoding_type == 0x01:  # C1 = 16-bit int
        val = int.from_bytes(f.read(2), "little")
        return str(val)
    elif encoding_type == 0x02:  # C2 = 32-bit int
        val = int.from_bytes(f.read(4), "little")
        return str(val)
    elif encoding_type == 0x03:  # C3 = LZF compressed
        raise Exception("C3 LZF compression not supported in this stage")
    else:
        raise Exception(f"Unknown string encoding: {hex(first_byte)}")
    

# datastore.py

def load_rdb_to_datastore(rdb_path):
    datastore = {}

    with open(rdb_path, "rb") as f:
        # 1. Read header (magic + 4-byte version). Do not consume the rest of the file.
        magic = f.read(5)
        if magic != b"REDIS":
            raise Exception("Unsupported RDB file: missing 'REDIS' magic")
        version = f.read(4)
        if not version or len(version) < 4:
            raise Exception("Unsupported RDB version")
        # optionally consume a single newline after the version
        maybe_nl = f.read(1)
        if maybe_nl not in (b"\n", b"\r", b""):
            f.seek(-1, 1)

        # 2. Skip metadata sections (0xFA ...)
        while True:
            byte = f.read(1)
            if not byte:
                break
            if byte == b'\xFA':
                # read metadata key and value (string encoded)
                _ = read_string(f)
                _ = read_string(f)
                continue
            # not a metadata marker, rewind one byte and continue to DB parsing
            f.seek(-1, 1)
            break

        # 3. Read database sections
        while True:
            byte = f.read(1)
            if not byte:
                break  # End of file
            if byte == b'\xFE':  # Database section
                db_index = read_length(f)

                # Hash table size info (optional)
                hash_size_marker = f.read(1)
                if hash_size_marker == b'\xFB':
                    read_length(f)  # key-value hash table size
                    read_length(f)  # expiry hash table size
                else:
                    f.seek(-1, 1)

                # Key-value pairs
                while True:
                    expiry = None
                    type_byte = f.read(1)
                    if not type_byte or type_byte == b'\xFF':
                        break
                    if type_byte in (b'\xFC', b'\xFD'):
                        expiry = read_expiry(f, type_byte)
                        type_byte = f.read(1)
                    value_type = type_byte
                    key = read_string(f)
                    value = read_value(f, value_type)
                    if value_type == b'\x00':
                        datastore[key] = {
                            "type": "string",
                            "value": value,
                            "expiry": expiry
                        }
            elif byte == b'\xFF':  # End of file section
                # After 0xFF, 8 bytes of checksum follow. Consume them.
                _ = f.read(8)
                # Ignore any extra bytes after checksum (be robust)
                break
            elif byte == b'\xFA':
                # Metadata section (shouldn't appear here, but skip if present)
                _ = read_string(f)
                _ = read_string(f)
            else:
                # Ignore any unknown/extra bytes after checksum
                break

    return datastore

