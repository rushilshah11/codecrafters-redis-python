import time
import threading

# The Lock ensures that only one thread can modify the store at a time,
# preventing data corruption (race conditions) when multiple clients run SET simultaneously.
DATA_LOCK = threading.Lock()

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