# app/parser.py

# The parser code remains exactly as optimized earlier.

# Example Input: data = b'*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n'
def parsed_resp_array(data: bytes) -> tuple[list[str], int]: # CHANGE: Return tuple (list, bytes_consumed)
    if not data or not data.startswith(b"*"):
        # If data is empty or not an array, return empty list and 0 consumed bytes
        return [], 0 # CHANGE
    
    try:
        # Find the first CRLF to get the number of elements
        crlf_index = data.find(b"\r\n")
        if crlf_index == -1:
            return [], 0 # CHANGE
        
        # count_bytes is bytes between * and \r\n (b'2' for example)
        count_bytes = data[1:crlf_index]
        if not count_bytes:
             print("Parser Error: No element count found.")
             return [], 0 # CHANGE

        # decode to string and convert to int so now it is 2 for example
        num_elements_str = count_bytes.decode()
        num_elements = int(num_elements_str)

    except ValueError:
        print(f"Parser Error: Invalid element count value: {data[1:crlf_index]}")
        return [], 0 # CHANGE
    

    parsed_elements = []
    # Move index to the start of the first element (past the initial CRLF (\r\n))
    index = crlf_index + 2
    
    print(f"Parser: Expecting {num_elements} elements.")
    
    for i in range(num_elements):

        # Confirms data at index is b"$" (Bulk String marker)
        if index >= len(data) or data[index: index + 1] != b"$":
            print(f"Parser Error: Element {i} not starting with $ at index {index}.")
            return [], 0 # CHANGE
        
        index += 1 # Skip $

        # Find next \r\n to get length of string. Find takes index as second arg to start searching from there. Returns index of \r\n
        crlf_index = data.find(b"\r\n", index)
        if crlf_index == -1:
            print(f"Parser Error: Element {i} missing length CRLF.")
            return [], 0 # CHANGE

        # length_bytes is bytes between $ and \r\n. This is '`4` for example'
        try:
            length_bytes = data[index:crlf_index]
            str_length = int(length_bytes.decode())
            print(f"Parser: Element {i} length is {str_length}.")
        except ValueError:
            print(f"Parser Error: Element {i} invalid length value: {length_bytes}")
            return [], 0 # CHANGE
        
        index = crlf_index + 2 # Skip length and \r\n

        # Extract value. This is b'ECHO' for example
        value_end_index = index + str_length
        if value_end_index + 2 > len(data): # +2 for trailing \r\n
            print(f"Parser Error: Element {i} incomplete data or missing trailing CRLF.")
            return [], 0 # CHANGE
        
        # Decode and append value
        value = data[index:value_end_index].decode()
        parsed_elements.append(value)
        print(f"Parser: Element {i} value: '{value}'")
        
        index = value_end_index + 2  # Skip value and \r\n
        
    return parsed_elements, index # CHANGE: Return the final index (bytes consumed)