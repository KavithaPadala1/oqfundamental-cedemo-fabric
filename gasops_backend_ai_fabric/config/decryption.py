import base64

def decode(encoded_string):
    """
    Decode a base64 encoded string.

    Args:
        encoded_string (str): The base64 encoded string to decode.

    Returns:
        str: The decoded string stored in dictionary with the appropriate key value pairs.
    """
    # Decode the base64 string
    decoded_bytes = base64.b64decode(encoded_string)

    # Convert bytes to string
    decoded_string = decoded_bytes.decode('utf-8')

    decoded_items = decoded_string.split('&')    
    decoded_dict = {"LoginMasterID": decoded_items[0],
                  "Database_Name": decoded_items[1],
                  "OrgID": decoded_items[2]}

    return decoded_dict
