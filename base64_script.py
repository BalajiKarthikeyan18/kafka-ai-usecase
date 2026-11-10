import base64
import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Retrieve the secret value using its key
# Replace 'MY_SECRET_KEY' with the actual key name from your .env file
secret_key = os.getenv("CONFLUENT_CLOUD_API_KEY")
secret_value = os.getenv("CONFLUENT_CLOUD_API_SECRET")

if secret_value:
    # Convert the string to bytes, encode it, and decode back to a string
    encoded_bytes = base64.b64encode(secret_value.encode('utf-8'))
    encoded_string = encoded_bytes.decode('utf-8')
    
    print(f"Key: {secret_key}")
    print(f"Original Value: {secret_value}")
    print(f"Base64 Encoded: {encoded_string}")
else:
    print(f"Error: Key '{secret_key}' not found in the .env file.")
