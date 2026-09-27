import os
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

BASE_URL = os.getenv("CONFLUENCE_BASE_URL")
EMAIL = os.getenv("CONFLUENCE_EMAIL")
API_TOKEN = os.getenv("CONFLUENCE_API_TOKEN")

PAGE_ID = "131075"

url = f"{BASE_URL}/wiki/rest/api/content/{PAGE_ID}"

params = {
    "expand": "body.storage,version,space"
}

response = requests.get(
    url,
    params=params,
    auth=(EMAIL, API_TOKEN),
    headers={
        "Accept": "application/json"
    }
)

print("Status code:", response.status_code)

if response.status_code == 200:

    data = response.json()

    title = data.get("title")
    version = data.get("version", {}).get("number")
    space = data.get("space", {}).get("name")

    html_content = (
        data.get("body", {})
            .get("storage", {})
            .get("value", "")
    )

    soup = BeautifulSoup(html_content, "html.parser")

    text_content = soup.get_text(
        "\n",
        strip=True
    )

    print("\nSUCCESS")
    print("Title:", title)
    print("Space:", space)
    print("Version:", version)

    print("\nPAGE CONTENT")
    print("=" * 70)
    print(text_content)
    print("=" * 70)

else:
    print("\nFAILED")
    print(response.text)