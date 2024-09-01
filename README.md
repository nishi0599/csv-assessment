import csv
import os
import asyncio
import aiohttp
import sqlite3
from PIL import Image
from io import BytesIO
import uuid
import requests

# Step 1: Define the directory to store images
output_dir = "processed_images"
os.makedirs(output_dir, exist_ok=True)

# Step 2: Set up the SQLite database
def setup_database(db_name="image_data.db"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Create table for storing image data
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS images (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id TEXT,
            serial_number INTEGER,
            product_name TEXT,
            input_image_urls TEXT,
            output_image_urls TEXT,
            status TEXT
        )
    ''')

    # Create table for storing request status
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS requests (
            request_id TEXT PRIMARY KEY,
            file_name TEXT,
            status TEXT
        )
    ''')

    conn.commit()
    return conn

# Step 3: Function to validate the CSV row data
def validate_row(row):
    required_columns = ['S. No.', 'Product Name', 'Input Image Urls']

    for col in required_columns:
        if col not in row or not row[col].strip():
            print(f"Validation error: Missing or empty field '{col}' in row {row}")
            return False

    if not row['S. No.'].isdigit():
        print(f"Validation error: 'S. No.' must be an integer in row {row}")
        return False

    if not row['Product Name'].strip():
        print(f"Validation error: 'Product Name' cannot be empty in row {row}")
        return False

    image_urls = row['Input Image Urls'].split(',')
    for url in image_urls:
        if not url.strip().startswith(('http://', 'https://')):
            print(f"Validation error: Invalid URL '{url}' in row {row}")
            return False

    return True

# Step 4: Function to download and process a single image asynchronously
async def download_and_process_image(session, url, product_name, index, output_dir):
    try:
        async with session.get(url.strip()) as response:
            if response.status == 200:
                img_data = await response.read()
                img = Image.open(BytesIO(img_data))
                img = img.resize((256, 256))  # Resizing the image
                output_path = os.path.join(output_dir, f"{product_name}_{index+1}.jpg")
                img.save(output_path, quality=50)  # Compressing and saving the image
                return output_path
            else:
                print(f"Failed to download image: {url}")
                return None
    except Exception as e:
        print(f"Error processing image {url}: {e}")
        return None

# Step 5: Function to save processed image data to the database
def save_to_database(conn, request_id, serial_number, product_name, input_image_urls, output_image_urls):
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO images (request_id, serial_number, product_name, input_image_urls, output_image_urls, status)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (request_id, serial_number, product_name, input_image_urls, output_image_urls, "processed"))
    conn.commit()

# Step 6: Function to update request status in the database
def update_request_status(conn, request_id, status):
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE requests SET status = ? WHERE request_id = ?
    ''', (status, request_id))
    conn.commit()

# Step 7: Function to trigger webhook
def trigger_webhook(request_id, webhook_url, output_file):
    with open(output_file, 'rb') as file:
        response = requests.post(webhook_url, files={'file': file}, data={'request_id': request_id})
    print(f"Webhook triggered with response: {response.status_code}")

# Step 8: Function to generate output CSV
def generate_output_csv(conn, request_id, output_file):
    cursor = conn.cursor()
    cursor.execute('''
        SELECT serial_number, product_name, input_image_urls, output_image_urls 
        FROM images WHERE request_id = ?
    ''', (request_id,))
    rows = cursor.fetchall()

    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['S. No.', 'Product Name', 'Input Image Urls', 'Output Image Urls'])
        for row in rows:
            writer.writerow(row)

# Step 9: Function to process images for a single row asynchronously
async def process_images_for_row(row, output_dir, conn, request_id):
    if not validate_row(row):
        print(f"Skipping row due to validation errors: {row}")
        return

    serial_number = int(row['S. No.'])
    product_name = row['Product Name']
    image_urls = row['Input Image Urls'].split(',')

    async with aiohttp.ClientSession() as session:
        tasks = [
            download_and_process_image(session, url, product_name, i, output_dir)
            for i, url in enumerate(image_urls)
        ]
        processed_image_paths = await asyncio.gather(*tasks)

    processed_image_paths = [path for path in processed_image_paths if path]
    print(f"Processed images for {product_name}: {processed_image_paths}")

    # Save the processed images' data to the database
    save_to_database(conn, request_id, serial_number, product_name, ','.join(image_urls), ','.join(processed_image_paths))

# Step 10: Process CSV and handle images asynchronously
async def process_csv(file_path, conn, request_id, webhook_url):
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        tasks = [process_images_for_row(row, output_dir, conn, request_id) for row in reader]
        await asyncio.gather(*tasks)

    # Mark the request as completed
    update_request_status(conn, request_id, "completed")

    # Generate the output CSV
    output_file = os.path.join(output_dir, f"{request_id}_output.csv")
    generate_output_csv(conn, request_id, output_file)

    # Trigger the webhook
    if webhook_url:
        trigger_webhook(request_id, webhook_url, output_file)

# Step 11: API to submit CSV file for processing and return a request ID
def submit_csv(file_path, webhook_url=None):
    conn = setup_database()
    request_id = str(uuid.uuid4())  # Generate a unique request ID
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO requests (request_id, file_name, status)
        VALUES (?, ?, ?)
    ''', (request_id, os.path.basename(file_path), "processing"))
    conn.commit()

    print(f"Request ID: {request_id} - Processing started")
    asyncio.run(process_csv(file_path, conn, request_id, webhook_url))
    conn.close()
    return request_id

# Step 12: API to check processing status using request ID
def check_status(request_id):
    conn = setup_database()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT status FROM requests WHERE request_id = ?
    ''', (request_id,))
    result = cursor.fetchone()
    conn.close()
    if result:
        return f"Request ID: {request_id} - Status: {result[0]}"
    else:
        return f"Request ID: {request_id} not found."

# Example usage:
# Submit a CSV file and get a request ID
csv_file_path = 'input.csv'  # Replace with the path to your CSV file
webhook_url = 'https://your-webhook-url.com/endpoint'  # Replace with your webhook URL
request_id = submit_csv(csv_file_path, webhook_url)

# Check the status of the processing using the request ID
status = check_status(request_id)
print(status)
