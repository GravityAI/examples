import requests
import sys
import json
import time
import os

# API URLs and constants
API_URL = "http://localhost:9090/"
API_CREATE_JOB_URL = API_URL + 'data/add-job'
API_GET_JOB_RESULT_URL = API_URL + 'data/result/json'
API_LICENSE_GET = API_URL + 'api/license'
API_LICENSE_URL = API_URL + 'api/license/file'
STATUS_URL = API_URL + 'data/status/'  # Status endpoint
LICENSE_FILE_NAME = 'your_license_file_name.gai_key'  # Update with your license file name
POLL_INTERVAL = 10  # Time in seconds between each status check

# Job configuration
config = {
    "MimeType": "text/csv; header=present",  # Change based on your file type
    "Mappings": "",  # Optional: Add Mapping objects as needed
    "OutputMappings": "",  # Optional: Add Mapping objects as needed
    "Pipeline": "",
    "CallbackUrl": "",
}

def check_license():
    """
    Check if the license exists by calling the API.
    
    Returns:
        bool: True if the license exists and is valid, False otherwise.
    """
    try:
        response = requests.get(API_LICENSE_GET)
        response.raise_for_status()  # Raise an error for bad status codes
        data = response.json()
        
        # Check license conditions
        if (data.get('data', '').startswith('Licensed To:') and 
            not data.get('isError', True) and 
            data.get('errorMessage') is None):
            return True
        else:
            print("License does not exist or is invalid.")
            return False
    except requests.RequestException as e:
        print(f"An error occurred: {e}")
        return False

def upload_license_file(file_name):
    """
    Upload the license file to the API.

    Args:
        file_name (str): The name of the license file to upload.
    """
    try:
        with open(file_name, 'rb') as file:
            files = {'License': file}
            response = requests.post(API_LICENSE_URL, files=files)
            response.raise_for_status()  # Raise an error for bad status codes
            print(response.status_code)
            print(response.text)
    except FileNotFoundError:
        print(f"File {file_name} not found.")
    except requests.RequestException as e:
        print(f"An error occurred while uploading the file: {e}")

def postJob(inputFilePath):
    """
    Post a new job (file) to the API and return the job ID.

    Args:
        inputFilePath (str): The path to the input file.

    Returns:
        str: The job ID if the job was submitted successfully, None otherwise.
    """
    try:
        with open(inputFilePath, 'rb') as inputFile:
            files = {"File": inputFile}
            data = {
                'Pipeline': config['Pipeline'],
                'Mappings': config['Mappings'],
                'OutputMappings': config['OutputMappings'],
                'CallbackUrl': config['CallbackUrl'],
                'MimeType': config['MimeType']
            }
            response = requests.post(API_CREATE_JOB_URL, data=data, files=files)
            response.raise_for_status()  # Raise an error for bad status codes
            result = response.json()
            
            if result.get('isError', False):
                print("Error: " + result.get('errorMessage'))
                return None
            
            if result.get('data').get('status') != "Created":
                print("Job Failed: " + (result.get('data').get('errorMessage') or 'No error message provided'))
                return None
            
            return result.get('data').get('id')
    except requests.RequestException as e:
        print(f"An error occurred while posting the job: {e}")
        print(f"Response content: {e.response.content if e.response else 'No response content'}")
        return None

def poll_job_status(job_id):
    """
    Poll the job status until it is complete.

    Args:
        job_id (str): The ID of the job to poll.

    Returns:
        bool: True if the job completed successfully, False otherwise.
    """
    while True:
        try:
            response = requests.get(f"{STATUS_URL}{job_id}")
            response.raise_for_status()  # Raise an error for bad status codes
            status_data = response.json()

            # Check if there's an error
            if status_data.get('isError', False):
                print(f"Job error: {status_data.get('errorMessage')}")
                return False

            # Check the job status
            if status_data.get('data', {}).get('status') == 'Complete':
                print("Job completed.")
                return True
            else:
                print(f"Job status: {status_data.get('data', {}).get('status', 'Unknown')}. Checking again in {POLL_INTERVAL} seconds.")
                time.sleep(POLL_INTERVAL)
        except requests.RequestException as e:
            print(f"An error occurred while checking the job status: {e}")
            return False

def displayResult(jobId):
    """
    Display the result of the job.

    Args:
        jobId (str): The ID of the job to display the result for.
    """
    try:
        url = f"{API_GET_JOB_RESULT_URL}/{jobId}"
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad status codes
        result = response.json()
        print(result)
        
    except requests.RequestException as e:
        print(f"An error occurred while fetching the job result: {e}")

def print_help():
    """
    Print the help message with usage instructions.
    """
    print("""
Usage: python local_job.py [input_file]

This script checks for a valid license, submits a job to the API, polls the job status, and displays the result.

Arguments:
  input_file    The path to the input file to be processed.

Make sure to place the license file in the same directory as this script.
The API is assumed to be running on localhost:9090. Update the API_URL constant if your container is set up differently.
""")

if __name__ == '__main__':
    if len(sys.argv) != 2 or sys.argv[1] == '-h':
        print_help()
        exit(1)

    # Ensure the license file is in the same directory as the script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    license_file_path = os.path.join(script_dir, LICENSE_FILE_NAME)
    
    # Check if the license exists, upload if necessary
    if not check_license():
        upload_license_file(license_file_path)
        if not check_license():
            print("Error: License upload failed. Please check the license file and try again.")
            exit(1)

    # Submit the job and get the job ID
    jobId = postJob(sys.argv[1])
    print("Job Id: ", jobId)
    if jobId is None:
        print("Error: Job submission failed.")
        exit(1)

    # Poll for job status until completed
    if poll_job_status(jobId):
        # Display the result if the job is completed successfully
        displayResult(jobId)
    else:
        print("Error: Job did not complete successfully. Please check the job status and try again.")
        exit(1)
