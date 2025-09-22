# -*- coding: utf-8 -*-
# gcp_getsecret
from google.cloud import secretmanager

def get_gcp_secret(project_id: str, secret_id: str, version_id: str = "latest") -> str:
    """
    Retrieves a secret from Google Cloud Secret Manager.

    This function accesses the specified version of a secret from the given
    Google Cloud project and returns the secret's payload as a decoded string.

    Args:
        project_id (str): The Google Cloud project ID.
        secret_id (str): The ID of the secret to access.
        version_id (str): The version of the secret to access. 
                          Defaults to "latest".

    Returns:
        str: The decoded secret payload.
        
    Raises:
        google.api_core.exceptions.GoogleAPICallError: If the API call fails.
        google.api_core.exceptions.NotFound: If the secret or version is not found.
    """
    try:
        # Create the Secret Manager client.
        client = secretmanager.SecretManagerServiceClient()

        # Build the resource name of the secret version.
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

        # Access the secret version.
        response = client.access_secret_version(request={"name": name})

        # Decode the payload to a UTF-8 string.
        secret_payload = response.payload.data.decode("UTF-8")

        return secret_payload

    except Exception as e:
        print(f"An error occurred while trying to access the secret {secret_id}: {e}")
        # Depending on your error handling strategy, you might want to return None,
        # an empty string, or re-raise the exception.
        raise