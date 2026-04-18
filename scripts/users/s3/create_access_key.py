from datetime import datetime
import json
import subprocess


def create_minio_user_with_policy(
    alias_name: str,
    access_key_name: str,
    policy_name: str,
    policy_document: dict | None = None,
) -> None:
    """
    Create a MinIO user with a pre-defined policy using mc (MinIO Client).

    Args:
        endpoint: MinIO server endpoint (e.g., 'minio.example.com')
        admin_access_key: Admin access key
        admin_secret_key: Admin secret key
        new_user: Username for the new user
        new_password: Password for the new user
        policy_name: Name of the policy to create/attach
        policy_document: Custom policy document (optional)
        secure: Use HTTPS (default True)

    Returns:
        Dictionary containing the new user credentials
    """
    # mc command
    mc_command = [
        "mc",
        "admin",
        "accesskey",
        "create",
        "--name",
        access_key_name,
        "--description",
        f"Créée le {datetime.now().strftime(format='%d/%m/%Y %H:%M:%S')}",
        alias_name,
    ]

    # Create the policy
    if policy_document is not None:
        policy_file = f"/tmp/{policy_name}.json"
        with open(file=policy_file, mode="w") as f:
            json.dump(obj=policy_document, fp=f)

        mc_command.extend(["--policy", policy_file])

    # Attach the policy to the user
    try:
        result = subprocess.run(mc_command, check=True, capture_output=True, text=True)
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Command failed with exit code {e.returncode}")
        print(f"Error output: {e.stderr}")
        raise


if __name__ == "__main__":
    from scripts.settings import get_settings

    settings = get_settings()

    # Define variables
    ALIAS_NAME = settings.s3_user.alias_name
    BUCKET = settings.s3_user.bucket
    KEY_ACCESS = settings.s3_user.key_access
    POLICY_NAME = settings.s3_user.policy_name
    ACCESS_KEY_NAME = (
        f"auto-{BUCKET.lower()}-{datetime.now().strftime(format='%Y%m%d-%H%M%S')}"
    )

    # Define policy
    policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                "Resource": [f"arn:aws:s3:::{BUCKET}/{KEY_ACCESS}"],
            },
            {
                "Effect": "Allow",
                "Action": ["s3:ListBucket"],
                "Resource": [f"arn:aws:s3:::{BUCKET}"],
                "Condition": {"StringLike": {"s3:prefix": [f"{KEY_ACCESS}/*"]}},
            },
        ],
    }

    # Example usage
    create_minio_user_with_policy(
        alias_name=ALIAS_NAME,
        access_key_name=ACCESS_KEY_NAME,
        policy_name=POLICY_NAME,
        policy_document=policy_document,
    )
