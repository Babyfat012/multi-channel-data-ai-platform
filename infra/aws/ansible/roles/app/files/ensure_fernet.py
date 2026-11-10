"""Create the Fernet key once on the data disk and keep it unchanged on reruns."""

import base64
import os
import secrets
import sys
from pathlib import Path


def main():
    """Write exclusively when no key exists and validate an existing key; never print the value."""
    target = Path(sys.argv[1])
    try:
        with target.open("x") as handle:
            os.chmod(target, 0o600)
            handle.write(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode() + "\n")
        print("created")
    except FileExistsError:
        if len(base64.urlsafe_b64decode(target.read_text().strip())) != 32:
            raise ValueError("Existing Fernet key is invalid; refusing to overwrite") from None
        print("existing")
    os.chown(target, int(sys.argv[2]), 0)
    os.chmod(target, 0o600)


if __name__ == "__main__":
    main()
