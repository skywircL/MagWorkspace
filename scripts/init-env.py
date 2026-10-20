"""Create local development credentials once, without printing them."""
from pathlib import Path
import os
import secrets

root = Path(__file__).resolve().parents[1]
text = (root / ".env.example").read_text()
text = text.replace("replace-with-a-local-password", secrets.token_hex(24))
text = text.replace("replace-with-a-different-root-password", secrets.token_hex(24))
text = text.replace("replace-with-minio-access-key", secrets.token_hex(12))
text = text.replace("replace-with-minio-secret-key", secrets.token_hex(24))
text = text.replace("replace-with-qdrant-api-key", secrets.token_hex(24))
text = text.replace("replace-with-redis-password", secrets.token_hex(24))
text = text.replace("replace-with-harness-service-key", secrets.token_hex(32))
text = text.replace("replace-with-reranker-api-key", secrets.token_hex(32))
descriptor = os.open(root / ".env", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(descriptor, "w") as output:
    output.write(text)
print("Created .env with local development credentials.")
