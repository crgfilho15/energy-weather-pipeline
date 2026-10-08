import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path

import kagglehub

from src import config
from src.storage import get_s3_client, object_exists, upload_file

DATASET = "robikscube/hourly-energy-consumption"
FILENAME = "PJME_hourly.csv"
KEY = "raw/source=pjm/region=pjme/PJME_hourly.csv"


def sha256_of(path):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    s3 = get_s3_client(config.AWS_PROFILE, config.AWS_REGION)

    if object_exists(s3, config.S3_BUCKET, KEY):
        logging.info("skip (exists): %s", KEY)
        return

    path = Path(kagglehub.dataset_download(DATASET)) / FILENAME
    metadata = {
        "source": "kaggle",
        "dataset": DATASET,
        "sha256": sha256_of(path),
        "ingested-at": datetime.now(timezone.utc).isoformat(),
    }
    upload_file(s3, config.S3_BUCKET, KEY, path, metadata)
    logging.info("uploaded: %s (%d bytes)", KEY, path.stat().st_size)


if __name__ == "__main__":
    main()
