"""Publish the synthetic event stream to Pub/Sub.

    python infra/publish.py --project my-project --topic lease-events [--file data/events.jsonl]
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from google.cloud import pubsub_v1  # noqa: E402

from lease_demo import generator  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--project", required=True)
ap.add_argument("--topic", default="lease-events")
ap.add_argument("--file")
a = ap.parse_args()

lines = Path(a.file).read_text().splitlines() if a.file else generator.generate()
pub = pubsub_v1.PublisherClient()
path = pub.topic_path(a.project, a.topic)
futures = [pub.publish(path, line.encode()) for line in lines]
for f in futures:
    f.result()
print(f"published {len(lines)} messages to {path}")
