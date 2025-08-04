import logging
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()
load_dotenv(Path(__file__).parent.parent / ".env")

logging.getLogger("py4j").setLevel(logging.INFO)
logging.getLogger("pyspark").setLevel(logging.WARNING)
