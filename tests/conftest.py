import logging
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()
load_dotenv(Path(__file__).parent.parent / ".env")

# NOTE: Set logging level for Spark engine.
logging.getLogger("py4j").setLevel(logging.INFO)
logging.getLogger("pyspark").setLevel(logging.WARNING)

# NOTE: Set logging level for Daft engine.
logging.getLogger("daft_io").setLevel(logging.INFO)
logging.getLogger("daft_logical_plan").setLevel(logging.INFO)
