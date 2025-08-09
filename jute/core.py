import json
import logging
import time
import traceback
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from textwrap import dedent
from typing import ClassVar, overload

from pydantic import BaseModel, Field, TypeAdapter, ValidationError
from typing_extensions import Self

from .__about__ import __version__
from .__types import DictData, StrOrPath
from .conf import substitute_env_vars
from .engine import Registry
from .errors import JuteCoreError, JuteValidationError
from .models import Context, Result
from .utils import get_dt_now, load_yaml, write_yaml

logger = logging.getLogger("jute")


class JuteModel(BaseModel):
    """Core Jute model that keep the parsing engine model and the original
    config data.
    """

    model: Registry = Field(
        description="A Jute model that already create on the registry module.",
    )
    data: DictData = Field(
        description="A configuration data that store with YAML template file."
    )
    extras: DictData = Field(
        default_factory=dict, description="An extra parameters."
    )
    created_at: datetime = Field(
        default_factory=get_dt_now,
        description=(
            "A created at of this config data if it load from YAML "
            "template file. It will use the current init time when this model "
            "created from dict."
        ),
    )

    @classmethod
    def from_yaml(
        cls,
        path: StrOrPath,
        *,
        extras: DictData | None = None,
    ) -> Self:
        """Construct Jute model with a YAML file path.

        Args:
            path (str | Path): A YAML path that want to load data.
            extras (DictData, default None): An extra parameters.
        """
        return cls.from_dict(load_yaml(path), extras=extras)

    @classmethod
    def from_dict(
        cls,
        data: DictData,
        *,
        extras: DictData | None = None,
    ) -> Self:
        """Construct Jute model with a dict object.

        Args:
            data (DictData): A dict value.
            extras (DictData, default None): An extra parameter.

        Raises:
            JuteValidationError: If model validation was failed.
        """
        try:
            _extras: DictData = extras or {}
            _data: DictData = data | _extras
            return cls.model_validate(
                {"model": _data, "data": _data, "extras": _extras}
            )
        except ValidationError as e:
            logger.exception(f"Create JuteModel from dict failed:\n\t{e}")
            raise JuteValidationError(e) from e

    def fetch(self, override: DictData | None = None) -> Self:
        """Recreate JuteModel `model` field with the current configuration data.

        Returns:
            Self: The new JuteModel instance with the current config data.
        """
        try:
            self.__dict__["model"] = TypeAdapter(Registry).validate_python(
                override or self.data
            )
        except ValidationError as e:
            logger.exception(f"Fetch model failed:\n\t{e}")
            raise JuteValidationError(e) from e


class Operator:
    """Jute Operator object is tha main Python interface object for this
    package.

    Attributes:
        c (JuteModel): A Jute model that include the `model` and `data` fields.
        _path (str | Path, default None):
        _config (DictData, default None):
    """

    @overload
    def __init__(self, *, path: StrOrPath) -> None:
        """Main construction with path of the YAML file."""

    @overload
    def __init__(self, *, config: DictData) -> None:
        """Main construction with dict of data."""

    def __init__(
        self,
        *,
        path: StrOrPath | None = None,
        config: DictData | None = None,
    ) -> None:
        """Main construction

        Args:
            path:
            config:

        Raises:
            JuteCoreError: If path and config parameters was pass together.
            JuteCoreError: If path and config be None value together.
        """

        # NOTE: Validate parameter with rule that should not supply both of its.
        if path and config:
            raise JuteCoreError("Please pass only one for `path` nor `config`.")
        elif not path and not config:
            raise JuteCoreError(
                "Both of `path` and `config` must not be empty."
            )

        self._path: StrOrPath | None = path
        self._config: DictData | None = config
        self.c: JuteModel = (
            JuteModel.from_yaml(path) if path else JuteModel.from_dict(config)
        )
        self.post_init()

    def __str__(self) -> str:
        """Override the `__str__` dunder method."""
        return f"Jute Operator Engine: {self.c.model.type}"

    def post_init(self) -> None:
        """Post initialize method."""

    def refresh(self) -> Self:
        """Refresh the config argument with reload again from config path or
        config dict object that pass from the initialize step.
        """
        self.c: JuteModel = (
            JuteModel.from_yaml(self._path)
            if self._path
            else JuteModel.from_dict(self._config)
        )
        return self

    def execute(
        self,
        *,
        allow_raise: bool = False,
    ) -> Result:
        """Execute the Jute operator with it model configuration.

        Args:
            allow_raise: bool
                If set be True, it will raise the error when execution was
                failed.

        Returns:
            Result: A Result object that already return from config engine
            execution result.
        """
        ts: float = time.monotonic()
        start_date: datetime = get_dt_now()
        context: Context = {
            "author": self.c.model.author,
            "owner": self.c.model.owner,
            "parent_dir": self.c.model.parent_dir,
        }
        rs: Result = Result()
        exception: Exception | None = None
        exception_name: str | None = None
        exception_traceback: str | None = None
        run_result: bool = True
        logger.info(f"🚀 Jute version: {__version__}")
        logger.info("Start core operator execution:")
        logger.info(f"... 👷 Author: {self.c.model.author}")
        logger.info(f"... 👨‍💻 Owner: {self.c.model.owner}")
        logger.info(f"... 🕒 Start: {start_date:%Y-%m-%d %H:%M:%S}")
        try:
            # IMPORTANT: Recreate model before start handle execution.
            #   We replace env var template before execution for secrete value.
            logger.info(
                "🔐 Passing environment variables to the Jute config data and "
                "refresh JuteModel before execute."
            )
            self.c.fetch(override=substitute_env_vars(self.c.data))
            rs: Result = self.c.model.handle_execute(context=context)
            logger.info("✅ Execute successful!!!")
        except Exception as e:
            run_result = False
            exception = e
            exception_name = e.__class__.__name__
            exception_traceback = traceback.format_exc()
            logger.error(f"😎👌🔥 Execution catch: {exception_name}")
            logger.error(f"Trackback:\n{exception_traceback}")
        finally:
            # NOTE: Start prepare metric data after engine execution end. It
            #   will catch error exception class to this metric data if the
            #   execution raise an error.
            emit_context: Context = context | {
                "run_result": run_result,
                "execution_time_ms": time.monotonic() - ts,
                "execution_start_time": start_date,
                "execution_end_time": get_dt_now(),
                "exception": exception,
                "exception_name": exception_name,
                "exception_traceback": exception_traceback,
            }
            # NOTE: Raise the raised exception class if the raise flag allow
            #   from method parameter.
            if allow_raise and exception:
                logging.error(json.dumps(emit_context, default=str, indent=1))
                raise exception

            self.c.model.emit(context=emit_context)
        return rs

    def ui(self):
        """Generate UI for this valid Jute configuration data."""


class SparkSubmitOperator(Operator):
    """Jute Spark Submit Operator."""

    yaml_filename: ClassVar[str] = "jute_conf.yml"
    python_filename: ClassVar[str] = "jute_pyspark.py"
    python_code: ClassVar[str] = dedent(
        """
        from jute import Operator
        opt = Operator(config_file="{file}")
        opt.execute()
        """.lstrip(
            "\n"
        )
    )

    def post_init(self) -> None:
        """Post validate model type after create JuteModel that should to be the
        `spark` or `spark-submit` types
        """
        if self.c.model.type not in ("spark", "spark-submit"):
            raise JuteValidationError(
                f"Spark Submit Operator does not support for engine type: "
                f"{self.c.model.type!r}"
            )

    def set_conf_files(self, temp_path: Path) -> None:
        """Add Jute yaml file in `spark-submit --files`"""
        file: str = f"file://{temp_path.absolute()}/{self.yaml_filename}"
        logger.info(f"config file path: {file}")
        files = self.c.data.get("files", [])
        files.append(file)
        self.c.data["files"] = files

    def set_conf_entrypoint(self, temp_path: Path) -> None:
        """Write entrypoint of program and set entrypoint for spark-submit."""
        filepath = f"{temp_path}/{self.yaml_filename}"
        write_yaml(filepath, data=self.c.data)

        py_filepath = f"{temp_path}/{self.python_filename}"
        logger.info(f"pyspark entrypoint file path: {py_filepath}")

        # NOTE:
        #   - for spark-submit YARN cluster mode, file will be place in working
        #     directory
        #   - for local mode, it uses the temporary path created by Python
        #     Temporary directory
        config_filepath = self.yaml_filename
        if self.c.data.get("master", "").startswith("local"):
            config_filepath: str = py_filepath

        code: str = self.python_code.format(file=config_filepath)
        with open(py_filepath, mode="w", encoding="utf-8") as f:
            f.write(code)

        self.c.data["entrypoint"] = py_filepath
        self.c.data["type"] = "spark-submit"

    def execute(
        self,
        *,
        allow_raise: bool = False,
    ) -> Result:
        """Prepare spark-submit command before run.

        Args:
            allow_raise: bool
                If set be True, it will raise the error when execution was
                failed.
        """
        from .engine.spark import SparkSubmit

        ts: float = time.monotonic()
        start_date: datetime = get_dt_now()
        context: Context = {
            "author": self.c.model.author,
            "owner": self.c.model.owner,
            "parent_dir": self.c.model.parent_dir,
        }
        exception: Exception | None = None
        exception_name: str | None = None
        exception_traceback: str | None = None
        run_result: bool = True
        logger.info(f"Jute version: {__version__}")
        logger.info("Start handle execution:")
        logger.info(f"... Author: {self.c.model.author}")
        logger.info(f"... Execute Start: {start_date:%Y-%m-%d %H:%M:%S}")
        try:
            with TemporaryDirectory(prefix="jute-") as tmp:
                temp_path: Path = Path(tmp)
                self.set_conf_files(temp_path)
                self.set_conf_entrypoint(temp_path)
                self.c.fetch(override=substitute_env_vars(self.c.data))
                model: SparkSubmit = SparkSubmit.model_validate(self.c.data)
                model.submit(context=context)
        except Exception as e:
            run_result = False
            exception = e
            exception_name = e.__class__.__name__
            exception_traceback = traceback.format_exc()
            logger.error(f"Execution catch error: {e.__class__.__name__}")
            logger.error(f"Trackback: {exception_traceback}")
        finally:
            emit_context: DictData = context | {
                "run_result": run_result,
                "execution_time_ms": time.monotonic() - ts,
                "execution_start_time": start_date,
                "execution_end_time": get_dt_now(),
                "exception_name": exception_name,
                "exception_traceback": exception_traceback,
            }
            logging.error(json.dumps(emit_context, default=str, indent=2))
            if allow_raise and exception:
                raise exception

        return Result()
