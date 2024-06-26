import os
import sys
from logging import Logger

from dotenv import load_dotenv
from pyaml_env import parse_config
import logging.handlers

from common.utils import Singleton


class Config(metaclass=Singleton):
    _allowed_logger_names = []

    def __init__(self):
        load_dotenv(dotenv_path=self.get_abs_main_path(".env"), override=True)

        self.data = parse_config(self.get_abs_main_path("./application.yaml"), encoding="utf-8")
        self.profiles: dict = self.data["profiles"]

        self.attachment_path = self.data["attachments"]["path"]
        self._logging_init()
        sys.excepthook = logging_excepthook

    @staticmethod
    def get_abs_main_path(path_from_main_root: str):
        import __main__
        return os.path.join(os.path.dirname(os.path.abspath(__main__.__file__)), path_from_main_root)

    @staticmethod
    def get_logger(logger_name: str) -> Logger:
        if not Config._is_root_logger_configured(logger_name):
            raise Exception(f"Логгер {logger_name} не задан в application.yaml")
        return logging.getLogger(logger_name)

    @staticmethod
    def _is_root_logger_configured(logger_name: str) -> bool:
        if logger_name.count(".") > 0:
            _log_name = ""
            for part in logger_name.split("."):
                _log_name = f"{_log_name}.{part}" if _log_name != "" else part
                if _log_name in Config._allowed_logger_names:
                    return True
        else:
            return logger_name in Config._allowed_logger_names

    @staticmethod
    def get_common_logger() -> Logger:
        return Config.get_logger("mailModule")

    def _logging_init(self):
        # TODO: попробовать loguru
        log_path = self.data["logging"]["path"]
        backup_count = self.data["logging"]["backupCount"]
        max_megabytes = self.data["logging"]["maxMegaBytes"]
        loggers = self.data["logging"]["loggers"]

        logging.basicConfig(
                level=logging.ERROR,
                format="%(asctime)s [%(levelname)s] %(name)s - %(filename)s - %(funcName)s: %(message)s",
                handlers=[
                    logging.StreamHandler(),
                    logging.handlers.RotatingFileHandler(
                            filename=log_path,
                            maxBytes=max_megabytes * 1024 * 1024,
                            backupCount=backup_count,
                            encoding="UTF-8",
                    ),
                ],
        )
        for logger in loggers:
            lvl = logging.getLevelName(logger["lvl"])
            logger_name = logger["name"]
            logging.getLogger(logger_name).setLevel(lvl)
            self._allowed_logger_names.append(logger_name)


def logging_excepthook(excType, excValue, traceback):
    Config.get_common_logger().error(
        "Logging an uncaught exception", exc_info=(excType, excValue, traceback)
    )
