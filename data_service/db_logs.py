import logging
import colorlog


class DBLogs:
    _logger = None

    def __new__(cls, *args, **kwargs):
        if cls._logger is None:
            cls._logger = super().__new__(cls, *args, **kwargs)
            cls._logger = logging.getLogger()

            c_handler = logging.StreamHandler()
            f_handler = logging.FileHandler('applogs.log')
            c_handler.setLevel(logging.DEBUG)
            f_handler.setLevel(logging.DEBUG)

            # Set the log color
            color_formatter = colorlog.ColoredFormatter(
                "%(log_color)s[%(levelname)s]-%(message)s%(reset)s",
                log_colors={
                    'DEBUG': 'cyan',
                    'INFO': 'green',
                    'WARNING': 'yellow',
                    'ERROR': 'red',
                    'CRITICAL': 'red',
                }
            )
            c_handler.setFormatter(color_formatter)

            file_formatter = logging.Formatter('[%(levelname)s]-%(message)s')
            f_handler.setFormatter(file_formatter)

            cls._logger.setLevel(logging.DEBUG)
            cls._logger.addHandler(c_handler)
            cls._logger.addHandler(f_handler)

        return cls._logger

