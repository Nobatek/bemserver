#!/usr/bin/env python3
"""Launch MQTT acquisition service."""

# import os
# import sys
# import argparse
# import time
# import re
# import logging
# from logging.handlers import TimedRotatingFileHandler
# from pathlib import Path

# import bemserver.services.acquisition_mqtt as svc


# TODO: config -> database file path
# TODO: config -> working dir path (to generate certificate from DB)


# svc_log = logging.getLogger(svc.SERVICE_LOGNAME)


# def main():
#     """Main program."""
#     # parse command line arguments
#     cmd_args = parse_command_line(sys.argv)

#     # initialize config from INI file
#     config_filepath = Path(cmd_args.config_filepath)
#     # svc_config = ServiceConfig(config_filepath)

#     # initialize logger
#     init_logger(svc_config, verbose=cmd_args.verbose)

#     # launch service scheduler
#     # app_log.info('Launching service scheduler (PID: %s)', current_pid)
#     # launch_scheduler(svc_config)

#     return True


# def _argtype_readable_file(str_value):
#     """Check that a path value exists as a valid readable file.

#     :param str str_value: Input string argument value.
#     :returns Path: A readable file absolute path.
#     :raises argparse.ArgumentTypeError:
#         When the path value is either not a valid file or not readable.
#     """
#     file_path = Path(str_value)
#     if not file_path.is_file():
#         raise argparse.ArgumentTypeError(
#             'invalid readable_file value, not a file: {}'.format(file_path))
#     absolute_file_path = file_path.resolve()
#     if not os.access(str(absolute_file_path), os.R_OK):
#         raise argparse.ArgumentTypeError(
#             'invalid readable_file value, not readable: {}'.format(file_path))
#     return absolute_file_path


# def parse_command_line(argv):
#     """Parse command line argument. See -h option

#     :param list argv: arguments on command line must include caller file name.
#     :returns argparse.Namespace: argument values.
#     """
#     formatter_class = argparse.RawDescriptionHelpFormatter
#     parser = argparse.ArgumentParser(
#         description=svc.__description__, formatter_class=formatter_class)

#     parser.add_argument(
#         '--version', action='version',
#         version='{} ({}) v{}\n{}\n(c) 2018 {}'.format(
#             svc.__binname__, svc.__appname__, svc.__version__,
#             svc.__description__, svc.__author__))

#     parser.add_argument(
#         dest='config_filepath', type=_argtype_readable_file,
#         help='configuration file path to use')
#     parser.add_argument(
#         '-v', '--verbose', dest='verbose', action='store_true', default=False,
#         help='print log messages')

#     arguments = parser.parse_args(argv[1:])

#     return arguments


# def init_logger(svc_config, *, verbose=False):
#     """
#     :param ServiceConfig svc_config: An instance of service configuration.
#     :param bool verbose: (optional, default False)
#         Print log messages in console output.
#     """
#     # Create our custom record formatters
#     formatter = logging.Formatter(svc_config.log_format)
#     formatter.converter = time.gmtime

#     # Configure root logger
#     logging.root.setLevel(svc_config.log_level)
#     # Create a stream handler (console out) for root logger
#     if verbose:
#         stream_handler = logging.StreamHandler()
#         stream_handler.setLevel(logging.NOTSET)  # inherits root log's level
#         stream_handler.setFormatter(formatter)
#         logging.root.addHandler(stream_handler)
#     # Create a daily rotated log file handler
#     # See example: http://stackoverflow.com/a/25387192
#     if svc_config.log_dirpath is not None:
#         logfile_handler = TimedRotatingFileHandler(
#             str(svc_config.log_dirpath / '{}.log'.format(svc.__binname__)),
#             when='midnight', backupCount=svc_config.log_history, utc=True)
#         logfile_handler.suffix = '%Y-%m-%d'
#         logfile_handler.extMatch = re.compile(r'^\d{4}-\d{2}-\d{2}$')
#         logfile_handler.setLevel(logging.NOTSET)  # inherits root log's level
#         logfile_handler.setFormatter(formatter)
#         logging.root.addHandler(logfile_handler)

#     # Configure all loggers to inherit from root's log level
#     svc_log.setLevel(logging.NOTSET)

#     svc_log.info('Logger initialized, [%s] level.', svc_config.log_levelname)
#     if svc_config.log_dirpath is not None:
#         svc_log.info('Current log folder path ([%d] days backup): [%s]',
#                      svc_config.log_history, str(svc_config.log_dirpath))


# if __name__ == "__main__":

#     sys.exit(main())
