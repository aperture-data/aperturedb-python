#!/usr/bin/env python3

import argparse
import os
import logging
import re
from urllib.parse import urlparse, urlunparse, ParseResult
import ipaddress
import sys
from typing import Union, Optional, Callable, Any
from collections.abc import Container
from contextlib import contextmanager
import itertools

# This class wraps argparse.ArgumentParser and adds a number of additional features:
#
# 1. Support for environment variables.
# Each argument automatically defaults to being set from a WF_ environment variable.
# The environment variable name is derived from the argument name by removing
# dashes and converting to uppercase, e.g. --crawl becomes WF_CRAWL.
# In legacy mode, if the `WF_` envar is not present, it will also check for the
# legacy envar name (without the `WF_` prefix).
# The intention of using the `WF_` prefix is to allow the code to detect
# # misspellings or unsupported parameters.
# If the environment variable is not set, the default value is used.
#
# 2. Fix bool from string
# Boolean values can be specified as strings like "true" or "false".
#
# 3. Support for lists
# argparse already supports lists by repeated argument,
# but this class adds support for separators in the environment variable,
# using the "sep" parameter which can be either a string or a regex.
#
# 4. Registry for type sanitation, validation, conversion
# This class provides a registry for type sanitation, validation, and conversion.
# If the type is specified as a string, it is looked up in the registry.
#
# 5. Freestanding validate function that can be used outside of this class
# This is convenient in cases where creating an ArgumentParser instance is too heavyweight.
#     validate(validator_type, value, envar, default, hidden, raise_errors, force_string, sep)
#
# 6. CLI
# This module also provides a CLI interface for the registry
# which can be used to sanitize and validate untrusted input in bash scripts.
#     VERIFY_HOSTNAME=$(wf-argparse --type bool --envar VERIFY_HOSTNAME --default true)
#
# usage: wf_argparse.py [-h] --type
#                       {bool,environment,hostname,log_level,non_negative_float,non_negative_int,port,positive_int,shell_safe,slug,string,web_url}
#                       [--envar ENVAR] [--value VALUE] [--default DEFAULT]
#                       [--hidden | --no-hidden] [--raise | --no-raise]
#                       [--log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}]

# Validate and sanitize workflow parameter values.

# options:
#   -h, --help            show this help message and exit
#   --type {bool,environment,hostname,log_level,non_negative_float,non_negative_int,port,positive_int,shell_safe,slug,string,web_url}
#                         Validator type to apply.
#   --envar ENVAR         Environment variable name. If --value is omitted, the
#                         value will be read from this variable.
#   --value VALUE         Value to validate. If omitted, value will be read from
#                         the environment variable.
#   --default DEFAULT     Default value to use if --value is omitted or empty
#                         and --envar is unset or empty.
#   --hidden, --no-hidden
#                         Whether the value is hidden. If set, the value will
#                         not appear in logging or error messages. (default:
#                         False)
#   --raise, --no-raise   Raise on validation error (use --no-raise to always
#                         exit 0). (default: True)
#   --log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}
#                         Logging verbosity level (default: WARNING).


# The WF_ prefix is also used by other parts of the workflow infrastructure,
# so we need to ignore some of them.

ENVAR_IGNORE_LIST = {"WF_LOGS_AWS_BUCKET", "WF_LOGS_AWS_CREDENTIALS", }

SEP_COMMA = ","
SEP_WHITESPACE = re.compile(r"\s+")

logger = logging.getLogger(__name__)


@contextmanager
def temporary_logging(level=logging.INFO, stream=sys.stderr, fmt="%(levelname)s: %(message)s"):
    """
    Context manager to temporarily enable minimal logging during parsing.
    Restores prior handlers and levels afterward.
    """
    root = logging.getLogger()
    old_handlers = list(root.handlers)
    old_level = root.level
    try:
        # Clear handlers and apply minimal setup
        for h in old_handlers:
            root.removeHandler(h)
        logging.basicConfig(level=level, format=fmt, stream=stream)
        yield
    finally:
        # Restore previous configuration
        root.handlers = old_handlers
        root.setLevel(old_level)


class ArgumentParser:
    def __init__(self, *args, support_legacy_envars=False, **kwargs):
        self.parser = argparse.ArgumentParser(*args, **kwargs)
        self.envars_used = set()
        self.support_legacy_envars = support_legacy_envars
        self.list_args = set()
        self.hidden_args = set()

    def add_argument(self, *names, required=False, type=None, default=None, sep=None, hidden=False,
                     **kwargs):
        # Force bool and str into validated types
        if type is bool:
            type = "bool"
        elif type in (str, None):
            type = "string"

        action = 'append' if sep is not None else None
        name = names[0]
        default = self.get_default(name, default, sep)
        required = required and default is None

        if isinstance(type, str):
            def validator(v): return validate(
                type, v, hidden=hidden, default=default, sep=sep)
            validator.__name__ = type  # for error messages
        else:
            validator = type

        result = self.parser.add_argument(
            *names, type=validator, default=default, action=action, required=required, **kwargs)

        if sep is not None:
            self.list_args.add(result.dest)

        if hidden:
            self.hidden_args.add(result.dest)

        return result

    def _envar_name(self, name: str):
        while name.startswith('-'):
            name = name[1:]
        name = name.upper().replace('-', '_')
        return "WF_" + name

    def get_default(self, name: str, default=None, sep=None):
        envar_name = self._envar_name(name)
        self.envars_used.add(envar_name)
        legacy_envar_name = envar_name[3:]  # remove WF_
        logger.debug(
            f"name: {name}, envar_name: {envar_name}, legacy_envar_name: {legacy_envar_name}")
        if envar_name in os.environ:
            result = os.environ.get(envar_name)
            # Treat empty strings from environment variables as unset
            # This handles cases where infrastructure sets env vars to empty strings
            if result == "":
                result = default
            else:
                logging.info(
                    f"Using {envar_name} from environment -> {result}")
        elif self.support_legacy_envars and legacy_envar_name in os.environ:
            result = os.environ.get(legacy_envar_name)
            # Treat empty strings from environment variables as unset
            if result == "":
                result = default
            else:
                logging.info(
                    f"Using legacy {legacy_envar_name} from environment -> {result}")
        else:
            result = default
        if sep and result is not None:
            if isinstance(sep, str):
                return result.split(sep)
            elif isinstance(sep, re.Pattern):
                return sep.split(result)
            else:
                raise ValueError(f"Invalid separator: {sep}")

        return result

    def check_envars(self):
        unused_envars = set()
        for envar in os.environ:
            if envar.startswith("WF_") \
                and envar not in self.envars_used \
                    and envar not in ENVAR_IGNORE_LIST:
                logging.error(f"Environment variable {envar} is not used.")
                unused_envars.add(envar)
        assert not unused_envars, "Unused environment variables found: " + \
            ", ".join(unused_envars)

    def parse_args(self, *args, **kwargs):
        with temporary_logging(level=logging.DEBUG):
            self.check_envars()
            result = self.parser.parse_args(*args, **kwargs)
            # Fix up list args so they don't have two levels of nesting
            for name in self.list_args:
                if hasattr(result, name) and isinstance(result.__dict__[name], list):
                    # Conditionally flatten list of lists into list of values
                    items = []
                    for item in result.__dict__[name]:
                        if isinstance(item, list):
                            items.extend(item)
                        else:
                            items.append(item)
                    result.__dict__[name] = items
            display_args = {
                k: v if k not in self.hidden_args else '**HIDDEN**' for k, v in result.__dict__.items()}
            logger.info(f"Parsed arguments: {display_args}")
            return result

    # proxy additional methods to self.parser
    def __getattr__(self, name):
        if name in self.__dict__:
            return self.__dict__[name]
        if hasattr(self.parser, name):
            return getattr(self.parser, name)
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'"
        )

# Validator functions perform various operations on the value:
# 1. Sanitize the value, for example strip and upper case it
# 2. Validate the value, for example check if it is a valid log level
# 3. Convert the value into an appropriate Pythonic type
#
# If validation fails, then an ArgumentTypeError is raised.
# If force_string is True, then the return value is either a string or a stringifiable type.


def validate_log_level(v: str, *, force_string=False) -> Union[int, str]:
    """
    Checks a logging level.
    Expects a string like "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL".
    Returns a numeric code.
    """
    v = v.strip().upper()
    # Canonicalize against logging module
    level = logging.getLevelName(v)
    # logging.getLevelName() is bi-directional: int -> str, str -> int
    if isinstance(level, str):
        # Unknown level string
        raise argparse.ArgumentTypeError(f"Unknown log level")
    if force_string:
        return logging.getLevelName(level)  # return canonical name (string)
    return level  # return numeric code


def validate_bool(v, *, force_string=False) -> Union[bool, str]:
    """
    Checks a boolean value.
    Expects a string like "true", "false".
    Returns a boolean value.
    """
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        result = True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        result = False
    else:
        raise argparse.ArgumentTypeError(f"Boolean value expected")
    if force_string:
        return "true" if result else "false"  # JSON
    return result


class URL(ParseResult):
    """
    A wrapper around urllib.parse.ParseResult that provides a useful string representation.
    """

    def __str__(self):
        return urlunparse(self)


def validate_web_url(v, *, force_string=False) -> URL:
    """
    Checks a web URL.
    Expects a string like "https://www.example.com".
    Returns a URL.
    """
    v = v.strip()
    parsed = URL._make(urlparse(v))

    if not parsed.scheme or not parsed.netloc:
        raise argparse.ArgumentTypeError(f"Invalid URL")

    # Sanitize the scheme to lowercase
    parsed = parsed._replace(scheme=parsed.scheme.lower())

    if parsed.scheme not in ("http", "https"):
        raise argparse.ArgumentTypeError(f"Invalid URL scheme")

    host = parsed.hostname
    if not host:
        raise argparse.ArgumentTypeError(f"No hostname")

    # Validate and sanitize the hostname
    validated_host = validate_hostname(host, force_string=True)

    # Reconstruct netloc with validated hostname
    # netloc format: [user:pass@]host[:port]
    netloc = validated_host
    if parsed.port:
        validated_port = validate_int_in_range(parsed.port, min=1, max=65535)
        netloc = f"{netloc}:{validated_port}"
    if parsed.username:
        if parsed.password:
            netloc = f"{parsed.username}:{parsed.password}@{netloc}"
        else:
            netloc = f"{parsed.username}@{netloc}"

    parsed = parsed._replace(netloc=netloc)

    # TODO: Consider checking for internal domains that might expose sensitive information,
    # especially in the context of cloud hosting.

    # TODO: Consider sanitizing query parameters to prevent injection attacks.

    return parsed


def validate_origin(v, *, force_string=False) -> str:
    """
    Checks a web origin (scheme + host + port, no path/query/fragment).
    Expects a string like "https://www.example.com" or "http://localhost:3000".
    Returns an origin string.

    An origin is used for CORS and consists of:
    - scheme (http or https)
    - hostname (validated)
    - optional port

    No path, query, or fragment is allowed (or only "/" path is accepted and removed).
    """
    v = v.strip()
    parsed = URL._make(urlparse(v))

    # Origins must not have path (except "/"), query, or fragment
    if parsed.path and parsed.path != "/":
        raise argparse.ArgumentTypeError(f"Origin must not include a path")
    if parsed.query:
        raise argparse.ArgumentTypeError(
            f"Origin must not include a query string")
    if parsed.fragment:
        raise argparse.ArgumentTypeError(f"Origin must not include a fragment")

    # Origins should not have username/password
    if parsed.username or parsed.password:
        raise argparse.ArgumentTypeError(
            f"Origin must not include username or password")

    # Use validate_web_url to do the rest of the validation
    validated_url = validate_web_url(v, force_string=False)

    # Return origin as scheme://host[:port] (strip path/query/fragment)
    origin = f"{validated_url.scheme}://{validated_url.netloc}"
    return origin


# Hostname regex: alphanumeric labels separated by dots, no leading/trailing dots or hyphens
# Each label: starts and ends with alphanumeric, can have hyphens in middle
# Minimum 2 characters total, maximum 253 (DNS limit)
HOSTNAME_RE = re.compile(
    r'^(?=.{2,253}$)'  # Length 2-253 chars
    # Optional labels ending with dot
    r'([a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)*'
    # Final label without trailing dot
    r'[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$'
)


def validate_hostname(v, *, force_string=False) -> str:
    """
    Checks a hostname.
    Expects a string like "www.example.com" or an IP address.
    Returns a hostname.
    """
    v = v.strip()

    # Check if it's a valid IP address; includes both IPv4 and IPv6
    try:
        ip = ipaddress.ip_address(v)
        return v  # Valid IP address
    except ValueError:
        pass  # Not an IP, continue to hostname validation

    # Validate as hostname/domain name using validate_string with regex
    # TODO: Consider excluding single-label hostnames (e.g. localhost)
    return validate_string(v, regex=HOSTNAME_RE, force_string=force_string)


INT_RE = re.compile(r"^[+-]?\d+$")


def validate_int_in_range(v, *, force_string=False, min=None, max=None) -> Union[int, str]:
    """
    Checks an integer in a range.
    Expects a string like "123".
    Returns an integer.
    """
    if isinstance(v, int):
        value = v
    else:
        v = v.strip()
        if not INT_RE.match(v):
            raise argparse.ArgumentTypeError(f"Invalid integer")
        value = int(v)

    if min is not None and value < min:
        raise argparse.ArgumentTypeError(
            f"Value must be greater than or equal to {min}")
    if max is not None and value > max:
        raise argparse.ArgumentTypeError(
            f"Value must be less than or equal to {max}")
    return str(value) if force_string else value


FLOAT_RE = re.compile(r"^[+-]?\d+(\.\d+)?$")


def validate_float_in_range(v, *, force_string=False, min=None, max=None) -> Union[float, str]:
    """
    Checks a float in a range.
    Expects a string like "123.45".
    Returns a float.
    """
    if isinstance(v, float):
        value = v
    else:
        v = v.strip()
        if not FLOAT_RE.match(v):
            raise argparse.ArgumentTypeError(f"Invalid float")
        value = float(v)
    if min is not None and value < min:
        raise argparse.ArgumentTypeError(
            f"Value must be greater than or equal to {min}")
    if max is not None and value > max:
        raise argparse.ArgumentTypeError(
            f"Value must be less than or equal to {max}")
    return str(value) if force_string else value


def validate_string(v, *, force_string=False, regex: Union[str, re.Pattern] = None,
                    choices: Optional[Container[str]] = None) -> str:
    """
    Checks a string, optionally against a regex and/or choices.
    Input is stripped of leading and trailing whitespace.
    Empty strings are not allowed.
    """
    v = v.strip()
    if not v:
        raise argparse.ArgumentTypeError("Empty string is not allowed")
    if regex is not None:
        if isinstance(regex, str):
            regex = re.compile(regex)
        assert isinstance(
            regex, re.Pattern), "regex must be a string or a re.Pattern"
        if not regex.match(v):
            raise argparse.ArgumentTypeError(
                f"Invalid string format. Must match regex: {regex.pattern}")
    if choices is not None:
        if v not in choices:
            raise argparse.ArgumentTypeError(
                f"Invalid string. Must be one of: {', '.join(choices)}")
    return v


def validate_json(v, *, force_string=False) -> str:
    """
    Validates that a string is valid JSON.
    Returns the original JSON string (not parsed).
    """
    import json
    v = v.strip()
    if not v:
        raise argparse.ArgumentTypeError("Empty JSON string is not allowed")
    try:
        json.loads(v)  # Validate it's parseable JSON
    except json.JSONDecodeError as e:
        raise argparse.ArgumentTypeError(f"Invalid JSON format: {e}")
    return v


# These regular expressions are used with validate_string
SLUG_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")  # e.g. word1-word2-word3
SHELL_SAFE_RE = re.compile(r"^[^$;&|><'\"` \t]+$")
FILE_PATH_RE = re.compile(r"^[^$;&|><'\"` \t]+$")
# AWS S3 bucket names: 3-63 chars, lowercase, numbers, hyphens, dots (not adjacent dots, no leading/trailing special chars)
AWS_BUCKET_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
# Slack channel names: 1-80 chars, lowercase letters, numbers, hyphens, underscores (without # prefix - that's added by wf-slack-alert)
SLACK_CHANNEL_RE = re.compile(r"^[a-z0-9_-]{1,80}$")
# AWS Access Key ID: 20 chars, starts with AKIA (long-term) or ASIA (temporary/STS), all uppercase alphanumeric
AWS_ACCESS_KEY_ID_RE = re.compile(r"^(AKIA|ASIA)[A-Z0-9]{16}$")
# AWS Secret Access Key: 40 chars, base64 (A-Za-z0-9/+)
AWS_SECRET_ACCESS_KEY_RE = re.compile(r"^[A-Za-z0-9/+=]{40}$")
# SQL identifier: alphanumeric (upper/lower), underscore. Must start with letter or underscore (PostgreSQL unquoted identifier rules)
SQL_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
# TODO: Update this to be more specific to the actual token format.
AUTH_TOKEN_RE = re.compile(r"^\S{4,}$")

ENVIRONMENT_CHOICES = {'develop', 'main'}
CLIP_MODEL_NAME_CHOICES = {'RN50', 'RN101', 'RN50x4', 'RN50x16', 'RN50x64', 'ViT-B/32',
                           # TODO: get from clip.available_models()
                           'ViT-B/16', 'ViT-L/14', 'ViT-L/14@336px'}
OCR_METHOD_CHOICES = {'tesseract', 'easyocr'}

VALIDATORS = {
    "log_level": validate_log_level,
    "bool": validate_bool,
    "web_url": validate_web_url,
    "origin": validate_origin,
    "hostname": validate_hostname,
    # ints
    "positive_int": lambda v, **kwargs: validate_int_in_range(v, min=1, **kwargs),
    "non_negative_int": lambda v, **kwargs: validate_int_in_range(v, min=0, **kwargs),
    "port": lambda v, **kwargs: validate_int_in_range(v, min=1, max=65535, **kwargs),
    # floats
    'non_negative_float': lambda v, **kwargs: validate_float_in_range(v, min=0, **kwargs),
    # strings
    'string': lambda v, **kwargs: validate_string(v, **kwargs),
    'slug': lambda v, **kwargs: validate_string(v, regex=SLUG_RE, **kwargs),
    'shell_safe': lambda v, **kwargs: validate_string(v, regex=SHELL_SAFE_RE, **kwargs),
    'environment': lambda v, **kwargs: validate_string(v, choices=ENVIRONMENT_CHOICES, **kwargs),
    'clip_model_name': lambda v, **kwargs: validate_string(v, choices=CLIP_MODEL_NAME_CHOICES, **kwargs),
    'ocr_method': lambda v, **kwargs: validate_string(v, choices=OCR_METHOD_CHOICES, **kwargs),
    'file_path': lambda v, **kwargs: validate_string(v, regex=FILE_PATH_RE, **kwargs),
    'aws_bucket_name': lambda v, **kwargs: validate_string(v, regex=AWS_BUCKET_NAME_RE, **kwargs),
    'slack_channel': lambda v, **kwargs: validate_string(v, regex=SLACK_CHANNEL_RE, **kwargs),
    'aws_access_key_id': lambda v, **kwargs: validate_string(v, regex=AWS_ACCESS_KEY_ID_RE, **kwargs),
    'aws_secret_access_key': lambda v, **kwargs: validate_string(v, regex=AWS_SECRET_ACCESS_KEY_RE, **kwargs),
    'sql_identifier': lambda v, **kwargs: validate_string(v, regex=SQL_IDENTIFIER_RE, **kwargs),
    'auth_token': lambda v, **kwargs: validate_string(v, regex=AUTH_TOKEN_RE, **kwargs),
    # JSON
    'json': validate_json,
}


def validate(validator_type: str, value: Optional[str] = None, envar: Optional[str] = None,
             default: Optional[str] = None, hidden: bool = False, raise_errors: bool = True,
             force_string: bool = False, sep=None, allow_unset: bool = False):
    """
    Validate and sanitize a value using the specified validator.

    Args:
        validator_type: Type of validator to use (from VALIDATORS dict)
        value: Value to validate (optional if envar is provided)
        envar: Environment variable name to read value from
        default: Default value if value/envar is not set.
            Default for default is None, meaning unset.
            If set, the value will be validated regardless of whether value or envar is provided.
        hidden: Whether to hide value in error messages
        raise_errors: Whether to raise ArgumentTypeError on validation failure
            Note that ValueError may still be raised even if raise_errors is false.
        force_string: Forces return value to be a string or stringifiable type
        sep: Separator to split value into a list. Can be a string or regex pattern.
            If provided, returns a list of validated values instead of a single value.
        allow_unset: Whether to allow unset value. If set, the value will be allowed to be unset and None will be returned.
    Returns:
        Validated and sanitized value, or list of values if sep is provided

    Raises:
        argparse.ArgumentTypeError: If validation fails and raise_errors is True
        ValueError: If validator_type is unknown or no value is provided
    """
    if hidden:
        logger.info(
            f"type={validator_type}, envar={envar}, value=**HIDDEN**, default={default}, hidden={hidden}, raise_errors={raise_errors}, force_string={force_string}, sep={sep}")
    else:
        logger.info(
            f"type={validator_type}, envar={envar}, value={value}, default={default}, hidden={hidden}, raise_errors={raise_errors}, force_string={force_string}, sep={sep}")

    # Determine value
    using_default = False
    if value is None or value == "":
        if envar:
            value = os.getenv(envar)
            if value is None or value == "":
                if default is not None:
                    using_default = True
                    logger.info(
                        f"Using default value: {'**HIDDEN**' if hidden else default}")
                    value = default
                elif allow_unset:
                    logger.info(
                        f"Returning None for {envar} because allow_unset is set.")
                    return None
                elif value is None or raise_errors:
                    raise ValueError(
                        f"Environment variable {envar} is not set.")
                else:
                    logger.warning(
                        f"Returning None for {envar} because raise_errors is false")
                    return None
            else:
                logger.info(
                    f"Using value from environment variable {envar}: {'**HIDDEN**' if hidden else value}")
        elif value is None or raise_errors:
            raise ValueError("Must provide either value or envar (or both).")
        else:
            logger.warning(
                f"Returning None for {envar} because value is None and raise_errors is false.")
            return None
    else:
        logger.info(
            f"Using value from command line: {'**HIDDEN**' if hidden else value}")

    if default is not None and not using_default:
        # We need to validate the default value to avoid latent errors,
        # even though we're not using it.
        try:
            default_result = _validate(
                validator_type, value=default, hidden=hidden, sep=sep)
            logger.info(
                f"Validated default value: {'**HIDDEN**' if hidden else default_result}")
        except argparse.ArgumentTypeError as e:
            if raise_errors:
                raise argparse.ArgumentTypeError(
                    f"Invalid default value for {validator_type} {envar or 'value'}: {e}")
            else:
                logger.error(
                    f"Invalid default value for {validator_type} {envar or 'value'}: {e}")

    # This is where we honor `raise_errors`
    try:
        result = _validate(validator_type, value=value,
                           hidden=hidden, force_string=force_string, sep=sep)
        logger.info(
            f"Validated {envar or 'value'}: {'**HIDDEN**' if hidden else result}")
        return result
    except argparse.ArgumentTypeError as e:
        label = f"Invalid value for {validator_type} {envar or 'value'}"
        error_msg = f"{label}: {e}" if hidden else f"{label}: {value}: {e}"

        if raise_errors:
            raise argparse.ArgumentTypeError(error_msg)
        else:
            logger.error(error_msg)
            return None


def _validate_list(validator_type: str, value: str, hidden: bool = False, force_string: bool = False, sep: str | re.Pattern = None) -> Any:
    """Split the value into a list and validate each value."""
    if value is None or value == "":
        return []
    elif isinstance(sep, str):
        values = value.split(sep)
    elif isinstance(sep, re.Pattern):
        values = sep.split(value)
    else:
        raise ValueError(f"Invalid separator: {sep}")

    # Validate each value in the list
    try:
        result = [_validate(validator_type, value=v, hidden=hidden, force_string=force_string)
                  for v in values]
        logger.info(f"Validated values: {'**HIDDEN**' if hidden else result}")
        if force_string and len(result) > 1:
            if isinstance(sep, str):
                return sep.join(result)
            elif sep == SEP_WHITESPACE:
                return " ".join(result)
            else:
                # The CLI only supports literal separators, not regex patterns.
                raise ValueError(
                    f"I don't know how to turn a list into a string with separator: {sep}")
        else:
            return result
    except argparse.ArgumentTypeError as e:
        if hidden:
            error_msg = f"Type {validator_type}: {str(e)}"
        else:
            error_msg = f"Type {validator_type}: Value {value}: {str(e)}"

        raise argparse.ArgumentTypeError(error_msg)


def _validate(validator_type: str, value: str, hidden: bool = False, force_string: bool = False, sep: Optional[str | re.Pattern] = None) -> Any:
    """This is the function that invokes the validator"""

    # Split value if separator is provided
    if sep is not None:
        return _validate_list(validator_type, value, hidden=hidden, force_string=force_string, sep=sep)

    validator = VALIDATORS.get(validator_type)
    if validator is None:
        raise ValueError(
            f"Unknown validator type: {validator_type}. Use one of: {', '.join(VALIDATORS.keys())}")

    try:
        result = validator(value, force_string=force_string)
        logger.info(f"Validated value: {'**HIDDEN**' if hidden else result}")
        return result
    except argparse.ArgumentTypeError as e:
        if hidden:
            error_msg = f"Type {validator_type}: {str(e)}"
        else:
            error_msg = f"Type {validator_type}: Value {value}: {str(e)}"

        raise argparse.ArgumentTypeError(error_msg)


EXIT_SUCCESS = 0
EXIT_VALIDATION_FAILURE = 1
EXIT_OTHER_ERROR = 2


def main():
    parser = argparse.ArgumentParser(
        description="Validate and sanitize workflow parameter values."
    )
    parser.add_argument(
        "--type",
        required=True,
        choices=sorted(VALIDATORS.keys()),
        help="Validator type to apply.",
    )
    parser.add_argument(
        "--envar",
        help="Environment variable name. If --value is omitted, the value will be read from this variable.",
        default=None,
    )
    parser.add_argument(
        "--value",
        help="Value to validate. If omitted, value will be read from the environment variable.",
        default=None,
    )
    parser.add_argument(
        "--default",
        help="Default value to use if --value is omitted or empty and --envar is unset or empty.",
        default=None,
    )
    parser.add_argument(
        "--sep",
        help="Separator to split value into a list (e.g., ',' for comma-separated values). Output will be comma-separated string.",
        default=None,
    )
    parser.add_argument(
        "--hidden",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Whether the value is hidden. If set, the value will not appear in logging or error messages.",
    )
    parser.add_argument(
        "--raise",
        dest="raise_errors",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Raise on validation error (use --no-raise to always exit 0)."
    )
    parser.add_argument(
        "--allow-unset",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Allow unset value. If true, the value will be allowed to be unset and an empty string will be returned.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging verbosity level (default: INFO)."
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(levelname)s: %(message)s",
        stream=sys.stderr,
    )

    try:
        result = validate(
            validator_type=args.type,
            value=args.value,
            envar=args.envar,
            default=args.default,
            hidden=args.hidden,
            raise_errors=args.raise_errors,
            force_string=True,
            sep=args.sep,
            allow_unset=args.allow_unset,
        )
        if result is not None:
            print(result)
        sys.exit(EXIT_SUCCESS)
    except ValueError as e:
        logging.error(str(e))
        sys.exit(EXIT_OTHER_ERROR)
    except argparse.ArgumentTypeError as e:
        name = args.envar or "value"
        logging.error(f"Invalid {name}: {e}")
        sys.exit(EXIT_VALIDATION_FAILURE)
    except Exception as e:
        logger.exception("Unexpected error")
        sys.exit(EXIT_OTHER_ERROR)


if __name__ == "__main__":
    main()
