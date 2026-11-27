#!/usr/bin/env python3
"""
Test suite for wf_argparse.py

Tests cover 5 modes of operation:
1. ArgumentParser with command line arguments
2. ArgumentParser with WF_ environment variables
3. ArgumentParser with legacy environment variables
4. Direct validate() function calls
5. CLI invocation

Each registered validator is tested with at least one positive and one negative example.
"""

import os
import sys
import subprocess
import argparse
import pytest

# Import the module under test
from aperturedb_workflows import wf_argparse


# =============================================================================
# Test Data
# =============================================================================

VALIDATOR_TEST_CASES = {
    'log_level': {
        'valid': ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        'invalid': ['INVALID', 'TRACE', ''],
    },
    'bool': {
        'valid': ['true', 'false', 'yes', 'no', '1', '0'],
        'invalid': ['maybe', 'invalid', ''],
    },
    'web_url': {
        'valid': ['http://example.com', 'https://example.com:8080/path'],
        'invalid': ['ftp://example.com', 'example.com', ''],
    },
    'hostname': {
        'valid': [
            'example.com',           # Basic domain
            'sub.example.com',       # Subdomain
            'localhost',             # Single label
            'my-server',             # Hyphen in middle
            '192.168.1.1',          # IPv4
            '::1',                   # IPv6
            'a.b',                   # Minimal two-label
            'xx',                    # Minimal single-label (2 chars)
        ],
        'invalid': [
            '',                      # Empty
            'x',                     # Too short (1 char)
            'invalid..com',          # Consecutive dots
            '-invalid.com',          # Label starts with hyphen
            'invalid-.com',          # Label ends with hyphen
            'http://example.com',    # Contains ://
            '.example.com',          # Starts with dot
            # Ends with dot (technically valid in DNS but we reject)
            'example.com.',
            'invalid_host',          # Underscore not allowed
            'host name',             # Space not allowed
        ],
    },
    'positive_int': {
        'valid': ['1', '100', '999999'],
        'invalid': ['0', '-1', 'abc', ''],
    },
    'non_negative_int': {
        'valid': ['0', '1', '100'],
        'invalid': ['-1', 'abc', ''],
    },
    'port': {
        'valid': ['80', '443', '8080', '65535'],
        'invalid': ['0', '-1', '65536', 'abc', ''],
    },
    'non_negative_float': {
        'valid': ['0', '0.0', '1.5', '100.5'],
        'invalid': ['-1', '-0.1', 'abc', ''],
    },
    'string': {
        'valid': ['hello', 'hello world', '123'],
        'invalid': [''],
    },
    'slug': {
        'valid': ['hello', 'hello-world', 'test-123'],
        'invalid': ['Hello', 'hello_world', 'hello world', ''],
    },
    'shell_safe': {
        'valid': ['hello', 'hello-world', '/path/to/file'],
        'invalid': ['hello world', 'hello;world', 'hello$world', ''],
    },
    'environment': {
        'valid': ['develop', 'main'],
        'invalid': ['production', 'staging', ''],
    },
}


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture(autouse=True)
def clean_wf_envars():
    """Clean WF_ environment variables before each test."""
    old_env = os.environ.copy()
    # Remove all WF_ variables except ignored ones
    for key in list(os.environ.keys()):
        if key.startswith('WF_'):
            del os.environ[key]
    yield
    # Restore environment
    os.environ.clear()
    os.environ.update(old_env)


# =============================================================================
# Mode 1: ArgumentParser with command line arguments
# =============================================================================

class TestMode1CommandLine:
    """Test Mode 1: ArgumentParser with command line arguments."""

    @pytest.mark.parametrize('validator_type,value', [
        (vtype, valid_val)
        for vtype, cases in VALIDATOR_TEST_CASES.items()
        for valid_val in cases['valid'][:1]  # Test first valid value
    ])
    def test_valid_values(self, validator_type, value):
        """Test valid values for each validator via command line."""
        parser = wf_argparse.ArgumentParser(description='Test')
        parser.add_argument('--test-arg', type=validator_type, required=False)
        args = parser.parse_args(['--test-arg', value])
        assert args.test_arg is not None

    @pytest.mark.parametrize('validator_type,value', [
        (vtype, invalid_val)
        for vtype, cases in VALIDATOR_TEST_CASES.items()
        for invalid_val in cases['invalid'][:1]  # Test first invalid value
    ])
    def test_invalid_values(self, validator_type, value):
        """Test invalid values for each validator via command line."""
        parser = wf_argparse.ArgumentParser(description='Test')
        parser.add_argument('--test-arg', type=validator_type, required=False)
        with pytest.raises(SystemExit):
            parser.parse_args(['--test-arg', value])

    def test_sep_with_command_line_combined(self):
        """Test sep parameter with command line (via append action)."""
        parser = wf_argparse.ArgumentParser(description='Test')
        parser.add_argument('--items', type='string', sep=',')
        args = parser.parse_args(['--items', 'a,b,c'])
        assert args.items == ['a', 'b', 'c']

    def test_sep_with_command_line_separate(self):
        """Test sep parameter with command line (via append action)."""
        parser = wf_argparse.ArgumentParser(description='Test')
        parser.add_argument('--items', type='string', sep=',')
        args = parser.parse_args(
            ['--items', 'a', '--items', 'b', '--items', 'c'])
        assert args.items == ['a', 'b', 'c']

# =============================================================================
# Mode 2: ArgumentParser with WF_ environment variables
# =============================================================================


class TestMode2WFEnvars:
    """Test Mode 2: ArgumentParser with WF_ environment variables."""

    @pytest.mark.parametrize('validator_type,value', [
        (vtype, valid_val)
        for vtype, cases in VALIDATOR_TEST_CASES.items()
        for valid_val in cases['valid'][:1]
    ])
    def test_valid_values(self, validator_type, value):
        """Test valid values for each validator via WF_ environment variable."""
        os.environ['WF_TEST_ARG'] = value
        parser = wf_argparse.ArgumentParser(description='Test')
        parser.add_argument('--test-arg', type=validator_type, required=False)
        args = parser.parse_args([])
        assert args.test_arg is not None

    @pytest.mark.parametrize('validator_type,value', [
        (vtype, invalid_val)
        for vtype, cases in VALIDATOR_TEST_CASES.items()
        for invalid_val in cases['invalid'][:1]
    ])
    def test_invalid_values(self, validator_type, value):
        """Test invalid values for each validator via WF_ environment variable."""
        os.environ['WF_TEST_ARG'] = value
        parser = wf_argparse.ArgumentParser(description='Test')
        parser.add_argument('--test-arg', type=validator_type, required=False)

        if value == "":
            # Empty strings from environment variables are treated as unset
            args = parser.parse_args([])
            assert args.test_arg is None
        else:
            # Other invalid values should still fail
            with pytest.raises(SystemExit):
                parser.parse_args([])

    def test_sep_comma(self):
        """Test sep parameter with comma separator."""
        os.environ['WF_ITEMS'] = 'a,b,c'
        parser = wf_argparse.ArgumentParser(description='Test')
        parser.add_argument('--items', type='string', sep=',')
        args = parser.parse_args([])
        assert args.items == ['a', 'b', 'c']

    def test_sep_whitespace(self):
        """Test sep parameter with whitespace regex."""
        os.environ['WF_ITEMS'] = 'a b  c'
        parser = wf_argparse.ArgumentParser(description='Test')
        parser.add_argument('--items', type='string',
                            sep=wf_argparse.SEP_WHITESPACE)
        args = parser.parse_args([])
        assert args.items == ['a', 'b', 'c']

    def test_unused_wf_envar_detection(self):
        """Test that unused WF_ environment variables are detected."""
        os.environ['WF_TEST_ARG'] = 'value'
        os.environ['WF_UNUSED'] = 'unused'
        parser = wf_argparse.ArgumentParser(description='Test')
        parser.add_argument('--test-arg', type='string')
        with pytest.raises(AssertionError, match='Unused environment variables'):
            parser.parse_args([])


# =============================================================================
# Mode 3: ArgumentParser with legacy environment variables
# =============================================================================

class TestMode3LegacyEnvars:
    """Test Mode 3: ArgumentParser with legacy environment variables."""

    @pytest.mark.parametrize('validator_type,value', [
        (vtype, valid_val)
        for vtype, cases in VALIDATOR_TEST_CASES.items()
        for valid_val in cases['valid'][:1]
    ])
    def test_valid_values(self, validator_type, value):
        """Test valid values for each validator via legacy environment variable."""
        os.environ['TEST_ARG'] = value
        parser = wf_argparse.ArgumentParser(
            description='Test', support_legacy_envars=True)
        parser.add_argument('--test-arg', type=validator_type, required=False)
        args = parser.parse_args([])
        assert args.test_arg is not None

    def test_wf_precedence(self):
        """Test that WF_ envar takes precedence over legacy envar."""
        os.environ['WF_TEST_ARG'] = 'wf_value'
        os.environ['TEST_ARG'] = 'legacy_value'
        parser = wf_argparse.ArgumentParser(
            description='Test', support_legacy_envars=True)
        parser.add_argument('--test-arg', type='string')
        args = parser.parse_args([])
        assert args.test_arg == 'wf_value'

    def test_legacy_not_used_without_flag(self):
        """Test that legacy envars are not used without support_legacy_envars=True."""
        os.environ['TEST_ARG'] = 'value'
        parser = wf_argparse.ArgumentParser(description='Test')
        parser.add_argument('--test-arg', type='string', default='default')
        args = parser.parse_args([])
        assert args.test_arg == 'default'

    def test_sep_with_legacy(self):
        """Test sep parameter with legacy envar."""
        os.environ['ITEMS'] = 'a,b,c'
        parser = wf_argparse.ArgumentParser(
            description='Test', support_legacy_envars=True)
        parser.add_argument('--items', type='string', sep=',')
        args = parser.parse_args([])
        assert args.items == ['a', 'b', 'c']


# =============================================================================
# Mode 4: Direct validate() function calls
# =============================================================================

class TestMode4ValidateFunction:
    """Test Mode 4: Direct validate() function calls."""

    @pytest.mark.parametrize('validator_type,value', [
        (vtype, valid_val)
        for vtype, cases in VALIDATOR_TEST_CASES.items()
        for valid_val in cases['valid'][:1]
    ])
    def test_valid_values_with_value_param(self, validator_type, value):
        """Test valid values for each validator via validate() with value parameter."""
        result = wf_argparse.validate(
            validator_type=validator_type,
            value=value,
            force_string=True
        )
        assert result is not None

    @pytest.mark.parametrize('validator_type,value', [
        (vtype, invalid_val)
        for vtype, cases in VALIDATOR_TEST_CASES.items()
        for invalid_val in cases['invalid'][:1]
    ])
    def test_invalid_values_with_value_param(self, validator_type, value):
        """Test invalid values for each validator via validate() with value parameter."""
        with pytest.raises(ValueError if value == "" else argparse.ArgumentTypeError):
            wf_argparse.validate(
                validator_type=validator_type,
                value=value,
                raise_errors=True
            )

    @pytest.mark.parametrize('validator_type,value', [
        (vtype, valid_val)
        for vtype, cases in VALIDATOR_TEST_CASES.items()
        for valid_val in cases['valid'][:1]
    ])
    def test_valid_values_with_envar_param(self, validator_type, value):
        """Test valid values for each validator via validate() with envar parameter."""
        os.environ['TEST_ENVAR'] = value
        result = wf_argparse.validate(
            validator_type=validator_type,
            envar='TEST_ENVAR',
            force_string=True
        )
        assert result is not None

    def test_with_default(self):
        """Test validate() with default value when envar is not set."""
        result = wf_argparse.validate(
            validator_type='string',
            envar='NONEXISTENT',
            default='default_value',
            force_string=True
        )
        assert result == 'default_value'

    def test_with_hidden_flag(self):
        """Test that hidden flag masks values in error messages."""
        with pytest.raises(argparse.ArgumentTypeError) as exc_info:
            wf_argparse.validate(
                validator_type='port',
                value='secret_invalid',
                hidden=True,
                raise_errors=True
            )
        # Error message should not contain the actual value
        assert 'secret_invalid' not in str(exc_info.value)

    def test_no_raise(self):
        """Test validate() with raise_errors=False returns original value on error."""
        result = wf_argparse.validate(
            validator_type='port',
            value='invalid',
            raise_errors=False
        )
        assert result is None


# =============================================================================
# Mode 5: CLI invocation
# =============================================================================

class TestMode5CLI:
    """Test Mode 5: CLI invocation."""

    @pytest.mark.parametrize('validator_type,value', [
        (vtype, valid_val)
        for vtype, cases in VALIDATOR_TEST_CASES.items()
        for valid_val in cases['valid'][:1]
    ])
    def test_valid_values(self, validator_type, value):
        """Test valid values for each validator via CLI."""
        result = subprocess.run(
            [sys.executable, wf_argparse.__file__,
                '--type', validator_type, '--value', value],
            capture_output=True,
            text=True
        )
        assert result.returncode == wf_argparse.EXIT_SUCCESS, \
            f"Failed: {result.stderr}"
        assert result.stdout.strip()  # Should output something

    @pytest.mark.parametrize('validator_type,value', [
        (vtype, invalid_val)
        for vtype, cases in VALIDATOR_TEST_CASES.items()
        for invalid_val in cases['invalid'][:1]
    ])
    def test_invalid_values(self, validator_type, value):
        """Test invalid values for each validator via CLI."""
        result = subprocess.run(
            [sys.executable, wf_argparse.__file__,
                '--type', validator_type, '--value', value],
            capture_output=True,
            text=True
        )
        assert result.returncode == (
            wf_argparse.EXIT_OTHER_ERROR if value == ""
            else wf_argparse.EXIT_VALIDATION_FAILURE)

    def test_with_envar(self):
        """Test CLI reading value from environment variable."""
        env = os.environ.copy()
        env['TEST_ENVAR'] = 'https://example.com'
        result = subprocess.run(
            [sys.executable, wf_argparse.__file__, '--type',
                'web_url', '--envar', 'TEST_ENVAR'],
            capture_output=True,
            text=True,
            env=env
        )
        assert result.returncode == wf_argparse.EXIT_SUCCESS
        assert 'example.com' in result.stdout

    def test_with_default(self):
        """Test CLI with default value when envar is not set."""
        result = subprocess.run(
            [sys.executable, wf_argparse.__file__, '--type', 'string',
             '--envar', 'NONEXISTENT', '--default', 'default_value'],
            capture_output=True,
            text=True
        )
        assert result.returncode == wf_argparse.EXIT_SUCCESS
        assert result.stdout.strip() == 'default_value'

    def test_no_raise_flag(self):
        """Test CLI with --no-raise flag returns exit code 0 even on validation error."""
        result = subprocess.run(
            [sys.executable, wf_argparse.__file__, '--type', 'port',
             '--value', '', '--no-raise'],
            capture_output=True,
            text=True
        )
        assert result.returncode == wf_argparse.EXIT_SUCCESS
        assert result.stdout.strip() == ''

    def test_hidden_flag(self):
        """Test CLI with --hidden flag masks values in error messages."""
        result = subprocess.run(
            [sys.executable, wf_argparse.__file__, '--type', 'port',
             '--value', 'secret_invalid', '--hidden'],
            capture_output=True,
            text=True
        )
        assert result.returncode == wf_argparse.EXIT_VALIDATION_FAILURE
        # Value should not appear in stderr
        assert 'secret_invalid' not in result.stderr


# =============================================================================
# Additional edge case tests
# =============================================================================

class TestEdgeCases:
    """Test edge cases and special scenarios."""

    def test_bool_type_shorthand(self):
        """Test that type=bool is converted to 'bool' validator."""
        os.environ['WF_TEST'] = 'true'
        parser = wf_argparse.ArgumentParser(description='Test')
        parser.add_argument('--test', type=bool)
        args = parser.parse_args([])
        assert args.test is True

    def test_trim_applied_to_str_type(self):
        """Test that str type gets trim applied."""
        os.environ['WF_TEST'] = '  value  '
        parser = wf_argparse.ArgumentParser(description='Test')
        parser.add_argument('--test', type=str)
        args = parser.parse_args([])
        assert args.test == 'value'

    def test_required_with_default_from_envar(self):
        """Test that required=True is overridden when default comes from envar."""
        os.environ['WF_TEST'] = 'value'
        parser = wf_argparse.ArgumentParser(description='Test')
        parser.add_argument('--test', type='string', required=True)
        args = parser.parse_args([])
        assert args.test == 'value'

    def test_command_line_overrides_envar(self):
        """Test that command line argument overrides environment variable."""
        os.environ['WF_TEST'] = 'envar_value'
        parser = wf_argparse.ArgumentParser(description='Test')
        parser.add_argument('--test', type='string')
        args = parser.parse_args(['--test', 'cmdline_value'])
        assert args.test == 'cmdline_value'

    @pytest.mark.parametrize('sep,envar_value,expected', [
        (wf_argparse.SEP_COMMA, 'a,b,c', ['a', 'b', 'c']),
        (wf_argparse.SEP_WHITESPACE, 'a b  c\td', ['a', 'b', 'c', 'd']),
    ])
    def test_multiple_sep_types(self, sep, envar_value, expected):
        """Test different separator types."""
        os.environ['WF_ITEMS'] = envar_value
        parser = wf_argparse.ArgumentParser(description='Test')
        parser.add_argument('--items', type='string', sep=sep)
        args = parser.parse_args([])
        assert args.items == expected


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
