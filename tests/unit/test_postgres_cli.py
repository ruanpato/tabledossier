"""The profile command without a database: arguments, exit codes and the offline promise."""

import json
import subprocess
import sys

import pytest

from tabledossier.cli import main
from tabledossier.config import default_config

FORBIDDEN_MODULES = ("pyspark", "py4j", "databricks", "psycopg", "psycopg2", "requests", "urllib3")


def _config(tmp_path, **overrides):
    config = default_config()
    config.update({"tables": ["analytics.customers"], **overrides})
    path = tmp_path / "config.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return str(path)


def test_dsn_env_takes_a_variable_name_and_never_echoes_a_connection_string(tmp_path, capsys):
    secret = "postgresql://reader:SECRET-PW@db.internal/prod"
    with pytest.raises(SystemExit) as caught:
        main(
            ["profile", "--engine", "postgres", "--config", _config(tmp_path), "--dsn-env", secret]
        )
    assert caught.value.code == 2
    err = capsys.readouterr().err
    assert "SECRET-PW" not in err and "db.internal" not in err
    assert "NAME of an environment variable" in err


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("--statement-timeout", "0"),
        ("--lock-timeout", "x"),
        ("--work-mem", "16 MB"),
        ("--engine", "mysql"),
    ],
)
def test_invalid_options_are_usage_errors(tmp_path, option, value):
    args = ["profile", "--engine", "postgres", "--config", _config(tmp_path), option, value]
    with pytest.raises(SystemExit) as caught:
        main(args)
    assert caught.value.code == 2


@pytest.mark.parametrize(
    ("overrides", "output", "message"),
    [
        ({"tables": []}, "results", "lists no tables"),
        ({}, None, "no output directory"),
        ({}, "dbfs:/tmp/x", "not a URI"),
        ({}, "/Volumes/c/s/v", "not a Databricks volume"),
    ],
)
def test_configuration_problems_exit_3_before_connecting(
    tmp_path, capsys, overrides, output, message
):
    args = ["profile", "--engine", "postgres", "--config", _config(tmp_path, **overrides)]
    if output:
        args += ["--output", output]
    assert main(args) == 3
    assert message in capsys.readouterr().err


def _run(code: str):
    return subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)


def test_offline_commands_never_import_a_driver(tmp_path):
    profile = "examples/demo/output/run/profile.json"
    code = f"""
import sys
from tabledossier.cli import main
out = {str(tmp_path)!r}
assert main(["init", "--output", out + "/c.json"]) == 0
assert main(["validate", "--config", out + "/c.json"]) == 0
assert main(["generate", "--config", out + "/c.json", "--output", out + "/nb.py"]) == 0
assert main(["render", "--input", {profile!r}, "--output", out + "/docs"]) == 0
assert main(["validate", "--profile", {profile!r}]) == 0
assert main(["schema", "profile", "--output", out + "/s.json"]) == 0
bad = sorted(m for m in sys.modules if m.split(".")[0] in {FORBIDDEN_MODULES!r})
print(bad)
sys.exit(1 if bad else 0)
"""
    result = _run(code)
    assert result.returncode == 0, result.stdout + result.stderr


def test_importing_the_connector_does_not_import_the_driver():
    result = _run(
        "import sys; import tabledossier.connectors.postgres, tabledossier.connectors.postgres_plan; "
        "sys.exit(1 if 'psycopg' in sys.modules else 0)"
    )
    assert result.returncode == 0, result.stderr


def test_a_missing_driver_exits_8_with_the_install_hint(tmp_path):
    config = _config(tmp_path)
    result = _run(
        "import sys; sys.modules['psycopg'] = None\n"
        "from tabledossier.cli import main\n"
        f"sys.exit(main(['profile', '--engine', 'postgres', '--config', {config!r}, '--output', {str(tmp_path / 'r')!r}]))"
    )
    assert result.returncode == 8, result.stderr
    assert 'pip install "tabledossier[postgres]"' in result.stderr
    assert not (tmp_path / "r").exists()
