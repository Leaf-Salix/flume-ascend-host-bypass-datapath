#!/usr/bin/env python3
"""Transactional tests for the Flume AICPU package installer wrapper."""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
import sys
import tempfile
from pathlib import Path


PACKAGE = "aicpu_flume_hcomm_payload.tar.gz"
PACKAGE_PATH = "opp/vendors/flume/aicpu/kernel"
BEGIN = f"# BEGIN FLUME MANAGED AICPU PACKAGE: {PACKAGE}"
END = f"# END FLUME MANAGED AICPU PACKAGE: {PACKAGE}"


def run_installer(installer: Path, whitelist: Path, *args: str,
                  env: dict[str, str] | None = None
                  ) -> subprocess.CompletedProcess[str]:
    command = [
        "bash", str(installer),
        "--flume-register-whitelist",
        f"--flume-whitelist-path={whitelist}",
        f"--flume-whitelist-package-path={PACKAGE_PATH}",
        *args,
    ]
    return subprocess.run(
        command, text=True, capture_output=True, check=False,
        env={**os.environ, **(env or {})})


def exact_entry() -> str:
    return (
        f"name:{PACKAGE}\n"
        "install_path:2\n"
        "optional:true\n"
        f"package_path:{PACKAGE_PATH}\n"
        "load_as_per_soc:false\n"
    )


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: test_hcomm_installer.py <repo-root>", file=sys.stderr)
        return 2
    repo = Path(sys.argv[1]).resolve()
    source = (repo / "custom_ops" / "hcomm_payload_copy" / "scripts" /
              "flume_install.sh")
    source_text = source.read_text(encoding="utf-8")
    assert '"npu-smi" "set"' not in source_text
    assert "npu-smi set" not in source_text

    cmake_text = (repo / "custom_ops" / "hcomm_payload_copy" /
                  "CMakeLists.txt").read_text(encoding="utf-8")
    assert "RENAME flume_base_install.sh" in cmake_text
    assert "RENAME install.sh" in cmake_text

    with tempfile.TemporaryDirectory(prefix="flume-installer-") as tmp_text:
        tmp = Path(tmp_text)
        scripts = tmp / "opp" / "vendors" / "flume" / "scripts"
        scripts.mkdir(parents=True)
        installer = scripts / "install.sh"
        shutil.copy2(source, installer)
        installer.chmod(0o755)
        base = scripts / "flume_base_install.sh"
        base.write_text(
            "#!/usr/bin/env bash\n"
            "printf '%s\\n' \"$*\" >> \"${FLUME_TEST_BASE_LOG}\"\n"
            "if [ \"${FLUME_TEST_MUTATE_WHITELIST:-n}\" = y ] && "
            "[ \"${1:-}\" != --uninstall ]; then\n"
            "  printf 'name:%s\\npackage_path:wrong/path\\n' "
            f"'{PACKAGE}' > \"${{FLUME_TEST_WHITELIST}}\"\n"
            "fi\n"
            "exit \"${FLUME_TEST_BASE_RC:-0}\"\n",
            encoding="utf-8")
        base.chmod(0o755)
        log = tmp / "base.log"
        common_env = {"FLUME_TEST_BASE_LOG": str(log)}

        whitelist = tmp / "ascend_package_load.ini"
        whitelist.write_text(
            "name:unrelated.tar.gz\npackage_path:opp/vendors/other\n",
            encoding="utf-8")
        whitelist.chmod(0o640)
        first = run_installer(installer, whitelist, "--install",
                              env=common_env)
        assert first.returncode == 0, first.stderr + first.stdout
        installed = whitelist.read_text(encoding="utf-8")
        assert installed.count(f"name:{PACKAGE}") == 1
        assert installed.count(BEGIN) == 1
        assert installed.count(END) == 1
        assert stat.S_IMODE(whitelist.stat().st_mode) == 0o640
        assert "flume_installer=whitelist-transaction-v1" in first.stdout
        assert "aicpu_whitelist_action=created-managed" in first.stdout

        symlink_target = tmp / "symlink-target.ini"
        symlink_target.write_text("# symlink target\n", encoding="utf-8")
        symlink_path = tmp / "symlink-whitelist.ini"
        symlink_path.symlink_to(symlink_target)
        symlink_install = run_installer(
            installer, symlink_path, "--install", env=common_env)
        assert symlink_install.returncode == 0
        assert symlink_path.is_symlink()
        assert BEGIN in symlink_target.read_text(encoding="utf-8")
        symlink_remove = run_installer(
            installer, symlink_path, "--uninstall", env=common_env)
        assert symlink_remove.returncode == 0
        assert symlink_path.is_symlink()
        assert BEGIN not in symlink_target.read_text(encoding="utf-8")

        second = run_installer(installer, whitelist, "--install",
                               env=common_env)
        assert second.returncode == 0, second.stderr + second.stdout
        assert whitelist.read_text(encoding="utf-8") == installed
        assert "aicpu_whitelist_action=preserved-existing" in second.stdout

        removed = run_installer(installer, whitelist, "--uninstall",
                                env=common_env)
        assert removed.returncode == 0, removed.stderr + removed.stdout
        after_remove = whitelist.read_text(encoding="utf-8")
        assert PACKAGE not in after_remove
        assert "name:unrelated.tar.gz" in after_remove
        assert stat.S_IMODE(whitelist.stat().st_mode) == 0o640
        assert "aicpu_whitelist_action=removed-managed" in removed.stdout

        base_failure = tmp / "base-failure.ini"
        base_failure.write_text("# unchanged\n", encoding="utf-8")
        failed_install = run_installer(
            installer, base_failure, "--install",
            env={"FLUME_TEST_BASE_LOG": str(tmp / "failed-install.log"),
                 "FLUME_TEST_BASE_RC": "9"})
        assert failed_install.returncode != 0
        assert base_failure.read_text(encoding="utf-8") == "# unchanged\n"

        external = tmp / "external.ini"
        external.write_text(exact_entry(), encoding="utf-8")
        external_install = run_installer(installer, external, "--install",
                                         env=common_env)
        assert external_install.returncode == 0
        external_uninstall = run_installer(installer, external, "--uninstall",
                                           env=common_env)
        assert external_uninstall.returncode == 0
        assert external.read_text(encoding="utf-8") == exact_entry()
        assert "aicpu_whitelist_action=preserved-external-or-missing" in (
            external_uninstall.stdout)

        managed_base_failure = tmp / "managed-base-failure.ini"
        managed_base_failure.write_text(
            BEGIN + "\n" + exact_entry() + END + "\n", encoding="utf-8")
        failed_uninstall = run_installer(
            installer, managed_base_failure, "--uninstall",
            env={"FLUME_TEST_BASE_LOG": str(tmp / "failed-uninstall.log"),
                 "FLUME_TEST_BASE_RC": "8"})
        assert failed_uninstall.returncode != 0
        assert BEGIN in managed_base_failure.read_text(encoding="utf-8")

        conflict = tmp / "conflict.ini"
        conflict.write_text(
            f"name:{PACKAGE}\npackage_path:wrong/path\n",
            encoding="utf-8")
        log.unlink(missing_ok=True)
        rejected = run_installer(installer, conflict, "--install",
                                 env=common_env)
        assert rejected.returncode != 0
        assert "conflicting whitelist entry" in rejected.stdout
        assert not log.exists(), "base installer ran before conflict rejection"

        malformed = tmp / "malformed.ini"
        malformed.write_text(BEGIN + "\n" + exact_entry(), encoding="utf-8")
        malformed_result = run_installer(installer, malformed, "--uninstall",
                                         env=common_env)
        assert malformed_result.returncode != 0
        assert malformed.read_text(encoding="utf-8").startswith(BEGIN)

        reversed_markers = tmp / "reversed-markers.ini"
        reversed_markers.write_text(
            END + "\n" + exact_entry() + BEGIN + "\n", encoding="utf-8")
        log.unlink(missing_ok=True)
        reversed_result = run_installer(
            installer, reversed_markers, "--uninstall", env=common_env)
        assert reversed_result.returncode != 0
        assert not log.exists(), "base installer ran before marker rejection"

        rollback = tmp / "rollback.ini"
        rollback.write_text("# initially clean\n", encoding="utf-8")
        rollback_log = tmp / "rollback.log"
        rollback_result = run_installer(
            installer, rollback, "--install",
            env={
                "FLUME_TEST_BASE_LOG": str(rollback_log),
                "FLUME_TEST_MUTATE_WHITELIST": "y",
                "FLUME_TEST_WHITELIST": str(rollback),
            })
        assert rollback_result.returncode != 0
        calls = rollback_log.read_text(encoding="utf-8").splitlines()
        assert len(calls) == 2
        assert "--install" in calls[0]
        assert "--uninstall" in calls[1]
        assert "rolling back package files" in rollback_result.stdout
        assert "aicpu_package_rollback=attempted" in rollback_result.stdout

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
