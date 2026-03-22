from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from server.services.config_store import ConfigStore, ConfigValidationError
from server.utils.paths import REPO_ROOT, dump_json, load_json, read_app_config


class RunStateError(RuntimeError):
    pass


@dataclass
class RunContext:
    run_id: str
    run_name: str
    config_name: str
    run_dir: Path
    result_dir: Path
    stdout_log: Path
    stderr_log: Path
    meta_path: Path
    snapshot_path: Path


class RunStore:
    def __init__(self) -> None:
        app_config = read_app_config()
        self.config_store = ConfigStore()
        self.runs_dir = (REPO_ROOT / app_config["runs_dir"]).resolve()
        self.lock_file = (REPO_ROOT / app_config["run_lock_file"]).resolve()
        self.script_path = (REPO_ROOT / "run_distributed_mdtest_final.sh").resolve()
        self._active_process: subprocess.Popen[str] | None = None
        self._lock = threading.Lock()

    def current_run(self) -> dict[str, Any] | None:
        if not self.lock_file.exists():
            return None
        payload = load_json(self.lock_file)
        if self._pid_alive(payload.get("pid")):
            return payload
        self._finalize_stale_run(payload)
        return None

    def start_run(self, config_name: str, config_payload: dict[str, Any] | None = None) -> dict[str, Any]:
        with self._lock:
            active = self.current_run()
            if active:
                raise RunStateError("an experiment is already running")

            config = config_payload or self.config_store.get_config(config_name)
            if config.get("config_name") != config_name:
                raise ConfigValidationError("config payload name mismatch")

            ctx = self._create_run_context(config_name, config)
            self._write_run_meta(
                ctx.meta_path,
                {
                    "run_id": ctx.run_id,
                    "run_name": ctx.run_name,
                    "config_name": config_name,
                    "status": "starting",
                    "started_at": datetime.now().isoformat(timespec="seconds"),
                    "ended_at": None,
                    "pid": None,
                    "return_code": None,
                    "result_dir": str(ctx.result_dir),
                    "stdout_log": str(ctx.stdout_log),
                    "stderr_log": str(ctx.stderr_log),
                    "config_snapshot": str(ctx.snapshot_path),
                },
            )

            dump_json(ctx.snapshot_path, config)
            ctx.stdout_log.parent.mkdir(parents=True, exist_ok=True)
            stdout_handle = ctx.stdout_log.open("w", encoding="utf-8")
            stderr_handle = ctx.stderr_log.open("w", encoding="utf-8")

            command = [
                "bash",
                str(self.script_path),
                "--config",
                str(ctx.snapshot_path),
                "--run-tag",
                ctx.run_id,
                "--output-dir",
                str(ctx.result_dir),
            ]

            popen_kwargs: dict[str, Any] = {
                "cwd": str(REPO_ROOT),
                "stdout": stdout_handle,
                "stderr": stderr_handle,
                "text": True,
            }
            if os.name == "nt":
                popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
            else:
                popen_kwargs["preexec_fn"] = os.setsid

            process = subprocess.Popen(command, **popen_kwargs)
            self._active_process = process
            lock_payload = {
                "run_id": ctx.run_id,
                "run_name": ctx.run_name,
                "config_name": config_name,
                "pid": process.pid,
                "started_at": datetime.now().isoformat(timespec="seconds"),
                "run_dir": str(ctx.run_dir),
                "result_dir": str(ctx.result_dir),
            }
            dump_json(self.lock_file, lock_payload)
            self._update_meta(ctx.meta_path, {"status": "running", "pid": process.pid})

            watcher = threading.Thread(
                target=self._watch_process,
                args=(process, ctx.meta_path, self.lock_file, stdout_handle, stderr_handle),
                daemon=True,
            )
            watcher.start()
            return self.get_run_detail(config_name, ctx.run_id)

    def stop_run(self) -> dict[str, Any]:
        with self._lock:
            current = self.current_run()
            if not current:
                raise RunStateError("no active experiment")

            pid = current["pid"]
            meta_path = Path(current["run_dir"]) / "run_meta.json"
            self._update_meta(meta_path, {"status": "stopping"})

            if os.name == "nt":
                os.kill(pid, signal.CTRL_BREAK_EVENT)
            else:
                os.killpg(pid, signal.SIGTERM)

            return self.get_run_detail(current["config_name"], current["run_id"])

    def get_run_detail(self, config_name: str, run_id: str) -> dict[str, Any]:
        meta_path = self.runs_dir / config_name / run_id / "run_meta.json"
        if not meta_path.exists():
            raise FileNotFoundError(run_id)
        return load_json(meta_path)

    def list_runs(self, config_name: str) -> list[dict[str, Any]]:
        run_root = self.runs_dir / config_name
        if not run_root.exists():
            return []
        runs: list[dict[str, Any]] = []
        for run_dir in sorted(run_root.iterdir(), reverse=True):
            meta_path = run_dir / "run_meta.json"
            if meta_path.exists():
                runs.append(load_json(meta_path))
        return runs

    def delete_run(self, config_name: str, run_id: str) -> None:
        run_dir = self.runs_dir / config_name / run_id
        if not run_dir.exists():
            raise FileNotFoundError(run_id)
        current = self.current_run()
        if current and current.get("config_name") == config_name and current.get("run_id") == run_id:
            raise RunStateError("cannot delete a running experiment")
        shutil.rmtree(run_dir)

    def read_log(self, config_name: str, run_id: str, log_type: str) -> str:
        filename = "stdout.log" if log_type == "stdout" else "stderr.log"
        path = self.runs_dir / config_name / run_id / "logs" / filename
        if not path.exists():
            raise FileNotFoundError(path)
        return path.read_text(encoding="utf-8", errors="replace")

    def read_snapshot(self, config_name: str, run_id: str) -> dict[str, Any]:
        path = self.runs_dir / config_name / run_id / "test.conf.json"
        if not path.exists():
            raise FileNotFoundError(path)
        return load_json(path)

    def read_summary(self, config_name: str, run_id: str) -> dict[str, Any]:
        result_dir = self.runs_dir / config_name / run_id / "results"
        summary_text_path = result_dir / "final_summary.txt"
        summary_json_path = result_dir / "summary.json"
        payload: dict[str, Any] = {
            "summary_text": "",
            "summary_json": None,
            "result_dir": str(result_dir),
        }
        if summary_text_path.exists():
            payload["summary_text"] = summary_text_path.read_text(encoding="utf-8", errors="replace")
        if summary_json_path.exists():
            payload["summary_json"] = load_json(summary_json_path)
        return payload

    def read_analysis_notes(self, config_name: str, run_id: str) -> dict[str, Any]:
        run_dir = self.runs_dir / config_name / run_id
        if not run_dir.exists():
            raise FileNotFoundError(run_id)
        path = run_dir / "analysis_notes.json"
        if not path.exists():
            return {"text": "", "updated_at": None}
        payload = load_json(path)
        return {
            "text": str(payload.get("text", "")),
            "updated_at": payload.get("updated_at"),
        }

    def write_analysis_notes(self, config_name: str, run_id: str, text: str) -> dict[str, Any]:
        run_dir = self.runs_dir / config_name / run_id
        if not run_dir.exists():
            raise FileNotFoundError(run_id)
        payload = {
            "text": text,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        dump_json(run_dir / "analysis_notes.json", payload)
        return payload

    def _create_run_context(self, config_name: str, config_payload: dict[str, Any]) -> RunContext:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_id = f"{config_name}_{timestamp}"
        run_dir = self.runs_dir / config_name / run_id
        result_dir = run_dir / "results"
        logs_dir = run_dir / "logs"
        run_dir.mkdir(parents=True, exist_ok=False)
        result_dir.mkdir(parents=True, exist_ok=True)
        logs_dir.mkdir(parents=True, exist_ok=True)
        return RunContext(
            run_id=run_id,
            run_name=run_id,
            config_name=config_name,
            run_dir=run_dir,
            result_dir=result_dir,
            stdout_log=logs_dir / "stdout.log",
            stderr_log=logs_dir / "stderr.log",
            meta_path=run_dir / "run_meta.json",
            snapshot_path=run_dir / "test.conf.json",
        )

    def _watch_process(
        self,
        process: subprocess.Popen[str],
        meta_path: Path,
        lock_file: Path,
        stdout_handle,
        stderr_handle,
    ) -> None:
        return_code = process.wait()
        stdout_handle.close()
        stderr_handle.close()
        existing = load_json(meta_path) if meta_path.exists() else {}
        if existing.get("status") == "stopping":
            status = "stopped"
        else:
            status = "finished" if return_code == 0 else "failed"
        self._update_meta(
            meta_path,
            {
                "status": status,
                "return_code": return_code,
                "ended_at": datetime.now().isoformat(timespec="seconds"),
            },
        )
        with self._lock:
            if lock_file.exists():
                lock_payload = load_json(lock_file)
                if lock_payload.get("pid") == process.pid:
                    lock_file.unlink()
            if self._active_process and self._active_process.pid == process.pid:
                self._active_process = None

    def _finalize_stale_run(self, payload: dict[str, Any]) -> None:
        meta_path = Path(payload["run_dir"]) / "run_meta.json"
        if meta_path.exists():
            self._update_meta(
                meta_path,
                {
                    "status": "stopped",
                    "ended_at": datetime.now().isoformat(timespec="seconds"),
                },
            )
        if self.lock_file.exists():
            self.lock_file.unlink()

    def _write_run_meta(self, path: Path, payload: dict[str, Any]) -> None:
        dump_json(path, payload)

    def _update_meta(self, path: Path, patch: dict[str, Any]) -> None:
        payload = load_json(path)
        payload.update(patch)
        dump_json(path, payload)

    def _pid_alive(self, pid: int | None) -> bool:
        if not isinstance(pid, int) or pid <= 0:
            return False
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True
