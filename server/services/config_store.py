from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from server.utils.paths import REPO_ROOT, dump_json, load_json, read_app_config


DEFAULT_CONFIG = {
    "config_name": "",
    "np_per_tenant": 64,
    "files_per_proc": 10000,
    "iterations": 3,
    "mdtest_args": "-F -C -T -r -R -u -w 4K -e 4K",
    "allow_run_as_root": True,
    "collect_ceph_status": True,
    "collect_mds_metrics": True,
    "metrics_interval_sec": 20,
    "generate_summary": True,
    "summary_mode": "both",
    "hostfile_a": "mpi_hosts_a",
    "hostfile_b": "mpi_hosts_b",
    "hostfile_c": "mpi_hosts_c",
    "ceph_host_file": "ceph_host",
    "base_mnt": "/mnt/tenant_a",
    "tenant_a_dir": "/mnt/tenant_a/perf_tenant_a",
    "tenant_b_dir": "/mnt/tenant_a/perf_tenant_b",
    "tenant_c_dir": "/mnt/tenant_a/perf_tenant_c",
    "mpi_bin": "/usr/lib64/openmpi/bin/mpirun",
    "mpi_prefix": "/usr/lib64/openmpi",
    "mds_remote_base": "/tmp/mds_metrics",
    "mds_collector_script": "collect_mds_metrics.sh",
    "metrics_report_endpoint": "http://127.0.0.1:18080/api/metrics/report",
    "metrics_schema_file": "conf/metrics_schema.json",
}


class ConfigValidationError(ValueError):
    pass


@dataclass(frozen=True)
class ConfigSummary:
    name: str
    updated_at: str
    path: str


class ConfigStore:
    def __init__(self) -> None:
        app_config = read_app_config()
        self.configs_dir = (REPO_ROOT / app_config["configs_dir"]).resolve()
        self.runs_dir = (REPO_ROOT / app_config["runs_dir"]).resolve()
        self.lock_file = (REPO_ROOT / app_config["run_lock_file"]).resolve()
        self.configs_dir.mkdir(parents=True, exist_ok=True)
        self.runs_dir.mkdir(parents=True, exist_ok=True)

    def list_configs(self) -> list[ConfigSummary]:
        records: list[ConfigSummary] = []
        for path in sorted(self.configs_dir.glob("*.json")):
            stat = path.stat()
            records.append(
                ConfigSummary(
                    name=path.stem,
                    updated_at=datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
                    path=str(path),
                )
            )
        return records

    def get_config_path(self, name: str) -> Path:
        self._validate_name(name)
        return self.configs_dir / f"{name}.json"

    def get_config(self, name: str) -> dict[str, Any]:
        path = self.get_config_path(name)
        if not path.exists():
            raise FileNotFoundError(name)
        return load_json(path)

    def create_config(self, name: str, payload: dict[str, Any]) -> dict[str, Any]:
        path = self.get_config_path(name)
        if path.exists():
            raise FileExistsError(name)
        normalized = self._normalize_payload(name, payload)
        dump_json(path, normalized)
        return normalized

    def update_config(self, name: str, payload: dict[str, Any]) -> dict[str, Any]:
        path = self.get_config_path(name)
        if not path.exists():
            raise FileNotFoundError(name)
        normalized = self._normalize_payload(name, payload)
        dump_json(path, normalized)
        return normalized

    def rename_config(self, old_name: str, new_name: str) -> None:
        old_path = self.get_config_path(old_name)
        new_path = self.get_config_path(new_name)
        if not old_path.exists():
            raise FileNotFoundError(old_name)
        if new_path.exists():
            raise FileExistsError(new_name)
        payload = load_json(old_path)
        payload["config_name"] = new_name
        dump_json(new_path, payload)
        old_path.unlink()
        old_run_dir = self.runs_dir / old_name
        new_run_dir = self.runs_dir / new_name
        if old_run_dir.exists() and not new_run_dir.exists():
            old_run_dir.rename(new_run_dir)

    def delete_config(self, name: str, force: bool = False) -> None:
        path = self.get_config_path(name)
        if not path.exists():
            raise FileNotFoundError(name)
        run_dir = self.runs_dir / name
        if run_dir.exists() and any(run_dir.iterdir()):
            if not force:
                raise ConfigValidationError("config has experiment runs and cannot be deleted without force")
            if self._has_active_run(name):
                raise ConfigValidationError("config has a running experiment and cannot be deleted")
        if run_dir.exists():
            shutil.rmtree(run_dir)
        path.unlink()

    def experiment_tree(self) -> list[dict[str, Any]]:
        tree: list[dict[str, Any]] = []
        for item in self.list_configs():
            run_root = self.runs_dir / item.name
            runs: list[dict[str, Any]] = []
            if run_root.exists():
                for run_dir in sorted(run_root.iterdir(), reverse=True):
                    meta_path = run_dir / "run_meta.json"
                    if meta_path.exists():
                        meta = load_json(meta_path)
                    else:
                        meta = {"run_id": run_dir.name, "status": "unknown"}
                    runs.append(meta)
            tree.append({"config": item.__dict__, "runs": runs})
        return tree

    def metrics_schema(self) -> dict[str, Any]:
        return load_json(REPO_ROOT / "conf" / "metrics_schema.json")

    def default_config(self) -> dict[str, Any]:
        return dict(DEFAULT_CONFIG)

    def _normalize_payload(self, name: str, payload: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ConfigValidationError("config payload must be a JSON object")
        merged = dict(DEFAULT_CONFIG)
        merged.update(payload)
        merged["config_name"] = name
        self._validate_config(merged)
        return merged

    def _validate_config(self, payload: dict[str, Any]) -> None:
        integer_fields = ["np_per_tenant", "files_per_proc", "iterations", "metrics_interval_sec"]
        for field_name in integer_fields:
            value = payload.get(field_name)
            if not isinstance(value, int) or value <= 0:
                raise ConfigValidationError(f"{field_name} must be a positive integer")
        for field_name in ["mdtest_args", "hostfile_a", "hostfile_b", "hostfile_c", "ceph_host_file"]:
            value = payload.get(field_name)
            if not isinstance(value, str) or not value.strip():
                raise ConfigValidationError(f"{field_name} must be a non-empty string")
        for field_name in ["allow_run_as_root", "collect_ceph_status", "collect_mds_metrics", "generate_summary"]:
            if not isinstance(payload.get(field_name), bool):
                raise ConfigValidationError(f"{field_name} must be boolean")
        if payload.get("summary_mode") not in {"both", "raw_only", "summary_only"}:
            raise ConfigValidationError("summary_mode must be one of both/raw_only/summary_only")

    def _validate_name(self, name: str) -> None:
        if not name or not isinstance(name, str):
            raise ConfigValidationError("config name is required")
        for char in name:
            if not (char.isalnum() or char in {"_", "-"}):
                raise ConfigValidationError("config name only supports letters, digits, '_' and '-'")

    def _has_active_run(self, config_name: str) -> bool:
        if not self.lock_file.exists():
            return False
        payload = load_json(self.lock_file)
        return payload.get("config_name") == config_name
