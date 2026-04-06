from __future__ import annotations

import json
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Iterable

from werkzeug.datastructures import FileStorage

from server.services.config_store import ConfigStore
from server.utils.paths import REPO_ROOT, dump_json, read_app_config


class ImportValidationError(ValueError):
    pass


@dataclass(frozen=True)
class UploadedEntry:
    relative_path: str
    storage: FileStorage


class ImportService:
    IMPORTED_CONFIG_NAME = "imported_experiments"

    def __init__(self) -> None:
        app_config = read_app_config()
        self.config_store = ConfigStore()
        self.runs_dir = (REPO_ROOT / app_config["runs_dir"]).resolve()

    def import_experiment(self, manifest: list[dict[str, Any]], files: list[FileStorage]) -> dict[str, Any]:
        entries = self._build_entries(manifest, files)
        if not entries:
            raise ImportValidationError("no files were uploaded")

        with tempfile.TemporaryDirectory(prefix="cephfs_import_") as tmp_dir:
            staging_root = Path(tmp_dir)
            source_dir_name = self._materialize_uploads(staging_root, entries)
            source_root = staging_root / source_dir_name
            summary_text = self._read_required_text(source_root / "final_summary.txt", "final_summary.txt is required")
            reports = self._build_reports(source_root)
            if not reports:
                raise ImportValidationError("no MDS raw metric samples were found under mds_metrics/*/data/raw/*/*.json")

            run_id = datetime.now().strftime("import_%Y%m%d_%H%M%S_%f")
            run_dir = self._prepare_run_dir(self.IMPORTED_CONFIG_NAME, run_id)
            result_dir = run_dir / "results"
            logs_dir = run_dir / "logs"
            metrics_dir = run_dir / "metrics"
            summary_json = self._build_summary_json(source_dir_name, reports, summary_text)
            started_at = min(report["ts"] for report in reports)
            ended_at = max(report["ts"] for report in reports)

            self._ensure_import_config()
            result_dir.mkdir(parents=True, exist_ok=True)
            logs_dir.mkdir(parents=True, exist_ok=True)
            metrics_dir.mkdir(parents=True, exist_ok=True)

            (result_dir / "final_summary.txt").write_text(summary_text, encoding="utf-8")
            dump_json(result_dir / "summary.json", summary_json)
            (logs_dir / "stdout.log").write_text(self._build_stdout_text(source_root), encoding="utf-8")
            (logs_dir / "stderr.log").write_text("", encoding="utf-8")
            dump_json(
                run_dir / "test.conf.json",
                {
                    "config_name": self.IMPORTED_CONFIG_NAME,
                    "imported": True,
                    "source_dir_name": source_dir_name,
                    "imported_at": datetime.now().isoformat(timespec="seconds"),
                    "report_count": len(reports),
                    "mds_names": sorted({report["mds_name"] for report in reports}),
                    "hosts": sorted({report["host"] for report in reports}),
                },
            )
            dump_json(
                run_dir / "run_meta.json",
                {
                    "run_id": run_id,
                    "run_name": source_dir_name,
                    "config_name": self.IMPORTED_CONFIG_NAME,
                    "status": "completed",
                    "started_at": started_at,
                    "ended_at": ended_at,
                    "pid": None,
                    "return_code": 0,
                    "result_dir": str(result_dir),
                    "stdout_log": str(logs_dir / "stdout.log"),
                    "stderr_log": str(logs_dir / "stderr.log"),
                    "config_snapshot": str(run_dir / "test.conf.json"),
                    "imported": True,
                    "source_dir_name": source_dir_name,
                },
            )

            reports_file = metrics_dir / "reports.jsonl"
            with reports_file.open("w", encoding="utf-8") as handle:
                for report in reports:
                    json.dump(report, handle, ensure_ascii=False)
                    handle.write("\n")
            dump_json(metrics_dir / "latest.json", reports[-1])
            return {
                "config_name": self.IMPORTED_CONFIG_NAME,
                "run_id": run_id,
                "source_dir_name": source_dir_name,
                "summary": summary_json,
            }

    def _build_entries(self, manifest: list[dict[str, Any]], files: list[FileStorage]) -> list[UploadedEntry]:
        if len(manifest) != len(files):
            raise ImportValidationError("uploaded file manifest does not match uploaded file count")
        entries: list[UploadedEntry] = []
        for item, storage in zip(manifest, files):
            relative_path = self._normalize_relative_path(str(item.get("path", "")))
            entries.append(UploadedEntry(relative_path=relative_path, storage=storage))
        return entries

    def _materialize_uploads(self, staging_root: Path, entries: Iterable[UploadedEntry]) -> str:
        source_dir_name: str | None = None
        for entry in entries:
            parts = PurePosixPath(entry.relative_path).parts
            if len(parts) < 2:
                raise ImportValidationError("uploaded files must include the selected directory name and nested files")
            root_name = parts[0]
            if source_dir_name is None:
                source_dir_name = root_name
            elif source_dir_name != root_name:
                raise ImportValidationError("uploaded files must come from a single selected experiment directory")
            target = staging_root.joinpath(*parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            entry.storage.save(target)
        if not source_dir_name:
            raise ImportValidationError("could not determine selected experiment directory name")
        return source_dir_name

    def _build_reports(self, source_root: Path) -> list[dict[str, Any]]:
        reports: list[dict[str, Any]] = []
        raw_paths = sorted(source_root.glob("mds_metrics/*/data/raw/*/*.json"))
        for path in raw_paths:
            payload = self._load_json(path)
            sample_meta = payload.get("sample_meta")
            perf_dump = payload.get("perf_dump")
            if not isinstance(sample_meta, dict) or not isinstance(perf_dump, dict):
                continue
            timestamp = sample_meta.get("timestamp")
            if not isinstance(timestamp, str) or not timestamp.strip():
                raise ImportValidationError(f"missing sample_meta.timestamp in {path.relative_to(source_root)}")
            host = self._coalesce_text(
                sample_meta.get("host"),
                path.parts[-5],
            )
            mds_name = self._coalesce_text(
                sample_meta.get("mds_name"),
                path.parts[-2].removeprefix("mds."),
                host,
            )
            report = {
                "ts": timestamp,
                "host": host,
                "mds_name": mds_name,
                "mds": self._coalesce_text(sample_meta.get("mds_daemon"), f"mds.{mds_name}"),
                "source_file": str(path.relative_to(source_root)),
                "sample_meta": sample_meta,
                "perf_dump": perf_dump,
            }
            proc_stat = payload.get("proc_stat")
            if isinstance(proc_stat, dict):
                report["proc_stat"] = proc_stat
            reports.append(report)
        reports.sort(key=lambda item: (item["ts"], item["host"], item["mds_name"], item["source_file"]))
        return reports

    def _build_summary_json(self, source_dir_name: str, reports: list[dict[str, Any]], summary_text: str) -> dict[str, Any]:
        return {
            "source_dir_name": source_dir_name,
            "imported": True,
            "report_count": len(reports),
            "host_count": len({report["host"] for report in reports}),
            "mds_count": len({report["mds_name"] for report in reports}),
            "started_at": min(report["ts"] for report in reports),
            "ended_at": max(report["ts"] for report in reports),
            "summary_preview": summary_text[:600],
        }

    def _build_stdout_text(self, source_root: Path) -> str:
        chunks = [
            f"Imported experiment directory: {source_root.name}",
            "",
        ]
        for path in sorted(source_root.glob("result_*.log")):
            chunks.append(f"===== {path.name} =====")
            chunks.append(path.read_text(encoding="utf-8", errors="replace"))
            chunks.append("")
        if len(chunks) == 2:
            chunks.append("No result_*.log files were present in the imported directory.")
        return "\n".join(chunks).strip() + "\n"

    def _ensure_import_config(self) -> None:
        try:
            self.config_store.get_config(self.IMPORTED_CONFIG_NAME)
        except FileNotFoundError:
            base = self.config_store.default_config()
            base["config_name"] = self.IMPORTED_CONFIG_NAME
            self.config_store.create_config(self.IMPORTED_CONFIG_NAME, base)

    def _prepare_run_dir(self, config_name: str, run_id: str) -> Path:
        run_dir = self.runs_dir / config_name / run_id
        if run_dir.exists():
            shutil.rmtree(run_dir)
        run_dir.mkdir(parents=True, exist_ok=True)
        return run_dir

    def _normalize_relative_path(self, raw_path: str) -> str:
        path = PurePosixPath(raw_path.replace("\\", "/"))
        if not raw_path.strip() or path.is_absolute():
            raise ImportValidationError("uploaded file path is invalid")
        parts = []
        for part in path.parts:
            if part in {"", "."}:
                continue
            if part == "..":
                raise ImportValidationError("uploaded file path cannot contain '..'")
            parts.append(part)
        if not parts:
            raise ImportValidationError("uploaded file path is empty")
        return str(PurePosixPath(*parts))

    def _read_required_text(self, path: Path, error_message: str) -> str:
        if not path.exists():
            raise ImportValidationError(error_message)
        return path.read_text(encoding="utf-8", errors="replace")

    def _load_json(self, path: Path) -> dict[str, Any]:
        try:
            with path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except json.JSONDecodeError as exc:
            raise ImportValidationError(f"invalid JSON in {path.name}: {exc.msg}") from exc
        if not isinstance(payload, dict):
            raise ImportValidationError(f"JSON payload in {path.name} must be an object")
        return payload

    def _coalesce_text(self, *values: Any) -> str:
        for value in values:
            if isinstance(value, str) and value.strip():
                return value.strip()
        raise ImportValidationError("required text field is missing")
