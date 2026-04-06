from __future__ import annotations

import json
from typing import Any

from server.utils.paths import REPO_ROOT, dump_json, read_app_config


class MetricsStore:
    METRIC_FALLBACKS = {
        "server.handle_client_request": ("mds_server.handle_client_request",),
        "server.journal_latency": ("mds_log.jlat",),
        "mds_cache.num_inodes": ("mds.inodes",),
        "mds_cache.num_caps": ("mds.caps",),
    }

    def __init__(self) -> None:
        app_config = read_app_config()
        self.runs_dir = (REPO_ROOT / app_config["runs_dir"]).resolve()

    def append_report(self, payload: dict[str, Any]) -> None:
        run_id = str(payload.get("run_id", "")).strip()
        config_name = str(payload.get("config_name", "")).strip()
        if not run_id or not config_name:
            raise ValueError("run_id and config_name are required")
        run_dir = self.runs_dir / config_name / run_id
        if not run_dir.exists():
            raise FileNotFoundError(run_id)

        metrics_dir = run_dir / "metrics"
        metrics_dir.mkdir(parents=True, exist_ok=True)
        report_file = metrics_dir / "reports.jsonl"
        with report_file.open("a", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False)
            handle.write("\n")

        latest_file = metrics_dir / "latest.json"
        dump_json(latest_file, payload)

    def read_reports(self, config_name: str, run_id: str) -> list[dict[str, Any]]:
        report_file = self.runs_dir / config_name / run_id / "metrics" / "reports.jsonl"
        if not report_file.exists():
            return []
        records: list[dict[str, Any]] = []
        with report_file.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                records.append(json.loads(line))
        return records

    def build_series(self, config_name: str, run_id: str, metric_key: str) -> dict[str, Any]:
        reports = self.read_reports(config_name, run_id)
        grouped: dict[str, list[dict[str, Any]]] = {}
        for report in reports:
            value = self._extract_metric(report, metric_key)
            if value is None:
                continue
            mds_name = str(report.get("mds_name") or report.get("mds") or "unknown")
            grouped.setdefault(mds_name, []).append(
                {
                    "ts": report.get("ts"),
                    "value": value,
                    "host": report.get("host"),
                }
            )
        for series in grouped.values():
            series.sort(key=lambda item: item.get("ts") or "")
        return {
            "metric": metric_key,
            "series": grouped,
        }

    def _extract_metric(self, report: dict[str, Any], metric_key: str) -> float | None:
        proc_stat = report.get("proc_stat", {})
        if metric_key in proc_stat:
            return self._to_number(proc_stat.get(metric_key))

        candidate_keys = (metric_key, *self.METRIC_FALLBACKS.get(metric_key, ()))
        for candidate in candidate_keys:
            value = self._extract_perf_dump_metric(report.get("perf_dump", {}), candidate)
            if value is not None:
                return value
        return None

    def _extract_perf_dump_metric(self, perf_dump: dict[str, Any], metric_key: str) -> float | None:
        node: Any = perf_dump
        parts = metric_key.split(".")
        if parts and parts[0] not in perf_dump and "mds" in perf_dump:
            node = perf_dump["mds"]
        for part in parts:
            if isinstance(node, dict) and part in node:
                node = node[part]
            else:
                return None
        if isinstance(node, dict):
            if "avgcount" in node and "sum" in node:
                avgcount = self._to_number(node.get("avgcount"))
                total = self._to_number(node.get("sum"))
                if avgcount and total is not None:
                    return total / avgcount
            for key in ("value", "avgtime", "latest"):
                if key in node:
                    return self._to_number(node.get(key))
            return None
        return self._to_number(node)

    def _to_number(self, value: Any) -> float | None:
        if value is None or value == "":
            return None
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(str(value))
        except ValueError:
            return None
