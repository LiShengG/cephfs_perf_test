#!/usr/bin/env python3
"""Offline parser and plotting tool for collect_mds_perf_raw.sh outputs."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import logging
import math
import re
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

MISSING_CORE_DEPS: list[str] = []
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except Exception:
    matplotlib = None
    plt = None
    MISSING_CORE_DEPS.append("matplotlib")

try:
    import numpy as np
except Exception:
    np = None
    MISSING_CORE_DEPS.append("numpy")

try:
    import pandas as pd
except Exception:
    pd = None
    MISSING_CORE_DEPS.append("pandas")

try:
    import yaml  # type: ignore
except Exception:  # optional
    yaml = None


DEFAULT_GAUGE_METRICS = {
    "mds.q",
    "mds.inodes",
    "mds.root_rfiles",
    "mds.inodes_pinned",
    "mds.inodes_with_caps",
    "mds.caps",
    "mds.subtrees",
    "mds_cache.num_strays",
    "mds_cache.num_strays_delayed",
    "mds_cache.num_strays_enqueuing",
    "mds_mem.ino",
    "mds_mem.dn",
    "mds_mem.cap",
    "mds_mem.rss",
    "mds_mem.heap",
}

DEFAULT_COUNTER_METRICS = {
    "mds.request",
    "mds.reply",
    "mds.forward",
    "mds.dir_fetch",
    "mds.openino_peer_discover",
    "mds.traverse",
    "mds.traverse_hit",
    "mds.traverse_dir_fetch",
    "mds.load_cent",
    "mds.exported",
    "mds.exported_inodes",
    "mds.imported",
    "mds.imported_inodes",
    "mds_cache.strays_created",
    "mds_cache.strays_enqueued",
    "mds_log.evadd",
    "mds_log.evex",
    "mds_log.evtrm",
    "mds_log.segadd",
    "mds_log.segex",
    "mds_log.segtrm",
    "mds_server.dispatch_client_request",
    "mds_server.handle_client_request",
    "mds_server.handle_client_session",
}

DEFAULT_LATENCY_BASES = {
    "mds.reply_latency",
    "mds_log.jlat",
    "mds_server.req_create_latency",
    "mds_server.req_lookup_latency",
    "mds_server.req_lookupino_latency",
    "mds_server.req_getattr_latency",
    "mds_server.req_readdir_latency",
    "mds_server.req_setxattr_latency",
    "mds_server.req_unlink_latency",
}

DEFAULT_METRICS = sorted(
    {
        "mds.request",
        "mds.reply",
        "mds.forward",
        "mds.dir_fetch",
        "mds.openino_peer_discover",
        "mds.traverse",
        "mds_cache.strays_created",
        "mds.root_rfiles",
        "mds_mem.rss",
        "mds.q",
        "mds_server.req_lookup_latency",
        "mds_server.req_lookupino_latency",
        "mds_server.req_unlink_latency",
        "mds_log.jlat",
    }
)

STANDARD_PER_MDS_PLOTS = [
    ("mds.request", "rate"),
    ("mds.reply", "rate"),
    ("mds.forward", "rate"),
    ("mds.dir_fetch", "rate"),
    ("mds.openino_peer_discover", "rate"),
    ("mds.traverse", "rate"),
    ("mds_cache.strays_created", "rate"),
    ("mds.root_rfiles", "rate"),
    ("mds_mem.rss", "raw"),
    ("mds.q", "raw"),
    ("mds_server.req_lookup_latency", "period_avg"),
    ("mds_server.req_lookupino_latency", "period_avg"),
    ("mds_server.req_unlink_latency", "period_avg"),
    ("mds_log.jlat", "period_avg"),
]

STANDARD_AGGREGATE_PLOTS = [
    ("mds.request", "rate"),
    ("mds.reply", "rate"),
    ("mds_cache.strays_created", "rate"),
    ("mds_mem.rss", "raw"),
]

RAW_TS_RE = re.compile(r"_(\d{8}T\d{6}[+-]\d{4})\.json$")


@dataclass
class AnalysisConfig:
    input_dirs: List[str]
    output_dir: Path
    metrics: List[str]
    transforms: List[str]
    group_by: str
    compare_mode: str
    align_time: str
    plot_formats: List[str]
    export_formats: List[str]
    default_plots: str
    reset_policy: str
    max_workers: int
    config_path: Optional[Path] = None
    gauge_metrics: set[str] = field(default_factory=lambda: set(DEFAULT_GAUGE_METRICS))
    counter_metrics: set[str] = field(default_factory=lambda: set(DEFAULT_COUNTER_METRICS))
    latency_bases: set[str] = field(default_factory=lambda: set(DEFAULT_LATENCY_BASES))


@dataclass
class Experiment:
    experiment_id: str
    run_dir: Path
    experiment_tag: str
    cluster_name: str
    start_ts_unix: float
    raw_index_rows: List[Dict[str, Any]]


@dataclass
class SampleRecord:
    experiment_id: str
    experiment_tag: str
    cluster_name: str
    host: str
    mds_name: str
    mds_daemon: str
    timestamp: str
    timestamp_unix: float
    time_offset_sec: float
    seq: int
    source_file: str
    flat_metrics: Dict[str, float]


def parse_csv_list(raw: Optional[str], default: Sequence[str]) -> List[str]:
    if not raw:
        return list(default)
    return [x.strip() for x in raw.split(",") if x.strip()]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analyze Ceph MDS perf raw data offline")
    p.add_argument("--input-dirs", required=True)
    p.add_argument("--output-dir")
    p.add_argument("--metrics")
    p.add_argument("--transforms", default="raw,delta,rate,period_avg")
    p.add_argument("--group-by", default="per_mds", choices=["per_mds", "per_host", "aggregate"])
    p.add_argument("--compare-mode", default="none", choices=["none", "overlay"])
    p.add_argument("--align-time", default="relative", choices=["absolute", "relative"])
    p.add_argument("--plot-formats", default="png")
    p.add_argument("--export-formats", default="csv,parquet")
    p.add_argument("--default-plots", default="standard", choices=["none", "standard"])
    p.add_argument("--reset-policy", default="mark", choices=["mark", "drop"])
    p.add_argument("--max-workers", type=int, default=4)
    p.add_argument("--config")
    return p.parse_args()


def load_config(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}
    if yaml is None:
        raise RuntimeError("YAML config requested but pyyaml is not installed")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def build_config(args: argparse.Namespace) -> AnalysisConfig:
    cfg_file = load_config(args.config)
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(args.output_dir or cfg_file.get("output_dir") or f"analysis_out_{now}").resolve()
    transforms = parse_csv_list(args.transforms or cfg_file.get("transforms"), ["raw", "delta", "rate", "period_avg"])
    export_formats = parse_csv_list(args.export_formats or cfg_file.get("export_formats"), ["csv", "parquet"])
    plot_formats = parse_csv_list(args.plot_formats or cfg_file.get("plot_formats"), ["png"])
    metrics = parse_csv_list(args.metrics or cfg_file.get("metrics"), DEFAULT_METRICS)
    gauge = set(cfg_file.get("gauge_metrics", DEFAULT_GAUGE_METRICS))
    counter = set(cfg_file.get("counter_metrics", DEFAULT_COUNTER_METRICS))
    latency = set(cfg_file.get("latency_bases", DEFAULT_LATENCY_BASES))
    return AnalysisConfig(
        input_dirs=parse_csv_list(args.input_dirs, []),
        output_dir=output_dir,
        metrics=metrics,
        transforms=transforms,
        group_by=args.group_by,
        compare_mode=args.compare_mode,
        align_time=args.align_time,
        plot_formats=plot_formats,
        export_formats=export_formats,
        default_plots=args.default_plots,
        reset_policy=args.reset_policy,
        max_workers=max(args.max_workers, 1),
        config_path=Path(args.config).resolve() if args.config else None,
        gauge_metrics=gauge,
        counter_metrics=counter,
        latency_bases=latency,
    )


def setup_output_dirs(root: Path) -> Dict[str, Path]:
    dirs = {
        "root": root,
        "meta": root / "meta",
        "tidy": root / "tidy",
        "derived": root / "derived",
        "plots": root / "plots",
        "plots_per_experiment": root / "plots" / "per_experiment",
        "plots_compare": root / "plots" / "compare",
        "logs": root / "logs",
    }
    for p in dirs.values():
        p.mkdir(parents=True, exist_ok=True)
    for scope in ["per_mds", "per_host", "aggregate"]:
        (dirs["derived"] / scope).mkdir(parents=True, exist_ok=True)
    return dirs


def configure_logging(log_dir: Path) -> Tuple[logging.Logger, logging.Logger]:
    logger = logging.getLogger("analyze")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh = logging.FileHandler(log_dir / "analyze.log", encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)

    error_logger = logging.getLogger("analyze.errors")
    error_logger.setLevel(logging.WARNING)
    error_logger.handlers.clear()
    efh = logging.FileHandler(log_dir / "errors.log", encoding="utf-8")
    efh.setFormatter(fmt)
    error_logger.addHandler(efh)
    return logger, error_logger


def read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_raw_index(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if not isinstance(obj, dict):
                raise ValueError(f"raw_index line {i} not object")
            rows.append(obj)
    return rows


def parse_raw_ts_from_path(path: Path) -> Tuple[str, float]:
    match = RAW_TS_RE.search(path.name)
    if not match:
        raise ValueError(f"cannot infer timestamp from filename: {path.name}")
    dt = datetime.strptime(match.group(1), "%Y%m%dT%H%M%S%z")
    return dt.isoformat(), dt.timestamp()


def build_index_rows_from_raw_tree(run_dir: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for node_dir in sorted((run_dir / "mds_metrics").glob("*/data/raw/*")):
        if not node_dir.is_dir():
            continue
        for seq, raw_file in enumerate(sorted(node_dir.glob("*.json")), 1):
            rows.append({"file": str(raw_file.relative_to(run_dir)), "seq": seq})
    return rows


def discover_one_run(run_dir: Path) -> Experiment:
    legacy_meta = run_dir / "meta" / "run_info.json"
    legacy_index = run_dir / "summary" / "raw_index.jsonl"
    if legacy_meta.exists() and legacy_index.exists():
        meta = read_json(legacy_meta)
        index_rows = load_raw_index(legacy_index)
        tag = str(meta.get("tag") or meta.get("experiment_tag") or run_dir.name)
        cluster = str(meta.get("cluster") or meta.get("cluster_name") or "unknown")
        start_ts = math.inf
        for row in index_rows:
            ts = row.get("timestamp_unix")
            if isinstance(ts, (int, float)):
                start_ts = min(start_ts, float(ts))
        if math.isinf(start_ts):
            start_ts = 0.0
        return Experiment(
            experiment_id=run_dir.name,
            run_dir=run_dir,
            experiment_tag=tag,
            cluster_name=cluster,
            start_ts_unix=start_ts,
            raw_index_rows=index_rows,
        )

    raw_root = run_dir / "mds_metrics"
    if raw_root.is_dir():
        index_rows = build_index_rows_from_raw_tree(run_dir)
        start_ts = math.inf
        for row in index_rows:
            try:
                _, ts_unix = parse_raw_ts_from_path(run_dir / str(row["file"]))
                start_ts = min(start_ts, ts_unix)
            except Exception:
                continue
        if math.isinf(start_ts):
            start_ts = 0.0
        return Experiment(
            experiment_id=run_dir.name,
            run_dir=run_dir,
            experiment_tag=run_dir.name,
            cluster_name="unknown",
            start_ts_unix=start_ts,
            raw_index_rows=index_rows,
        )

    raise FileNotFoundError(f"unsupported run layout: {run_dir}")


def normalize_input_run(path: Path) -> Path:
    path = path.resolve()
    if path.name == "mds_metrics":
        return path.parent
    if path.name == "data" and path.parent.parent.name == "mds_metrics":
        return path.parent.parent.parent
    return path


def discover_runs(cfg: AnalysisConfig, error_logger: logging.Logger, logger: logging.Logger) -> Tuple[List[Experiment], List[Dict[str, Any]]]:
    experiments: List[Experiment] = []
    statuses: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for run in cfg.input_dirs:
        run_dir = normalize_input_run(Path(run))
        if str(run_dir) in seen:
            continue
        seen.add(str(run_dir))
        status = {"run_dir": str(run_dir), "status": "ok", "error": ""}
        try:
            experiments.append(discover_one_run(run_dir))
        except Exception as exc:
            status["status"] = "failed"
            status["error"] = str(exc)
            error_logger.warning("Failed to load run %s: %s", run_dir, exc)
        statuses.append(status)
    logger.info("Discovered %d experiments (%d ok)", len(statuses), len(experiments))
    return experiments, statuses


def flatten_perf_dump(obj: Any, parent: str = "") -> Dict[str, float]:
    out: Dict[str, float] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{parent}.{k}" if parent else str(k)
            out.update(flatten_perf_dump(v, p))
    elif isinstance(obj, list):
        return out
    else:
        if isinstance(obj, (int, float)) and not isinstance(obj, bool):
            out[parent] = float(obj)
    return out


def _build_sample_from_legacy_raw(exp: Experiment, rel: str, raw: Dict[str, Any]) -> SampleRecord:
    meta = raw["sample_meta"]
    perf_dump = raw["perf_dump"]
    required = ["timestamp", "timestamp_unix", "host", "mds_name", "mds_daemon", "seq"]
    for k in required:
        if k not in meta:
            raise ValueError(f"sample_meta missing {k} in {rel}")
    _ = datetime.fromisoformat(str(meta["timestamp"]))
    ts_unix = float(meta["timestamp_unix"])
    flat = flatten_perf_dump(perf_dump)
    return SampleRecord(
        experiment_id=exp.experiment_id,
        experiment_tag=exp.experiment_tag,
        cluster_name=exp.cluster_name,
        host=str(meta["host"]),
        mds_name=str(meta["mds_name"]),
        mds_daemon=str(meta["mds_daemon"]),
        timestamp=str(meta["timestamp"]),
        timestamp_unix=ts_unix,
        time_offset_sec=ts_unix - exp.start_ts_unix,
        seq=int(meta["seq"]),
        source_file=str(rel),
        flat_metrics=flat,
    )


def _build_sample_from_raw_tree(exp: Experiment, rel: str, raw: Dict[str, Any], index_row: Dict[str, Any]) -> SampleRecord:
    file_path = exp.run_dir / rel
    parts = file_path.parts
    try:
        raw_idx = parts.index("raw")
    except ValueError as exc:
        raise ValueError(f"raw path missing raw segment: {rel}") from exc
    if raw_idx < 2 or raw_idx + 1 >= len(parts):
        raise ValueError(f"unexpected raw path layout: {rel}")

    host = parts[raw_idx - 2]
    mds_daemon = parts[raw_idx + 1]
    timestamp, ts_unix = parse_raw_ts_from_path(file_path)
    seq = int(index_row.get("seq") or file_path.stem.split("_", 1)[0])

    perf_payload: Any = raw
    if isinstance(raw.get("perf_dump"), dict):
        perf_payload = raw["perf_dump"]
    elif isinstance(raw.get("mds"), dict):
        perf_payload = {"mds": raw["mds"]}

    flat = flatten_perf_dump(perf_payload)
    return SampleRecord(
        experiment_id=exp.experiment_id,
        experiment_tag=exp.experiment_tag,
        cluster_name=exp.cluster_name,
        host=host,
        mds_name=mds_daemon,
        mds_daemon=mds_daemon,
        timestamp=timestamp,
        timestamp_unix=ts_unix,
        time_offset_sec=ts_unix - exp.start_ts_unix,
        seq=seq,
        source_file=str(rel),
        flat_metrics=flat,
    )


def _load_one_sample(exp: Experiment, index_row: Dict[str, Any]) -> Tuple[Optional[SampleRecord], Optional[str]]:
    rel = index_row.get("file")
    if not rel:
        return None, "index row missing file"
    file_path = exp.run_dir / rel
    if not file_path.exists():
        return None, f"raw file missing: {rel}"
    try:
        raw = read_json(file_path)
        if "sample_meta" in raw and "perf_dump" in raw:
            return _build_sample_from_legacy_raw(exp, str(rel), raw), None
        return _build_sample_from_raw_tree(exp, str(rel), raw, index_row), None
    except Exception as exc:
        return None, f"failed parsing {rel}: {exc}"


def build_raw_dataframe(experiments: List[Experiment], cfg: AnalysisConfig, error_logger: logging.Logger, logger: logging.Logger) -> Tuple[pd.DataFrame, Dict[str, int]]:
    records: List[Dict[str, Any]] = []
    total_files = 0
    bad_files = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=cfg.max_workers) as ex:
        futures = []
        for exp in experiments:
            for row in exp.raw_index_rows:
                total_files += 1
                futures.append(ex.submit(_load_one_sample, exp, row))
        for fut in concurrent.futures.as_completed(futures):
            rec, err = fut.result()
            if err:
                bad_files += 1
                error_logger.warning(err)
                continue
            assert rec is not None
            base = asdict(rec)
            flat = base.pop("flat_metrics")
            if not flat:
                continue
            for metric_path, value in flat.items():
                if cfg.metrics and metric_path not in cfg.metrics and not any(
                    metric_path.startswith(f"{m}.") for m in cfg.metrics
                ):
                    continue
                row = dict(base)
                row["metric_path"] = metric_path
                row["raw_value"] = value
                records.append(row)

    df = pd.DataFrame.from_records(records)
    if df.empty:
        logger.info("Built raw dataframe rows=0")
        return df, {"total_files": total_files, "bad_files": bad_files, "ok_files": total_files - bad_files}

    df = df.sort_values(["experiment_id", "mds_daemon", "metric_path", "timestamp_unix", "seq"]).reset_index(drop=True)
    logger.info("Built raw dataframe rows=%d", len(df))
    return df, {"total_files": total_files, "bad_files": bad_files, "ok_files": total_files - bad_files}


def infer_metric_type(metric_path: str, cfg: AnalysisConfig) -> str:
    if metric_path in cfg.gauge_metrics:
        return "gauge"
    if metric_path in cfg.counter_metrics:
        return "counter"
    for base in cfg.latency_bases:
        if metric_path == base or metric_path.startswith(base + "."):
            return "latency_pair"
    return "unknown"


def add_group_key(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    out = df.copy()
    if mode == "per_mds":
        out["group_key"] = out["mds_name"].map(lambda x: f"mds.{x}")
    elif mode == "per_host":
        out["group_key"] = out["host"]
    else:
        out["group_key"] = "all"
    return out


def make_transform_rows(df: pd.DataFrame, cfg: AnalysisConfig, error_logger: logging.Logger) -> pd.DataFrame:
    if df.empty:
        return df

    work = df.copy()
    work["metric_type"] = work["metric_path"].map(lambda m: infer_metric_type(m, cfg))
    work = add_group_key(work, cfg.group_by)

    base_cols = [
        "experiment_id",
        "experiment_tag",
        "cluster_name",
        "host",
        "mds_name",
        "mds_daemon",
        "group_key",
        "timestamp",
        "timestamp_unix",
        "time_offset_sec",
        "seq",
        "metric_path",
        "metric_type",
        "source_file",
    ]

    rows: List[pd.DataFrame] = []
    if "raw" in cfg.transforms:
        raw_df = work[base_cols].copy()
        raw_df["transform"] = "raw"
        raw_df["value"] = work["raw_value"]
        raw_df["unit"] = ""
        raw_df["reset_detected"] = False
        raw_df["is_valid"] = True
        rows.append(raw_df)

    calc_df = work.sort_values(["experiment_id", "group_key", "metric_path", "timestamp_unix", "seq"]).copy()
    grp_cols = ["experiment_id", "group_key", "metric_path"]
    calc_df["prev_value"] = calc_df.groupby(grp_cols)["raw_value"].shift(1)
    calc_df["prev_ts"] = calc_df.groupby(grp_cols)["timestamp_unix"].shift(1)
    calc_df["delta"] = calc_df["raw_value"] - calc_df["prev_value"]
    calc_df["interval_sec"] = calc_df["timestamp_unix"] - calc_df["prev_ts"]
    calc_df["reset_detected"] = False

    eligible_delta = (calc_df["metric_type"] == "counter") | (
        (calc_df["metric_type"] == "latency_pair")
        & calc_df["metric_path"].str.endswith((".avgcount", ".sum"))
    )

    reset_mask = eligible_delta & calc_df["prev_value"].notna() & (calc_df["raw_value"] < calc_df["prev_value"])
    calc_df.loc[reset_mask, "reset_detected"] = True
    for _, row in calc_df.loc[reset_mask].iterrows():
        error_logger.warning(
            "reset detected exp=%s group=%s metric=%s ts=%s",
            row["experiment_id"],
            row["group_key"],
            row["metric_path"],
            row["timestamp"],
        )

    if "delta" in cfg.transforms:
        ddf = calc_df[base_cols + ["delta", "reset_detected", "prev_value"]].copy()
        ddf["transform"] = "delta"
        ddf["value"] = ddf["delta"]
        ddf["unit"] = ""
        ddf["is_valid"] = eligible_delta.values & ddf["prev_value"].notna()
        ddf.loc[~ddf["is_valid"], "value"] = np.nan
        if cfg.reset_policy == "mark":
            ddf.loc[ddf["reset_detected"], ["value", "is_valid"]] = [np.nan, False]
            rows.append(ddf)
        else:
            rows.append(ddf.loc[~ddf["reset_detected"]].copy())

    if "rate" in cfg.transforms:
        rdf = calc_df[base_cols + ["delta", "interval_sec", "reset_detected", "prev_value"]].copy()
        rdf["transform"] = "rate"
        rdf["value"] = rdf["delta"] / rdf["interval_sec"]
        rdf["unit"] = "per_sec"
        rdf["is_valid"] = eligible_delta.values & rdf["prev_value"].notna() & (rdf["interval_sec"] > 0)
        rdf.loc[~rdf["is_valid"], "value"] = np.nan
        if cfg.reset_policy == "mark":
            rdf.loc[rdf["reset_detected"], ["value", "is_valid"]] = [np.nan, False]
            rows.append(rdf)
        else:
            rows.append(rdf.loc[~rdf["reset_detected"]].copy())

    if "period_avg" in cfg.transforms:
        latency = calc_df[calc_df["metric_type"] == "latency_pair"].copy()
        latency["lat_base"] = latency["metric_path"].str.replace(r"\.(avgcount|sum|avgtime)$", "", regex=True)
        ac = latency[latency["metric_path"].str.endswith(".avgcount")].copy()
        sm = latency[latency["metric_path"].str.endswith(".sum")].copy()
        key = ["experiment_id", "group_key", "mds_daemon", "host", "mds_name", "timestamp", "timestamp_unix", "time_offset_sec", "seq", "lat_base"]
        merged = ac.merge(
            sm,
            on=key,
            how="inner",
            suffixes=("_ac", "_sum"),
        )
        if not merged.empty:
            out = pd.DataFrame(
                {
                    "experiment_id": merged["experiment_id"],
                    "experiment_tag": merged["experiment_tag_ac"],
                    "cluster_name": merged["cluster_name_ac"],
                    "host": merged["host"],
                    "mds_name": merged["mds_name"],
                    "mds_daemon": merged["mds_daemon"],
                    "group_key": merged["group_key"],
                    "timestamp": merged["timestamp"],
                    "timestamp_unix": merged["timestamp_unix"],
                    "time_offset_sec": merged["time_offset_sec"],
                    "seq": merged["seq"],
                    "metric_path": merged["lat_base"],
                    "metric_type": "latency_pair",
                    "source_file": merged["source_file_ac"],
                    "transform": "period_avg",
                    "value": merged["delta_sum"] / merged["delta_ac"],
                    "unit": "sec",
                    "reset_detected": merged["reset_detected_ac"] | merged["reset_detected_sum"],
                    "is_valid": (merged["delta_ac"] > 0) & merged["delta_ac"].notna() & merged["delta_sum"].notna(),
                    "weight": merged["delta_ac"],
                }
            )
            out.loc[~out["is_valid"], "value"] = np.nan
            if cfg.reset_policy == "mark":
                out.loc[out["reset_detected"], ["value", "is_valid"]] = [np.nan, False]
                rows.append(out)
            else:
                rows.append(out.loc[~out["reset_detected"]].copy())

    if not rows:
        return pd.DataFrame()
    out_df = pd.concat(rows, ignore_index=True, sort=False)
    if "weight" not in out_df.columns:
        out_df["weight"] = np.nan
    out_df = out_df.sort_values(["experiment_id", "group_key", "metric_path", "transform", "timestamp_unix", "seq"]).reset_index(drop=True)
    return out_df


def aggregate_series(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    if df.empty or mode == "per_mds":
        return df
    work = df.copy()
    by = ["experiment_id", "experiment_tag", "cluster_name", "timestamp", "timestamp_unix", "time_offset_sec", "seq", "metric_path", "metric_type", "transform"]

    rows: List[Dict[str, Any]] = []
    for key, g in work.groupby(by, dropna=False):
        g_valid = g[g["is_valid"] & g["value"].notna()]
        if g_valid.empty:
            value = np.nan
            is_valid = False
        else:
            metric_type = key[8]
            transform = key[9]
            if transform == "period_avg":
                w = g_valid["weight"]
                if w.notna().any() and float(w.sum()) > 0:
                    value = float(np.average(g_valid["value"], weights=w.fillna(0)))
                else:
                    value = float(g_valid["value"].mean())
            elif metric_type == "gauge" and transform == "raw":
                value = float(g_valid["value"].mean())
            else:
                value = float(g_valid["value"].sum())
            is_valid = True
        row = {
            "experiment_id": key[0],
            "experiment_tag": key[1],
            "cluster_name": key[2],
            "host": "all" if mode == "aggregate" else "mixed",
            "mds_name": "all",
            "mds_daemon": "all",
            "group_key": "all" if mode == "aggregate" else "host_aggregated",
            "timestamp": key[3],
            "timestamp_unix": key[4],
            "time_offset_sec": key[5],
            "seq": key[6],
            "metric_path": key[7],
            "metric_type": key[8],
            "transform": key[9],
            "value": value,
            "unit": g["unit"].iloc[0],
            "reset_detected": bool(g["reset_detected"].any()),
            "is_valid": is_valid,
            "source_file": "aggregate",
            "weight": g_valid["weight"].sum() if "weight" in g_valid.columns else np.nan,
        }
        rows.append(row)
    return pd.DataFrame(rows)


def build_metric_catalog(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["metric_path", "metric_type", "first_seen_experiment", "non_null_count", "sample_count"])
    g = df.groupby(["metric_path", "metric_type"], as_index=False).agg(
        first_seen_experiment=("experiment_id", "min"),
        non_null_count=("value", lambda s: int(s.notna().sum())),
        sample_count=("value", "size"),
    )
    return g.sort_values(["metric_path", "metric_type"])


def export_tidy_data(df: pd.DataFrame, out_dir: Path, export_formats: List[str], logger: logging.Logger, error_logger: logging.Logger) -> None:
    if "csv" in export_formats:
        df.to_csv(out_dir / "timeseries_tidy.csv", index=False)
    if "jsonl" in export_formats:
        df.to_json(out_dir / "timeseries_tidy.jsonl", orient="records", lines=True)
    if "parquet" in export_formats:
        try:
            df.to_parquet(out_dir / "timeseries_tidy.parquet", index=False)
        except Exception as exc:
            msg = f"Parquet export skipped/failed: {exc}"
            logger.warning(msg)
            error_logger.warning(msg)


def sanitize_metric_name(metric_path: str) -> str:
    return metric_path.replace(".", "__")


def plot_metric_series(df: pd.DataFrame, out_path: Path, title: str, xlabel: str, ylabel: str, series_by: str, x_col: str) -> bool:
    try:
        fig, ax = plt.subplots(figsize=(10, 5))
        for key, g in df.groupby(series_by):
            g = g.sort_values(x_col)
            ax.plot(g[x_col], g["value"], label=str(key))
        ax.set_title(title)
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        ax.legend()
        fig.tight_layout()
        fig.savefig(out_path)
        plt.close(fig)
        return True
    except Exception:
        plt.close("all")
        return False


def plot_standard_suite(df: pd.DataFrame, cfg: AnalysisConfig, dirs: Dict[str, Path], logger: logging.Logger, error_logger: logging.Logger) -> int:
    if cfg.default_plots == "none" or df.empty:
        return 0
    x_col = "timestamp_unix" if cfg.align_time == "absolute" else "time_offset_sec"
    xlabel = "timestamp_unix" if cfg.align_time == "absolute" else "time_offset_sec"
    count = 0

    for exp_id, exp_df in df.groupby("experiment_id"):
        exp_dir = dirs["plots_per_experiment"] / exp_id
        exp_dir.mkdir(parents=True, exist_ok=True)

        for metric, transform in STANDARD_PER_MDS_PLOTS:
            p = exp_df[(exp_df["metric_path"] == metric) & (exp_df["transform"] == transform)]
            if p.empty:
                continue
            for fmt in cfg.plot_formats:
                f = exp_dir / f"per_mds__{sanitize_metric_name(metric)}__{transform}__per_mds.{fmt}"
                ok = plot_metric_series(p, f, f"{exp_id} per_mds {metric} {transform}", xlabel, "value", "group_key", x_col)
                if ok:
                    count += 1
                else:
                    error_logger.warning("plot failed: %s", f)

        agg = aggregate_series(exp_df, "aggregate")
        for metric, transform in STANDARD_AGGREGATE_PLOTS:
            p = agg[(agg["metric_path"] == metric) & (agg["transform"] == transform)]
            if p.empty:
                continue
            for fmt in cfg.plot_formats:
                f = exp_dir / f"aggregate__{sanitize_metric_name(metric)}__{transform}__aggregate.{fmt}"
                ok = plot_metric_series(p, f, f"{exp_id} aggregate {metric} {transform}", xlabel, "value", "group_key", x_col)
                if ok:
                    count += 1
                else:
                    error_logger.warning("plot failed: %s", f)

    if cfg.compare_mode == "overlay" and df["experiment_id"].nunique() > 1:
        agg_all = aggregate_series(df, "aggregate")
        for metric, transform in STANDARD_AGGREGATE_PLOTS:
            p = agg_all[(agg_all["metric_path"] == metric) & (agg_all["transform"] == transform)]
            if p.empty:
                continue
            for fmt in cfg.plot_formats:
                f = dirs["plots_compare"] / f"overlay__{sanitize_metric_name(metric)}__{transform}__aggregate.{fmt}"
                ok = plot_metric_series(p, f, f"overlay aggregate {metric} {transform}", xlabel, "value", "experiment_id", x_col)
                if ok:
                    count += 1
                else:
                    error_logger.warning("plot failed: %s", f)

    logger.info("Generated %d plots", count)
    return count


def write_analysis_meta(dirs: Dict[str, Path], cfg: AnalysisConfig, run_status: List[Dict[str, Any]], stats: Dict[str, int], plot_count: int) -> None:
    with (dirs["meta"] / "input_runs.json").open("w", encoding="utf-8") as f:
        json.dump(run_status, f, ensure_ascii=False, indent=2)

    run_info = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "input_parameters": {
            "input_dirs": cfg.input_dirs,
            "metrics": cfg.metrics,
            "transforms": cfg.transforms,
            "group_by": cfg.group_by,
            "compare_mode": cfg.compare_mode,
            "align_time": cfg.align_time,
            "plot_formats": cfg.plot_formats,
            "export_formats": cfg.export_formats,
            "default_plots": cfg.default_plots,
            "reset_policy": cfg.reset_policy,
            "max_workers": cfg.max_workers,
            "config": str(cfg.config_path) if cfg.config_path else None,
        },
        "input_runs": len(run_status),
        "success_runs": sum(1 for r in run_status if r["status"] == "ok"),
        "failed_runs": sum(1 for r in run_status if r["status"] != "ok"),
        "parsed_files": stats.get("total_files", 0),
        "success_files": stats.get("ok_files", 0),
        "failed_files": stats.get("bad_files", 0),
        "export_formats": cfg.export_formats,
        "plot_count": plot_count,
    }
    with (dirs["meta"] / "analysis_run_info.json").open("w", encoding="utf-8") as f:
        json.dump(run_info, f, ensure_ascii=False, indent=2)


def main() -> int:
    args = parse_args()
    if MISSING_CORE_DEPS:
        print(f"Missing core dependencies: {', '.join(MISSING_CORE_DEPS)}", file=sys.stderr)
        return 2
    cfg = build_config(args)
    dirs = setup_output_dirs(cfg.output_dir)
    logger, error_logger = configure_logging(dirs["logs"])

    experiments, run_status = discover_runs(cfg, error_logger, logger)
    if not experiments:
        logger.error("No valid experiments found")
        write_analysis_meta(dirs, cfg, run_status, {"total_files": 0, "ok_files": 0, "bad_files": 0}, 0)
        return 2

    raw_df, stats = build_raw_dataframe(experiments, cfg, error_logger, logger)
    tidy_df = make_transform_rows(raw_df, cfg, error_logger)

    for scope in ["per_mds", "per_host", "aggregate"]:
        if scope == "per_mds":
            scope_df = tidy_df
        else:
            scope_df = aggregate_series(tidy_df, scope)
        (dirs["derived"] / scope / "timeseries.csv").parent.mkdir(parents=True, exist_ok=True)
        scope_df.to_csv(dirs["derived"] / scope / "timeseries.csv", index=False)

    export_tidy_data(tidy_df, dirs["tidy"], cfg.export_formats, logger, error_logger)
    metric_catalog = build_metric_catalog(tidy_df)
    metric_catalog.to_csv(dirs["meta"] / "metric_catalog.csv", index=False)

    plot_count = plot_standard_suite(tidy_df, cfg, dirs, logger, error_logger)
    write_analysis_meta(dirs, cfg, run_status, stats, plot_count)

    logger.info(
        "Summary: runs=%d ok_runs=%d files=%d bad_files=%d rows=%d",
        len(run_status),
        sum(1 for r in run_status if r["status"] == "ok"),
        stats.get("total_files", 0),
        stats.get("bad_files", 0),
        len(tidy_df),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
