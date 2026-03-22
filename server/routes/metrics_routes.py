from __future__ import annotations

from http import HTTPStatus

from flask import Blueprint, jsonify, request

from server.services.metrics_store import MetricsStore


metrics_bp = Blueprint("metrics_bp", __name__)
store = MetricsStore()


def _json_error(message: str, status: HTTPStatus):
    response = jsonify({"ok": False, "error": message})
    response.status_code = status
    return response


@metrics_bp.post("/api/metrics/report")
def report_metrics():
    body = request.get_json(silent=True) or {}
    try:
        store.append_report(body)
    except ValueError as exc:
        return _json_error(str(exc), HTTPStatus.BAD_REQUEST)
    except FileNotFoundError:
        return _json_error("run not found", HTTPStatus.NOT_FOUND)
    return jsonify({"ok": True})


@metrics_bp.get("/api/configs/<config_name>/runs/<run_id>/metrics")
def get_metrics(config_name: str, run_id: str):
    return jsonify({"ok": True, "reports": store.read_reports(config_name, run_id)})


@metrics_bp.get("/api/configs/<config_name>/runs/<run_id>/metrics/series")
def get_metric_series(config_name: str, run_id: str):
    metric = str(request.args.get("metric", "")).strip()
    if not metric:
        return _json_error("metric is required", HTTPStatus.BAD_REQUEST)
    return jsonify({"ok": True, "data": store.build_series(config_name, run_id, metric)})
