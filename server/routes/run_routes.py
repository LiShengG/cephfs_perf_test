from __future__ import annotations

from http import HTTPStatus

from flask import Blueprint, jsonify, request

from server.services.config_store import ConfigValidationError
from server.services.run_store import RunStateError, RunStore


run_bp = Blueprint("run_bp", __name__)
store = RunStore()


def _json_error(message: str, status: HTTPStatus):
    response = jsonify({"ok": False, "error": message})
    response.status_code = status
    return response


@run_bp.get("/api/runs/current")
def current_run():
    return jsonify({"ok": True, "run": store.current_run()})


@run_bp.post("/api/runs")
def start_run():
    body = request.get_json(silent=True) or {}
    config_name = str(body.get("config_name", "")).strip()
    if not config_name:
        return _json_error("config_name is required", HTTPStatus.BAD_REQUEST)
    try:
        run = store.start_run(config_name)
    except FileNotFoundError:
        return _json_error("config not found", HTTPStatus.NOT_FOUND)
    except ConfigValidationError as exc:
        return _json_error(str(exc), HTTPStatus.BAD_REQUEST)
    except RunStateError as exc:
        return _json_error(str(exc), HTTPStatus.CONFLICT)
    return jsonify({"ok": True, "run": run})


@run_bp.post("/api/runs/stop")
def stop_run():
    try:
        run = store.stop_run()
    except RunStateError as exc:
        return _json_error(str(exc), HTTPStatus.CONFLICT)
    return jsonify({"ok": True, "run": run})


@run_bp.get("/api/configs/<config_name>/runs")
def list_runs(config_name: str):
    return jsonify({"ok": True, "runs": store.list_runs(config_name)})


@run_bp.get("/api/configs/<config_name>/runs/<run_id>")
def get_run(config_name: str, run_id: str):
    try:
        return jsonify({"ok": True, "run": store.get_run_detail(config_name, run_id)})
    except FileNotFoundError:
        return _json_error("run not found", HTTPStatus.NOT_FOUND)


@run_bp.delete("/api/configs/<config_name>/runs/<run_id>")
def delete_run(config_name: str, run_id: str):
    try:
        store.delete_run(config_name, run_id)
    except FileNotFoundError:
        return _json_error("run not found", HTTPStatus.NOT_FOUND)
    except RunStateError as exc:
        return _json_error(str(exc), HTTPStatus.CONFLICT)
    return jsonify({"ok": True})


@run_bp.get("/api/configs/<config_name>/runs/<run_id>/logs/<log_type>")
def read_log(config_name: str, run_id: str, log_type: str):
    if log_type not in {"stdout", "stderr"}:
        return _json_error("log_type must be stdout or stderr", HTTPStatus.BAD_REQUEST)
    try:
        content = store.read_log(config_name, run_id, log_type)
    except FileNotFoundError:
        return _json_error("log not found", HTTPStatus.NOT_FOUND)
    return jsonify({"ok": True, "content": content})


@run_bp.get("/api/configs/<config_name>/runs/<run_id>/summary")
def read_summary(config_name: str, run_id: str):
    try:
        payload = store.read_summary(config_name, run_id)
    except FileNotFoundError:
        return _json_error("summary not found", HTTPStatus.NOT_FOUND)
    return jsonify({"ok": True, "summary": payload})


@run_bp.get("/api/configs/<config_name>/runs/<run_id>/snapshot")
def read_snapshot(config_name: str, run_id: str):
    try:
        payload = store.read_snapshot(config_name, run_id)
    except FileNotFoundError:
        return _json_error("snapshot not found", HTTPStatus.NOT_FOUND)
    return jsonify({"ok": True, "snapshot": payload})


@run_bp.get("/api/configs/<config_name>/runs/<run_id>/analysis-notes")
def read_analysis_notes(config_name: str, run_id: str):
    try:
        payload = store.read_analysis_notes(config_name, run_id)
    except FileNotFoundError:
        return _json_error("run not found", HTTPStatus.NOT_FOUND)
    return jsonify({"ok": True, "analysis_notes": payload})


@run_bp.put("/api/configs/<config_name>/runs/<run_id>/analysis-notes")
def write_analysis_notes(config_name: str, run_id: str):
    body = request.get_json(silent=True) or {}
    text = str(body.get("text", ""))
    try:
        payload = store.write_analysis_notes(config_name, run_id, text)
    except FileNotFoundError:
        return _json_error("run not found", HTTPStatus.NOT_FOUND)
    return jsonify({"ok": True, "analysis_notes": payload})
