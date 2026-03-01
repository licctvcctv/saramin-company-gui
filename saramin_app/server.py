#!/usr/bin/env python3
from __future__ import annotations

import argparse
import errno
import socket
import os
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, request, send_file, send_from_directory

from saramin_app.job_runner import JobRunner


def create_app() -> Flask:
    app = Flask(__name__)
    runner = JobRunner()
    static_root = Path(__file__).resolve().parent / "static"

    @app.get("/")
    def index() -> Any:
        return send_from_directory(static_root, "index.html")

    @app.get("/static/<path:filename>")
    def static_files(filename: str) -> Any:
        return send_from_directory(static_root, filename)

    @app.get("/api/status")
    def status() -> Any:
        return jsonify(runner.snapshot())

    @app.post("/api/start")
    def start_job() -> Any:
        payload = request.get_json(silent=True) or {}
        try:
            runner.start(payload)
        except RuntimeError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 409
        except Exception as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400

        return jsonify({"ok": True})

    @app.post("/api/stop")
    def stop_job() -> Any:
        if runner.stop():
            return jsonify({"ok": True})
        return jsonify({"ok": False, "error": "当前无运行任务"}), 409

    @app.get("/api/download/<kind>")
    def download(kind: str) -> Any:
        path = runner.get_download_path(kind)
        if not path:
            return jsonify({"ok": False, "error": "文件不存在或任务未完成"}), 404
        return send_file(
            path,
            as_attachment=True,
            download_name=path.name,
        )

    return app


def _resolve_port(host: str, requested_port: int) -> int:
    for attempt in range(20):
        check_port = requested_port + attempt
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind((host, check_port))
                return check_port
        except OSError as exc:
            if exc.errno != errno.EADDRINUSE:
                raise
            continue

    raise RuntimeError(f"在 {host} 无法找到可用端口，起始尝试为 {requested_port}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Saramin 企业采集 GUI 后台服务")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=19180)
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args(argv)

    if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        # Flask 开发模式重启时避免重复打开浏览器
        os.environ.pop("WERKZEUG_RUN_MAIN", None)

    serve_port = _resolve_port(args.host, args.port)
    if serve_port != args.port:
        print(
            f"[WARN] 端口 {args.port} 被占用，已自动切换到 {serve_port}",
            flush=True,
        )

    app = create_app()
    if not args.no_browser:
        import webbrowser

        webbrowser.open(f"http://{args.host}:{serve_port}")

    print(f"[INFO] 服务启动：http://{args.host}:{serve_port}", flush=True)
    app.run(host=args.host, port=serve_port, debug=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
