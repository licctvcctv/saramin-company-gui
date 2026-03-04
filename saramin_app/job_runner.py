#!/usr/bin/env python3
from __future__ import annotations

import argparse
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
import time
from pathlib import Path
import threading
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from saramin_scraper import parse_args as parse_saramin_args
from saramin_scraper import run_scrape as run_saramin_scrape


@dataclass
class JobState:
    job_id: str
    status: str
    args: dict
    log_lines: deque[str] = field(default_factory=lambda: deque(maxlen=400))
    progress: int = 0
    total: int | None = None
    csv_path: Path | None = None
    xlsx_path: Path | None = None
    started_at: float | None = None
    finished_at: float | None = None
    error: str | None = None


class JobRunner:
    @staticmethod
    def _normalize_locations(value: object) -> list[str]:
        if value is None:
            return []
        candidates: list[str] = []
        if isinstance(value, str):
            candidates.extend([x for x in value.split(",") if x])
        elif isinstance(value, (list, tuple, set)):
            for item in value:
                candidates.extend(str(item).split(","))
        else:
            candidates.append(str(value))

        codes: list[str] = []
        for item in candidates:
            code = str(item).strip()
            if code.isdigit() and code not in codes:
                codes.append(code)
        return codes

    @staticmethod
    def _build_url_with_locations(url: str, locations: list[str]) -> str:
        if not locations:
            return url
        parsed = urlsplit(url)
        params = []
        for key, value in parse_qsl(parsed.query, keep_blank_values=True):
            if key in {"loc_mcd", "loc_mcd[]"}:
                continue
            params.append((key, value))
        for code in locations:
            params.append(("loc_mcd", code))
        return urlunsplit(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                urlencode(params, doseq=True),
                parsed.fragment,
            )
        )

    @staticmethod
    def _coerce_float(value: object, key: str) -> float:
        try:
            return float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{key} 必须是数字") from exc

    def __init__(self) -> None:
        self._job: JobState | None = None
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

    def _append_log(self, job: JobState, line: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        with self._lock:
            job.log_lines.append(f"[{ts}] {line}")

    def _update_progress(self, job: JobState, progress: int, total: int | None = None) -> None:
        with self._lock:
            job.progress = progress
            if total is not None:
                job.total = total

    def _safe_args(self, params: dict) -> argparse.Namespace:
        defaults = parse_saramin_args([])
        argv = [
            "--output-csv",
            str(params.get("output_csv", Path("outputs") / "saramin_jobs.csv")),
            "--output-xlsx",
            str(params.get("output_xlsx", Path("outputs") / "saramin_jobs.xlsx")),
        ]

        if params.get("url"):
            argv.extend(["--url", str(params["url"])])
        locations = self._normalize_locations(params.get("locations"))

        for key in (
            "start_page",
            "max_pages",
            "max_items",
            "page_size",
            "workers",
            "sleep",
            "jitter",
            "timeout",
            "max_retries",
            "backoff",
            "save_every",
            "save_interval",
            "fsync_every",
            "split_every",
        ):
            value = params.get(key)
            if value is None:
                continue
            if key in {"start_page", "max_pages", "page_size", "save_every", "workers", "fsync_every"} and int(value) < 1:
                raise ValueError(f"{key} 不能小于 1")
            if key in {"max_items", "split_every"} and int(value) < 0:
                raise ValueError(f"{key} 不能小于 0")
            if key in {"sleep", "jitter", "timeout", "backoff", "save_interval"} and self._coerce_float(value, key) < 0:
                raise ValueError(f"{key} 不能小于 0")
            argv.extend(["--" + key.replace("_", "-"), str(value)])

        if params.get("max_items") is None:
            max_companies = params.get("max_companies")
            if max_companies is not None:
                max_items_int = int(max_companies)
                if max_items_int < 0:
                    raise ValueError("max_companies 不能小于 0")
                argv.extend(["--max-items", str(max_items_int)])

        if params.get("verbose", False):
            argv.append("--verbose")

        parsed = parse_saramin_args(argv)

        # 支持多链接：前端传入 urls 列表时优先使用
        raw_urls = params.get("urls")
        if raw_urls and isinstance(raw_urls, (list, tuple)):
            cleaned_urls = [u.strip() for u in raw_urls if isinstance(u, str) and u.strip()]
            if cleaned_urls:
                parsed.urls = cleaned_urls
                # 第一个 URL 作为 fallback 的 args.url
                parsed.url = cleaned_urls[0]
            else:
                parsed.urls = None
                if locations:
                    parsed.url = self._build_url_with_locations(parsed.url, locations)
        else:
            parsed.urls = None
            if locations:
                parsed.url = self._build_url_with_locations(parsed.url, locations)

        parsed.output_csv = str(Path(parsed.output_csv).resolve())
        parsed.output_xlsx = str(Path(parsed.output_xlsx).resolve())

        for field_name, default_value in vars(defaults).items():
            if not hasattr(parsed, field_name):
                setattr(parsed, field_name, default_value)

        setattr(parsed, "source", "saramin")
        return parsed

    def _run(self, job: JobState, args: argparse.Namespace) -> None:
        try:
            self._append_log(
                job,
                f"参数: source=Saramin, start_page={args.start_page}, max_pages={args.max_pages}",
            )
            urls = getattr(args, "urls", None)
            if urls and len(urls) > 1:
                self._append_log(job, f"多链接模式: 共 {len(urls)} 个链接")
                for i, u in enumerate(urls):
                    self._append_log(job, f"  链接 {i + 1}: {u[:100]}{'...' if len(u) > 100 else ''}")
            args.output_csv = str(Path(args.output_csv).resolve())
            args.output_xlsx = str(Path(args.output_xlsx).resolve())
            job.csv_path = Path(args.output_csv)
            job.xlsx_path = Path(args.output_xlsx)
            for parent in [job.csv_path.parent, job.xlsx_path.parent]:
                parent.mkdir(parents=True, exist_ok=True)
            self._append_log(job, f"输出目标: CSV={args.output_csv}")
            self._append_log(job, f"输出目标: Excel={args.output_xlsx}")

            run_saramin_scrape(
                args,
                progress_callback=lambda text: self._append_log(job, text),
                progress_update_callback=lambda count, total=None: self._update_progress(job, count, total),
                stop_event=self._stop_event,
            )

            status = "canceled" if self._stop_event.is_set() else "done"
            with self._lock:
                job.status = status
                if Path(args.output_csv).exists():
                    job.finished_at = Path(args.output_csv).stat().st_mtime
        except Exception as exc:
            with self._lock:
                job.status = "failed"
                job.error = str(exc)
            self._append_log(job, f"任务执行失败: {exc}")
        finally:
            with self._lock:
                if job.status == "running":
                    job.status = "done"
                if job.finished_at is None and Path(args.output_csv).exists():
                    job.finished_at = Path(args.output_csv).stat().st_mtime

    def start(self, params: dict) -> str:
        with self._lock:
            if self._thread and self._thread.is_alive():
                raise RuntimeError("已有任务在运行中")
            self._stop_event.clear()
            args = self._safe_args(params)
            job_id = datetime.now().strftime("%Y%m%d_%H%M%S")
            job = JobState(job_id=job_id, status="running", args=vars(args), started_at=time.time())
            self._job = job
            self._thread = threading.Thread(target=self._run, args=(job, args), daemon=True)

        self._append_log(job, "任务已提交")
        assert self._thread is not None
        self._thread.start()
        return job_id

    def stop(self) -> bool:
        with self._lock:
            if not self._thread or not self._thread.is_alive() or not self._job:
                return False
            self._stop_event.set()
            job = self._job

        self._append_log(job, "用户已发起停止")
        return True

    def snapshot(self) -> dict:
        with self._lock:
            if not self._job:
                return {"running": False, "status": "idle", "logs": []}
            return {
                "running": self._thread is not None and self._thread.is_alive(),
                "job": {
                    "id": self._job.job_id,
                    "status": self._job.status,
                    "args": self._job.args,
                    "progress": self._job.progress,
                    "total": self._job.total,
                    "log_lines": list(self._job.log_lines),
                    "started_at": self._job.started_at,
                    "finished_at": self._job.finished_at,
                    "error": self._job.error,
                    "output_csv": str(self._job.csv_path) if self._job.csv_path else None,
                    "output_xlsx": str(self._job.xlsx_path) if self._job.xlsx_path else None,
                },
            }

    def get_download_path(self, kind: str) -> Path | None:
        with self._lock:
            if not self._job:
                return None
            path = self._job.csv_path if kind == "csv" else self._job.xlsx_path if kind == "xlsx" else None
            if path and path.exists():
                return path
            return None
