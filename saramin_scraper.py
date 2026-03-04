#!/usr/bin/env python3
from __future__ import annotations

import concurrent.futures
import argparse
import csv
import json
import os
import re
import random
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup
from openpyxl import Workbook


DEFAULT_START_URL = (
    "https://www.saramin.co.kr/zf_user/jobs/list/domestic?"
    "loc_mcd=101000&panel_type=&search_optional_item=n&search_done=y&panel_count=y&preview=y"
)
BASE_ORIGIN = "https://www.saramin.co.kr"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/138.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8,zh-CN;q=0.7",
    "Accept": (
        "application/json, text/javascript, */*;q=0.01"
    ),
    "X-Requested-With": "XMLHttpRequest",
    "Origin": BASE_ORIGIN,
    "Referer": BASE_ORIGIN + "/",
}

MISSING = "无"
RETRYABLE_STATUS = {403, 429, 500, 502, 503, 504}
OFFICIAL_WEB_HOST_EXCLUDES = {
    "saramin.co.kr",
    "hiring.saramin.co.kr",
    "career.saramin.co.kr",
    "billing.saramin.co.kr",
    "business.saramin.co.kr",
    "map.kakao.com",
    "kakaomap.com",
    "smartstore.naver.com",
    "blog.naver.com",
    "cafe.naver.com",
    "m.blog.naver.com",
    "naver.com",
    "facebook.com",
    "instagram.com",
    "youtube.com",
    "youtube-nocookie.com",
    "x.com",
    "twitter.com",
    "kakao.com",
    "facebook.co.kr",
    "instagram.co.kr",
    "youtube.co.kr",
    "twitter.co.kr",
    "linkedin.com",
}


@dataclass
class SaraminRecord:
    rec_id: str
    title: str
    company_name: str
    title_url: str
    company_url: str
    company_csn: str
    location: str
    career_and_type: str
    tags: list[str]
    badges: list[str]
    remain: str
    updated_info: str
    company_owner: str
    company_website: str
    company_intro: str
    company_industry: str
    company_address: str

    def to_row(self) -> list[str]:
        return [
            self.company_name,
            self.company_owner,
            self.company_website,
            self.company_url,
        ]


@dataclass
class CompanyInfo:
    owner: str
    website: str
    intro: str
    industry: str
    address: str


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="사람인 공고列表采集（职位、公司、地区、经验、学历）"
    )
    parser.add_argument("--url", default=DEFAULT_START_URL, help="起始列表页 URL")
    parser.add_argument("--start-page", type=int, default=1, help="起始页码")
    parser.add_argument("--max-pages", type=int, default=200, help="最多采集多少页")
    parser.add_argument("--max-items", type=int, default=0, help="最多采集多少条数据，0 表示不限制")
    parser.add_argument("--page-size", type=int, default=50, help="每页数量（page_count）")
    parser.add_argument("--sleep", type=float, default=0.2, help="每次请求后的等待秒数")
    parser.add_argument("--jitter", type=float, default=0.3, help="随机抖动秒数上限")
    parser.add_argument("--timeout", type=float, default=20.0, help="HTTP 超时秒数")
    parser.add_argument("--max-retries", type=int, default=4, help="单请求最大重试次数")
    parser.add_argument("--backoff", type=float, default=1.2, help="重试退避基数秒")
    parser.add_argument(
        "--output-xlsx",
        default="outputs/saramin_jobs.xlsx",
        help="Excel 输出路径",
    )
    parser.add_argument(
        "--output-csv",
        default="outputs/saramin_jobs.csv",
        help="CSV 输出路径",
    )
    parser.add_argument(
        "--save-every",
        type=int,
        default=200,
        help="Excel 每 N 条保存一次（默认 200）",
    )
    parser.add_argument(
        "--save-interval",
        type=float,
        default=3.0,
        help="Excel 至少每 N 秒保存一次（默认 3.0）",
    )
    parser.add_argument(
        "--fsync-every",
        type=int,
        default=50,
        help="CSV 每 N 条执行一次 fsync（默认 50）",
    )
    parser.add_argument(
        "--split-every",
        type=int,
        default=0,
        help="每 N 条切分到新文件，0 表示不分片（示例：20000）",
    )
    parser.add_argument("--verbose", action="store_true", help="打印日志")
    parser.add_argument("--workers", type=int, default=1, help="并发抓取企业详情页")
    return parser.parse_args(argv)


def normalize_text(value: str | None) -> str:
    if value is None:
        return MISSING
    text = " ".join(str(value).split())
    if not text or text in {"-", "--", "None", "none", "null", "無", "없음"}:
        return MISSING
    return text


def normalize_website(value: str | None) -> str:
    value = normalize_text(value)
    if value == MISSING:
        return MISSING
    if value.startswith("/"):
        value = urljoin(BASE_ORIGIN, value)
    if "://" not in value and not value.startswith("javascript:"):
        if not re.match(r"^[A-Za-z0-9][-A-Za-z0-9._]*\.[A-Za-z]{2,}", value):
            return MISSING
        value = f"https://{value}"
    if value.lower().startswith("javascript:") or value in {"#", "/"}:
        return MISSING
    parsed = urlsplit(value)
    if parsed.scheme and "saramin.co.kr" in parsed.netloc.lower():
        path = parsed.path or ""
        if "company-info/view" in path:
            return MISSING
    return value


def _log(message: str, verbose: bool, callback=None) -> None:
    if verbose:
        print(message, flush=True)
    if callback is not None:
        callback(message)


def extract_company_csn(company_url: str | None) -> str:
    if company_url is None:
        return MISSING
    text = normalize_text(company_url)
    if text == MISSING:
        return MISSING
    parsed = urlsplit(text)
    if not parsed.query:
        return MISSING

    query_map: dict[str, list[str]] = {}
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        key_l = key.lower()
        if key_l not in query_map:
            query_map[key_l] = []
        query_map[key_l].append(value.strip())

    csn_values = query_map.get("csn", [])
    return normalize_text(csn_values[0]) if csn_values else MISSING


def normalize_company_url(company_url: str) -> str:
    csn = extract_company_csn(company_url)
    if csn == MISSING:
        return normalize_text(company_url)
    return f"{BASE_ORIGIN}/zf_user/company-info/view?csn={csn}"


def build_company_detail_warning(rec_id: str, company_url: str, exc: Exception) -> str:
    # 404/请求失败等技术细节不直接暴露给终端用户，避免把单条异常误判成全局失败。
    _ = exc
    safe_rec = normalize_text(rec_id)
    safe_url = normalize_text(company_url)
    return f"[WARN] 单个链接未获取 rec_id={safe_rec} url={safe_url}，已跳过"


def _clean_address(address: str | None) -> str:
    value = normalize_text(address)
    if value == MISSING:
        return MISSING
    suffix = "지도보기"
    if value.endswith(suffix):
        value = value[: -len(suffix)].strip()
    return value or MISSING


def _extract_label_map(html: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    details: dict[str, str] = {}

    for dt in soup.select("dl dt"):
        dd = dt.find_next_sibling("dd")
        if not dd:
            continue
        key = normalize_text(dt.get_text(" ", strip=True))
        if key == MISSING:
            continue
        if key not in {"업종", "대표자명", "홈페이지", "사업내용", "주소", "SNS"}:
            continue
        value = normalize_text(dd.get_text(" ", strip=True))
        if key == MISSING or value == MISSING:
            continue
        details[key] = value
    return details


def _extract_sns_links(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []
    for dt in soup.select("dl dt"):
        key = normalize_text(dt.get_text(" ", strip=True))
        if key != "SNS":
            continue
        dd = dt.find_next_sibling("dd")
        if not dd:
            continue
        for anchor in dd.select("a[href]"):
            href = normalize_text(anchor.get("href"))
            if href == MISSING:
                continue
            links.append(href)
    # 去重保序
    seen = set()
    unique_links: list[str] = []
    for link in links:
        if link in seen:
            continue
        seen.add(link)
        unique_links.append(link)
    return unique_links


def _coerce_str(value: object) -> str:
    if value is None:
        return MISSING
    if isinstance(value, str):
        return normalize_text(value)
    if isinstance(value, list):
        for v in value:
            text = _coerce_str(v)
            if text != MISSING:
                return text
        return MISSING
    if isinstance(value, dict):
        for candidate in (value.get("name"), value.get("givenName"), value.get("alternateName")):
            text = _coerce_str(candidate)
            if text != MISSING:
                return text
        return MISSING
    return normalize_text(str(value))


def _extract_address(value: object) -> str:
    if value is None:
        return MISSING
    if isinstance(value, list):
        for item in value:
            text = _extract_address(item)
            if text != MISSING:
                return text
        return MISSING
    if not isinstance(value, dict):
        return MISSING

    parts: list[str] = []
    for key in (
        "addressLocality",
        "addressRegion",
        "addressCountry",
        "streetAddress",
    ):
        text = _coerce_str(value.get(key))
        if text != MISSING:
            parts.append(text)
    return _clean_address(" ".join(parts))


def _is_official_website(url: str) -> bool:
    parsed = urlsplit(url)
    if not parsed.scheme or not parsed.netloc:
        return False
    host = parsed.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    if not host:
        return False
    if host in OFFICIAL_WEB_HOST_EXCLUDES:
        return False
    if any(host.endswith(f".{domain}") for domain in OFFICIAL_WEB_HOST_EXCLUDES):
        return False
    return True


def extract_homepage_from_description(description: str) -> str:
    """从职位详情页的 meta description 中提取“홈페이지:”后面的内容。"""
    description = normalize_text(description)
    if description == MISSING:
        return MISSING

    patterns = (
        r"홈페이지\s*[:：]\s*([^,\n>]+)",
        r"홈페이지\s*[:：]\s*([^\n]+)",
    )

    for pattern in patterns:
        match = re.search(pattern, description)
        if not match:
            continue
        candidate = normalize_text(match.group(1))
        if candidate == MISSING:
            continue
        if candidate.startswith(("<", "[", "《", "「", "“", "\"", "'")):
            candidate = candidate[1:]
        if candidate.endswith((">", "]", "》", "」", "”", "\"", "'")):
            candidate = candidate[:-1]
        if candidate:
            homepage = normalize_website(candidate)
            if homepage != MISSING:
                return homepage

    return MISSING


def extract_homepage_from_html(html: str) -> str:
    html = html or ""
    if not html.strip():
        return MISSING

    soup = BeautifulSoup(html, "html.parser")
    meta = soup.find("meta", attrs={"name": "description"})
    if meta is not None:
        homepage = extract_homepage_from_description(meta.get("content", ""))
        if homepage != MISSING:
            return homepage

    # 先从“dl -> dt/dd”里抓带“홈페이지”标签的字段
    for dt in soup.select("dl dt"):
        key = normalize_text(dt.get_text(" ", strip=True))
        if key != "홈페이지":
            continue
        dd = dt.find_next_sibling("dd")
        if not dd:
            continue
        for anchor in dd.select("a[href]"):
            homepage = normalize_website(anchor.get("href"))
            if homepage != MISSING:
                return homepage
        homepage = normalize_text(dd.get_text(" ", strip=True))
        if homepage != MISSING:
            homepage = normalize_website(homepage)
            if homepage != MISSING:
                return homepage
        sns_links = _extract_sns_links(str(dd))
        for link in sns_links:
            if not _is_official_website(link):
                continue
            website = normalize_website(link)
            if website != MISSING:
                return website

    for link in _extract_sns_links(html):
        if not _is_official_website(link):
            continue
        website = normalize_website(link)
        if website != MISSING:
            return website

    return MISSING


def _extract_homepage_from_track_html(html: str) -> str:
    """从 track-apply-form 的页面片段提取官网链接。"""
    # 兼容 location="..." 和 script 中的字符串拼接写法。
    redirect_patterns = (
        r"document\.location\.(?:replace|assign)\(\s*[\"']([^\"']+)[\"']\)",
        r"window\.location\.(?:replace|assign)\(\s*[\"']([^\"']+)[\"']\)",
        r"document\.location\.href\s*=\s*[\"']([^\"']+)[\"']",
        r"window\.location\.href\s*=\s*[\"']([^\"']+)[\"']",
        r"window\.location\s*=\s*[\"']([^\"']+)[\"']",
        r"location\.(?:replace|assign)\(\s*[\"']([^\"']+)[\"']\)",
        r"location\.href\s*=\s*[\"']([^\"']+)[\"']",
        r"location\s*=\s*[\"']([^\"']+)[\"']",
    )
    for pattern in redirect_patterns:
        match = re.search(pattern, html)
        if not match:
            continue
        candidate = normalize_website(match.group(1))
        if candidate != MISSING:
            return candidate
    return MISSING


def request_relay_view_ajax(
    session: requests.Session,
    rec_id: str,
    timeout: float,
    max_retries: int,
    backoff: float,
    jitter: float,
    verbose: bool = False,
) -> str:
    """抓取 채용详情（relay view ajax）页面，优先返回包含公司信息的片段。"""
    if rec_id == MISSING:
        raise RuntimeError("职位ID为空")

    url = f"{BASE_ORIGIN}/zf_user/jobs/relay/view-ajax"
    # rec_seq 在列表页中可能不是 0/1 固定，先尝试常见值，避免返回空片段。
    seq_candidates = ("0", "1", "")
    last_error: Exception | None = None

    payload_base = {
        "rec_idx": rec_id,
        "utm_source": "",
        "utm_medium": "",
        "utm_term": "",
        "utm_campaign": "",
        "t_ref": "",
        "t_ref_content": "",
        "t_ref_scnid": "",
        "search_uuid": "",
        "refer": "",
        "searchType": "",
        "searchword": "",
        "ref_dp": "",
        "dpId": "",
        "recommendRecIdx": "",
        "referNonce": "",
        "trainingStudentCode": "",
        "view_type": "list",
    }

    for attempt in range(1, max_retries + 1):
        for rec_seq in seq_candidates:
            payload = dict(payload_base)
            payload["rec_seq"] = rec_seq
            try:
                response = session.post(
                    url,
                    data=payload,
                    timeout=timeout,
                    headers={
                        "Accept": "text/html, */*; q=0.01",
                        "X-Requested-With": "XMLHttpRequest",
                    },
                )
                if response.status_code in RETRYABLE_STATUS:
                    raise requests.HTTPError(f"status={response.status_code}", response=response)
                response.raise_for_status()
                if not response.encoding or response.encoding.lower() == "iso-8859-1":
                    response.encoding = response.apparent_encoding or "utf-8"
                if response.text and response.text.strip():
                    return response.text

            except Exception as exc:  # noqa: BLE001
                last_error = exc
                continue

        if attempt >= max_retries:
            break

        wait = backoff * attempt
        _log(
            f"[SARAMIN][RETRY] relay-view-ajax {rec_id} attempt={attempt}/{max_retries} "
            f"wait={wait:.2f}s err={last_error}",
            verbose=verbose,
        )
        polite_sleep(wait, jitter)

    raise RuntimeError(f"request relay view-ajax 失败: {url} / {last_error}") from last_error


def _extract_company_from_ldjson(html: str, current: CompanyInfo) -> CompanyInfo:
    soup = BeautifulSoup(html, "html.parser")
    for script in soup.select("script[type='application/ld+json'], script[type=\"application/ld+json\"]"):
        text = normalize_text(script.get_text(" ", strip=True))
        if text == MISSING:
            continue
        try:
            payload = json.loads(text)
        except Exception:
            continue

        payloads: list[object] = payload if isinstance(payload, list) else [payload]
        for item in payloads:
            if not isinstance(item, dict):
                continue
            type_name = normalize_text(item.get("@type") or item.get("type"))
            if type_name == MISSING or "organization" not in type_name.lower():
                continue

            owner = _coerce_str(item.get("founder") or item.get("founderName") or item.get("founders"))
            website = _coerce_str(item.get("sameAs") or item.get("url") or item.get("homepage") or item.get("website"))
            intro = _coerce_str(item.get("description"))
            address = _extract_address(item.get("address"))

            if current.owner == MISSING and owner != MISSING:
                current.owner = owner
            if current.website == MISSING and website != MISSING:
                normalized = normalize_website(website)
                if _is_official_website(normalized):
                    current.website = normalized
            if current.intro == MISSING and intro != MISSING:
                current.intro = intro
            if current.address == MISSING and address != MISSING:
                current.address = address
    return current


def extract_company_info(html: str) -> CompanyInfo:
    details = _extract_label_map(html)

    info = CompanyInfo(
        owner=normalize_text(details.get("대표자명")),
        website=normalize_website(details.get("홈페이지")),
        intro=normalize_text(details.get("사업내용")),
        industry=normalize_text(details.get("업종")),
        address=_clean_address(details.get("주소")),
    )
    if info.website == MISSING:
        info.website = extract_homepage_from_html(html)

    info = _extract_company_from_ldjson(html, info)

    return info


def polite_sleep(base: float, jitter: float) -> None:
    wait = max(0.0, base)
    if jitter > 0:
        wait += random.uniform(0.0, jitter)
    if wait > 0:
        time.sleep(wait)


def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    return session


def build_page_url(base_url: str, page: int, page_size: int) -> str:
    if page < 1:
        page = 1
    parsed = urlsplit(base_url)
    raw_params = parse_qsl(parsed.query, keep_blank_values=True)
    params: dict[str, str] = {}

    for key, value in raw_params:
        if not key:
            continue
        normalized_key = key[:-2] if key.endswith("[]") else key
        if normalized_key not in params:
            params[normalized_key] = value
            continue
        if params[normalized_key]:
            params[normalized_key] = f"{params[normalized_key]},{value}"
        else:
            params[normalized_key] = value

    params["page"] = str(page)
    params.setdefault("isAjaxRequest", "0")
    params.setdefault("sort", "RL")
    params.setdefault("type", "domestic")
    params.setdefault("is_param", "1")
    params.setdefault("isSearchResultEmpty", "1")
    params.setdefault("isSectionHome", "0")
    params.setdefault("searchParamCount", "1")
    params.setdefault("page_count", str(page_size))
    return urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, urlencode(params, doseq=True), "")
    )


def request_page_json(
    session: requests.Session,
    url: str,
    timeout: float,
    max_retries: int,
    backoff: float,
    jitter: float,
    verbose: bool = False,
) -> dict:
    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, timeout=timeout)
            if resp.status_code in RETRYABLE_STATUS:
                raise requests.HTTPError(f"status={resp.status_code}", response=resp)
            resp.raise_for_status()
            if not resp.encoding or resp.encoding.lower() == "iso-8859-1":
                resp.encoding = resp.apparent_encoding or "utf-8"
            if resp.text.lstrip().startswith("{"):
                return resp.json()
            raise ValueError("页面返回非 JSON：可能接口参数已失效")
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt >= max_retries:
                break
            wait = backoff * attempt
            _log(
                f"[SARAMIN][RETRY] {url} attempt={attempt}/{max_retries} "
                f"wait={wait:.2f}s err={exc}",
                verbose=verbose,
            )
            polite_sleep(wait, jitter)

    raise RuntimeError(f"请求失败: {url} / {last_error}") from last_error


def request_company_html(
    session: requests.Session,
    url: str,
    timeout: float,
    max_retries: int,
    backoff: float,
    jitter: float,
    verbose: bool = False,
) -> str:
    if url == MISSING:
        raise RuntimeError("企业详情URL为空")

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, timeout=timeout)
            if resp.status_code in RETRYABLE_STATUS:
                raise requests.HTTPError(f"status={resp.status_code}", response=resp)
            resp.raise_for_status()
            if not resp.encoding or resp.encoding.lower() == "iso-8859-1":
                resp.encoding = resp.apparent_encoding or "utf-8"
            return resp.text
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if isinstance(exc, requests.HTTPError):
                status = getattr(exc.response, "status_code", None)
                if status not in RETRYABLE_STATUS:
                    break
            if attempt >= max_retries:
                break
            wait = backoff * attempt
            _log(
                f"[SARAMIN][RETRY] company-html {url} attempt={attempt}/{max_retries} "
                f"wait={wait:.2f}s err={exc}",
                verbose=verbose,
                callback=None,
            )
            polite_sleep(wait, jitter)

    raise RuntimeError(f"企业详情请求失败: {url} / {last_error}") from last_error


def request_relay_homepage(
    session: requests.Session,
    rec_id: str,
    timeout: float,
    max_retries: int,
    backoff: float,
    jitter: float,
    verbose: bool = False,
) -> str:
    """从职位详情页兜底抓企业官网。"""
    if rec_id == MISSING:
        return MISSING

    relay_target = [
        (
            "track",
            f"{BASE_ORIGIN}/zf_user/track-apply-form/render-homepage?rec_idx={rec_id}",
        ),
        (
            "relay-list",
            f"{BASE_ORIGIN}/zf_user/jobs/relay/view?view_type=list&rec_idx={rec_id}",
        ),
        (
            "relay-etc",
            f"{BASE_ORIGIN}/zf_user/jobs/relay/view?view_type=etc&rec_idx={rec_id}",
        ),
    ]

    def _extract_from_track(html: str) -> str:
        return _extract_homepage_from_track_html(html)

    def _extract_homepage(html: str, source: str) -> str:
        if source == "view_ajax":
            homepage = extract_homepage_from_html(html)
            if homepage != MISSING:
                return homepage
            homepage = _extract_from_track(html)
            if homepage != MISSING:
                return homepage
            info = extract_company_info(html)
            return info.website

        if source == "track":
            homepage = _extract_from_track(html)
            if homepage != MISSING:
                return homepage
            # 先尝试从页面结构中直接找 홈페이지 字段
            homepage = extract_homepage_from_html(html)
            if homepage != MISSING:
                return homepage
            info = extract_company_info(html)
            if info.website != MISSING:
                return info.website
            return MISSING

        homepage = extract_homepage_from_html(html)
        if homepage != MISSING:
            return homepage
        info = extract_company_info(html)
        if info.website != MISSING:
            return info.website
        return MISSING

    for attempt in range(1, max_retries + 1):
        try:
            # 优先调用 relay view-ajax，常常包含 dt 字段的 회사 홈페이지。
            try:
                relay_ajax_html = request_relay_view_ajax(
                    session=session,
                    rec_id=rec_id,
                    timeout=timeout,
                    max_retries=1,
                    backoff=backoff,
                    jitter=jitter,
                    verbose=verbose,
                )
                homepage = _extract_homepage(relay_ajax_html, "view_ajax")
                if homepage != MISSING:
                    return homepage
            except Exception:
                pass

            for source, url in relay_target:
                response = session.get(url, timeout=timeout)
                if response.status_code in RETRYABLE_STATUS:
                    raise requests.HTTPError(f"status={response.status_code}", response=response)
                response.raise_for_status()
                if not response.encoding or response.encoding.lower() == "iso-8859-1":
                    response.encoding = response.apparent_encoding or "utf-8"
                homepage = _extract_homepage(response.text, source)
                if homepage != MISSING:
                    return homepage
            return MISSING
        except Exception as exc:  # noqa: BLE001
            if attempt >= max_retries:
                return MISSING
            wait = backoff * attempt
            _log(
                f"[SARAMIN][RETRY] relay-homepage {rec_id} attempt={attempt}/{max_retries} "
                f"wait={wait:.2f}s err={exc}",
                verbose=verbose,
            )
            polite_sleep(wait, jitter)

    return MISSING


def enrich_company_info(
    session: requests.Session,
    record: SaraminRecord,
    cache: dict[str, CompanyInfo],
    cache_lock: threading.Lock,
    timeout: float,
    max_retries: int,
    backoff: float,
    jitter: float,
    verbose: bool,
) -> SaraminRecord:
    if record.company_csn == MISSING or record.company_url == MISSING:
        return record

    with cache_lock:
        cached = cache.get(record.company_csn)
    if cached is not None:
        record.company_owner = cached.owner
        record.company_website = cached.website
        record.company_intro = cached.intro
        record.company_industry = cached.industry
        record.company_address = cached.address
        return record

    html = request_company_html(
        session=session,
        url=record.company_url,
        timeout=timeout,
        max_retries=max_retries,
        backoff=backoff,
        jitter=jitter,
        verbose=verbose,
    )
    info = extract_company_info(html)

    with cache_lock:
        cache[record.company_csn] = info
    record.company_owner = info.owner
    record.company_website = info.website
    record.company_intro = info.intro
    record.company_industry = info.industry
    record.company_address = info.address
    return record


def enrich_company_homepage_from_relay(
    session: requests.Session,
    record: SaraminRecord,
    relay_homepage_cache: dict[str, str],
    relay_homepage_cache_lock: threading.Lock,
    timeout: float,
    max_retries: int,
    backoff: float,
    jitter: float,
    verbose: bool,
) -> SaraminRecord:
    if record.company_website != MISSING or record.rec_id == MISSING:
        return record

    with relay_homepage_cache_lock:
        cached = relay_homepage_cache.get(record.rec_id)
    if cached is not None:
        if cached != MISSING:
            record.company_website = cached
        return record

    homepage = request_relay_homepage(
        session=session,
        rec_id=record.rec_id,
        timeout=timeout,
        max_retries=max_retries,
        backoff=backoff,
        jitter=jitter,
        verbose=verbose,
    )

    with relay_homepage_cache_lock:
        relay_homepage_cache[record.rec_id] = homepage

    if homepage != MISSING:
        record.company_website = homepage
    return record


def parse_listing_item(item: BeautifulSoup) -> SaraminRecord:
    rec_id = item.get("id", "").replace("rec-", "")
    title_el = item.select_one(".job_tit a.str_tit")
    company_el = item.select_one(".company_nm .str_tit")
    company_btn = item.select_one(".company_nm button.interested_corp[csn]")
    recruit_info_map: dict[str, str] = {}

    for p in item.select(".recruit_info p"):
        p_class = " ".join(p.get("class", []))
        value = normalize_text(p.get_text(" ", strip=True))
        if not value or value == MISSING:
            continue
        if "work_place" in p_class:
            recruit_info_map["location"] = value
        elif "career" in p_class:
            recruit_info_map["career"] = value
        elif "education" in p_class:
            recruit_info_map["education"] = value
        else:
            # 保底：按顺序填充
            if "location" not in recruit_info_map:
                recruit_info_map["location"] = value
            elif "career" not in recruit_info_map:
                recruit_info_map["career"] = value
            elif "education" not in recruit_info_map:
                recruit_info_map["education"] = value

    tags = [
        normalize_text(x.get_text(" ", strip=True))
        for x in item.select(".job_sector span")
        if normalize_text(x.get_text(" ", strip=True)) != MISSING
    ]
    tags = [x for x in tags if x and x != "外"]
    badges = [
        normalize_text(x.get_text(" ", strip=True))
        for x in item.select(".job_badge span")
        if normalize_text(x.get_text(" ", strip=True)) not in (MISSING, "외")
    ]

    remain_el = item.select_one(".support_info .date")
    updated_el = item.select_one(".support_info .deadlines")

    title_url = normalize_text(title_el.get("href", "")) if title_el else MISSING
    raw_company_url = normalize_text(company_el.get("href", "")) if company_el else MISSING
    company_name = normalize_text(company_el.get_text(" ", strip=True)) if company_el else MISSING
    if title_url != MISSING and title_url.startswith("/"):
        title_url = urljoin(BASE_ORIGIN, title_url)
    if raw_company_url != MISSING and raw_company_url.startswith("/"):
        raw_company_url = urljoin(BASE_ORIGIN, raw_company_url)

    company_csn = extract_company_csn(raw_company_url)
    company_url = normalize_company_url(raw_company_url)
    if company_csn == MISSING and company_btn is not None:
        company_btn_csn = normalize_text(company_btn.get("csn"))
        if company_btn_csn != MISSING:
            company_csn = company_btn_csn
            company_url = normalize_company_url(f"{BASE_ORIGIN}/zf_user/company-info/view?csn={company_csn}")
        if company_name == MISSING:
            company_name = normalize_text(company_btn.get_text(" ", strip=True))

    return SaraminRecord(
        rec_id=normalize_text(rec_id) if rec_id else MISSING,
        title=normalize_text(title_el.get_text(" ", strip=True)) if title_el else MISSING,
        company_name=company_name,
        title_url=title_url,
        company_url=company_url,
        company_csn=company_csn,
        location=recruit_info_map.get("location", MISSING),
        career_and_type=recruit_info_map.get("career", MISSING),
        tags=tags,
        badges=badges,
        remain=normalize_text(remain_el.get_text(" ", strip=True)) if remain_el else MISSING,
        updated_info=normalize_text(updated_el.get_text(" ", strip=True)) if updated_el else MISSING,
        company_owner=MISSING,
        company_website=MISSING,
        company_intro=MISSING,
        company_industry=MISSING,
        company_address=MISSING,
    )


def parse_listing_response(response_json: dict) -> tuple[list[SaraminRecord], int | None]:
    contents = response_json.get("contents", "")
    total_count = response_json.get("total_count")
    if not isinstance(contents, str):
        raise ValueError("response.contents 不是 HTML 片段")

    soup = BeautifulSoup(contents, "html.parser")
    records = [parse_listing_item(item) for item in soup.select("[id^='rec-']")]
    return records, total_count if isinstance(total_count, int) else None


def iter_list_records(
    session: requests.Session,
    base_url: str,
    start_page: int,
    max_pages: int,
    max_items: int,
    page_size: int,
    sleep_sec: float,
    jitter: float,
    timeout: float,
    max_retries: int,
    backoff: float,
    verbose: bool,
    progress_callback=None,
    stop_event=None,
    total_callback=None,
) -> Iterable[SaraminRecord]:
    seen_ids: set[str] = set()
    got = 0
    page = max(1, start_page)
    total_count: int | None = None
    for _ in range(max(1, max_pages)):
        if stop_event is not None and stop_event.is_set():
            _log("任务被用户中止。", verbose=verbose, callback=progress_callback)
            return
        page_url = build_page_url(base_url, page, page_size)
        page_data = request_page_json(
            session=session,
            url=page_url,
            timeout=timeout,
            max_retries=max_retries,
            backoff=backoff,
            jitter=jitter,
            verbose=verbose,
        )
        records, total_count = parse_listing_response(page_data)
        _log(
            f"[SARAMIN] page={page} got={len(records)} total={total_count or 'unknown'}",
            verbose=verbose,
            callback=progress_callback,
        )
        if total_callback is not None:
            total_callback(total_count)

        if not records:
            break

        for rec in records:
            if stop_event is not None and stop_event.is_set():
                _log("任务被用户中止。", verbose=verbose, callback=progress_callback)
                return
            if rec.rec_id == MISSING or rec.rec_id in seen_ids:
                continue
            seen_ids.add(rec.rec_id)
            yield rec
            got += 1
            if 0 < max_items <= got:
                return

        page += 1
        polite_sleep(sleep_sec, jitter)


def persist_csv_row(fh) -> None:
    fh.flush()
    try:
        os.fsync(fh.fileno())
    except OSError:
        pass


def build_output_part_path(base_path: Path, part_index: int, split_every: int) -> Path:
    if split_every <= 0 or part_index <= 1:
        return base_path
    return base_path.with_name(f"{base_path.stem}_part{part_index:04d}{base_path.suffix}")


def run_scrape(
    args: argparse.Namespace,
    progress_callback=None,
    progress_update_callback=None,
    stop_event=None,
) -> int:
    session = create_session()
    csv_path = Path(args.output_csv).resolve()
    xlsx_path = Path(args.output_xlsx).resolve()
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    xlsx_path.parent.mkdir(parents=True, exist_ok=True)

    header = [
        "公司名",
        "老板名",
        "公司官网",
        "来源链接",
    ]

    count = 0
    total_count = {"value": None}
    save_every = max(1, int(getattr(args, "save_every", 200)))
    save_interval = max(0.0, float(getattr(args, "save_interval", 3.0)))
    fsync_every = max(1, int(getattr(args, "fsync_every", 50)))
    split_every = max(0, int(getattr(args, "split_every", 0)))
    max_workers = max(1, int(getattr(args, "workers", 1)))
    batch_size = max(1, args.page_size) if hasattr(args, "page_size") else 20
    write_lock = threading.Lock()
    company_cache: dict[str, CompanyInfo] = {}
    company_cache_lock = threading.Lock()
    relay_homepage_cache: dict[str, str] = {}
    relay_homepage_cache_lock = threading.Lock()
    had_error = False

    wb: Workbook | None = None
    ws = None
    writer: csv.writer | None = None
    output_file = None
    part_index = 0
    part_row_count = 0
    rows_since_fsync = 0
    last_excel_save_at = 0.0
    active_csv_path: Path | None = None
    active_xlsx_path: Path | None = None
    generated_csv_paths: list[Path] = []
    generated_xlsx_paths: list[Path] = []

    def _ensure_output_open() -> None:
        nonlocal wb, ws, writer, output_file
        nonlocal part_index, part_row_count, rows_since_fsync, last_excel_save_at
        nonlocal active_csv_path, active_xlsx_path

        if output_file is not None:
            return

        part_index += 1
        active_csv_path = build_output_part_path(csv_path, part_index, split_every)
        active_xlsx_path = build_output_part_path(xlsx_path, part_index, split_every)
        active_csv_path.parent.mkdir(parents=True, exist_ok=True)
        active_xlsx_path.parent.mkdir(parents=True, exist_ok=True)

        output_file = active_csv_path.open("w", newline="", encoding="utf-8-sig")
        writer = csv.writer(output_file)
        writer.writerow(header)
        output_file.flush()

        wb = Workbook()
        ws = wb.active
        ws.title = "saramin"
        ws.append(header)
        wb.save(active_xlsx_path)
        last_excel_save_at = time.monotonic()
        part_row_count = 0
        rows_since_fsync = 0

        generated_csv_paths.append(active_csv_path)
        generated_xlsx_paths.append(active_xlsx_path)

    def _close_output(flush_csv: bool = True) -> None:
        nonlocal wb, ws, writer, output_file
        nonlocal part_row_count, rows_since_fsync

        if output_file is None:
            return

        if flush_csv and rows_since_fsync > 0:
            persist_csv_row(output_file)
            rows_since_fsync = 0
        output_file.close()
        output_file = None
        writer = None

        if wb is not None and active_xlsx_path is not None:
            wb.save(active_xlsx_path)
            wb.close()
        wb = None
        ws = None
        part_row_count = 0

    def _append_row(record: SaraminRecord, index: int) -> None:
        nonlocal part_row_count, rows_since_fsync, last_excel_save_at
        with write_lock:
            _ensure_output_open()
            row = record.to_row()
            assert writer is not None
            assert output_file is not None
            assert ws is not None
            assert wb is not None
            assert active_xlsx_path is not None

            writer.writerow(row)
            part_row_count += 1
            rows_since_fsync += 1
            if rows_since_fsync >= fsync_every:
                persist_csv_row(output_file)
                rows_since_fsync = 0

            ws.append(row)
            now = time.monotonic()
            if index % save_every == 0 or (
                save_interval > 0 and now - last_excel_save_at >= save_interval
            ):
                wb.save(active_xlsx_path)
                last_excel_save_at = now

            if split_every > 0 and part_row_count >= split_every:
                wb.save(active_xlsx_path)
                last_excel_save_at = time.monotonic()
                _close_output(flush_csv=True)

    def _safe_enrich(record: SaraminRecord) -> SaraminRecord:
        if stop_event is not None and stop_event.is_set():
            return record
        try:
            enriched = enrich_company_info(
                session=session,
                record=record,
                cache=company_cache,
                cache_lock=company_cache_lock,
                timeout=args.timeout,
                max_retries=args.max_retries,
                backoff=args.backoff,
                jitter=args.jitter,
                verbose=args.verbose,
            )
        except Exception as exc:  # noqa: BLE001
            _log(
                build_company_detail_warning(
                    rec_id=record.rec_id,
                    company_url=record.company_url,
                    exc=exc,
                ),
                verbose=args.verbose,
                callback=progress_callback,
            )
            return enrich_company_homepage_from_relay(
                session=session,
                record=record,
                relay_homepage_cache=relay_homepage_cache,
                relay_homepage_cache_lock=relay_homepage_cache_lock,
                timeout=args.timeout,
                max_retries=args.max_retries,
                backoff=args.backoff,
                jitter=args.jitter,
                verbose=args.verbose,
            )

        if enriched.company_website != MISSING or enriched.rec_id == MISSING:
            return enriched

        return enrich_company_homepage_from_relay(
            session=session,
            record=enriched,
            relay_homepage_cache=relay_homepage_cache,
            relay_homepage_cache_lock=relay_homepage_cache_lock,
            timeout=args.timeout,
            max_retries=args.max_retries,
            backoff=args.backoff,
            jitter=args.jitter,
            verbose=args.verbose,
        )

    def _iter_enriched(records: list[SaraminRecord]) -> Iterable[SaraminRecord]:
        if max_workers <= 1 or len(records) <= 1:
            for r in records:
                yield _safe_enrich(r)
            return

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures: dict[concurrent.futures.Future[SaraminRecord], int] = {}
            for idx, item in enumerate(records):
                future = executor.submit(_safe_enrich, item)
                futures[future] = idx

            done_map: dict[int, SaraminRecord] = {}
            for future in concurrent.futures.as_completed(futures):
                done_map[futures[future]] = future.result()

            for index in sorted(done_map):
                yield done_map[index]

    try:
        with write_lock:
            _ensure_output_open()

        urls: list[str] = getattr(args, "urls", None) or [args.url]
        global_seen_ids: set[str] = set()
        url_totals: dict[int, int] = {}

        def _iter_all_urls() -> Iterable[SaraminRecord]:
            items_yielded = 0
            for url_index, url in enumerate(urls):
                if stop_event is not None and stop_event.is_set():
                    return
                if len(urls) > 1:
                    _log(
                        f"[SARAMIN] 开始第 {url_index + 1}/{len(urls)} 个链接",
                        verbose=True,
                        callback=progress_callback,
                    )

                # 用 default arg 捕获当前 url_index 值，避免闭包引用问题
                def _on_total(value: int | None, _idx: int = url_index) -> None:
                    if value is not None and _idx not in url_totals:
                        url_totals[_idx] = value
                        total_count["value"] = sum(url_totals.values())

                remaining_items = 0
                if 0 < args.max_items:
                    remaining_items = args.max_items - items_yielded
                    if remaining_items <= 0:
                        return

                for record in iter_list_records(
                    session=session,
                    base_url=url,
                    start_page=args.start_page,
                    max_pages=args.max_pages,
                    max_items=remaining_items if 0 < args.max_items else 0,
                    page_size=args.page_size,
                    sleep_sec=args.sleep,
                    jitter=args.jitter,
                    timeout=args.timeout,
                    max_retries=args.max_retries,
                    backoff=args.backoff,
                    verbose=args.verbose,
                    progress_callback=progress_callback,
                    stop_event=stop_event,
                    total_callback=_on_total,
                ):
                    if record.rec_id != MISSING and record.rec_id in global_seen_ids:
                        continue
                    if record.rec_id != MISSING:
                        global_seen_ids.add(record.rec_id)
                    yield record
                    items_yielded += 1

        records = _iter_all_urls()

        cache_batch: list[SaraminRecord] = []
        for record in records:
            if stop_event is not None and stop_event.is_set():
                break
            cache_batch.append(record)
            if len(cache_batch) >= batch_size:
                for enriched_record in _iter_enriched(cache_batch):
                    count += 1
                    _append_row(enriched_record, count)
                    if progress_update_callback is not None:
                        progress_update_callback(count, total_count["value"])
                    if count % 20 == 0:
                        _log(
                            f"[SARAMIN] 进度 {count}",
                            verbose=args.verbose,
                            callback=progress_callback,
                        )
                cache_batch.clear()

        if cache_batch:
            for enriched_record in _iter_enriched(cache_batch):
                count += 1
                _append_row(enriched_record, count)
                if progress_update_callback is not None:
                    progress_update_callback(count, total_count["value"])
                if count % 20 == 0:
                    _log(
                        f"[SARAMIN] 进度 {count}",
                        verbose=args.verbose,
                        callback=progress_callback,
                    )

    except Exception as exc:  # noqa: BLE001
        had_error = True
        _log(f"采集失败: {exc}", verbose=True, callback=progress_callback)
        raise
    finally:
        try:
            with write_lock:
                _close_output(flush_csv=True)
        except Exception as exc:  # noqa: BLE001
            _log(
                f"[SARAMIN][WARN] 保存 Excel 失败: {exc}",
                verbose=args.verbose,
                callback=progress_callback,
            )

    if not had_error:
        _log(f"完成采集: {count} 条", verbose=True, callback=progress_callback)
        if split_every > 0 and len(generated_csv_paths) > 1:
            _log(
                f"CSV 分片: {len(generated_csv_paths)} 个，首个 {generated_csv_paths[0]}，最后 {generated_csv_paths[-1]}",
                verbose=True,
                callback=progress_callback,
            )
            _log(
                f"Excel 分片: {len(generated_xlsx_paths)} 个，首个 {generated_xlsx_paths[0]}，最后 {generated_xlsx_paths[-1]}",
                verbose=True,
                callback=progress_callback,
            )
        else:
            _log(f"CSV 输出: {csv_path}", verbose=True, callback=progress_callback)
            _log(f"Excel 输出: {xlsx_path}", verbose=True, callback=progress_callback)
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return run_scrape(args)


if __name__ == "__main__":
    raise SystemExit(main())
