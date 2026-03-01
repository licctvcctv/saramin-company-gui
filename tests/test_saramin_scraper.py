from __future__ import annotations

from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from saramin_scraper import (
    MISSING,
    CompanyInfo,
    build_company_detail_warning,
    extract_company_csn,
    extract_company_info,
    extract_homepage_from_description,
    extract_homepage_from_html,
    SaraminRecord,
    build_page_url,
    parse_args,
    request_relay_homepage,
    parse_listing_response,
    run_scrape,
)


SAMPLE_CONTENT = """
<section class="list_recruiting">
  <div id="rec-111" class="list_item">
    <div class="box_item">
      <div class="col company_nm">
        <a class="str_tit" href="/zf_user/company-info/view-inner-recruit?csn=abc">测试公司</a>
      </div>
      <div class="col notification_info">
        <div class="job_tit">
          <a class="str_tit" href="/zf_user/jobs/relay/view?view_type=list&rec_idx=111">测试岗位</a>
        </div>
        <div class="job_meta"><span class="job_sector"><span>市场</span><span>开发</span></span></div>
      </div>
      <div class="col recruit_info">
        <ul>
          <li><p class="work_place">上海</p></li>
          <li><p class="career">3년</p></li>
          <li><p class="education">대졸</p></li>
        </ul>
      </div>
      <div class="col support_info">
        <p class="support_detail">
          <span class="date">D-1</span>
          <span class="deadlines">今天</span>
        </p>
      </div>
    </div>
  </div>
  <div id="rec-222" class="list_item effect">
    <div class="box_item">
      <div class="col company_nm">
        <span class="str_tit">测试公司B</span>
        <button class="interested_corp" csn="def" aria-pressed="false"></button>
      </div>
      <div class="col notification_info">
        <div class="job_tit">
          <a class="str_tit" href="/zf_user/jobs/relay/view?view_type=list&rec_idx=222">测试岗位B</a>
        </div>
        <div class="job_meta"><span class="job_sector"><span>测试</span></span></div>
      </div>
      <div class="col recruit_info">
        <ul><li><p>서울 강남구</p></li><li><p>경력 2년</p></li><li><p>학력무관</p></li></ul>
      </div>
    </div>
  </div>
</section>
"""

COMPANY_DETAILS_DL = """
<html>
  <body>
    <dl class="company_details">
      <dt>업종</dt><dd>모바일 앱</dd>
      <dt>대표자명</dt><dd>이현철</dd>
      <dt>홈페이지</dt><dd>https://www.hectoinnovation.co.kr/</dd>
      <dt>사업내용</dt><dd>휴대폰번호 도용방지 서비스</dd>
      <dt>주소</dt><dd>서울 강남구 테헤란로 7길 7 지도보기</dd>
    </dl>
  </body>
</html>
"""

COMPANY_DETAILS_LDJSON = """
<html>
  <body>
    <script type="application/ld+json">
    {
      "@context": "https://schema.org",
      "@type": "Organization",
      "name": "테스트 회사",
      "founder": {
        "name": "홍길동"
      },
      "sameAs": "https://www.example.com",
      "description": "领先的移动安全解决方案服务商",
      "address": {
        "addressLocality": "서울 강남구",
        "streetAddress": "테헤란로"
      }
    }
    </script>
  </body>
</html>
"""

HOME_DESCRIPTION_VARIANTS = [
    "岗位名称, 公司名, 홈페이지: https://www.example.com",
    "岗位名称, 公司名, 홈페이지：https://www.example.cn",
    "岗位名称, 公司名, 홈페이지 : www.example.org",
    "岗位名称, 公司名, 홈페이지:https://www.example.net, 마감일:2026-12-31",
]

HOME_HTML_WITH_DL_NO_CLASS = """
<html>
  <body>
    <dl>
      <dt>업종</dt><dd>互联网</dd>
      <dt>대표자명</dt><dd>김대표</dd>
      <dt>홈페이지</dt><dd>https://www.example.io</dd>
      <dt>주소</dt><dd>서울</dd>
    </dl>
  </body>
</html>
"""

VIEW_AJAX_WITH_HOME_HTML = """
<div>
  <dl>
    <dt>홈페이지</dt>
    <dd>
      <a href="https://www.ajax-homepage.com">homepage</a>
    </dd>
  </dl>
</div>
"""


class SaraminPageUrlTest(TestCase):
    def test_parse_args_supports_persistence_tuning_options(self) -> None:
        args = parse_args(
            [
                "--fsync-every",
                "30",
                "--save-interval",
                "2.5",
                "--split-every",
                "20000",
            ]
        )
        self.assertEqual(args.fsync_every, 30)
        self.assertEqual(args.save_interval, 2.5)
        self.assertEqual(args.split_every, 20000)

    def test_build_page_url(self) -> None:
        url = build_page_url(
            "https://www.saramin.co.kr/zf_user/jobs/list/domestic?loc_mcd=101000&panel_type=",
            page=3,
            page_size=20,
        )
        self.assertIn("page=3", url)
        self.assertIn("page_count=20", url)
        self.assertIn("loc_mcd=101000", url)

    def test_build_page_url_supports_bracket_array_params(self) -> None:
        url = build_page_url(
            "https://www.saramin.co.kr/zf_user/jobs/list/domestic?loc_mcd[]=101000&loc_mcd[]=101010&panel_type=",
            page=1,
            page_size=20,
        )
        parsed = parse_qs(urlsplit(url).query)
        self.assertEqual(["101000,101010"], parsed["loc_mcd"])
        self.assertNotIn("loc_mcd[]", parsed)


class SaraminParseTest(TestCase):
    def test_extract_company_csn(self) -> None:
        self.assertEqual(
            extract_company_csn("https://www.saramin.co.kr/zf_user/company-info/view-inner-recruit?csn=abc"),
            "abc",
        )
        self.assertEqual(
            extract_company_csn("/zf_user/company-info/view-inner-recruit?loc=101000&csn=abc123"),
            "abc123",
        )
        self.assertEqual(
            extract_company_csn("https://www.saramin.co.kr/zf_user/company-info/view?csn=xyz"),
            "xyz",
        )
        self.assertEqual(
            extract_company_csn("https://www.saramin.co.kr/zf_user/company-info/view-inner-recruit"),
            MISSING,
        )

    def test_extract_company_info_from_dl(self) -> None:
        info = extract_company_info(COMPANY_DETAILS_DL)
        self.assertIsInstance(info, CompanyInfo)
        self.assertEqual(info.industry, "모바일 앱")
        self.assertEqual(info.owner, "이현철")
        self.assertEqual(info.website, "https://www.hectoinnovation.co.kr/")
        self.assertEqual(info.intro, "휴대폰번호 도용방지 서비스")
        self.assertEqual(info.address, "서울 강남구 테헤란로 7길 7")

    def test_extract_company_info_from_ldjson_fallback(self) -> None:
        info = extract_company_info(COMPANY_DETAILS_LDJSON)
        self.assertIsInstance(info, CompanyInfo)
        self.assertEqual(info.owner, "홍길동")
        self.assertEqual(info.website, "https://www.example.com")
        self.assertEqual(info.intro, "领先的移动安全解决方案服务商")
        self.assertEqual(info.address, "서울 강남구 테헤란로")
        self.assertEqual(info.industry, MISSING)

    def test_extract_homepage_from_description_variants(self) -> None:
        expected = {
            "홈페이지: https://www.example.com": "https://www.example.com",
            "홈페이지：https://www.example.cn": "https://www.example.cn",
            "홈페이지 : www.example.org": "https://www.example.org",
            "홈페이지:https://www.example.net,": "https://www.example.net",
        }
        for content in HOME_DESCRIPTION_VARIANTS:
            matched = next(
                (value for key, value in expected.items() if key in content),
                MISSING,
            )
            self.assertEqual(extract_homepage_from_description(content), matched)

    def test_extract_homepage_from_html_dl_without_class(self) -> None:
        info = extract_company_info(HOME_HTML_WITH_DL_NO_CLASS)
        self.assertEqual(info.website, "https://www.example.io")

    def test_extract_homepage_from_html_meta_with_anchor(self) -> None:
        home_html = """
        <html>
          <head>
            <meta name="description" content="岗位测试, 회사명, 홈페이지: https://www.meta-example.com">
          </head>
        </html>
        """
        self.assertEqual(extract_homepage_from_html(home_html), "https://www.meta-example.com")

    def test_parse_listing_response(self) -> None:
        items, total = parse_listing_response({"contents": SAMPLE_CONTENT, "total_count": 2})
        self.assertEqual(len(items), 2)
        self.assertEqual(total, 2)
        first = items[0]
        self.assertIsInstance(first, SaraminRecord)
        self.assertEqual(first.rec_id, "111")
        self.assertEqual(first.title, "测试岗位")
        self.assertEqual(first.location, "上海")
        self.assertEqual(first.career_and_type, "3년")
        self.assertEqual(first.company_csn, "abc")
        self.assertIn("市场", first.tags)
        self.assertEqual(first.remain, "D-1")
        self.assertEqual(first.company_owner, MISSING)
        self.assertEqual(first.company_website, MISSING)
        self.assertEqual(first.company_intro, MISSING)
        self.assertEqual(first.company_industry, MISSING)
        self.assertEqual(first.company_address, MISSING)

        second = items[1]
        self.assertEqual(second.company_url, "https://www.saramin.co.kr/zf_user/company-info/view?csn=def")
        self.assertEqual(second.company_csn, "def")
        self.assertEqual(second.title_url, "https://www.saramin.co.kr/zf_user/jobs/relay/view?view_type=list&rec_idx=222")
        self.assertEqual(second.updated_info, MISSING)

    def test_build_company_detail_warning_hides_raw_http_error(self) -> None:
        warning = build_company_detail_warning(
            rec_id="53185622",
            company_url="https://www.saramin.co.kr/zf_user/company-info/view?csn=abc",
            exc=RuntimeError("企业详情请求失败: xxx / 404 Client Error: Not Found"),
        )
        self.assertIn("单个链接未获取", warning)
        self.assertIn("53185622", warning)
        self.assertIn("https://www.saramin.co.kr/zf_user/company-info/view?csn=abc", warning)
        self.assertNotIn("404", warning)
        self.assertNotIn("请求失败", warning)


class SaraminRunScrapeTest(TestCase):
    def test_run_scrape_with_mock(self) -> None:
        csv_out = Path("/tmp/saramin_scraper_test.csv")
        xlsx_out = Path("/tmp/saramin_scraper_test.xlsx")
        if csv_out.exists():
            csv_out.unlink()
        if xlsx_out.exists():
            xlsx_out.unlink()

        with patch("saramin_scraper.iter_list_records") as mock_iter:
            mock_iter.return_value = [
                SaraminRecord(
                    rec_id="111",
                    title="岗位A",
                    company_name="公司A",
                    title_url="https://www.saramin.co.kr/job/111",
                    company_url="https://www.saramin.co.kr/company/1",
                    company_csn="csn-1",
                    location="北京",
                    career_and_type="경력 1년",
                    tags=["A", "B"],
                    badges=["hot"],
                    remain="D-1",
                    updated_info=MISSING,
                    company_owner="老板A",
                    company_website="https://example.com",
                    company_intro="测试简介",
                    company_industry="互联网",
                    company_address="测试地址",
                )
            ]
            args = SimpleNamespace(
                url="https://www.saramin.co.kr/zf_user/jobs/list/domestic",
                start_page=1,
                max_pages=1,
                max_items=1,
                page_size=50,
                sleep=0,
                jitter=0,
                timeout=20.0,
                max_retries=1,
                backoff=1.0,
                verbose=False,
                output_csv=str(csv_out),
                output_xlsx=str(xlsx_out),
                save_every=1,
                workers=1,
            )
            result = run_scrape(args)
            self.assertEqual(result, 0)

        text = csv_out.read_text(encoding="utf-8-sig").splitlines()
        self.assertEqual(len(text), 2)
        self.assertEqual(
            text[0],
            "公司名,老板名,公司官网,来源链接",
        )
        self.assertIn("公司A,老板A,https://example.com,https://www.saramin.co.kr/company/1", text[1])
        self.assertTrue(xlsx_out.exists())

    def test_export_row_keeps_only_target_fields(self) -> None:
        row = SaraminRecord(
            rec_id="1",
            title="岗位A",
            company_name="公司A",
            title_url="https://www.saramin.co.kr/job/111",
            company_url="https://www.saramin.co.kr/company/1",
            company_csn="csn-1",
            location="北京",
            career_and_type="경력 1년",
            tags=[],
            badges=[],
            remain=MISSING,
            updated_info=MISSING,
            company_owner="老板A",
            company_website="https://example.com",
            company_intro="测试简介",
            company_industry="互联网",
            company_address="测试地址",
        )
        self.assertEqual(
            row.to_row(),
            ["公司A", "老板A", "https://example.com", "https://www.saramin.co.kr/company/1"],
        )


class SaraminRelayTest(TestCase):
    def test_extract_homepage_from_sns_only_for_official_domain(self) -> None:
        html = """
        <html>
          <body>
            <dl>
              <dt>업종</dt><dd>금융</dd>
              <dt>SNS</dt><dd>
                <a href="https://www.instagram.com/company">인스타</a>
                <a href="https://blog.company-kr.com/">공식블로그</a>
                <a href="https://map.kakao.com/link/map/abc">지도</a>
              </dd>
            </dl>
          </body>
        </html>
        """
        self.assertEqual(
            extract_homepage_from_html(html),
            "https://blog.company-kr.com/",
        )

    def test_request_relay_homepage_extracts_track_window_location(self) -> None:
        class FakeResponse:
            status_code = 200
            encoding = "utf-8"
            apparent_encoding = "utf-8"

            def __init__(self, text: str) -> None:
                self.text = text

            def raise_for_status(self) -> None:
                return None

        class FakeSession:
            def __init__(self, html: str) -> None:
                self._html = html
                self._calls: list[str] = []

            def get(self, url: str, timeout: float) -> FakeResponse:  # noqa: ARG002
                self._calls.append(url)
                return FakeResponse(self._html)

        session = FakeSession("<script>window.location='https://www.official-company.com'</script>")
        result = request_relay_homepage(
            session=session,
            rec_id="12345",
            timeout=20,
            max_retries=1,
            backoff=1.0,
            jitter=0.0,
            verbose=False,
        )
        self.assertEqual(result, "https://www.official-company.com")

    def test_request_relay_homepage_extracts_window_location_href(self) -> None:
        class FakeResponse:
            status_code = 200
            encoding = "utf-8"
            apparent_encoding = "utf-8"

            def __init__(self, text: str) -> None:
                self.text = text

            def raise_for_status(self) -> None:
                return None

        class FakeSession:
            def __init__(self, html: str) -> None:
                self._html = html

            def post(self, url: str, data: dict[str, object], timeout: float, headers: dict[str, str]) -> FakeResponse:  # noqa: ARG002
                return FakeResponse("")

            def get(self, url: str, timeout: float) -> FakeResponse:  # noqa: ARG002
                return FakeResponse(self._html)

        session = FakeSession("<script>window.location.href='https://www.location-href-company.com'</script>")
        result = request_relay_homepage(
            session=session,
            rec_id="4321",
            timeout=20,
            max_retries=1,
            backoff=1.0,
            jitter=0.0,
            verbose=False,
        )
        self.assertEqual(result, "https://www.location-href-company.com")

    def test_request_relay_homepage_prefers_view_ajax(self) -> None:
        class FakeResponse:
            status_code = 200
            encoding = "utf-8"
            apparent_encoding = "utf-8"

            def __init__(self, text: str) -> None:
                self.text = text

            def raise_for_status(self) -> None:
                return None

        class FakeSession:
            def __init__(self) -> None:
                self.post_calls: list[tuple[str, dict[str, object], float, dict[str, str]]] = []
                self.get_calls: list[str] = []

            def post(
                self,
                url: str,
                data: dict[str, object],
                timeout: float,
                headers: dict[str, str],
            ) -> FakeResponse:  # noqa: ARG002
                self.post_calls.append((url, data, timeout, headers))
                return FakeResponse(VIEW_AJAX_WITH_HOME_HTML)

            def get(self, url: str, timeout: float) -> FakeResponse:  # noqa: ARG002
                self.get_calls.append(url)
                return FakeResponse("")

        session = FakeSession()
        result = request_relay_homepage(
            session=session,
            rec_id="9988",
            timeout=20,
            max_retries=1,
            backoff=1.0,
            jitter=0.0,
            verbose=False,
        )
        self.assertEqual(result, "https://www.ajax-homepage.com")
        self.assertEqual(len(session.post_calls), 1)
        self.assertEqual(len(session.get_calls), 0)
