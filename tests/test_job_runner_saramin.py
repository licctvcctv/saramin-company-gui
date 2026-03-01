from __future__ import annotations

from unittest import TestCase

from saramin_app.job_runner import JobRunner


class JobRunnerSaraminTest(TestCase):
    def test_safe_args_accepts_basic_payload(self) -> None:
        runner = JobRunner()
        args = runner._safe_args(
            {
                "source": "saramin",
                "url": "https://www.saramin.co.kr/zf_user/jobs/list/domestic?loc_mcd=101000",
                "start_page": 1,
                "max_pages": 4,
                "max_items": 120,
                "workers": 6,
                "output_csv": "/tmp/saramin_repo.csv",
                "output_xlsx": "/tmp/saramin_repo.xlsx",
            }
        )
        self.assertEqual(args.source, "saramin")
        self.assertEqual(args.max_items, 120)
        self.assertIn("loc_mcd=101000", args.url)

    def test_locations_are_applied_to_url(self) -> None:
        runner = JobRunner()
        args = runner._safe_args(
            {
                "url": "https://www.saramin.co.kr/zf_user/jobs/list/domestic?panel_type=&search_done=y",
                "locations": ["101000", "101010", "117000"],
                "start_page": 1,
                "max_pages": 1,
                "output_csv": "/tmp/saramin_repo_loc.csv",
                "output_xlsx": "/tmp/saramin_repo_loc.xlsx",
            }
        )
        self.assertIn("loc_mcd=101000", args.url)
        self.assertIn("loc_mcd=101010", args.url)
        self.assertIn("loc_mcd=117000", args.url)
