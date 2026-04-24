from datetime import date

from src.crawler.lzh_crawler import LzhCrawler


def test_validate_date_allows_tomorrow_for_schedule() -> None:
    crawler = LzhCrawler.__new__(LzhCrawler)

    assert crawler._validate_date(
        target_date=date(2026, 4, 24),
        data_type="schedule",
        today=date(2026, 4, 23),
    )


def test_validate_date_rejects_tomorrow_for_performance() -> None:
    crawler = LzhCrawler.__new__(LzhCrawler)

    assert not crawler._validate_date(
        target_date=date(2026, 4, 24),
        data_type="performance",
        today=date(2026, 4, 23),
    )


def test_validate_date_rejects_day_after_tomorrow_for_schedule() -> None:
    crawler = LzhCrawler.__new__(LzhCrawler)

    assert not crawler._validate_date(
        target_date=date(2026, 4, 25),
        data_type="schedule",
        today=date(2026, 4, 23),
    )
