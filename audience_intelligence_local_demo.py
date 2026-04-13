#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd


def build_source_frames() -> dict[str, pd.DataFrame]:
    ticket_purchases = pd.DataFrame(
        [
            (101, "Lions FC", "2026-02-10", 2, 180.0),
            (101, "Lions FC", "2026-03-18", 1, 95.0),
            (102, "Lions FC", "2026-03-02", 4, 320.0),
            (103, "Tigers FC", "2026-03-05", 1, 70.0),
            (104, "Lions FC", "2026-03-20", 2, 160.0),
        ],
        columns=["fan_id", "club", "purchase_date", "tickets", "ticket_revenue"],
    )

    web_sessions = pd.DataFrame(
        [
            (101, "2026-03-19", 5, 22),
            (101, "2026-03-25", 3, 11),
            (102, "2026-03-24", 8, 31),
            (103, "2026-03-22", 2, 6),
            (104, "2026-03-28", 6, 19),
            (105, "2026-03-29", 4, 14),
        ],
        columns=["fan_id", "session_date", "sessions", "page_views"],
    )

    email_engagement = pd.DataFrame(
        [
            (101, "season_launch", 5, 3, 1),
            (102, "season_launch", 4, 1, 0),
            (103, "renewal_push", 3, 2, 0),
            (104, "vip_offer", 6, 5, 2),
            (105, "welcome_series", 2, 1, 1),
        ],
        columns=["fan_id", "campaign_name", "emails_sent", "emails_opened", "emails_clicked"],
    )

    subscriptions = pd.DataFrame(
        [
            (101, "member", "active", "2026-12-31", 199.0),
            (102, "member", "active", "2026-10-31", 149.0),
            (103, "newsletter", "inactive", None, 0.0),
            (104, "member", "active", "2027-01-15", 249.0),
            (105, "newsletter", "active", None, 0.0),
        ],
        columns=["fan_id", "subscription_type", "subscription_status", "renewal_date", "subscription_value"],
    )

    return {
        "ticket_purchases": ticket_purchases,
        "web_sessions": web_sessions,
        "email_engagement": email_engagement,
        "subscriptions": subscriptions,
    }


def build_outputs(output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    frames = build_source_frames()

    conn = duckdb.connect()
    for name, frame in frames.items():
        conn.register(name, frame)

    fan_360 = conn.execute(
        """
        with ticket_agg as (
            select
                fan_id,
                any_value(club) as favorite_club,
                sum(tickets) as tickets_purchased,
                sum(ticket_revenue) as ticket_ltv,
                max(cast(purchase_date as date)) as last_ticket_purchase
            from ticket_purchases
            group by fan_id
        ),
        web_agg as (
            select
                fan_id,
                sum(sessions) as total_sessions,
                sum(page_views) as total_page_views,
                max(cast(session_date as date)) as last_session_date
            from web_sessions
            group by fan_id
        ),
        email_agg as (
            select
                fan_id,
                sum(emails_sent) as emails_sent,
                sum(emails_opened) as emails_opened,
                sum(emails_clicked) as emails_clicked,
                case
                    when sum(emails_sent) = 0 then 0
                    else round(sum(emails_opened) * 1.0 / sum(emails_sent), 3)
                end as open_rate
            from email_engagement
            group by fan_id
        )
        select
            coalesce(t.fan_id, w.fan_id, e.fan_id, s.fan_id) as fan_id,
            coalesce(t.favorite_club, 'Unknown') as favorite_club,
            coalesce(t.tickets_purchased, 0) as tickets_purchased,
            coalesce(t.ticket_ltv, 0) + coalesce(s.subscription_value, 0) as total_value,
            coalesce(w.total_sessions, 0) as total_sessions,
            coalesce(w.total_page_views, 0) as total_page_views,
            coalesce(e.open_rate, 0) as email_open_rate,
            coalesce(e.emails_clicked, 0) as email_clicks,
            coalesce(s.subscription_status, 'unknown') as subscription_status,
            t.last_ticket_purchase,
            w.last_session_date,
            s.renewal_date,
            case
                when coalesce(t.ticket_ltv, 0) + coalesce(s.subscription_value, 0) >= 350
                     and coalesce(e.open_rate, 0) >= 0.5 then 'high_value_engaged'
                when coalesce(t.tickets_purchased, 0) >= 2
                     or coalesce(w.total_sessions, 0) >= 5 then 'active_fan'
                when coalesce(e.open_rate, 0) > 0 then 'reachable_low_value'
                else 'at_risk'
            end as engagement_segment
        from ticket_agg t
        full outer join web_agg w using (fan_id)
        full outer join email_agg e using (fan_id)
        full outer join subscriptions s using (fan_id)
        order by fan_id
        """
    ).df()

    if fan_360["fan_id"].isna().any():
        raise ValueError("fan_id should never be null in the modeled output")
    if fan_360["fan_id"].duplicated().any():
        raise ValueError("fan_id should be unique in the modeled output")

    summary = conn.execute(
        """
        select
            engagement_segment,
            count(*) as fans,
            round(sum(total_value), 2) as segment_value,
            round(avg(email_open_rate), 3) as avg_open_rate
        from fan_360
        group by engagement_segment
        order by segment_value desc
        """
    ).df()

    fan_360_path = output_dir / "fan_360.parquet"
    summary_path = output_dir / "segment_summary.csv"
    fan_360.to_parquet(fan_360_path, index=False)
    summary.to_csv(summary_path, index=False)

    print("Wrote fan_360 dataset to", fan_360_path)
    print("Wrote segment summary to", summary_path)
    print(summary.to_string(index=False))

    return fan_360_path, summary_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a local audience intelligence demo dataset")
    parser.add_argument("--output-dir", default="audience_intelligence_output", help="Directory for parquet/csv outputs")
    args = parser.parse_args()
    build_outputs(Path(args.output_dir))


if __name__ == "__main__":
    main()