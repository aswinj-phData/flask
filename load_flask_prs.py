"""
Load closed pull requests from pallets/flask into Snowflake PR_EVAL.RAW.

Tables created (replaced on every run):
  RAW.PULL_REQUESTS   one row per PR (with size stats)
  RAW.PR_REVIEWS      one row per review
  RAW.PR_FILES        one row per changed file
  RAW.AGENT_VERDICTS  your agent's verdicts (from a CSV, or empty)

Only CLOSED PRs are loaded, because those have a known outcome
(merged or closed unmerged) to compare your agent against.

Setup:
  pip install requests snowflake-connector-python

Environment variables (never hard-code these or commit them):
  GITHUB_TOKEN         GitHub personal access token (read-only public repo access)
  SNOWFLAKE_ACCOUNT    e.g. abc12345-xy67890 (your TRIAL account identifier)
  SNOWFLAKE_USER
  SNOWFLAKE_PASSWORD
  SNOWFLAKE_ROLE       default ACCOUNTADMIN
  SNOWFLAKE_WAREHOUSE  default DBT_WH
  SNOWFLAKE_DATABASE   default PR_EVAL

Usage:
  python load_flask_prs.py --limit 300
  python load_flask_prs.py --limit 300 --verdicts agent_verdicts.csv
  python load_flask_prs.py --limit 300 --fake-verdicts   # random, pipeline testing only

agent_verdicts.csv columns: pr_number,verdict,reasoning
  verdict values: approve / reject
"""
import argparse
import csv
import os
import random
import sys
import time
from datetime import datetime

import requests
import snowflake.connector

REPO = "pallets/flask"
API = "https://api.github.com"


def gh_session():
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        sys.exit("Set GITHUB_TOKEN first.")
    s = requests.Session()
    s.headers.update(
        {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
    )
    return s


def gh_get(session, url, params=None):
    """GET with simple rate-limit handling."""
    while True:
        r = session.get(url, params=params, timeout=30)
        remaining = int(r.headers.get("X-RateLimit-Remaining", "1"))
        if r.status_code in (403, 429) and remaining == 0:
            reset = int(r.headers.get("X-RateLimit-Reset", time.time() + 60))
            wait = max(reset - int(time.time()), 1) + 2
            print(f"Rate limited, sleeping {wait}s ...")
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json()


def gh_paginate(session, url, params=None, max_items=None):
    items, page = [], 1
    params = dict(params or {})
    params["per_page"] = 100
    while True:
        params["page"] = page
        batch = gh_get(session, url, params)
        if not batch:
            break
        items.extend(batch)
        if max_items and len(items) >= max_items:
            return items[:max_items]
        if len(batch) < 100:
            break
        page += 1
    return items


def ts(value):
    """GitHub ISO timestamp -> naive UTC datetime (or None)."""
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")


def sf_connect():
    for var in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"):
        if not os.environ.get(var):
            sys.exit(f"Set {var} first.")
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "DBT_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "PR_EVAL"),
        schema="RAW",
    )


DDL = {
    "PULL_REQUESTS": """
        create or replace table pull_requests (
            number integer, title string, body string, user_login string,
            state string, draft boolean,
            created_at timestamp_ntz, updated_at timestamp_ntz,
            closed_at timestamp_ntz, merged_at timestamp_ntz,
            additions integer, deletions integer, changed_files integer,
            commits integer, comments integer, review_comments integer,
            base_ref string, head_ref string
        )""",
    "PR_REVIEWS": """
        create or replace table pr_reviews (
            pr_number integer, review_id integer, reviewer_login string,
            state string, submitted_at timestamp_ntz
        )""",
    "PR_FILES": """
        create or replace table pr_files (
            pr_number integer, filename string, status string,
            additions integer, deletions integer, changes integer
        )""",
    "AGENT_VERDICTS": """
        create or replace table agent_verdicts (
            pr_number integer, verdict string, reasoning string,
            created_at timestamp_ntz
        )""",
}


def insert(cur, table, columns, rows):
    if not rows:
        print(f"  {table}: 0 rows")
        return
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"insert into {table} ({', '.join(columns)}) values ({placeholders})"
    # insert in chunks to keep statements a reasonable size
    for i in range(0, len(rows), 5000):
        cur.executemany(sql, rows[i : i + 5000])
    print(f"  {table}: {len(rows)} rows")


def load_verdicts(path, fake, pr_numbers):
    now = datetime.utcnow()
    if path:
        with open(path, newline="", encoding="utf-8") as f:
            return [
                (int(r["pr_number"]), r["verdict"].strip().lower(), r.get("reasoning", ""), now)
                for r in csv.DictReader(f)
            ]
    if fake:
        print("WARNING: generating RANDOM verdicts. For pipeline testing only, "
              "not a real evaluation.")
        return [
            (n, random.choice(["approve", "reject"]), "FAKE verdict for pipeline testing", now)
            for n in pr_numbers
        ]
    return []


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=300, help="number of closed PRs to load")
    ap.add_argument("--verdicts", help="CSV with pr_number,verdict,reasoning")
    ap.add_argument("--fake-verdicts", action="store_true",
                    help="generate random verdicts (testing only)")
    args = ap.parse_args()

    gh = gh_session()

    print(f"Fetching up to {args.limit} closed PRs from {REPO} ...")
    listing = gh_paginate(
        gh, f"{API}/repos/{REPO}/pulls",
        {"state": "closed", "sort": "created", "direction": "desc"},
        max_items=args.limit,
    )

    pr_rows, review_rows, file_rows = [], [], []
    for i, item in enumerate(listing, 1):
        n = item["number"]
        # The list endpoint has no size stats, so fetch each PR's detail.
        d = gh_get(gh, f"{API}/repos/{REPO}/pulls/{n}")
        pr_rows.append((
            n, d["title"], d.get("body"), (d.get("user") or {}).get("login"),
            d["state"], d.get("draft"),
            ts(d["created_at"]), ts(d["updated_at"]), ts(d["closed_at"]), ts(d["merged_at"]),
            d["additions"], d["deletions"], d["changed_files"],
            d["commits"], d["comments"], d["review_comments"],
            d["base"]["ref"], d["head"]["ref"],
        ))

        for rv in gh_paginate(gh, f"{API}/repos/{REPO}/pulls/{n}/reviews"):
            review_rows.append((
                n, rv["id"], (rv.get("user") or {}).get("login"),
                rv["state"], ts(rv.get("submitted_at")),
            ))

        for fl in gh_paginate(gh, f"{API}/repos/{REPO}/pulls/{n}/files"):
            file_rows.append((
                n, fl["filename"], fl["status"],
                fl["additions"], fl["deletions"], fl["changes"],
            ))

        if i % 25 == 0:
            print(f"  fetched {i}/{len(listing)} PRs")

    verdict_rows = load_verdicts(args.verdicts, args.fake_verdicts, [r[0] for r in pr_rows])

    print("Loading into Snowflake ...")
    conn = sf_connect()
    try:
        cur = conn.cursor()
        for ddl in DDL.values():
            cur.execute(ddl)

        insert(cur, "pull_requests",
               ["number", "title", "body", "user_login", "state", "draft",
                "created_at", "updated_at", "closed_at", "merged_at",
                "additions", "deletions", "changed_files",
                "commits", "comments", "review_comments", "base_ref", "head_ref"],
               pr_rows)
        insert(cur, "pr_reviews",
               ["pr_number", "review_id", "reviewer_login", "state", "submitted_at"],
               review_rows)
        insert(cur, "pr_files",
               ["pr_number", "filename", "status", "additions", "deletions", "changes"],
               file_rows)
        insert(cur, "agent_verdicts",
               ["pr_number", "verdict", "reasoning", "created_at"],
               verdict_rows)
    finally:
        conn.close()

    print("Done.")


if __name__ == "__main__":
    main()
