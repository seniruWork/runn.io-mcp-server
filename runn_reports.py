"""
Runn API client and reporting helpers.

Produces per-project, per-person billable hours grouped by calendar month.
Usage (examples):
  export RUNN_API_KEY=LIVE_...
  python3 runn_reports.py --start 2025-01-01 --end 2025-12-31 --output billable.csv
  python3 runn_reports.py --format pdf --output billable.pdf
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
from collections import defaultdict
from typing import Dict, Generator, Iterable, List, Optional

import requests


DEFAULT_BASE_URL = "https://api.runn.io"
DEFAULT_ACCEPT_VERSION = "1.0.0"
DEFAULT_TIMEOUT = 30


class RunnClient:
    """Minimal client for the Runn API v1."""

    def __init__(
        self,
        api_key: str,
        base_url: str = DEFAULT_BASE_URL,
        accept_version: str = DEFAULT_ACCEPT_VERSION,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {api_key}",
                "Accept-Version": accept_version,
            }
        )

    def _paginate(
        self, path: str, params: Optional[Dict[str, object]] = None, limit: int = 200
    ) -> Generator[Dict, None, None]:
        """Stream paginated 'values' arrays."""
        cursor = None
        params = dict(params or {})
        params.setdefault("limit", limit)

        while True:
            page_params = dict(params)
            if cursor:
                page_params["cursor"] = cursor

            payload = self.request("GET", path, params=page_params)

            for item in payload.get("values", []):
                yield item

            cursor = payload.get("nextCursor")
            if not cursor:
                break

    def _normalize_path(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            raise ValueError("Path must be relative, e.g. /projects")
        return path if path.startswith("/") else f"/{path}"

    def request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, object]] = None,
        json_body: Optional[Dict[str, object]] = None,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> object:
        """Make a raw Runn API request and return parsed JSON when possible."""
        method = method.upper()
        path = self._normalize_path(path)

        resp = self.session.request(
            method,
            f"{self.base_url}{path}",
            params=params,
            json=json_body,
            timeout=timeout,
        )
        resp.raise_for_status()

        if resp.status_code == 204:
            return {"status_code": resp.status_code}

        content_type = resp.headers.get("content-type", "")
        if "application/json" in content_type:
            return resp.json()

        return {"status_code": resp.status_code, "text": resp.text}

    def paginate(
        self, path: str, params: Optional[Dict[str, object]] = None, limit: int = 200
    ) -> Generator[Dict, None, None]:
        """Public pagination helper for list endpoints."""
        yield from self._paginate(path, params=params, limit=limit)

    def iter_actuals(self, limit: int = 200) -> Generator[Dict, None, None]:
        return self._paginate("/actuals", limit=limit)

    def iter_people(self, limit: int = 200) -> Generator[Dict, None, None]:
        return self._paginate("/people", limit=limit)

    def iter_projects(self, limit: int = 200) -> Generator[Dict, None, None]:
        return self._paginate("/projects", limit=limit)

    def people_lookup(self) -> Dict[int, str]:
        return {
            person["id"]: f"{person.get('firstName', '').strip()} {person.get('lastName', '').strip()}".strip()
            or person["email"]
            for person in self.iter_people()
        }

    def projects_lookup(self) -> Dict[int, str]:
        return {project["id"]: project.get("name", f"Project {project['id']}") for project in self.iter_projects()}


def month_start(date_str: str) -> dt.date:
    d = dt.date.fromisoformat(date_str)
    return d.replace(day=1)


def build_billable_hours_report(
    client: RunnClient,
    start: Optional[dt.date] = None,
    end: Optional[dt.date] = None,
) -> List[Dict[str, object]]:
    """Aggregate billable hours per project/person/month."""
    people = client.people_lookup()
    projects = client.projects_lookup()

    buckets = defaultdict(float)
    for actual in client.iter_actuals():
        date = dt.date.fromisoformat(actual["date"])
        if start and date < start:
            continue
        if end and date > end:
            continue

        minutes = actual.get("billableMinutes") or 0
        if minutes <= 0:
            continue

        key = (actual["projectId"], actual["personId"], date.replace(day=1))
        buckets[key] += minutes / 60.0

    rows = []
    for (project_id, person_id, month), hours in sorted(buckets.items(), key=lambda x: (x[0][0], x[0][2], x[0][1])):
        rows.append(
            {
                "project_id": project_id,
                "project_name": projects.get(project_id, f"Project {project_id}"),
                "person_id": person_id,
                "person_name": people.get(person_id, f"Person {person_id}"),
                "month": month.isoformat(),
                "billable_hours": round(hours, 2),
            }
        )
    return rows


def write_csv(rows: Iterable[Dict[str, object]], output_path: Optional[str]) -> None:
    rows = list(rows)
    if not rows:
        return

    fieldnames = ["project_id", "project_name", "person_id", "person_name", "month", "billable_hours"]
    out_file = open(output_path, "w", newline="") if output_path else None
    writer = csv.DictWriter(out_file or os.sys.stdout, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    if out_file:
        out_file.close()


def write_pdf(rows: Iterable[Dict[str, object]], output_path: str) -> None:
    """Render tabular report to PDF using ReportLab."""
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.units import inch
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet
    except ImportError as exc:
        raise SystemExit(
            "ReportLab is required for PDF output. Install into your environment: "
            "`python3 -m pip install reportlab`"
        ) from exc

    rows = list(rows)
    if not rows:
        raise SystemExit("No billable hours found; PDF not generated.")

    doc = SimpleDocTemplate(output_path, pagesize=letter, leftMargin=0.5 * inch, rightMargin=0.5 * inch)
    styles = getSampleStyleSheet()
    elements = [
        Paragraph("Billable Hours by Project / Person / Month", styles["Title"]),
        Spacer(1, 0.2 * inch),
    ]

    headers = ["Project", "Person", "Month", "Billable Hours"]
    table_data = [headers]
    for row in rows:
        table_data.append(
            [
                f"{row['project_name']} (#{row['project_id']})",
                f"{row['person_name']} (#{row['person_id']})",
                row["month"],
                f"{row['billable_hours']:.2f}",
            ]
        )

    table = Table(table_data, repeatRows=1)
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
                ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                ("ALIGN", (-1, 1), (-1, -1), "RIGHT"),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.lightgoldenrodyellow]),
            ]
        )
    )

    elements.append(table)
    doc.build(elements)


def parse_date(value: Optional[str]) -> Optional[dt.date]:
    return dt.date.fromisoformat(value) if value else None


def main() -> None:
    parser = argparse.ArgumentParser(description="Export billable hours grouped by project/person/month from Runn.")
    parser.add_argument("--api-key", default=os.getenv("RUNN_API_KEY"), help="Runn API key (or set RUNN_API_KEY).")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Override the Runn API base URL.")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD) inclusive.")
    parser.add_argument("--end", help="End date (YYYY-MM-DD) inclusive.")
    parser.add_argument(
        "--format",
        choices=["csv", "pdf"],
        default="csv",
        help="Output format. CSV writes to stdout by default; PDF requires --output.",
    )
    parser.add_argument("--output", help="Output path (defaults to stdout for CSV).")
    args = parser.parse_args()

    if not args.api_key:
        parser.error("Provide --api-key or set RUNN_API_KEY.")

    client = RunnClient(api_key=args.api_key, base_url=args.base_url)
    start_date = parse_date(args.start)
    end_date = parse_date(args.end)

    rows = build_billable_hours_report(client, start=start_date, end=end_date)
    if not rows:
        print("No billable hours found for given filters.")
        return

    if args.format == "csv":
        write_csv(rows, args.output)
    else:
        if not args.output:
            parser.error("PDF output requires --output path.")
        write_pdf(rows, args.output)


if __name__ == "__main__":
    main()
