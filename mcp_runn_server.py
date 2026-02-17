"""
MCP server exposing Runn billable hours, projects, and people.

Transport: streamable HTTP (default) on port 8000.

Usage:
  # install deps (in existing .venv):
  pip install "mcp[cli]"
  # run server
  RUNN_API_KEY=LIVE_... python3 mcp_runn_server.py

Tools:
  - list_projects() -> list of {id, name}
  - list_people(full=False, params=None, paginate=True, limit=200) -> list of {id, name, email} by default
  - billable_hours(start=None, end=None, project_id=None, person_id=None)
      returns aggregated billable hours grouped by project/person/month
  - list_clients(params=None, paginate=True, limit=200)
  - list_assignments(params=None, paginate=True, limit=200)
  - list_assignments_by_person(person_id, start=None, end=None, active_only=False)
  - list_assignments_by_project(project_id, start=None, end=None, active_only=False)
  - list_assignments_by_role(role_id, start=None, end=None, active_only=False)
  - list_assignments_by_team(team_id, start=None, end=None, active_only=False, include_archived=False)
  - list_actuals(params=None, paginate=True, limit=200)
  - list_actuals_by_date_range(start, end, person_id=None, project_id=None)
  - list_actuals_by_person(person_id, start=None, end=None)
  - list_actuals_by_project(project_id, start=None, end=None)
  - list_actuals_by_role(role_id, start=None, end=None)
  - list_actuals_by_team(team_id, start=None, end=None, include_archived=False)
  - list_roles(params=None, paginate=True, limit=200)
  - list_roles_by_person(person_id)
  - list_skills(params=None, paginate=True, limit=200)
  - list_skills_by_person(person_id)
  - list_teams(params=None, paginate=True, limit=200)
  - list_people_by_team(team_id, include_archived=False)
  - list_people_by_skill(skill_id, min_level=None, include_archived=False)
  - list_people_by_tag(tag_id=None, tag_name=None, include_archived=False)
  - list_people_by_manager(manager_id, include_archived=False)
  - list_rate_cards(params=None, paginate=True, limit=200)
  - list_rate_cards_by_project(project_id)
  - runn_request(method, path, params=None, json_body=None, paginate=False, limit=200)
      calls any Runn API endpoint (optionally paginated for list endpoints)

Notes:
  - Requires env RUNN_API_KEY.
  - Uses existing RunnClient from runn_reports.py.
"""

from __future__ import annotations

import datetime as dt
import os
from typing import Dict, List, Optional

from mcp.server.fastmcp import FastMCP

from runn_reports import RunnClient, build_billable_hours_report, parse_date


def get_client(api_key: Optional[str] = None) -> RunnClient:
    key = api_key or os.getenv("RUNN_API_KEY")
    # Try to read from .vscode/mcp.json if not set
    if not key:
        import json
        import pathlib
        mcp_json_path = pathlib.Path(__file__).parent.parent / ".vscode" / "mcp.json"
        if mcp_json_path.exists():
            try:
                with open(mcp_json_path, "r", encoding="utf-8") as f:
                    mcp_config = json.load(f)
                # Traverse to Authorization header if present
                key = (
                    mcp_config.get("servers", {})
                    .get("runn-local", {})
                    .get("headers", {})
                    .get("Authorization")
                )
            except Exception:
                pass
    if not key:
        raise RuntimeError("RUNN_API_KEY not set and not found in mcp.json headers")
    return RunnClient(api_key=key)


def _list_endpoint(
    client: RunnClient,
    path: str,
    params: Optional[Dict[str, object]] = None,
    paginate: bool = True,
    limit: int = 200,
) -> object:
    if paginate:
        return list(client.paginate(path, params=params, limit=limit))
    resp = client.request("GET", path, params=params)
    if isinstance(resp, dict) and "values" in resp:
        return resp.get("values", [])
    return resp


def _to_date(value: Optional[str]) -> Optional[dt.date]:
    return parse_date(value) if value else None


def _in_range(date_value: Optional[dt.date], start: Optional[dt.date], end: Optional[dt.date]) -> bool:
    if date_value is None:
        return False
    if start and date_value < start:
        return False
    if end and date_value > end:
        return False
    return True


def _range_overlaps(
    start_a: Optional[dt.date],
    end_a: Optional[dt.date],
    start_b: Optional[dt.date],
    end_b: Optional[dt.date],
) -> bool:
    if start_a is None and end_a is None:
        return True
    if end_a is None:
        end_a = start_a
    if start_a is None:
        start_a = end_a
    if start_b and end_a and end_a < start_b:
        return False
    if end_b and start_a and start_a > end_b:
        return False
    return True


def _people_ids_for_team(
    client: RunnClient,
    team_id: int,
    include_archived: bool = False,
) -> List[int]:
    people = _list_endpoint(client, "/people", paginate=True)
    ids = []
    for p in people:
        if p.get("teamId") != team_id:
            continue
        if not include_archived and p.get("isArchived"):
            continue
        ids.append(p["id"])
    return ids


def _person_has_tag(person: Dict[str, object], tag_id: Optional[int], tag_name: Optional[str]) -> bool:
    tags = person.get("tags") or []
    tag_name_norm = tag_name.lower() if isinstance(tag_name, str) else None
    for tag in tags:
        if tag_id is not None and tag.get("id") == tag_id:
            return True
        if tag_name_norm and str(tag.get("name", "")).lower() == tag_name_norm:
            return True
    return False


def _person_has_skill(person: Dict[str, object], skill_id: int, min_level: Optional[int]) -> bool:
    skills = person.get("skills") or []
    for skill in skills:
        if skill.get("id") != skill_id:
            continue
        level = skill.get("level")
        if min_level is None:
            return True
        if level is None:
            continue
        try:
            if int(level) >= int(min_level):
                return True
        except (TypeError, ValueError):
            continue
    return False


mcp = FastMCP("Runn MCP Server", json_response=True)


@mcp.tool()
def list_projects(api_key: Optional[str] = None) -> List[Dict[str, object]]:
    """List all projects (id, name)."""
    client = get_client(api_key)
    return [{"id": pid, "name": name} for pid, name in sorted(client.projects_lookup().items())]


@mcp.tool()
def list_people(
    full: bool = False,
    params: Optional[Dict[str, object]] = None,
    paginate: bool = True,
    limit: int = 200,
    api_key: Optional[str] = None,
) -> object:
    """List people. Default returns {id, name, email}; set full=True for raw API objects."""
    client = get_client(api_key)
    people_raw = _list_endpoint(client, "/people", params=params, paginate=paginate, limit=limit)
    if full:
        return people_raw

    return [
        {"id": p["id"], "name": f"{p.get('firstName', '')} {p.get('lastName', '')}".strip(), "email": p.get("email")}
        for p in people_raw
    ]


@mcp.tool()
def billable_hours(
    start: Optional[str] = None,
    end: Optional[str] = None,
    project_id: Optional[int] = None,
    person_id: Optional[int] = None,
    api_key: Optional[str] = None,
) -> List[Dict[str, object]]:
    """Aggregate billable hours grouped by project/person/month."""
    client = get_client(api_key)
    rows = build_billable_hours_report(client, start=parse_date(start), end=parse_date(end))

    def matches(row: Dict[str, object]) -> bool:
        if project_id and row["project_id"] != project_id:
            return False
        if person_id and row["person_id"] != person_id:
            return False
        return True

    return [row for row in rows if matches(row)]


@mcp.tool()
def list_clients(
    params: Optional[Dict[str, object]] = None,
    paginate: bool = True,
    limit: int = 200,
    api_key: Optional[str] = None,
) -> object:
    """List clients (raw API objects)."""
    client = get_client(api_key)
    return _list_endpoint(client, "/clients", params=params, paginate=paginate, limit=limit)


@mcp.tool()
def list_assignments(
    params: Optional[Dict[str, object]] = None,
    paginate: bool = True,
    limit: int = 200,
    api_key: Optional[str] = None,
) -> object:
    """List assignments (raw API objects)."""
    client = get_client(api_key)
    return _list_endpoint(client, "/assignments", params=params, paginate=paginate, limit=limit)


@mcp.tool()
def list_assignments_by_person(
    person_id: int,
    start: Optional[str] = None,
    end: Optional[str] = None,
    active_only: bool = False,
    api_key: Optional[str] = None,
) -> List[Dict[str, object]]:
    """List assignments for a person, optionally filtered by date range."""
    client = get_client(api_key)
    assignments = _list_endpoint(client, "/assignments", paginate=True)
    start_date = _to_date(start)
    end_date = _to_date(end)

    results = []
    for a in assignments:
        if a.get("personId") != person_id:
            continue
        if active_only and not a.get("isActive", False):
            continue
        if start_date or end_date:
            a_start = _to_date(a.get("startDate"))
            a_end = _to_date(a.get("endDate"))
            if not _range_overlaps(a_start, a_end, start_date, end_date):
                continue
        results.append(a)
    return results


@mcp.tool()
def list_assignments_by_project(
    project_id: int,
    start: Optional[str] = None,
    end: Optional[str] = None,
    active_only: bool = False,
    api_key: Optional[str] = None,
) -> List[Dict[str, object]]:
    """List assignments for a project, optionally filtered by date range."""
    client = get_client(api_key)
    assignments = _list_endpoint(client, "/assignments", paginate=True)
    start_date = _to_date(start)
    end_date = _to_date(end)

    results = []
    for a in assignments:
        if a.get("projectId") != project_id:
            continue
        if active_only and not a.get("isActive", False):
            continue
        if start_date or end_date:
            a_start = _to_date(a.get("startDate"))
            a_end = _to_date(a.get("endDate"))
            if not _range_overlaps(a_start, a_end, start_date, end_date):
                continue
        results.append(a)
    return results


@mcp.tool()
def list_assignments_by_role(
    role_id: int,
    start: Optional[str] = None,
    end: Optional[str] = None,
    active_only: bool = False,
    api_key: Optional[str] = None,
) -> List[Dict[str, object]]:
    """List assignments for a role, optionally filtered by date range."""
    client = get_client(api_key)
    assignments = _list_endpoint(client, "/assignments", paginate=True)
    start_date = _to_date(start)
    end_date = _to_date(end)

    results = []
    for a in assignments:
        if a.get("roleId") != role_id:
            continue
        if active_only and not a.get("isActive", False):
            continue
        if start_date or end_date:
            a_start = _to_date(a.get("startDate"))
            a_end = _to_date(a.get("endDate"))
            if not _range_overlaps(a_start, a_end, start_date, end_date):
                continue
        results.append(a)
    return results


@mcp.tool()
def list_assignments_by_team(
    team_id: int,
    start: Optional[str] = None,
    end: Optional[str] = None,
    active_only: bool = False,
    include_archived: bool = False,
    api_key: Optional[str] = None,
) -> List[Dict[str, object]]:
    """List assignments for all people in a team, optionally filtered by date range."""
    client = get_client(api_key)
    person_ids = set(_people_ids_for_team(client, team_id, include_archived=include_archived))
    assignments = _list_endpoint(client, "/assignments", paginate=True)
    start_date = _to_date(start)
    end_date = _to_date(end)

    results = []
    for a in assignments:
        if a.get("personId") not in person_ids:
            continue
        if active_only and not a.get("isActive", False):
            continue
        if start_date or end_date:
            a_start = _to_date(a.get("startDate"))
            a_end = _to_date(a.get("endDate"))
            if not _range_overlaps(a_start, a_end, start_date, end_date):
                continue
        results.append(a)
    return results


@mcp.tool()
def list_actuals(
    params: Optional[Dict[str, object]] = None,
    paginate: bool = True,
    limit: int = 200,
    api_key: Optional[str] = None,
) -> object:
    """List actuals (raw API objects)."""
    client = get_client(api_key)
    return _list_endpoint(client, "/actuals", params=params, paginate=paginate, limit=limit)


@mcp.tool()
def list_actuals_by_date_range(
    start: str,
    end: str,
    person_id: Optional[int] = None,
    project_id: Optional[int] = None,
    api_key: Optional[str] = None,
) -> List[Dict[str, object]]:
    """List actuals within a date range, optionally filtered by person/project."""
    client = get_client(api_key)
    actuals = _list_endpoint(client, "/actuals", paginate=True)
    start_date = _to_date(start)
    end_date = _to_date(end)

    results = []
    for a in actuals:
        if person_id is not None and a.get("personId") != person_id:
            continue
        if project_id is not None and a.get("projectId") != project_id:
            continue
        if not _in_range(_to_date(a.get("date")), start_date, end_date):
            continue
        results.append(a)
    return results


@mcp.tool()
def list_actuals_by_person(
    person_id: int,
    start: Optional[str] = None,
    end: Optional[str] = None,
    api_key: Optional[str] = None,
) -> List[Dict[str, object]]:
    """List actuals for a person, optionally filtered by date range."""
    client = get_client(api_key)
    actuals = _list_endpoint(client, "/actuals", paginate=True)
    start_date = _to_date(start)
    end_date = _to_date(end)

    results = []
    for a in actuals:
        if a.get("personId") != person_id:
            continue
        if start_date or end_date:
            if not _in_range(_to_date(a.get("date")), start_date, end_date):
                continue
        results.append(a)
    return results


@mcp.tool()
def list_actuals_by_project(
    project_id: int,
    start: Optional[str] = None,
    end: Optional[str] = None,
    api_key: Optional[str] = None,
) -> List[Dict[str, object]]:
    """List actuals for a project, optionally filtered by date range."""
    client = get_client(api_key)
    actuals = _list_endpoint(client, "/actuals", paginate=True)
    start_date = _to_date(start)
    end_date = _to_date(end)

    results = []
    for a in actuals:
        if a.get("projectId") != project_id:
            continue
        if start_date or end_date:
            if not _in_range(_to_date(a.get("date")), start_date, end_date):
                continue
        results.append(a)
    return results


@mcp.tool()
def list_actuals_by_role(
    role_id: int,
    start: Optional[str] = None,
    end: Optional[str] = None,
    api_key: Optional[str] = None,
) -> List[Dict[str, object]]:
    """List actuals for a role, optionally filtered by date range."""
    client = get_client(api_key)
    actuals = _list_endpoint(client, "/actuals", paginate=True)
    start_date = _to_date(start)
    end_date = _to_date(end)

    results = []
    for a in actuals:
        if a.get("roleId") != role_id:
            continue
        if start_date or end_date:
            if not _in_range(_to_date(a.get("date")), start_date, end_date):
                continue
        results.append(a)
    return results


@mcp.tool()
def list_actuals_by_team(
    team_id: int,
    start: Optional[str] = None,
    end: Optional[str] = None,
    include_archived: bool = False,
    api_key: Optional[str] = None,
) -> List[Dict[str, object]]:
    """List actuals for all people in a team, optionally filtered by date range."""
    client = get_client(api_key)
    person_ids = set(_people_ids_for_team(client, team_id, include_archived=include_archived))
    actuals = _list_endpoint(client, "/actuals", paginate=True)
    start_date = _to_date(start)
    end_date = _to_date(end)

    results = []
    for a in actuals:
        if a.get("personId") not in person_ids:
            continue
        if start_date or end_date:
            if not _in_range(_to_date(a.get("date")), start_date, end_date):
                continue
        results.append(a)
    return results


@mcp.tool()
def list_roles(
    params: Optional[Dict[str, object]] = None,
    paginate: bool = True,
    limit: int = 200,
    api_key: Optional[str] = None,
) -> object:
    """List roles (raw API objects)."""
    client = get_client(api_key)
    return _list_endpoint(client, "/roles", params=params, paginate=paginate, limit=limit)


@mcp.tool()
def list_roles_by_person(person_id: int, api_key: Optional[str] = None) -> List[Dict[str, object]]:
    """List roles that include the given person_id."""
    client = get_client(api_key)
    roles = _list_endpoint(client, "/roles", paginate=True)
    return [r for r in roles if person_id in (r.get("personIds") or [])]


@mcp.tool()
def list_skills(
    params: Optional[Dict[str, object]] = None,
    paginate: bool = True,
    limit: int = 200,
    api_key: Optional[str] = None,
) -> object:
    """List skills (raw API objects)."""
    client = get_client(api_key)
    return _list_endpoint(client, "/skills", params=params, paginate=paginate, limit=limit)


@mcp.tool()
def list_skills_by_person(person_id: int, api_key: Optional[str] = None) -> List[Dict[str, object]]:
    """List skills for a person with level and name (if available)."""
    client = get_client(api_key)
    people = _list_endpoint(client, "/people", paginate=True)
    person = next((p for p in people if p.get("id") == person_id), None)
    if not person:
        raise ValueError(f"Person {person_id} not found.")

    skill_entries = person.get("skills") or []
    skill_ids = {s.get("id") for s in skill_entries if s.get("id") is not None}
    skills = _list_endpoint(client, "/skills", paginate=True)
    skill_name_by_id = {s.get("id"): s.get("name") for s in skills}

    results = []
    for s in skill_entries:
        sid = s.get("id")
        if sid not in skill_ids:
            continue
        results.append({"id": sid, "name": skill_name_by_id.get(sid), "level": s.get("level")})
    return results


@mcp.tool()
def list_teams(
    params: Optional[Dict[str, object]] = None,
    paginate: bool = True,
    limit: int = 200,
    api_key: Optional[str] = None,
) -> object:
    """List teams (raw API objects)."""
    client = get_client(api_key)
    return _list_endpoint(client, "/teams", params=params, paginate=paginate, limit=limit)


@mcp.tool()
def list_people_by_team(
    team_id: int,
    include_archived: bool = False,
    api_key: Optional[str] = None,
) -> List[Dict[str, object]]:
    """List people in a team."""
    client = get_client(api_key)
    people = _list_endpoint(client, "/people", paginate=True)
    results = []
    for p in people:
        if p.get("teamId") != team_id:
            continue
        if not include_archived and p.get("isArchived"):
            continue
        results.append(p)
    return results


@mcp.tool()
def list_people_by_skill(
    skill_id: int,
    min_level: Optional[int] = None,
    include_archived: bool = False,
    api_key: Optional[str] = None,
) -> List[Dict[str, object]]:
    """List people who have a specific skill (optionally at/above min_level)."""
    client = get_client(api_key)
    people = _list_endpoint(client, "/people", paginate=True)
    results = []
    for p in people:
        if not include_archived and p.get("isArchived"):
            continue
        if _person_has_skill(p, skill_id=skill_id, min_level=min_level):
            results.append(p)
    return results


@mcp.tool()
def list_people_by_tag(
    tag_id: Optional[int] = None,
    tag_name: Optional[str] = None,
    include_archived: bool = False,
    api_key: Optional[str] = None,
) -> List[Dict[str, object]]:
    """List people with a given tag (by id or name)."""
    if tag_id is None and tag_name is None:
        raise ValueError("Provide tag_id or tag_name.")
    client = get_client(api_key)
    people = _list_endpoint(client, "/people", paginate=True)
    results = []
    for p in people:
        if not include_archived and p.get("isArchived"):
            continue
        if _person_has_tag(p, tag_id=tag_id, tag_name=tag_name):
            results.append(p)
    return results


@mcp.tool()
def list_people_by_manager(
    manager_id: int,
    include_archived: bool = False,
    api_key: Optional[str] = None,
) -> List[Dict[str, object]]:
    """List people managed by a given manager id."""
    client = get_client(api_key)
    people = _list_endpoint(client, "/people", paginate=True)
    results = []
    for p in people:
        if not include_archived and p.get("isArchived"):
            continue
        managers = p.get("managers") or []
        if any(m.get("id") == manager_id for m in managers):
            results.append(p)
    return results


@mcp.tool()
def list_rate_cards(
    params: Optional[Dict[str, object]] = None,
    paginate: bool = True,
    limit: int = 200,
    api_key: Optional[str] = None,
) -> object:
    """List rate cards (raw API objects)."""
    client = get_client(api_key)
    return _list_endpoint(client, "/rate-cards", params=params, paginate=paginate, limit=limit)


@mcp.tool()
def list_rate_cards_by_project(project_id: int, api_key: Optional[str] = None) -> List[Dict[str, object]]:
    """List rate cards that include the given project_id."""
    client = get_client(api_key)
    rate_cards = _list_endpoint(client, "/rate-cards", paginate=True)
    return [rc for rc in rate_cards if project_id in (rc.get("projectIds") or [])]


@mcp.tool()
def runn_request(
    method: str,
    path: str,
    params: Optional[Dict[str, object]] = None,
    json_body: Optional[Dict[str, object]] = None,
    paginate: bool = False,
    limit: int = 200,
    api_key: Optional[str] = None,
) -> object:
    """Call any Runn API endpoint and return the JSON response."""
    client = get_client(api_key)
    if paginate:
        if method.upper() != "GET":
            raise ValueError("paginate=True only supports GET requests.")
        return list(client.paginate(path, params=params, limit=limit))
    return client.request(method, path, params=params, json_body=json_body)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run Runn MCP server.")
    parser.add_argument(
        "--transport",
        choices=["streamable-http", "stdio"],
        default="streamable-http",
        help="Transport for MCP (default: streamable-http).",
    )
    args = parser.parse_args()

    # FastMCP.run only accepts transport + optional mount_path
    mcp.run(transport=args.transport)
