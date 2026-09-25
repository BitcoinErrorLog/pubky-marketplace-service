#!/usr/bin/env python3
"""Verifies a Railway target before any command reaches it.

Reads `railway status --project <id> --environment <id> --json` on stdin and
exits non-zero unless the project id matches, the environment id belongs to
that project, and the named service (the name `railway connect` requires)
has exactly the expected service id.

usage: railway_target.py <project-id> <environment-id> <service-name> <service-id>
"""

import json
import sys


def verify(status, project_id, environment_id, service_name, service_id):
    if status.get("id") != project_id:
        return f"project {status.get('id')!r} is not the expected {project_id!r}"
    environments = [
        edge["node"]["id"] for edge in status.get("environments", {}).get("edges", [])
    ]
    if environment_id not in environments:
        return f"environment {environment_id!r} is not in project {project_id!r}"
    matches = [
        edge["node"]["id"]
        for edge in status.get("services", {}).get("edges", [])
        if edge["node"]["name"] == service_name
    ]
    if matches != [service_id]:
        return (
            f"service {service_name!r} resolves to {matches!r}, "
            f"not exactly the expected {service_id!r}"
        )
    return None


def main():
    if len(sys.argv) != 5:
        print(__doc__.strip().splitlines()[-1], file=sys.stderr)
        return 2
    try:
        status = json.load(sys.stdin)
    except json.JSONDecodeError:
        print("railway status output is not JSON", file=sys.stderr)
        return 1
    problem = verify(status, *sys.argv[1:])
    if problem:
        print(f"refusing target: {problem}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
