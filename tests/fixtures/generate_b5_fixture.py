"""Regenerate with: python tests/fixtures/generate_b5_fixture.py <event-handler-repo>.

Execute the producer's pure builder functions, excluding service-startup imports.
This deliberately does not copy or reimplement the message-building logic.
"""
import ast
import json
from pathlib import Path
import sys
from types import SimpleNamespace
from typing import Any, Sequence


def main():
    source = Path(sys.argv[1]) / "src/bl/automations/publish_matches.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    functions = {"_validate_required_alert_fields", "_alert_snapshot", "build_messages"}
    nodes = [
        node for node in tree.body
        if (isinstance(node, ast.FunctionDef) and node.name in functions)
        or (isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "_REQUIRED_ALERT_FIELDS"
            for target in node.targets
        ))
    ]
    namespace = dict(json=json, Any=Any, Sequence=Sequence,
                     AutomationMatch=SimpleNamespace, MatchedContractError=ValueError)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(source), "exec"), namespace)
    alert = {
        "id": "evt-b5-789", "fingerprint": "payments-api-east",
        "time_created": "2026-10-25T10:00:00Z", "application": "payments",
        "component": "api", "site": "east", "status": "firing",
        "severity": "critical", "environment": "prod", "operator": "payments",
    }
    match = SimpleNamespace(
        automation_id="3f2b8c1e-4d6a-4b2f-9e77-0a1b2c3d4e5f",
        cooldown=SimpleNamespace(fields=("site",), seconds=300, scheme_ver=1),
    )
    payload = namespace["build_messages"]("keep", alert, [match])[0]
    Path(__file__).with_name("b5_matched_message.json").write_text(
        json.dumps(payload, indent=2) + "\n", encoding="utf-8", newline="\n"
    )


if __name__ == "__main__":
    main()
