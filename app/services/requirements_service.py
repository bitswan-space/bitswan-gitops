import os
import re
from datetime import datetime, timezone
from typing import Optional


REQUIREMENTS_FILENAME = "testable-requirements.toml"


def _parse_requirements_toml(content: str) -> list:
    """Parse a testable-requirements.toml file into a list of requirement dicts."""
    reqs = []
    current = None

    for line in content.split("\n"):
        line = line.strip()
        if line == "[[requirement]]":
            if current and current.get("id"):
                reqs.append(current)
            current = {}
            continue
        if current is None:
            continue

        match = re.match(r'^(\w+)\s*=\s*"((?:[^"\\]|\\.)*)"$', line)
        if match:
            key = match.group(1)
            value = match.group(2).replace('\\"', '"').replace("\\\\", "\\")
            current[key] = value

    if current and current.get("id"):
        reqs.append(current)

    # Ensure all fields are present
    for req in reqs:
        req.setdefault("description", "")
        req.setdefault("status", "pending")
        req.setdefault("parent", "")

    return reqs


def _serialize_requirements_toml(reqs: list) -> str:
    """Serialize a list of requirement dicts to testable-requirements.toml format."""
    def esc(s: str) -> str:
        return s.replace("\\", "\\\\").replace('"', '\\"')

    blocks = []
    for r in reqs:
        blocks.append(
            f'[[requirement]]\n'
            f'id = "{esc(r.get("id", ""))}"\n'
            f'parent = "{esc(r.get("parent", ""))}"\n'
            f'description = "{esc(r.get("description", ""))}"\n'
            f'status = "{esc(r.get("status", "pending"))}"'
        )
    return "\n\n".join(blocks) + "\n" if blocks else ""


class RequirementsService:
    def __init__(self, workspace_dir: str):
        self.workspace_dir = workspace_dir

    def _get_file_path(self, business_process: str) -> str:
        return os.path.join(self.workspace_dir, business_process, REQUIREMENTS_FILENAME)

    def _validate_business_process(self, business_process: str) -> None:
        bp_dir = os.path.join(self.workspace_dir, business_process)
        process_toml = os.path.join(bp_dir, "process.toml")
        if not os.path.exists(process_toml):
            raise ValueError(f"Business process '{business_process}' not found")

    def get_requirements(self, business_process: str) -> list:
        self._validate_business_process(business_process)
        path = self._get_file_path(business_process)
        if not os.path.exists(path):
            return []
        with open(path, "r") as f:
            return _parse_requirements_toml(f.read())

    def save_requirements(self, business_process: str, requirements: list) -> list:
        self._validate_business_process(business_process)
        path = self._get_file_path(business_process)
        with open(path, "w") as f:
            f.write(_serialize_requirements_toml(requirements))
        return requirements

    def add_requirement(self, business_process: str, text: str, parent: str = "") -> dict:
        reqs = self.get_requirements(business_process)
        max_num = 0
        for r in reqs:
            m = re.search(r"\d+$", r.get("id", ""))
            if m:
                max_num = max(max_num, int(m.group()))
        req = {
            "id": f"REQ-{max_num + 1:03d}",
            "description": text,
            "status": "pending",
            "parent": parent,
        }
        reqs.append(req)
        self.save_requirements(business_process, reqs)
        return req

    def update_requirement(
        self,
        business_process: str,
        req_id: str,
        status: Optional[str] = None,
        text: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> dict:
        reqs = self.get_requirements(business_process)
        for req in reqs:
            if req["id"] == req_id:
                if status is not None:
                    req["status"] = status
                if text is not None:
                    req["description"] = text
                if notes is not None:
                    req["notes"] = notes
                self.save_requirements(business_process, reqs)
                return req
        raise ValueError(f"Requirement {req_id} not found")

    def remove_requirement(self, business_process: str, req_id: str) -> bool:
        reqs = self.get_requirements(business_process)
        reqs = [r for r in reqs if r["id"] != req_id]
        self.save_requirements(business_process, reqs)
        return True

    def get_next_requirement(self, business_process: str) -> Optional[dict]:
        """Return the first non-passing requirement in DFS order, or None."""
        reqs = self.get_requirements(business_process)
        # Build tree
        by_id = {r["id"]: r for r in reqs}
        children: dict[str, list] = {"": []}
        for r in reqs:
            parent = r.get("parent", "")
            children.setdefault(parent, []).append(r)

        # DFS traversal
        def dfs(parent_id: str):
            for r in children.get(parent_id, []):
                if r.get("status") != "pass":
                    return r
                child = dfs(r["id"])
                if child:
                    return child
            return None

        return dfs("")
