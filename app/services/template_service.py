"""
Discover automation templates and scaffold new automations from them.

Ported from the dashboard server's `services/templates.ts` so gitops owns the
single write path into `/workspace-repo` and can broadcast `automations`
events inline (no filesystem-watcher round-trip).

The `examples/` directory the editor uses is bind-mounted at
`/workspace/examples`; workspace-local overrides live at
`<workspace_repo>/templates/`. Workspace overrides win on id collision.
"""

import logging
import os
import re
import shutil
import uuid
from dataclasses import dataclass, field
from typing import Optional

from app.utils import (
    GitLockContext,
    call_git_command,
    call_git_command_with_output,
)

logger = logging.getLogger(__name__)

# Same root the editor uses. Mounted read-only into the gitops container by
# bitswan-automation-server's compose generation.
TEMPLATES_ROOT = "/workspace/examples"

# Editor-compatible automation directory naming.
_AUTOMATION_NAME_RE = re.compile(r"^[a-z][a-z0-9-]*$")

# Same regex extraction strategy as the editor and the dashboard server. A real
# TOML parser would be overkill for three fields and would lose the option of
# preserving authoring conventions when we rewrite `automation.toml`.
_NAME_RE = re.compile(r'\bname\s*=\s*"([^"]+)"')
_SHORT_DESC_RE = re.compile(r'\bshort_description\s*=\s*"""([\s\S]*?)"""')
_ICON_RE = re.compile(r'\bicon\s*=\s*"""([\s\S]*?)"""')


@dataclass
class TemplateInfo:
    id: str
    name: str
    short_description: str
    icon_svg: str
    source_dir: str

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "shortDescription": self.short_description,
            "iconSvg": self.icon_svg,
        }


@dataclass
class TemplateGroupInfo:
    id: str
    name: str
    short_description: str
    icon_svg: str
    automations: list[str] = field(default_factory=list)
    source_dir: str = ""

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "shortDescription": self.short_description,
            "iconSvg": self.icon_svg,
            "automations": list(self.automations),
        }


def _read_metadata(path: str) -> Optional[dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
    except OSError:
        return None
    m = _NAME_RE.search(content)
    if not m:
        return None
    sd = _SHORT_DESC_RE.search(content)
    ic = _ICON_RE.search(content)
    return {
        "name": m.group(1).strip(),
        "short_description": (sd.group(1) if sd else "").strip(),
        "icon_svg": (ic.group(1) if ic else "").strip(),
    }


def _read_template(dir_path: str) -> Optional[TemplateInfo]:
    toml_path = os.path.join(dir_path, "template.toml")
    if not os.path.exists(toml_path):
        return None
    meta = _read_metadata(toml_path)
    if not meta:
        return None
    return TemplateInfo(
        id=os.path.basename(dir_path),
        name=meta["name"],
        short_description=meta["short_description"],
        icon_svg=meta["icon_svg"],
        source_dir=dir_path,
    )


def _read_group(dir_path: str) -> Optional[TemplateGroupInfo]:
    toml_path = os.path.join(dir_path, "group.toml")
    if not os.path.exists(toml_path):
        return None
    meta = _read_metadata(toml_path)
    if not meta:
        return None
    automations: list[str] = []
    try:
        for name in os.listdir(dir_path):
            sub = os.path.join(dir_path, name)
            if not os.path.isdir(sub):
                continue
            if os.path.exists(os.path.join(sub, "automation.toml")) or os.path.exists(
                os.path.join(sub, "pipelines.conf")
            ):
                automations.append(name)
    except OSError:
        pass
    if not automations:
        return None
    return TemplateGroupInfo(
        id=os.path.basename(dir_path),
        name=meta["name"],
        short_description=meta["short_description"],
        icon_svg=meta["icon_svg"],
        automations=sorted(automations),
        source_dir=dir_path,
    )


def _discover_in_root(
    root_dir: str,
) -> tuple[list[TemplateInfo], list[TemplateGroupInfo]]:
    templates: list[TemplateInfo] = []
    groups: list[TemplateGroupInfo] = []
    if not os.path.isdir(root_dir):
        return templates, groups
    try:
        names = os.listdir(root_dir)
    except OSError:
        return templates, groups
    for name in names:
        full = os.path.join(root_dir, name)
        if not os.path.isdir(full):
            continue
        t = _read_template(full)
        if t:
            templates.append(t)
            continue
        g = _read_group(full)
        if g:
            groups.append(g)
    return templates, groups


def discover_templates(workspace_root: str) -> dict:
    """Return the merged `{templates, groups}` listing.

    Workspace overrides at `<workspace_root>/templates/` win against built-ins
    of the same id, matching the editor's behaviour.
    """
    builtin_t, builtin_g = _discover_in_root(TEMPLATES_ROOT)
    override_t, override_g = _discover_in_root(
        os.path.join(workspace_root, "templates")
    )

    by_id_t: dict[str, TemplateInfo] = {t.id: t for t in builtin_t}
    by_id_g: dict[str, TemplateGroupInfo] = {g.id: g for g in builtin_g}
    for t in override_t:
        by_id_t[t.id] = t
    for g in override_g:
        by_id_g[g.id] = g

    return {
        "templates": [
            t.to_dict() for t in sorted(by_id_t.values(), key=lambda x: x.name.lower())
        ],
        "groups": [
            g.to_dict() for g in sorted(by_id_g.values(), key=lambda x: x.name.lower())
        ],
    }


def sanitize_automation_name(name: str) -> str:
    """lowercase, non-`[a-z0-9-]` collapsed to `-`, trim leading/trailing dashes."""
    s = name.lower()
    s = re.sub(r"[^a-z0-9-]+", "-", s)
    s = re.sub(r"^-+|-+$", "", s)
    return s


def _copy_dir_recursive(src: str, dest: str, skip: Optional[set[str]] = None) -> None:
    """Symlink-preserving recursive copy. Symlinks are re-created with their
    original (verbatim) target, since templates ship links pointing at
    in-container paths (e.g. `bitswan_lib` → `/workspace/bitswan_lib`)."""
    if not os.path.isdir(src):
        raise ValueError(f"Template source is not a directory: {src}")
    os.makedirs(dest, exist_ok=True)
    for name in os.listdir(src):
        if skip and name in skip:
            continue
        s = os.path.join(src, name)
        d = os.path.join(dest, name)
        if os.path.islink(s):
            target = os.readlink(s)
            os.symlink(target, d)
        elif os.path.isdir(s):
            _copy_dir_recursive(s, d)
        elif os.path.isfile(s):
            shutil.copyfile(s, d)


def _ensure_automation_id(target_dir: str) -> None:
    """Add `[deployment] id = "<uuid>"` to `automation.toml` if missing.

    Targeted string edit (not a TOML round-trip) so authoring conventions
    (comments, blank lines) survive. Mirrors the editor + dashboard logic.
    """
    toml_path = os.path.join(target_dir, "automation.toml")
    new_id = str(uuid.uuid4())
    if not os.path.exists(toml_path):
        with open(toml_path, "w", encoding="utf-8") as f:
            f.write(f'[deployment]\nid = "{new_id}"\n')
        return
    with open(toml_path, "r", encoding="utf-8") as f:
        content = f.read()
    # If `[deployment]` already has an `id`, leave alone.
    if re.search(r"\[deployment\][\s\S]*?(^\s*id\s*=\s*)", content, re.MULTILINE):
        return

    newline = "\r\n" if "\r\n" in content else "\n"
    lines = content.split(newline)
    deployment_idx = -1
    for i, raw in enumerate(lines):
        trimmed = raw.strip()
        if trimmed.startswith("[") and trimmed.endswith("]"):
            if trimmed.lower() == "[deployment]":
                deployment_idx = i
                break
    if deployment_idx >= 0:
        lines.insert(deployment_idx + 1, f'id = "{new_id}"')
    else:
        if lines and lines[-1] != "":
            lines.append("")
        lines.append("[deployment]")
        lines.append(f'id = "{new_id}"')
    with open(toml_path, "w", encoding="utf-8") as f:
        f.write(newline.join(lines))


def _bp_destination(
    workspace_root: str, bp: str, worktree: Optional[str]
) -> tuple[str, str]:
    """Returns `(bp_full_path, bp_relative_path)`."""
    rel = os.path.join("worktrees", worktree, bp) if worktree else bp
    return os.path.join(workspace_root, rel), rel


async def _commit(
    cwd: str, message: str, author_email: str = "gitops@bitswan.local"
) -> Optional[str]:
    """Stage everything under `cwd` and commit. Returns commit hash or None
    if there was nothing to commit. Raises on hard failures."""
    async with GitLockContext(timeout=15.0):
        ok = await call_git_command("git", "add", "-A", cwd=cwd)
        if not ok:
            raise RuntimeError("git add -A failed")

        author = f"{author_email} <{author_email}>"
        stdout, stderr, rc = await call_git_command_with_output(
            "git", "commit", "-m", message, "--author", author, cwd=cwd
        )
        if rc != 0:
            if "nothing to commit" in stdout or "nothing to commit" in stderr:
                return None
            raise RuntimeError(f"git commit failed: {stderr.strip()}")

    hash_stdout, _, hash_rc = await call_git_command_with_output(
        "git", "rev-parse", "HEAD", cwd=cwd
    )
    return hash_stdout.strip() if hash_rc == 0 else None


async def create_automation_from_template(
    *,
    workspace_root: str,
    bp: str,
    template_id: Optional[str] = None,
    group_id: Optional[str] = None,
    name: Optional[str] = None,
    worktree: Optional[str] = None,
) -> dict:
    """Copy a template (or every automation in a group) into the BP directory
    and commit. Returns `{ "created": [ { "name", "relative_path" } ] }`.

    Validation of `bp`/`worktree` shape is expected to happen at the route layer.
    """
    if not bp:
        raise ValueError("bp is required")
    if not template_id and not group_id:
        raise ValueError("template_id or group_id is required")
    if template_id and group_id:
        raise ValueError("template_id and group_id are mutually exclusive")

    bp_full, bp_rel = _bp_destination(workspace_root, bp, worktree)
    os.makedirs(bp_full, exist_ok=True)

    created: list[dict] = []

    if template_id:
        tpl: Optional[TemplateInfo] = None
        # Re-resolve against the in-memory discovery to fetch source_dir.
        for t in _all_templates(workspace_root):
            if t.id == template_id:
                tpl = t
                break
        if tpl is None:
            raise ValueError(f"Unknown template: {template_id}")

        sanitized = sanitize_automation_name(name or "")
        if not sanitized or not _AUTOMATION_NAME_RE.match(sanitized):
            raise ValueError("Invalid automation name")

        dest = os.path.join(bp_full, sanitized)
        if os.path.exists(dest):
            raise FileExistsError(
                f'A folder named "{sanitized}" already exists in this business process.'
            )

        # Skip `template.toml` itself — it's metadata for the gallery, not part
        # of the resulting automation.
        _copy_dir_recursive(tpl.source_dir, dest, skip={"template.toml"})
        _ensure_automation_id(dest)
        created.append(
            {"name": sanitized, "relative_path": os.path.join(bp_rel, sanitized)}
        )

        message = f"Add automation {sanitized}"
    else:
        # group_id branch
        group: Optional[TemplateGroupInfo] = None
        for g in _all_groups(workspace_root):
            if g.id == group_id:
                group = g
                break
        if group is None:
            raise ValueError(f"Unknown group: {group_id}")

        # Validate up front — refuse the whole operation rather than half-creating.
        for automation in group.automations:
            if os.path.exists(os.path.join(bp_full, automation)):
                raise FileExistsError(
                    f'A folder named "{automation}" already exists in this business process.'
                )

        for automation in group.automations:
            src = os.path.join(group.source_dir, automation)
            dest = os.path.join(bp_full, automation)
            _copy_dir_recursive(src, dest)
            _ensure_automation_id(dest)
            created.append(
                {"name": automation, "relative_path": os.path.join(bp_rel, automation)}
            )

        names = ", ".join(c["name"] for c in created)
        message = f"Add automations: {names}"

    # Commit in the workspace root (or worktree). `git` finds the right worktree
    # from the cwd, so commits land on the right branch automatically.
    commit_cwd = (
        os.path.join(workspace_root, "worktrees", worktree)
        if worktree
        else workspace_root
    )
    try:
        await _commit(commit_cwd, message)
    except Exception as e:  # noqa: BLE001 — surface commit failures but don't undo the copy
        logger.warning("template commit failed: %s", e)

    return {"created": created}


def _all_templates(workspace_root: str) -> list[TemplateInfo]:
    """Helper to re-fetch the full TemplateInfo (incl. source_dir) — `discover_templates`
    returns dicts trimmed for HTTP response."""
    builtin_t, _ = _discover_in_root(TEMPLATES_ROOT)
    override_t, _ = _discover_in_root(os.path.join(workspace_root, "templates"))
    merged: dict[str, TemplateInfo] = {t.id: t for t in builtin_t}
    for t in override_t:
        merged[t.id] = t
    return list(merged.values())


def _all_groups(workspace_root: str) -> list[TemplateGroupInfo]:
    _, builtin_g = _discover_in_root(TEMPLATES_ROOT)
    _, override_g = _discover_in_root(os.path.join(workspace_root, "templates"))
    merged: dict[str, TemplateGroupInfo] = {g.id: g for g in builtin_g}
    for g in override_g:
        merged[g.id] = g
    return list(merged.values())
