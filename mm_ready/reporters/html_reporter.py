"""HTML report renderer — standalone HTML with sidebar navigation and To Do list."""

from __future__ import annotations

import html

from mm_ready.models import ScanReport, Severity


_CSS = """
/* ── Reset & base ─────────────────────────────────────────────── */
*, *::before, *::after { box-sizing: border-box; }
body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    margin: 0; padding: 0; color: #333; line-height: 1.6; background: #f9fafb;
}

/* ── Sidebar ──────────────────────────────────────────────────── */
.sidebar {
    position: fixed; top: 0; left: 0; width: 270px; height: 100vh;
    background: #1e293b; color: #e2e8f0; overflow-y: auto;
    padding: 20px 0; z-index: 100;
    display: flex; flex-direction: column;
}
.sidebar-header {
    padding: 0 20px 16px; border-bottom: 1px solid #334155;
    margin-bottom: 8px; font-size: 0.85em; color: #94a3b8;
}
.sidebar-header strong { color: #f1f5f9; font-size: 1.15em; }
.sidebar-nav { flex: 1; overflow-y: auto; }
.sidebar-footer {
    padding: 12px 20px; border-top: 1px solid #334155;
    font-size: 0.8em; color: #64748b;
}

/* Tree nodes */
.tree-section { margin-bottom: 2px; }
.tree-toggle {
    display: flex; align-items: center; gap: 8px;
    padding: 8px 20px; cursor: pointer; user-select: none;
    font-weight: 600; font-size: 0.9em; transition: background 0.15s;
}
.tree-toggle:hover { background: #334155; }
.tree-toggle .arrow {
    display: inline-block; width: 12px; font-size: 0.7em;
    transition: transform 0.2s;
}
.tree-toggle .arrow.open { transform: rotate(90deg); }
.tree-badge {
    margin-left: auto; padding: 1px 7px; border-radius: 10px;
    font-size: 0.75em; font-weight: 700;
}
.tree-badge-critical { background: #dc2626; color: white; }
.tree-badge-warning { background: #d97706; color: white; }
.tree-badge-consider { background: #0891b2; color: white; }
.tree-badge-info { background: #2563eb; color: white; }
.tree-badge-errors { background: #991b1b; color: white; }

.tree-children { overflow: hidden; transition: max-height 0.25s ease; }
.tree-children.collapsed { max-height: 0 !important; }
.tree-child {
    display: block; padding: 5px 20px 5px 44px; color: #cbd5e1;
    text-decoration: none; font-size: 0.82em; transition: background 0.15s;
}
.tree-child:hover { background: #334155; color: #f1f5f9; }
.tree-child.active { background: #1d4ed8; color: white; }
.tree-child-count { color: #64748b; margin-left: 4px; }

/* Todo link in sidebar */
.tree-link {
    display: block; padding: 8px 20px; color: #cbd5e1;
    text-decoration: none; font-weight: 600; font-size: 0.9em;
    transition: background 0.15s; margin-top: 4px;
    border-top: 1px solid #334155;
}
.tree-link:hover { background: #334155; color: #f1f5f9; }

/* ── Main content ─────────────────────────────────────────────── */
.main {
    margin-left: 270px; padding: 32px 40px; max-width: 1000px;
}
h1 { border-bottom: 3px solid #2563eb; padding-bottom: 10px; margin-top: 0; }
h2 { color: #1e40af; margin-top: 2.5em; }
h3 { color: #374151; margin-top: 1.5em; }
h4 { color: #4b5563; margin-bottom: 4px; }
table { border-collapse: collapse; width: 100%; margin: 1em 0; }
th, td { border: 1px solid #d1d5db; padding: 8px 12px; text-align: left; }
th { background: #f3f4f6; }
code { background: #f3f4f6; padding: 2px 6px; border-radius: 3px; font-size: 0.9em; }
pre { background: #f3f4f6; padding: 12px; border-radius: 6px; overflow-x: auto;
      white-space: pre-wrap; word-wrap: break-word; }
blockquote { border-left: 4px solid #2563eb; margin: 1em 0; padding: 8px 16px;
             background: #eff6ff; }
hr { border: none; border-top: 1px solid #e5e7eb; margin: 1.5em 0; }

/* Badges */
.badge { display: inline-block; padding: 2px 10px; border-radius: 4px;
         font-size: 0.8em; font-weight: bold; color: white; }
.badge-critical { background: #dc2626; }
.badge-warning { background: #d97706; }
.badge-consider { background: #0891b2; }
.badge-info { background: #2563eb; }

/* Summary cards */
.summary-box { display: flex; gap: 16px; flex-wrap: wrap; margin: 1em 0; }
.summary-card { border: 1px solid #d1d5db; border-radius: 8px; padding: 16px 24px;
                text-align: center; min-width: 110px; background: white; }
.summary-card .number { font-size: 2em; font-weight: bold; }
.summary-card.critical .number { color: #dc2626; }
.summary-card.warning .number { color: #d97706; }
.summary-card.consider .number { color: #0891b2; }
.summary-card.info .number { color: #2563eb; }
.summary-card.passed .number { color: #16a34a; }

/* Finding cards */
.finding-card {
    margin-bottom: 1.2em; padding: 14px 18px; border: 1px solid #e5e7eb;
    border-radius: 8px; background: white;
}
.finding-card p { margin: 6px 0; }
.finding-detail { white-space: pre-wrap; }

/* ── To Do section ────────────────────────────────────────────── */
.todo-summary {
    padding: 12px 18px; border-radius: 8px; margin-bottom: 1.5em;
    font-weight: 600;
}
.todo-summary-red { background: #fef2f2; border: 1px solid #fecaca; color: #991b1b; }
.todo-summary-amber { background: #fffbeb; border: 1px solid #fde68a; color: #92400e; }
.todo-summary-cyan { background: #ecfeff; border: 1px solid #a5f3fc; color: #155e75; }
.todo-summary-green { background: #f0fdf4; border: 1px solid #bbf7d0; color: #166534; }

.todo-group-label {
    font-weight: 700; font-size: 0.9em; padding: 6px 12px; border-radius: 4px;
    margin: 1.2em 0 0.6em; display: inline-block;
}
.todo-group-critical { background: #fef2f2; color: #991b1b; }
.todo-group-warning { background: #fffbeb; color: #92400e; }
.todo-group-consider { background: #ecfeff; color: #155e75; }

.todo-item {
    display: flex; gap: 12px; padding: 10px 14px; border: 1px solid #e5e7eb;
    border-radius: 6px; margin-bottom: 8px; background: white;
    align-items: flex-start;
}
.todo-item.checked { opacity: 0.55; text-decoration: line-through; }
.todo-item input[type="checkbox"] {
    margin-top: 4px; width: 16px; height: 16px; flex-shrink: 0;
    accent-color: #2563eb; cursor: pointer;
}
.todo-content { flex: 1; }
.todo-title { font-weight: 600; font-size: 0.92em; }
.todo-object { font-size: 0.82em; color: #6b7280; }
.todo-remediation {
    font-size: 0.85em; color: #374151; margin-top: 4px;
    white-space: pre-wrap; word-wrap: break-word;
}

/* ── Print ────────────────────────────────────────────────────── */
@media print {
    .sidebar { display: none; }
    .main { margin-left: 0; padding: 20px; max-width: 100%; }
    .finding-card, .todo-item { break-inside: avoid; }
    .todo-item.checked { opacity: 0.4; }
}

/* ── Responsive (narrow screens) ──────────────────────────────── */
@media (max-width: 800px) {
    .sidebar { display: none; }
    .main { margin-left: 0; padding: 20px; }
}
"""

_JS = """
// Sidebar tree toggle
document.querySelectorAll('.tree-toggle').forEach(function(el) {
    el.addEventListener('click', function() {
        var children = this.nextElementSibling;
        var arrow = this.querySelector('.arrow');
        if (children && children.classList.contains('tree-children')) {
            children.classList.toggle('collapsed');
            arrow.classList.toggle('open');
        }
    });
});

// Smooth scroll for sidebar links
document.querySelectorAll('.tree-child, .tree-link').forEach(function(el) {
    el.addEventListener('click', function(e) {
        var href = this.getAttribute('href');
        if (href && href.startsWith('#')) {
            e.preventDefault();
            var target = document.getElementById(href.substring(1));
            if (target) {
                target.scrollIntoView({ behavior: 'smooth', block: 'start' });
            }
        }
    });
});

// Scroll tracking — highlight sidebar links as sections scroll into view
(function() {
    var headings = document.querySelectorAll('h2[id], h3[id]');
    if (!headings.length) return;

    var sidebarLinks = {};
    document.querySelectorAll('.tree-child').forEach(function(link) {
        var href = link.getAttribute('href');
        if (href && href.startsWith('#')) {
            sidebarLinks[href.substring(1)] = link;
        }
    });

    var observer = new IntersectionObserver(function(entries) {
        entries.forEach(function(entry) {
            var link = sidebarLinks[entry.target.id];
            if (link) {
                if (entry.isIntersecting) {
                    // Remove active from all
                    document.querySelectorAll('.tree-child.active').forEach(function(c) {
                        c.classList.remove('active');
                    });
                    link.classList.add('active');

                    // Auto-expand the parent tree section if collapsed
                    var section = link.closest('.tree-section');
                    if (section) {
                        var children = section.querySelector('.tree-children');
                        var arrow = section.querySelector('.arrow');
                        if (children && children.classList.contains('collapsed')) {
                            children.classList.remove('collapsed');
                            if (arrow) arrow.classList.add('open');
                        }
                    }
                }
            }
        });
    }, {
        rootMargin: '-10% 0px -80% 0px',
        threshold: 0
    });

    headings.forEach(function(heading) {
        if (sidebarLinks[heading.id]) {
            observer.observe(heading);
        }
    });
})();

// To Do checkboxes
document.querySelectorAll('.todo-item input[type="checkbox"]').forEach(function(cb) {
    cb.addEventListener('change', function() {
        this.closest('.todo-item').classList.toggle('checked', this.checked);
        updateTodoCount();
    });
});

function updateTodoCount() {
    var total = document.querySelectorAll('.todo-item').length;
    var done = document.querySelectorAll('.todo-item.checked').length;
    var counter = document.getElementById('todo-counter');
    if (counter) {
        counter.textContent = done + ' of ' + total + ' completed';
    }
}
"""


def _slug(text: str) -> str:
    """Create a URL-safe anchor slug."""
    return text.lower().replace(" ", "-").replace("/", "-")


def _esc(text: str) -> str:
    return html.escape(text)


def _render_detail(text: str) -> str:
    """Render detail text, preserving newlines and escaping HTML."""
    return _esc(text)


def render(report: ScanReport) -> str:
    """Render a ScanReport as a standalone HTML document with sidebar navigation."""
    all_findings = report.findings
    badge_map = {
        Severity.CRITICAL: ("badge-critical", "tree-badge-critical"),
        Severity.WARNING: ("badge-warning", "tree-badge-warning"),
        Severity.CONSIDER: ("badge-consider", "tree-badge-consider"),
        Severity.INFO: ("badge-info", "tree-badge-info"),
    }

    # ── Build severity → category → findings structure ──
    sev_cat_map: dict[Severity, dict[str, list]] = {}
    for severity in [Severity.CRITICAL, Severity.WARNING, Severity.CONSIDER, Severity.INFO]:
        sev_findings = [f for f in all_findings if f.severity == severity]
        if not sev_findings:
            continue
        cat_map: dict[str, list] = {}
        for f in sev_findings:
            cat_map.setdefault(f.category, []).append(f)
        sev_cat_map[severity] = dict(sorted(cat_map.items()))

    errors = [r for r in report.results if r.error]

    # Collect To Do items: CRITICAL, WARNING, and CONSIDER findings with remediation
    todo_items = []
    for severity in [Severity.CRITICAL, Severity.WARNING, Severity.CONSIDER]:
        for f in all_findings:
            if f.severity == severity and f.remediation:
                todo_items.append(f)

    # ── Build sidebar HTML ──
    sidebar_lines = []
    sidebar_lines.append('<div class="sidebar">')
    sidebar_lines.append('<div class="sidebar-header">')
    sidebar_lines.append('<strong>MM-Ready Report</strong><br>')
    sidebar_lines.append(f'{_esc(report.database)}')
    sidebar_lines.append('</div>')
    sidebar_lines.append('<nav class="sidebar-nav">')

    collapsed_severities = {Severity.CONSIDER, Severity.INFO}
    for severity, cat_map in sev_cat_map.items():
        sev_label = severity.value
        sev_slug = _slug(sev_label)
        sev_count = sum(len(fs) for fs in cat_map.values())
        _, tree_badge = badge_map[severity]
        collapsed = "collapsed" if severity in collapsed_severities else ""
        arrow_cls = "arrow" if severity in collapsed_severities else "arrow open"

        sidebar_lines.append('<div class="tree-section">')
        sidebar_lines.append(f'<div class="tree-toggle">')
        sidebar_lines.append(f'<span class="{arrow_cls}">&#9654;</span>')
        sidebar_lines.append(f'{sev_label}')
        sidebar_lines.append(f'<span class="tree-badge {tree_badge}">{sev_count}</span>')
        sidebar_lines.append('</div>')
        sidebar_lines.append(f'<div class="tree-children {collapsed}" style="max-height:500px">')
        for cat, findings in cat_map.items():
            anchor = f"sev-{sev_slug}-{_slug(cat)}"
            sidebar_lines.append(
                f'<a class="tree-child" href="#{anchor}">'
                f'{_esc(cat)}<span class="tree-child-count">({len(findings)})</span></a>'
            )
        sidebar_lines.append('</div>')
        sidebar_lines.append('</div>')

    if errors:
        sidebar_lines.append(
            f'<a class="tree-link" href="#errors">'
            f'Errors <span class="tree-badge tree-badge-errors">{len(errors)}</span></a>'
        )

    if todo_items:
        sidebar_lines.append(
            f'<a class="tree-link" href="#todo">To Do List '
            f'<span class="tree-badge tree-badge-warning">{len(todo_items)}</span></a>'
        )

    sidebar_lines.append('</nav>')
    sidebar_lines.append('<div class="sidebar-footer">mm-ready v0.1.0</div>')
    sidebar_lines.append('</div>')

    # ── Build main content ──
    main = []
    main.append('<div class="main">')

    # Header
    main.append('<h1>MM-Ready: Spock 5 Readiness Report</h1>')
    main.append(f'<p><strong>Database:</strong> {_esc(report.database)}<br>')
    if report.scan_mode == "analyze":
        main.append(f'<strong>Source File:</strong> {_esc(report.host)}<br>')
    else:
        main.append(f'<strong>Host:</strong> {_esc(report.host)}:{report.port}<br>')
    main.append(f'<strong>PostgreSQL:</strong> {_esc(report.pg_version)}<br>')
    main.append(f'<strong>Scan Time:</strong> {report.timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")}<br>')
    main.append(f'<strong>Mode:</strong> {_esc(report.scan_mode)}<br>')
    main.append(f'<strong>Target:</strong> Spock {report.spock_target}</p>')

    # Summary cards
    main.append('<div class="summary-box">')
    main.append(f'<div class="summary-card"><div class="number">{report.checks_total}</div>Checks Run</div>')
    main.append(f'<div class="summary-card passed"><div class="number">{report.checks_passed}</div>Passed</div>')
    main.append(f'<div class="summary-card critical"><div class="number">{report.critical_count}</div>Critical</div>')
    main.append(f'<div class="summary-card warning"><div class="number">{report.warning_count}</div>Warnings</div>')
    main.append(f'<div class="summary-card consider"><div class="number">{report.consider_count}</div>Consider</div>')
    main.append(f'<div class="summary-card info"><div class="number">{report.info_count}</div>Info</div>')
    main.append('</div>')

    # Verdict
    if report.critical_count == 0 and report.warning_count == 0:
        main.append('<blockquote style="border-left-color: #16a34a; background: #f0fdf4;">')
        main.append('<strong>READY</strong> — No critical or warning issues found.')
    elif report.critical_count == 0:
        main.append('<blockquote style="border-left-color: #d97706; background: #fffbeb;">')
        main.append('<strong>CONDITIONALLY READY</strong> — No critical issues, but warnings should be reviewed.')
    else:
        main.append('<blockquote style="border-left-color: #dc2626; background: #fef2f2;">')
        main.append(f'<strong>NOT READY</strong> — {report.critical_count} critical issue(s) must be resolved.')
    main.append('</blockquote>')

    # ── Findings by severity → category ──
    for severity, cat_map in sev_cat_map.items():
        sev_label = severity.value
        sev_slug = _slug(sev_label)
        sev_count = sum(len(fs) for fs in cat_map.values())
        badge_cls, _ = badge_map[severity]

        main.append(f'<h2 id="sev-{sev_slug}">'
                     f'<span class="badge {badge_cls}">{sev_label}</span> ({sev_count})</h2>')

        for cat, findings in cat_map.items():
            anchor = f"sev-{sev_slug}-{_slug(cat)}"
            main.append(f'<h3 id="{anchor}">{_esc(cat)} ({len(findings)})</h3>')

            for finding in findings:
                main.append('<div class="finding-card">')
                main.append(f'<h4>{_esc(finding.title)}</h4>')
                if finding.object_name:
                    main.append(f'<p><strong>Object:</strong> <code>{_esc(finding.object_name)}</code></p>')
                main.append(f'<p class="finding-detail">{_render_detail(finding.detail)}</p>')
                if finding.remediation:
                    main.append(f'<p><strong>Remediation:</strong></p>'
                                f'<pre>{_esc(finding.remediation)}</pre>')
                main.append('</div>')

    # ── Errors ──
    if errors:
        main.append('<h2 id="errors">Errors</h2>')
        main.append('<ul>')
        for r in errors:
            main.append(f'<li><strong>{_esc(r.category)}/{_esc(r.check_name)}</strong>: {_esc(r.error)}</li>')
        main.append('</ul>')

    # ── To Do List ──
    if todo_items:
        crit_todos = [f for f in todo_items if f.severity == Severity.CRITICAL]
        warn_todos = [f for f in todo_items if f.severity == Severity.WARNING]
        consider_todos = [f for f in todo_items if f.severity == Severity.CONSIDER]

        main.append('<h2 id="todo">To Do List</h2>')

        # Summary banner
        if crit_todos:
            css_class = "todo-summary todo-summary-red"
        elif warn_todos:
            css_class = "todo-summary todo-summary-amber"
        elif consider_todos:
            css_class = "todo-summary todo-summary-cyan"
        else:
            css_class = "todo-summary todo-summary-green"

        parts = []
        if crit_todos:
            parts.append(f'{len(crit_todos)} critical')
        if warn_todos:
            parts.append(f'{len(warn_todos)} warning{"s" if len(warn_todos) != 1 else ""}')
        if consider_todos:
            parts.append(f'{len(consider_todos)} to consider')

        main.append(f'<div class="{css_class}">')
        main.append(f'{len(todo_items)} item{"s" if len(todo_items) != 1 else ""} to address'
                     f' ({", ".join(parts)})')
        main.append(f' &mdash; <span id="todo-counter">0 of {len(todo_items)} completed</span>')
        main.append('</div>')

        for group_sev, group_items, group_label, group_cls in [
            (Severity.CRITICAL, crit_todos, "CRITICAL", "todo-group-critical"),
            (Severity.WARNING, warn_todos, "WARNING", "todo-group-warning"),
            (Severity.CONSIDER, consider_todos, "CONSIDER", "todo-group-consider"),
        ]:
            if not group_items:
                continue
            main.append(f'<div class="todo-group-label {group_cls}">{group_label}</div>')
            for finding in group_items:
                obj_html = ""
                if finding.object_name:
                    obj_html = f'<div class="todo-object"><code>{_esc(finding.object_name)}</code></div>'
                main.append(
                    f'<div class="todo-item">'
                    f'<input type="checkbox">'
                    f'<div class="todo-content">'
                    f'<div class="todo-title">{_esc(finding.title)}</div>'
                    f'{obj_html}'
                    f'<div class="todo-remediation">{_esc(finding.remediation)}</div>'
                    f'</div></div>'
                )

    # ── Footer ──
    main.append('<hr>')
    main.append('<p><em>Generated by mm-ready v0.1.0</em></p>')
    main.append('</div>')

    # ── Assemble document ──
    doc = []
    doc.append("<!DOCTYPE html>")
    doc.append('<html lang="en">')
    doc.append("<head>")
    doc.append('<meta charset="UTF-8">')
    doc.append('<meta name="viewport" content="width=device-width, initial-scale=1.0">')
    doc.append(f"<title>MM-Ready Report: {_esc(report.database)}</title>")
    doc.append(f"<style>{_CSS}</style>")
    doc.append("</head>")
    doc.append("<body>")
    doc.extend(sidebar_lines)
    doc.extend(main)
    doc.append(f"<script>{_JS}</script>")
    doc.append("</body></html>")

    return "\n".join(doc)
