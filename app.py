from flask import Flask, request, redirect, url_for, flash, render_template_string
import sqlite3
from pathlib import Path
from datetime import datetime
import os

# =============================================================
# Colts Front Office GM – Stage 3 Version
# This file is designed to satisfy:
#   • Stage 1 + Stage 2 requirements (CRUD, joins, reports, business rules)
#   • Stage 3 requirements:
#       - SQL injection protection (prepared statements + validation)
#       - Indexes + explanation of which queries they support
#       - Transactions + isolation level choice for multi-step operations
#   • Demo anchors are labeled "DEMO SCRIPT PART X.Y" throughout.
# =============================================================

app = Flask(__name__)
app.config["SECRET_KEY"] = "dev-secret-key"

# If running on App Engine standard, use /tmp (only writable place there).
# Locally, still use a file in the current directory.
if os.environ.get("GAE_ENV") == "standard":
    DB_PATH = Path("/tmp/colts_front_office.db")
else:
    DB_PATH = Path("colts_front_office.db")


# =============================================================
# DEMO SCRIPT PART 3.1 – SQL Injection Protection: Safe Connection Helper
# =============================================================
def get_connection():
    """
    Create a new SQLite connection with:
      • row_factory = sqlite3.Row so we can access columns by name
      • foreign_keys enforced
    NOTE: No user input ever touches this function. All SQL later uses
    '?' placeholders with a separate args tuple to avoid SQL injection.
    """
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


# =============================================================
# DEMO SCRIPT PART 3.2 – Indexes Created in Schema
#   We keep Stage 2 schema and add extra indexes for Stage 3.
# =============================================================
def init_db():
    conn = get_connection()
    cur = conn.cursor()

    # ---------------------------------------------------------
    # TABLE: people
    # Stage 1 / Part 1.1 – main entity referenced by contracts
    # ---------------------------------------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS people (
            person_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        )
        """
    )

    # ---------------------------------------------------------
    # TABLE: units
    # ---------------------------------------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS units (
            unit_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        )
        """
    )

    # ---------------------------------------------------------
    # TABLE: positions
    # ---------------------------------------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS positions (
            position_id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL UNIQUE,
            description TEXT NOT NULL,
            unit_id INTEGER NOT NULL,
            FOREIGN KEY (unit_id) REFERENCES units(unit_id)
        )
        """
    )

    # ---------------------------------------------------------
    # TABLE: contracts  (main table for CRUD in Stage 1)
    # ---------------------------------------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS contracts (
            contract_id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id INTEGER NOT NULL,
            position_id INTEGER NOT NULL,
            unit_id INTEGER NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            years INTEGER NOT NULL,
            salary_millions REAL NOT NULL,
            cap_hit_millions REAL NOT NULL,
            FOREIGN KEY (person_id) REFERENCES people(person_id),
            FOREIGN KEY (position_id) REFERENCES positions(position_id),
            FOREIGN KEY (unit_id) REFERENCES units(unit_id)
        )
        """
    )

    # ------------------- INDEXES (Stage 3) -------------------
    # Existing from Stage 2:
    #   idx_contracts_unit_salary (unit_id, salary_millions)
    # Supports: report() query that filters on unit and salary range.
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_contracts_unit_salary
        ON contracts (unit_id, salary_millions)
        """
    )

    # New index 1: contracts(person_id)
    #   • Supports:
    #       - delete_contract(): COUNT(*) WHERE person_id = ?
    #       - delete_unit()/delete_position(): checking for orphan people
    #       - joins on contracts.person_id = people.person_id in
    #         list_contracts() and report()
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_contracts_person
        ON contracts (person_id)
        """
    )

    # New index 2: positions(unit_id)
    #   • Supports:
    #       - list_positions(): ORDER BY unit / JOIN units
    #       - delete_unit(): DELETE FROM positions WHERE unit_id = ?
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_positions_unit
        ON positions (unit_id)
        """
    )

    conn.commit()
    conn.close()


# =============================================================
# DEMO SCRIPT PART 3.1 – SQL Injection Protection: Query Helpers
# =============================================================
def query_db(query: str, args=(), one: bool = False):
    """
    Safe SELECT helper.
    All variables are passed via the args tuple, never concatenated
    into the SQL string. This is the classic prepared-statement pattern
    that prevents SQL injection.
    """
    conn = get_connection()
    try:
        cur = conn.execute(query, args)
        rows = cur.fetchall()
    finally:
        conn.close()

    if one:
        return rows[0] if rows else None
    return rows


def execute_db(query: str, args=()):
    """
    Safe helper for single-statement INSERT/UPDATE/DELETE.
    For multi-step operations that must be atomic, we use
    execute_in_transaction() below.
    """
    conn = get_connection()
    try:
        cur = conn.execute(query, args)
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


# =============================================================
# DEMO SCRIPT PART 3.3 – Transactions & Isolation Levels
# =============================================================
def execute_in_transaction(work, isolation: str = "IMMEDIATE"):
    """
    Run a multi-step unit of work inside a single transaction.

    • isolation="IMMEDIATE" means:
        - SQLite acquires a RESERVED/WRITE lock up front.
        - Prevents the classic lost update problem if two users try
          to edit/delete the same logical data at once.
        - Good fit for our small OLTP-style web app.

    • If anything fails, we ROLLBACK so the database is never left
      half-updated.

    The 'work' callback receives a connection object and should do
    multiple execute() calls using only that connection.
    """
    conn = sqlite3.connect(DB_PATH, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    try:
        conn.execute(f"BEGIN {isolation}")
        result = work(conn)
        conn.execute("COMMIT")
        return result
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


# -------------------------------------------------------------
# TEMPLATES – kept simple but still Bootstrap-based
# These satisfy Stage 1/2 UI requirements and are used in the
# Stage 3 demo script.
# -------------------------------------------------------------
BASE_NAV = """
<nav class="navbar navbar-expand-lg navbar-dark bg-primary mb-4">
  <div class="container-fluid">
    <a class="navbar-brand" href="{{ url_for('list_contracts') }}">Colts Front Office GM</a>
    <div class="navbar-nav">
      <a class="nav-link {% if active=='contracts' %}active{% endif %}" href="{{ url_for('list_contracts') }}">Contracts</a>
      <a class="nav-link {% if active=='units' %}active{% endif %}" href="{{ url_for('list_units') }}">Units</a>
      <a class="nav-link {% if active=='positions' %}active{% endif %}" href="{{ url_for('list_positions') }}">Positions</a>
      <a class="nav-link {% if active=='report' %}active{% endif %}" href="{{ url_for('report') }}">Reports</a>
    </div>
  </div>
</nav>
"""

CONTRACTS_TEMPLATE = (
    """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>Contracts</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
  </head>
  <body class="bg-light">
    """
    + BASE_NAV
    + """
    <div class="container">
      {% with messages = get_flashed_messages() %}
        {% if messages %}
          <div class="alert alert-info">
            {% for m in messages %}<div>{{ m }}</div>{% endfor %}
          </div>
        {% endif %}
      {% endwith %}

      <!-- ===================================================
           DEMO SCRIPT PART 1.3 – Joined roster view
           Shows people, positions, units via multi-table JOIN.
           =================================================== -->
      <div class="d-flex justify-content-between align-items-center mb-3">
        <h1 class="h3 mb-0">Contracts</h1>
        <a class="btn btn-primary" href="{{ url_for('create_contract') }}">+ New Contract</a>
      </div>

      <table class="table table-striped table-hover align-middle bg-white shadow-sm">
        <thead class="table-light">
          <tr>
            <th>Person</th>
            <th>Position</th>
            <th>Unit</th>
            <th>Years</th>
            <th>Salary (M)</th>
            <th>Cap Hit (M)</th>
            <th>Term</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {% for c in contracts %}
          <tr>
            <td><strong>{{ c['name'] }}</strong><br><span class="text-muted small">ID {{ c['person_id'] }}</span></td>
            <td>{{ c['position_code'] }} – {{ c['position_description'] }}</td>
            <td>{{ c['unit_name'] }}</td>
            <td>{{ c['years'] }}</td>
            <td>{{ '%.1f'|format(c['salary_millions']) }}</td>
            <td>{{ '%.1f'|format(c['cap_hit_millions']) }}</td>
            <td class="small">{{ c['start_date'] }} → {{ c['end_date'] }}</td>
            <td class="text-end">
              <a class="btn btn-sm btn-outline-secondary" href="{{ url_for('edit_contract', contract_id=c['contract_id']) }}">Edit</a>
              <form method="post" action="{{ url_for('delete_contract', contract_id=c['contract_id']) }}"
                    style="display:inline-block" onsubmit="return confirm('Delete this contract?');">
                <button class="btn btn-sm btn-outline-danger">Delete</button>
              </form>
            </td>
          </tr>
          {% else %}
          <tr><td colspan="8" class="text-center text-muted">No contracts yet.</td></tr>
          {% endfor %}
        </tbody>
      </table>
    </div>
  </body>
</html>
"""
)

CONTRACT_FORM_TEMPLATE = (
    """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>Contract Form</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
  </head>
  <body class="bg-light">
    """
    + BASE_NAV
    + """
    <div class="container">
      {% with messages = get_flashed_messages() %}
        {% if messages %}
          <div class="alert alert-info">
            {% for m in messages %}<div>{{ m }}</div>{% endfor %}
          </div>
        {% endif %}
      {% endwith %}

      <!-- ===================================================
           DEMO SCRIPT PART 1.1 – Main table CRUD form
           Also used in Stage 3 when we talk about validation.
           =================================================== -->
      <h1 class="h3 mb-3">
        {% if contract %}Edit Contract{% else %}New Contract{% endif %}
      </h1>

      <form method="post" class="card p-3 shadow-sm bg-white">
        <div class="row mb-3">
          <div class="col-md-5">
            <label class="form-label">Person name</label>
            {% if contract %}
              <input class="form-control" type="text" value="{{ person_name }}" readonly>
              <input type="hidden" name="person_id" value="{{ contract['person_id'] }}">
            {% else %}
              <input class="form-control" type="text" name="person_name" required>
            {% endif %}
          </div>
          <div class="col-md-3">
            <label class="form-label">Unit</label>
            <select class="form-select" name="unit_id" id="unit-select" required>
              <option value="">-- choose unit --</option>
              {% for u in units %}
                <option value="{{ u['unit_id'] }}"
                    {% if contract and contract['unit_id']==u['unit_id'] %}selected{% endif %}>
                  {{ u['name'] }}
                </option>
              {% endfor %}
            </select>
          </div>
          <div class="col-md-4">
            <label class="form-label">Position</label>
            <select class="form-select" name="position_id" id="position-select" required>
              <option value="">-- choose position --</option>
              {% for p in positions %}
                <option value="{{ p['position_id'] }}" data-unit-id="{{ p['unit_id'] }}"
                    {% if contract and contract['position_id']==p['position_id'] %}selected{% endif %}>
                  {{ p['code'] }} – {{ p['description'] }}
                </option>
              {% endfor %}
            </select>
          </div>
        </div>

        <div class="row mb-3">
          <div class="col-md-3">
            <label class="form-label">Start date</label>
            <input class="form-control" type="date" name="start_date"
                   value="{{ contract['start_date'] if contract else '' }}" required>
          </div>
          <div class="col-md-3">
            <label class="form-label">End date</label>
            <input class="form-control" type="date" name="end_date"
                   value="{{ contract['end_date'] if contract else '' }}" required>
          </div>
          <div class="col-md-2">
            <label class="form-label">Years</label>
            <input class="form-control" type="number" min="1" name="years"
                   value="{{ contract['years'] if contract else 1 }}" required>
          </div>
          <div class="col-md-2">
            <label class="form-label">Salary (M)</label>
            <input class="form-control" type="number" step="0.1" min="0" name="salary_millions"
                   value="{{ contract['salary_millions'] if contract else 10.0 }}" required>
          </div>
          <div class="col-md-2">
            <label class="form-label">Cap hit (M)</label>
            <input class="form-control" type="number" step="0.1" min="0" name="cap_hit_millions"
                   value="{{ contract['cap_hit_millions'] if contract else 9.5 }}" required>
          </div>
        </div>

        <div class="d-flex justify-content-between">
          <a class="btn btn-outline-secondary" href="{{ url_for('list_contracts') }}">Cancel</a>
          <button class="btn btn-success" type="submit">
            {% if contract %}Save changes{% else %}Create contract{% endif %}
          </button>
        </div>
      </form>

      <script>
        // Simple client-side helper so that positions are filtered by unit.
        function filterPositionsByUnit() {
          var unitSelect = document.getElementById('unit-select');
          var posSelect = document.getElementById('position-select');
          if (!unitSelect || !posSelect) return;
          var unitId = unitSelect.value;
          for (var i = 0; i < posSelect.options.length; i++) {
            var opt = posSelect.options[i];
            if (!opt.value) { opt.hidden = false; continue; }
            var pid = opt.getAttribute('data-unit-id');
            var show = !unitId || (pid === unitId);
            opt.hidden = !show;
          }
        }
        document.addEventListener('DOMContentLoaded', function() {
          var unitSelect = document.getElementById('unit-select');
          if (unitSelect) {
            unitSelect.addEventListener('change', filterPositionsByUnit);
            filterPositionsByUnit();
          }
        });
      </script>
    </div>
  </body>
</html>
"""
)

UNITS_TEMPLATE = (
    """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>Units</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
  </head>
  <body class="bg-light">
    """
    + BASE_NAV
    + """
    <div class="container">
      {% with messages = get_flashed_messages() %}
        {% if messages %}
          <div class="alert alert-info">
            {% for m in messages %}<div>{{ m }}</div>{% endfor %}
          </div>
        {% endif %}
      {% endwith %}

      <!-- DEMO SCRIPT PART 1.2 – Supporting table CRUD: Units -->
      <h1 class="h3 mb-3">Units</h1>

      <form method="post" class="card p-3 mb-3 shadow-sm bg-white">
        <div class="row g-2 align-items-end">
          <div class="col-md-6">
            <label class="form-label">New unit name</label>
            <input class="form-control" type="text" name="unit_name" required>
          </div>
          <div class="col-md-3">
            <button class="btn btn-success w-100" type="submit">Add Unit</button>
          </div>
        </div>
      </form>

      <table class="table table-striped table-hover bg-white shadow-sm">
        <thead class="table-light">
          <tr><th>ID</th><th>Name</th><th></th></tr>
        </thead>
        <tbody>
          {% for u in units %}
          <tr>
            <td>{{ u['unit_id'] }}</td>
            <td>{{ u['name'] }}</td>
            <td class="text-end">
              <a class="btn btn-sm btn-outline-secondary" href="{{ url_for('edit_unit', unit_id=u['unit_id']) }}">Edit</a>
              <form method="post" action="{{ url_for('delete_unit', unit_id=u['unit_id']) }}"
                    style="display:inline-block" onsubmit="return confirm('Delete unit and cascade?');">
                <button class="btn btn-sm btn-outline-danger">Delete</button>
              </form>
            </td>
          </tr>
          {% else %}
          <tr><td colspan="3" class="text-center text-muted">No units yet.</td></tr>
          {% endfor %}
        </tbody>
      </table>
    </div>
  </body>
</html>
"""
)

UNIT_EDIT_TEMPLATE = (
    """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>Edit Unit</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
  </head>
  <body class="bg-light">
    """
    + BASE_NAV
    + """
    <div class="container">
      {% with messages = get_flashed_messages() %}
        {% if messages %}
          <div class="alert alert-info">
            {% for m in messages %}<div>{{ m }}</div>{% endfor %}
          </div>
        {% endif %}
      {% endwith %}

      <h1 class="h3 mb-3">Edit Unit</h1>

      <form method="post" class="card p-3 shadow-sm bg-white">
        <div class="mb-3">
          <label class="form-label">Unit name</label>
          <input class="form-control" type="text" name="unit_name" value="{{ unit['name'] }}" required>
        </div>
        <div class="d-flex justify-content-between">
          <a class="btn btn-outline-secondary" href="{{ url_for('list_units') }}">Cancel</a>
          <button class="btn btn-success" type="submit">Save</button>
        </div>
      </form>
    </div>
  </body>
</html>
"""
)

POSITIONS_TEMPLATE = (
    """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>Positions</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
  </head>
  <body class="bg-light">
    """
    + BASE_NAV
    + """
    <div class="container">
      {% with messages = get_flashed_messages() %}
        {% if messages %}
          <div class="alert alert-info">
            {% for m in messages %}<div>{{ m }}</div>{% endfor %}
          </div>
        {% endif %}
      {% endwith %}

      <!-- DEMO SCRIPT PART 2.1 – Positions table tied to Units -->
      <h1 class="h3 mb-3">Positions</h1>

      <form method="post" class="card p-3 mb-3 shadow-sm bg-white">
        <div class="row g-2 align-items-end">
          <div class="col-md-2">
            <label class="form-label">Code</label>
            <input class="form-control" type="text" name="code" required>
          </div>
          <div class="col-md-4">
            <label class="form-label">Description</label>
            <input class="form-control" type="text" name="description" required>
          </div>
          <div class="col-md-4">
            <label class="form-label">Unit</label>
            <select class="form-select" name="unit_id" required>
              <option value="">-- choose unit --</option>
              {% for u in units %}
                <option value="{{ u['unit_id'] }}">{{ u['name'] }}</option>
              {% endfor %}
            </select>
          </div>
          <div class="col-md-2">
            <button class="btn btn-success w-100" type="submit">Add Position</button>
          </div>
        </div>
      </form>

      <table class="table table-striped table-hover bg-white shadow-sm">
        <thead class="table-light">
          <tr><th>ID</th><th>Code</th><th>Description</th><th>Unit</th><th></th></tr>
        </thead>
        <tbody>
          {% for p in positions %}
          <tr>
            <td>{{ p['position_id'] }}</td>
            <td>{{ p['code'] }}</td>
            <td>{{ p['description'] }}</td>
            <td>{{ p['unit_name'] }}</td>
            <td class="text-end">
              <a class="btn btn-sm btn-outline-secondary" href="{{ url_for('edit_position', position_id=p['position_id']) }}">Edit</a>
              <form method="post" action="{{ url_for('delete_position', position_id=p['position_id']) }}"
                    style="display:inline-block" onsubmit="return confirm('Delete position and cascade?');">
                <button class="btn btn-sm btn-outline-danger">Delete</button>
              </form>
            </td>
          </tr>
          {% else %}
          <tr><td colspan="5" class="text-center text-muted">No positions yet.</td></tr>
          {% endfor %}
        </tbody>
      </table>
    </div>
  </body>
</html>
"""
)

POSITION_EDIT_TEMPLATE = (
    """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>Edit Position</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
  </head>
  <body class="bg-light">
    """
    + BASE_NAV
    + """
    <div class="container">
      {% with messages = get_flashed_messages() %}
        {% if messages %}
          <div class="alert alert-info">
            {% for m in messages %}<div>{{ m }}</div>{% endfor %}
          </div>
        {% endif %}
      {% endwith %}

      <h1 class="h3 mb-3">Edit Position</h1>

      <form method="post" class="card p-3 shadow-sm bg-white">
        <div class="mb-3">
          <label class="form-label">Code</label>
          <input class="form-control" type="text" name="code" value="{{ position['code'] }}" required>
        </div>
        <div class="mb-3">
          <label class="form-label">Description</label>
          <input class="form-control" type="text" name="description" value="{{ position['description'] }}" required>
        </div>
        <div class="mb-3">
          <label class="form-label">Unit</label>
          <select class="form-select" name="unit_id" required>
            {% for u in units %}
              <option value="{{ u['unit_id'] }}" {% if u['unit_id']==position['unit_id'] %}selected{% endif %}>
                {{ u['name'] }}
              </option>
            {% endfor %}
          </select>
        </div>
        <div class="d-flex justify-content-between">
          <a class="btn btn-outline-secondary" href="{{ url_for('list_positions') }}">Cancel</a>
          <button class="btn btn-success" type="submit">Save</button>
        </div>
      </form>
    </div>
  </body>
</html>
"""
)

REPORT_TEMPLATE = (
    """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>Cap & Roster Report</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
  </head>
  <body class="bg-light">
    """
    + BASE_NAV
    + """
    <div class="container">
      {% with messages = get_flashed_messages() %}
        {% if messages %}
          <div class="alert alert-info">
            {% for m in messages %}<div>{{ m }}</div>{% endfor %}
          </div>
        {% endif %}
      {% endwith %}

      <!-- ===================================================
           DEMO SCRIPT PART 2.3 – Dynamic report with filters
           DEMO SCRIPT PART 3.2 – Index support for this query
           DEMO SCRIPT PART 3.1 – Show safe prepared statements.
           =================================================== -->
      <h1 class="h3 mb-3">Cap & Roster Report</h1>

      <form method="post" class="card p-3 mb-3 shadow-sm bg-white">
        <div class="row g-2">
          <div class="col-md-4">
            <label class="form-label">Unit</label>
            <select class="form-select" name="unit_id" id="report-unit">
              <option value="">(All)</option>
              {% for u in units %}
                <option value="{{ u['unit_id'] }}" {% if filters.unit_id==u['unit_id'] %}selected{% endif %}>
                  {{ u['name'] }}
                </option>
              {% endfor %}
            </select>
          </div>
          <div class="col-md-4">
            <label class="form-label">Position</label>
            <select class="form-select" name="position_id" id="report-position">
              <option value="">(All)</option>
              {% for p in positions %}
                <option value="{{ p['position_id'] }}" data-unit-id="{{ p['unit_id'] }}"
                    {% if filters.position_id==p['position_id'] %}selected{% endif %}>
                  {{ p['code'] }} – {{ p['description'] }}
                </option>
              {% endfor %}
            </select>
          </div>
          <div class="col-md-4">
            <label class="form-label">Salary range (M)</label>
            <div class="d-flex gap-2">
              <input class="form-control" type="number" step="0.1" min="0" name="min_salary"
                     value="{{ filters.min_salary if filters.min_salary is not none else '' }}" placeholder="Min">
              <input class="form-control" type="number" step="0.1" min="0" name="max_salary"
                     value="{{ filters.max_salary if filters.max_salary is not none else '' }}" placeholder="Max">
            </div>
          </div>
        </div>
        <div class="mt-3 text-end">
          <button class="btn btn-primary" type="submit">Run report</button>
        </div>
      </form>

      <div class="card mb-3 shadow-sm bg-white">
        <div class="card-header"><strong>Summary</strong></div>
        <div class="card-body">
          {% if stats and stats['count_contracts'] > 0 %}
            <p>Contracts: <strong>{{ stats['count_contracts'] }}</strong></p>
            <p>Avg salary: <strong>{{ '%.2f'|format(stats['avg_salary']) }} M</strong></p>
            <p>Avg cap hit: <strong>{{ '%.2f'|format(stats['avg_cap_hit']) }} M</strong></p>
            <p>Total cap hit: <strong>{{ '%.2f'|format(stats['total_cap_hit']) }} M</strong></p>
          {% else %}
            <p class="text-muted mb-0">No contracts match the filters.</p>
          {% endif %}
        </div>
      </div>

      <div class="card shadow-sm bg-white">
        <div class="card-header"><strong>Matching contracts</strong></div>
        <div class="card-body p-0">
          <table class="table table-striped table-hover mb-0 align-middle">
            <thead class="table-light">
              <tr>
                <th>Person</th>
                <th>Position</th>
                <th>Unit</th>
                <th>Years</th>
                <th>Salary (M)</th>
                <th>Cap hit (M)</th>
              </tr>
            </thead>
            <tbody>
              {% for c in contracts %}
                <tr>
                  <td><strong>{{ c['name'] }}</strong> <span class="text-muted small">ID {{ c['person_id'] }}</span></td>
                  <td>{{ c['position_code'] }} – {{ c['position_description'] }}</td>
                  <td>{{ c['unit_name'] }}</td>
                  <td>{{ c['years'] }}</td>
                  <td>{{ '%.1f'|format(c['salary_millions']) }}</td>
                  <td>{{ '%.1f'|format(c['cap_hit_millions']) }}</td>
                </tr>
              {% else %}
                <tr><td colspan="6" class="text-center text-muted">No contracts.</td></tr>
              {% endfor %}
            </tbody>
          </table>
        </div>
      </div>

      <script>
        // Filter positions in the report by unit (client-side convenience)
        function filterReportPositions() {
          var unitSelect = document.getElementById('report-unit');
          var posSelect = document.getElementById('report-position');
          if (!unitSelect || !posSelect) return;
          var unitId = unitSelect.value;
          for (var i = 0; i < posSelect.options.length; i++) {
            var opt = posSelect.options[i];
            if (!opt.value) { opt.hidden = false; continue; }
            var pid = opt.getAttribute('data-unit-id');
            var show = !unitId || (pid === unitId);
            opt.hidden = !show;
          }
        }
        document.addEventListener('DOMContentLoaded', function() {
          var unitSelect = document.getElementById('report-unit');
          if (unitSelect) {
            unitSelect.addEventListener('change', filterReportPositions);
            filterReportPositions();
          }
        });
      </script>
    </div>
  </body>
</html>
"""
)


@app.route("/")
def index():
    return redirect(url_for("list_contracts"))


@app.route("/contracts")
def list_contracts():
    # =========================================================
    # DEMO SCRIPT PART 1.3 / 2.2:
    #   Multi-table JOIN: contracts + people + positions + units
    #   Uses parameterized query pattern even though there are
    #   no dynamic WHERE clauses here.
    # =========================================================
    contracts = query_db(
        """
        SELECT
          c.contract_id,
          c.start_date,
          c.end_date,
          c.years,
          c.salary_millions,
          c.cap_hit_millions,
          p.person_id,
          p.name,
          pos.code AS position_code,
          pos.description AS position_description,
          u.name AS unit_name
        FROM contracts c
          JOIN people p   ON c.person_id   = p.person_id
          JOIN positions pos ON c.position_id = pos.position_id
          JOIN units u    ON c.unit_id     = u.unit_id
        ORDER BY p.person_id
        """
    )
    return render_template_string(CONTRACTS_TEMPLATE, contracts=contracts, active="contracts")


def _validate_contract_payload(form, editing=False):
    """
    Shared validation logic for create/edit contract.

    Stage 1 / Part 1.2 – Business rules:
      • All fields required
      • years must match year difference between start and end
      • end_date >= start date
    Also numeric casting here defuses any attempts to inject via
    "weird" numeric-looking input.
    """
    errors = []
    person_name = None
    person_id = None

    if editing:
        # person_id is a hidden field when editing
        person_id_raw = form.get("person_id")
        if not person_id_raw:
            errors.append("Missing person id.")
        else:
            try:
                person_id = int(person_id_raw)
            except ValueError:
                errors.append("Invalid person id.")
    else:
        person_name = (form.get("person_name") or "").strip()
        if not person_name:
            errors.append("Person name is required.")

    position_id_raw = form.get("position_id")
    unit_id_raw = form.get("unit_id")
    start_date = form.get("start_date")
    end_date = form.get("end_date")
    years_raw = form.get("years")
    salary_raw = form.get("salary_millions")
    cap_hit_raw = form.get("cap_hit_millions")

    if not (position_id_raw and unit_id_raw and start_date and end_date and years_raw and salary_raw and cap_hit_raw):
        errors.append("All fields are required.")

    # convert numeric fields
    try:
        position_id = int(position_id_raw) if position_id_raw else None
        unit_id = int(unit_id_raw) if unit_id_raw else None
        years = int(years_raw) if years_raw else None
        salary_m = float(salary_raw) if salary_raw else None
        cap_hit_m = float(cap_hit_raw) if cap_hit_raw else None
    except (TypeError, ValueError):
        errors.append("Numeric fields must be valid numbers.")
        return None, errors

    # date validation
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
    except Exception:
        errors.append("Dates must be valid (YYYY-MM-DD).")
        return None, errors

    if end_dt < start_dt:
        errors.append("End date cannot be before start date.")
        return None, errors

    year_diff = end_dt.year - start_dt.year
    expected_years = max(1, year_diff)
    if years != expected_years:
        errors.append(f"Years must equal {expected_years} based on start/end date.")
        return None, errors

    payload = {
        "person_name": person_name,
        "person_id": person_id,
        "position_id": position_id,
        "unit_id": unit_id,
        "start_date": start_date,
        "end_date": end_date,
        "years": years,
        "salary_millions": salary_m,
        "cap_hit_millions": cap_hit_m,
    }
    return payload, errors


def _position_unit_matches(conn, position_id, unit_id):
    """
    Helper to enforce contracts.unit_id = positions.unit_id.
    Uses a parameterized SELECT; protects against injection in
    the WHERE clause.
    """
    cur = conn.execute(
        "SELECT unit_id FROM positions WHERE position_id = ?",
        (position_id,),
    )
    row = cur.fetchone()
    return row is not None and int(row["unit_id"]) == int(unit_id)


@app.route("/contracts/new", methods=["GET", "POST"])
def create_contract():
    positions = query_db("SELECT * FROM positions ORDER BY code")
    units = query_db("SELECT * FROM units ORDER BY name")

    if request.method == "POST":
        payload, errors = _validate_contract_payload(request.form, editing=False)
        if errors:
            for e in errors:
                flash(e)
            return render_template_string(
                CONTRACT_FORM_TEMPLATE,
                contract=None,
                person_name=None,
                positions=positions,
                units=units,
                active="contracts",
            )

        # =====================================================
        # DEMO SCRIPT PART 2.2 – Smart person_id allocation
        # DEMO SCRIPT PART 3.3 – Transaction for multi-step:
        #   1) find lowest free person_id
        #   2) insert people row
        #   3) insert contracts row
        #   All in BEGIN IMMEDIATE transaction.
        # =====================================================
        def work(conn):
            # calculate lowest unused positive person_id
            cur = conn.execute("SELECT person_id FROM people ORDER BY person_id")
            existing_ids = [r["person_id"] for r in cur.fetchall()]
            next_id = 1
            for pid in existing_ids:
                if pid == next_id:
                    next_id += 1
                elif pid > next_id:
                    break

            conn.execute(
                "INSERT INTO people (person_id, name) VALUES (?, ?)",
                (next_id, payload["person_name"]),
            )

            if not _position_unit_matches(conn, payload["position_id"], payload["unit_id"]):
                raise ValueError("Selected position does not belong to selected unit.")

            conn.execute(
                """
                INSERT INTO contracts
                  (person_id, position_id, unit_id,
                   start_date, end_date, years,
                   salary_millions, cap_hit_millions)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    next_id,
                    payload["position_id"],
                    payload["unit_id"],
                    payload["start_date"],
                    payload["end_date"],
                    payload["years"],
                    payload["salary_millions"],
                    payload["cap_hit_millions"],
                ),
            )

        try:
            execute_in_transaction(work, isolation="IMMEDIATE")
            flash("Contract created.")
            return redirect(url_for("list_contracts"))
        except Exception as ex:
            flash(str(ex))

    return render_template_string(
        CONTRACT_FORM_TEMPLATE,
        contract=None,
        person_name=None,
        positions=positions,
        units=units,
        active="contracts",
    )


@app.route("/contracts/<int:contract_id>/edit", methods=["GET", "POST"])
def edit_contract(contract_id):
    contract = query_db(
        "SELECT * FROM contracts WHERE contract_id = ?",
        (contract_id,),
        one=True,
    )
    if contract is None:
        flash("Contract not found.")
        return redirect(url_for("list_contracts"))

    positions = query_db("SELECT * FROM positions ORDER BY code")
    units = query_db("SELECT * FROM units ORDER BY name")
    person_row = query_db(
        "SELECT name FROM people WHERE person_id = ?",
        (contract["person_id"],),
        one=True,
    )
    person_name = person_row["name"] if person_row else ""

    if request.method == "POST":
        payload, errors = _validate_contract_payload(request.form, editing=True)
        if errors:
            for e in errors:
                flash(e)
            return render_template_string(
                CONTRACT_FORM_TEMPLATE,
                contract=contract,
                person_name=person_name,
                positions=positions,
                units=units,
                active="contracts",
            )

        def work(conn):
            if not _position_unit_matches(conn, payload["position_id"], payload["unit_id"]):
                raise ValueError("Selected position does not belong to selected unit.")
            conn.execute(
                """
                UPDATE contracts
                SET person_id = ?, position_id = ?, unit_id = ?,
                    start_date = ?, end_date = ?, years = ?,
                    salary_millions = ?, cap_hit_millions = ?
                WHERE contract_id = ?
                """,
                (
                    payload["person_id"],
                    payload["position_id"],
                    payload["unit_id"],
                    payload["start_date"],
                    payload["end_date"],
                    payload["years"],
                    payload["salary_millions"],
                    payload["cap_hit_millions"],
                    contract_id,
                ),
            )

        try:
            execute_in_transaction(work, isolation="IMMEDIATE")
            flash("Contract updated.")
            return redirect(url_for("list_contracts"))
        except Exception as ex:
            flash(str(ex))

    return render_template_string(
        CONTRACT_FORM_TEMPLATE,
        contract=contract,
        person_name=person_name,
        positions=positions,
        units=units,
        active="contracts",
    )


@app.route("/contracts/<int:contract_id>/delete", methods=["POST"])
def delete_contract(contract_id):
    # =========================================================
    # DEMO SCRIPT PART 3.3 – Transactional delete:
    #   delete contract row + maybe delete orphan person.
    # =========================================================
    def work(conn):
        cur = conn.execute(
            "SELECT person_id FROM contracts WHERE contract_id = ?",
            (contract_id,),
        )
        row = cur.fetchone()
        if not row:
            return
        person_id = row["person_id"]

        conn.execute("DELETE FROM contracts WHERE contract_id = ?", (contract_id,))
        cur2 = conn.execute(
            "SELECT COUNT(*) AS cnt FROM contracts WHERE person_id = ?",
            (person_id,),
        )
        remaining = cur2.fetchone()["cnt"]
        if remaining == 0:
            conn.execute("DELETE FROM people WHERE person_id = ?", (person_id,))

    execute_in_transaction(work, isolation="IMMEDIATE")
    flash("Contract deleted.")
    return redirect(url_for("list_contracts"))


@app.route("/units", methods=["GET", "POST"])
def list_units():
    if request.method == "POST":
        unit_name = (request.form.get("unit_name") or "").strip()
        if not unit_name:
            flash("Unit name is required.")
        else:
            try:
                execute_db(
                    "INSERT INTO units (name) VALUES (?)",
                    (unit_name,),
                )
                flash("Unit added.")
            except sqlite3.IntegrityError:
                flash("Unit name must be unique.")

    units = query_db("SELECT * FROM units ORDER BY name")
    return render_template_string(UNITS_TEMPLATE, units=units, active="units")


@app.route("/units/<int:unit_id>/edit", methods=["GET", "POST"])
def edit_unit(unit_id):
    unit = query_db("SELECT * FROM units WHERE unit_id = ?", (unit_id,), one=True)
    if unit is None:
        flash("Unit not found.")
        return redirect(url_for("list_units"))

    if request.method == "POST":
        unit_name = (request.form.get("unit_name") or "").strip()
        if not unit_name:
            flash("Unit name is required.")
        else:
            try:
                execute_db(
                    "UPDATE units SET name = ? WHERE unit_id = ?",
                    (unit_name, unit_id),
                )
                flash("Unit updated.")
                return redirect(url_for("list_units"))
            except sqlite3.IntegrityError:
                flash("Unit name must be unique.")

    return render_template_string(UNIT_EDIT_TEMPLATE, unit=unit, active="units")


@app.route("/units/<int:unit_id>/delete", methods=["POST"])
def delete_unit(unit_id):
    # =========================================================
    # DEMO SCRIPT PART 2.2 / 3.3 – Application-level cascade:
    #   delete contracts for this unit, delete orphan people,
    #   delete positions in this unit, delete the unit itself.
    #   All wrapped in a single IMMEDIATE transaction so
    #   concurrent users don't see half-done cascades.
    # =========================================================
    def work(conn):
        # find affected people
        cur = conn.execute(
            "SELECT DISTINCT person_id FROM contracts WHERE unit_id = ?",
            (unit_id,),
        )
        person_ids = [r["person_id"] for r in cur.fetchall()]

        conn.execute("DELETE FROM contracts WHERE unit_id = ?", (unit_id,))

        for pid in person_ids:
            cur2 = conn.execute(
                "SELECT COUNT(*) AS cnt FROM contracts WHERE person_id = ?",
                (pid,),
            )
            if cur2.fetchone()["cnt"] == 0:
                conn.execute("DELETE FROM people WHERE person_id = ?", (pid,))

        conn.execute("DELETE FROM positions WHERE unit_id = ?", (unit_id,))
        conn.execute("DELETE FROM units WHERE unit_id = ?", (unit_id,))

    execute_in_transaction(work, isolation="IMMEDIATE")
    flash("Unit and related data deleted.")
    return redirect(url_for("list_units"))


@app.route("/positions", methods=["GET", "POST"])
def list_positions():
    if request.method == "POST":
        code = (request.form.get("code") or "").strip()
        description = (request.form.get("description") or "").strip()
        unit_id_raw = request.form.get("unit_id")
        if not (code and description and unit_id_raw):
            flash("All fields are required.")
        else:
            try:
                unit_id = int(unit_id_raw)
            except ValueError:
                flash("Invalid unit.")
            else:
                try:
                    execute_db(
                        "INSERT INTO positions (code, description, unit_id) VALUES (?, ?, ?)",
                        (code, description, unit_id),
                    )
                    flash("Position added.")
                except sqlite3.IntegrityError:
                    flash("Position code must be unique.")

    positions = query_db(
        """
        SELECT p.position_id, p.code, p.description, p.unit_id,
               u.name AS unit_name
        FROM positions p
          JOIN units u ON p.unit_id = u.unit_id
        ORDER BY u.name, p.code
        """
    )
    units = query_db("SELECT * FROM units ORDER BY name")
    return render_template_string(
        POSITIONS_TEMPLATE,
        positions=positions,
        units=units,
        active="positions",
    )


@app.route("/positions/<int:position_id>/edit", methods=["GET", "POST"])
def edit_position(position_id):
    position = query_db(
        "SELECT * FROM positions WHERE position_id = ?",
        (position_id,),
        one=True,
    )
    if position is None:
        flash("Position not found.")
        return redirect(url_for("list_positions"))

    units = query_db("SELECT * FROM units ORDER BY name")

    if request.method == "POST":
        code = (request.form.get("code") or "").strip()
        description = (request.form.get("description") or "").strip()
        unit_id_raw = request.form.get("unit_id")
        if not (code and description and unit_id_raw):
            flash("All fields are required.")
        else:
            try:
                unit_id = int(unit_id_raw)
            except ValueError:
                flash("Invalid unit.")
            else:
                def work(conn):
                    conn.execute(
                        "UPDATE positions SET code = ?, description = ?, unit_id = ? WHERE position_id = ?",
                        (code, description, unit_id, position_id),
                    )
                    # Keep contracts.unit_id in sync if position moves units.
                    conn.execute(
                        "UPDATE contracts SET unit_id = ? WHERE position_id = ?",
                        (unit_id, position_id),
                    )

                try:
                    execute_in_transaction(work, isolation="IMMEDIATE")
                    flash("Position updated.")
                    return redirect(url_for("list_positions"))
                except sqlite3.IntegrityError:
                    flash("Position code must be unique.")

    return render_template_string(
        POSITION_EDIT_TEMPLATE,
        position=position,
        units=units,
        active="positions",
    )


@app.route("/positions/<int:position_id>/delete", methods=["POST"])
def delete_position(position_id):
    # =========================================================
    # DEMO SCRIPT PART 2.2 / 3.3 – Cascade delete by position:
    #   remove contracts, delete orphan people, then position.
    # =========================================================
    def work(conn):
        cur = conn.execute(
            "SELECT DISTINCT person_id FROM contracts WHERE position_id = ?",
            (position_id,),
        )
        person_ids = [r["person_id"] for r in cur.fetchall()]

        conn.execute("DELETE FROM contracts WHERE position_id = ?", (position_id,))

        for pid in person_ids:
            cur2 = conn.execute(
                "SELECT COUNT(*) AS cnt FROM contracts WHERE person_id = ?",
                (pid,),
            )
            if cur2.fetchone()["cnt"] == 0:
                conn.execute("DELETE FROM people WHERE person_id = ?", (pid,))

        conn.execute("DELETE FROM positions WHERE position_id = ?", (position_id,))

    execute_in_transaction(work, isolation="IMMEDIATE")
    flash("Position and related data deleted.")
    return redirect(url_for("list_positions"))


@app.route("/report", methods=["GET", "POST"])
def report():
    units = query_db("SELECT * FROM units ORDER BY name")
    positions = query_db("SELECT * FROM positions ORDER BY code")

    unit_id = None
    position_id = None
    min_salary = None
    max_salary = None

    if request.method == "POST":
        unit_id_raw = request.form.get("unit_id") or None
        position_id_raw = request.form.get("position_id") or None
        min_salary_raw = request.form.get("min_salary")
        max_salary_raw = request.form.get("max_salary")

        # All parsing is explicit; any non-numeric values just become None.
        try:
            unit_id = int(unit_id_raw) if unit_id_raw else None
        except ValueError:
            unit_id = None
        try:
            position_id = int(position_id_raw) if position_id_raw else None
        except ValueError:
            position_id = None
        try:
            min_salary = float(min_salary_raw) if min_salary_raw else None
        except ValueError:
            min_salary = None
        try:
            max_salary = float(max_salary_raw) if max_salary_raw else None
        except ValueError:
            max_salary = None

    conditions = []
    params = []

    # =========================================================
    # DEMO SCRIPT PART 3.1 – Show how dynamic WHERE clause is
    #   built safely:
    #     • We only append static column expressions like
    #         "c.unit_id = ?" or "c.salary_millions >= ?"
    #     • The actual values go into the params list and are
    #       bound as prepared-statement parameters.
    #   There is *no* way for the user to inject raw SQL here.
    # =========================================================
    if unit_id is not None:
        conditions.append("c.unit_id = ?")
        params.append(unit_id)
    if position_id is not None:
        conditions.append("c.position_id = ?")
        params.append(position_id)
    if min_salary is not None:
        conditions.append("c.salary_millions >= ?")
        params.append(min_salary)
    if max_salary is not None:
        conditions.append("c.salary_millions <= ?")
        params.append(max_salary)

    where_clause = ""
    if conditions:
        where_clause = " WHERE " + " AND ".join(conditions)

    contracts_query = (
        """
        SELECT
          c.contract_id,
          c.years,
          c.salary_millions,
          c.cap_hit_millions,
          p.person_id,
          p.name,
          pos.code AS position_code,
          pos.description AS position_description,
          u.name AS unit_name
        FROM contracts c
          JOIN people p   ON c.person_id   = p.person_id
          JOIN positions pos ON c.position_id = pos.position_id
          JOIN units u    ON c.unit_id     = u.unit_id
        """
        + where_clause
        + " ORDER BY p.person_id"
    )

    stats_query = (
        """
        SELECT
          COUNT(*) AS count_contracts,
          AVG(c.salary_millions) AS avg_salary,
          AVG(c.cap_hit_millions) AS avg_cap_hit,
          SUM(c.cap_hit_millions) AS total_cap_hit
        FROM contracts c
          JOIN people p ON c.person_id = p.person_id
        """
        + where_clause
    )

    contracts = query_db(contracts_query, params)
    stats = query_db(stats_query, params, one=True)

    class Filters:
        pass

    filters = Filters()
    filters.unit_id = unit_id
    filters.position_id = position_id
    filters.min_salary = min_salary
    filters.max_salary = max_salary

    return render_template_string(
        REPORT_TEMPLATE,
        units=units,
        positions=positions,
        contracts=contracts,
        stats=stats,
        filters=filters,
        active="report",
    )


# Initialize DB schema + indexes whenever the module is imported.
# Safe because all CREATE TABLEs are IF NOT EXISTS.
init_db()

if __name__ == "__main__":
    # Local dev only – App Engine will run gunicorn with app:app
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=True)
