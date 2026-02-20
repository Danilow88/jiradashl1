# Jira Utilities

A command-line tool for interacting with Cornelis Networks' Jira instance. This utility provides various functions for querying projects, tickets, releases, and performing bulk updates.

## Table of Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [Authentication](#authentication)
- [Usage](#usage)
- [Command Reference](#command-reference)
- [Examples](#examples)
- [Output Formats](#output-formats)
- [Date Filters](#date-filters)
- [Dashboard Management](#dashboard-management)
- [L1 Dashboard](#l1-dashboard)
- [Draw.io Diagram Generation](#drawio-diagram-generation)

## Requirements

- Python 3.9 or higher
- Access to Cornelis Networks Jira instance
- Jira API token

### Python Dependencies

- `jira` - Jira API client library
- `python-dotenv` - Load environment variables from .env file
- `requests` - HTTP library for API calls

## Installation

1. Clone or download this repository

2. Create and activate a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

   Or install manually:
   ```bash
   pip install jira python-dotenv requests
   ```

## Authentication

This script uses Jira API tokens for authentication. You need to configure your credentials before running.

### Option 1: Using a .env File (Recommended)

The script automatically loads credentials from a `.env` file in the project directory.

1. Copy the example file:
   ```bash
   cp .env.example .env
   ```

2. Edit `.env` and fill in your credentials:
   ```bash
   JIRA_EMAIL=your.email@cornelisnetworks.com
   JIRA_API_TOKEN=your_api_token_here
   ```

3. Generate an API token at: https://id.atlassian.com/manage-profile/security/api-tokens

> **Note:** The `.env` file is gitignored for security - your credentials will not be committed to version control.

### Option 2: Using Environment Variables

Alternatively, you can set environment variables directly:

```bash
export JIRA_EMAIL="your.email@cornelisnetworks.com"
export JIRA_API_TOKEN="your_api_token_here"
```

> **IMPORTANT:** Never commit credentials to version control.

## Usage

Basic syntax:
```bash
python jira_utils.py [OPTIONS]
```

With virtual environment:
```bash
source venv/bin/activate && python jira_utils.py [OPTIONS]
```

> **Note:** The script automatically loads credentials from `.env` - no need to source it manually.

## Command Reference

### Global Options

| Option | Description |
|--------|-------------|
| `-h, --help` | Show help message and exit |
| `-v, --verbose` | Enable verbose/debug output to stdout |
| `-q, --quiet` | Minimal stdout output |
| `--show-jql` | Print the equivalent JQL statement and save to `jql.txt` |

### Project Information

| Option | Description |
|--------|-------------|
| `--list` | List all available Jira projects |
| `--project KEY` | Specify project key to operate on (required for most commands; optional for `--get-children` and `--get-related` when a ticket key is provided) |
| `--get-workflow` | Display workflow statuses for the specified project |
| `--get-issue-types` | Display issue types for the specified project |
| `--get-fields [TYPE ...]` | Display create/edit/transition fields. Optionally filter by issue types |
| `--get-versions` | Display versions (releases) with detailed info |
| `--get-components` | List all components for the project |
| `--get-children KEY` | Display full child hierarchy for the given ticket (recurses via parent links; project is optional) |
| `--get-related KEY` | Display linked issues for the given ticket; add `--hierarchy [DEPTH]` to recurse across links (project is optional) |
| `--hierarchy [DEPTH]` | When used with `--get-related`, traverse linked issues recursively. Omit DEPTH or use `-1` for unlimited depth; use a positive integer to limit depth (e.g., 1 = direct links only, 2 = links of links). |

### Release Management

| Option | Description |
|--------|-------------|
| `--releases [PATTERN]` | List releases for the project. Optional glob pattern (e.g., `"12.*"`) |
| `--release-tickets RELEASE` | Get tickets associated with a specific release (supports glob patterns like `"12.1*"`) |
| `--no-release` | Get tickets with no release assigned |

### Ticket Queries

| Option | Description |
|--------|-------------|
| `--total` | Show ticket count |
| `--get-tickets` | Get tickets |
| `--jql QUERY` | Run a custom JQL query |

### Query Filters

| Option | Description |
|--------|-------------|
| `--issue-types TYPE [TYPE ...]` | Filter by issue types (e.g., Bug Story Task Sub-task) |
| `--status STATUS [STATUS ...]` | Filter by status (case-insensitive). Multiple statuses allowed. Use `^` prefix to exclude (e.g., `^Closed`) |
| `--date FILTER` | Date filter (see [Date Filters](#date-filters)) |
| `--limit N` | Limit the number of tickets retrieved |

### Output Options

| Option | Description |
|--------|-------------|
| `--dump-file FILE` | Output filename for dumping tickets (extension added automatically) |
| `--dump-format FORMAT` | Output format: `csv` (default) or `json` |

### Bulk Update Operations

| Option | Description |
|--------|-------------|
| `--bulk-update` | Perform bulk update on tickets from input file |
| `--input-file FILE` | Input CSV file containing ticket keys for bulk update |
| `--set-release RELEASE` | Set the release/fixVersion on tickets |
| `--remove-release` | Remove all releases from tickets |
| `--transition STATUS` | Transition tickets to the specified status |
| `--assign USER` | Assign tickets to user (email or "unassigned") |
| `--dry-run` | Preview changes without applying them (default: True) |
| `--execute` | Execute the bulk update (disables dry-run) |
| `--max-updates N` | Maximum number of tickets to update |

## Examples

### List Projects
```bash
python jira_utils.py --list
```

### Project Information
```bash
# Show workflow statuses
python jira_utils.py --project PROJ --get-workflow

# Show issue types
python jira_utils.py --project PROJ --get-issue-types

# Show all fields for all issue types
python jira_utils.py --project PROJ --get-fields

# Show fields for specific issue types
python jira_utils.py --project PROJ --get-fields --issue-types Bug Task

# Show versions/releases
python jira_utils.py --project PROJ --get-versions
# Show components
python jira_utils.py --project PROJ --get-components

# Show full child hierarchy for a ticket (project optional when key is provided)
python jira_utils.py --get-children PROJ-123

# Get linked issues (direct links only)
python jira_utils.py --get-related PROJ-123

# Recursively traverse linked issues (unlimited depth)
python jira_utils.py --get-related PROJ-123 --hierarchy

# Traverse linked issues up to depth 2
python jira_utils.py --get-related PROJ-123 --hierarchy 2

# Scope hierarchy to a project explicitly (optional)
python jira_utils.py --project PROJ --get-children PROJ-123

# Limit hierarchy results to 20 issues and dump to JSON
python jira_utils.py --get-children PROJ-123 --limit 20 --dump-file children --dump-format json

# Combine linked traversal with limit and JSON dump (depth-limited)
python jira_utils.py --get-related PROJ-123 --hierarchy 3 --limit 50 --dump-file related --dump-format json

# Combine hierarchy with limit and CSV dump
python jira_utils.py --get-children PROJ-123 --limit 50 --dump-file children

# Show only components with tickets created in the last 2 weeks
python jira_utils.py --project PROJ --get-components --date week

# Show components with activity this month
python jira_utils.py --project PROJ --get-components --date month

# Dump components to file
python jira_utils.py --project PROJ --get-components --dump-file components

# Dump active components with ticket counts
python jira_utils.py --project PROJ --get-components --date week --dump-file active_components

# Show all project info at once
python jira_utils.py --project PROJ --get-workflow --get-issue-types --get-fields --get-versions --get-components
```

### Release Queries
```bash
# List all releases
python jira_utils.py --project PROJ --releases

# List releases matching a pattern
python jira_utils.py --project PROJ --releases "12.*"

# Dump releases to file
python jira_utils.py --project PROJ --releases --dump-file all_releases
python jira_utils.py --project PROJ --releases "12.*" --dump-file 12x_releases

# List releases with exclusions (12.* but not containing "Samples")
python jira_utils.py --project PROJ --releases "12.*,^*Samples*"

# Multiple exclusions
python jira_utils.py --project PROJ --releases "12.*,^*Samples*,^*Test*"

# Multiple includes with exclusions
python jira_utils.py --project PROJ --releases "12.*,13.*,^*Samples*"

# Get tickets for a specific release
python jira_utils.py --project PROJ --release-tickets "v1.0"

# Get tickets for all releases matching a pattern
python jira_utils.py --project PROJ --releases "12.*" --get-tickets

# Get tickets for releases with exclusions
python jira_utils.py --project PROJ --releases "12.*,^*Samples*" --get-tickets

# Get Open tickets for 12.x releases and dump to file
python jira_utils.py --project PROJ --releases "12.*" --get-tickets --status Open --dump-file 12x_tickets

# Get tickets with no release assigned
python jira_utils.py --project PROJ --no-release

# Get Open Bugs with no release
python jira_utils.py --project PROJ --no-release --issue-types Bug --status Open
```

### Ticket Counts
```bash
# Total ticket count
python jira_utils.py --project PROJ --total

# Count for specific issue types
python jira_utils.py --project PROJ --total --issue-types Bug Task

# Count for Open tickets
python jira_utils.py --project PROJ --total --status Open

# Count for Open/In Progress Bugs
python jira_utils.py --project PROJ --total --issue-types Bug --status Open "In Progress"

# Count for tickets created this week
python jira_utils.py --project PROJ --total --date week
```

### Get Tickets
```bash
# Get all tickets
python jira_utils.py --project PROJ --get-tickets

# Get up to 50 Open Bugs
python jira_utils.py --project PROJ --get-tickets --issue-types Bug --status Open --limit 50

# Get Closed tickets from this month
python jira_utils.py --project PROJ --get-tickets --status Closed --date month

# Get tickets created in date range
python jira_utils.py --project PROJ --get-tickets --date 01-01-2024:12-31-2024

# Dump all tickets to CSV
python jira_utils.py --project PROJ --get-tickets --dump-file tickets

# Dump Open Bugs to JSON
python jira_utils.py --project PROJ --get-tickets --issue-types Bug --status Open --dump-file bugs --dump-format json
```

### Custom JQL Queries
```bash
# Run a custom JQL query
python jira_utils.py --jql "project = PROJ AND status = Open"

# JQL with limit
python jira_utils.py --jql "assignee = currentUser() AND status != Done" --limit 20

# Dump JQL results to CSV
python jira_utils.py --jql "project = PROJ" --dump-file results --dump-format csv
```

### Show Equivalent JQL
```bash
# Show the JQL that would be generated
python jira_utils.py --project PROJ --get-tickets --issue-types Bug --status Open --show-jql

# The JQL is displayed at the end and saved to jql.txt
```

### Bulk Updates
```bash
# Step 1: Find tickets and dump to CSV
python jira_utils.py --jql "project = PROJ AND fixVersion is EMPTY" --dump-file orphans

# Step 2: Preview bulk update (dry-run is default)
python jira_utils.py --bulk-update --input-file orphans.csv --set-release "12.1.1.x"

# Step 3: Execute bulk update
python jira_utils.py --bulk-update --input-file orphans.csv --set-release "12.1.1.x" --execute

# Transition tickets to Closed
python jira_utils.py --bulk-update --input-file tickets.csv --transition "Closed" --execute

# Assign tickets to a user
python jira_utils.py --bulk-update --input-file tickets.csv --assign "user@email.com" --execute

# Remove releases from tickets
python jira_utils.py --bulk-update --input-file tickets.csv --remove-release --execute

# Limit number of updates
python jira_utils.py --bulk-update --input-file tickets.csv --set-release "v2.0" --max-updates 10 --execute
```

## Output Formats

### CSV Format
The default output format. Includes columns:
- Key
- Project
- Type
- Status
- Priority
- Summary
- Assignee
- Reporter
- Created
- Updated
- Resolved
- Fix Version
- Affects Version (for bugs)

### Console Output
The console table displays:
- Key
- Type
- Status
- Priority
- Created
- Updated
- Fix Version
- Assignee
- Summary

### JSON Format
Use `--dump-format json` for JSON output. Each ticket is a JSON object with full field data.

## Release Pattern Syntax

The `--releases` option supports glob patterns with exclusions:

| Pattern | Description |
|---------|-------------|
| `*` | Match all releases |
| `12.*` | Match releases starting with "12." |
| `*beta*` | Match releases containing "beta" |
| `12.*,^*Samples*` | Match 12.* but exclude those containing "Samples" |
| `12.*,^*Samples*,^*Test*` | Multiple exclusions |
| `12.*,13.*` | Match either 12.* or 13.* |
| `12.*,13.*,^*Samples*` | Multiple includes with exclusion |

### Pattern Rules

- Use `*` as a wildcard (matches any characters)
- Use `?` to match a single character
- Separate multiple patterns with commas
- Prefix exclusion patterns with `^`
- Exclusions apply to all include patterns

### Examples

```bash
# All 12.x releases except samples
python jira_utils.py --project PROJ --releases "12.*,^*Samples*"

# All 12.x and 13.x releases except test releases
python jira_utils.py --project PROJ --releases "12.*,13.*,^*Test*,^*test*"

# Get tickets for filtered releases
python jira_utils.py --project PROJ --releases "12.*,^*Samples*" --get-tickets --dump-file filtered_tickets
```

## Date Filters

The `--date` option supports the following filters:

| Filter | Description |
|--------|-------------|
| `today` | Tickets created today |
| `week` | Tickets created in the last 7 days |
| `month` | Tickets created in the last 30 days |
| `year` | Tickets created in the last 365 days |
| `all` | All tickets (no date filter) |
| `MM-DD-YYYY:MM-DD-YYYY` | Tickets created within date range |

### Date Range Examples
```bash
# Tickets created in January 2024
python jira_utils.py --project PROJ --get-tickets --date 01-01-2024:01-31-2024

# Tickets created in Q1 2024
python jira_utils.py --project PROJ --get-tickets --date 01-01-2024:03-31-2024
```

## Logging

The script maintains two types of output:

1. **stdout** - Clean, formatted tables and user-facing messages
2. **Log file** (`jira_utils.log`) - Detailed logging with timestamps, function names, and line numbers

Use `-v` (verbose) for debug-level output to stdout, or `-q` (quiet) for minimal output.

## Files Generated

| File | Description |
|------|-------------|
| `jira_utils.log` | Detailed execution log (overwritten each run) |
| `jql.txt` | Last JQL query when using `--show-jql` |
| `*.csv` / `*.json` | Ticket dumps when using `--dump-file` |

## Configuration Files

| File | Description |
|------|-------------|
| `.env` | Your local credentials (gitignored, never committed) |
| `.env.example` | Template showing required environment variables |
| `requirements.txt` | Python package dependencies |

## Error Handling

The script handles common errors gracefully:

- **Missing credentials** - Prompts to set environment variables
- **Invalid project** - Lists available projects
- **Invalid issue type/status** - Shows valid options
- **Rate limiting** - Automatic retry with backoff
- **Network errors** - Clear error messages

## Dashboard Management

Dashboard management features allow you to create, update, delete, and copy Jira dashboards, as well as manage gadgets on those dashboards.

### Dashboard Options

| Option | Description |
|--------|-------------|
| `--dashboards` | List all accessible dashboards |
| `--dashboard ID` | Get dashboard details by ID |
| `--owner USER` | Filter dashboards by owner (use "me" for current user) |
| `--shared` | Show only dashboards shared with current user |
| `--create-dashboard NAME` | Create a new dashboard with the specified name |
| `--update-dashboard ID` | Update an existing dashboard by ID |
| `--delete-dashboard ID` | Delete a dashboard by ID |
| `--copy-dashboard ID` | Copy/clone an existing dashboard |
| `--description TEXT` | Dashboard description (for create/update) |
| `--name NAME` | New name for update or copy operations |
| `--share-permissions JSON` | Share permissions as JSON array |
| `--force` | Skip confirmation prompt for delete operations |

### Gadget Management Options

| Option | Description |
|--------|-------------|
| `--gadgets DASHBOARD_ID` | List all gadgets on a dashboard |
| `--add-gadget MODULE_KEY` | Add a gadget to a dashboard (requires `--dashboard`) |
| `--remove-gadget GADGET_ID` | Remove a gadget from a dashboard (requires `--dashboard`) |
| `--update-gadget GADGET_ID` | Update a gadget on a dashboard (requires `--dashboard`) |
| `--position ROW,COL` | Gadget position (row and column) |
| `--color COLOR` | Gadget chrome color |
| `--gadget-properties JSON` | Gadget properties as JSON object |

#### Available Gadget Colors

- `blue`
- `red`
- `yellow`
- `green`
- `cyan`
- `purple`
- `gray`
- `white`

### Dashboard Examples

#### Listing Dashboards

```bash
# List all accessible dashboards
python jira_utils.py --dashboards

# Get details for a specific dashboard
python jira_utils.py --dashboard 10001

# List dashboards owned by current user
python jira_utils.py --dashboards --owner me

# List dashboards owned by a specific user
python jira_utils.py --dashboards --owner "user@cornelisnetworks.com"

# List dashboards shared with current user
python jira_utils.py --dashboards --shared
```

#### Creating Dashboards

```bash
# Create a simple dashboard
python jira_utils.py --create-dashboard "My Dashboard"

# Create a dashboard with description
python jira_utils.py --create-dashboard "Sprint Dashboard" --description "Dashboard for tracking sprint progress"

# Create a dashboard with share permissions
python jira_utils.py --create-dashboard "Team Dashboard" --share-permissions '[{"type": "project", "projectId": "10000"}]'
```

#### Updating Dashboards

```bash
# Update dashboard name
python jira_utils.py --update-dashboard 10001 --name "New Dashboard Name"

# Update dashboard description
python jira_utils.py --update-dashboard 10001 --description "Updated description"

# Update both name and description
python jira_utils.py --update-dashboard 10001 --name "Renamed Dashboard" --description "New description"
```

#### Deleting Dashboards

```bash
# Delete a dashboard (with confirmation prompt)
python jira_utils.py --delete-dashboard 10001

# Delete a dashboard without confirmation
python jira_utils.py --delete-dashboard 10001 --force
```

#### Copying Dashboards

```bash
# Copy a dashboard with a new name
python jira_utils.py --copy-dashboard 10001 --name "Copy of Dashboard"

# Copy a dashboard with new name and description
python jira_utils.py --copy-dashboard 10001 --name "My Copy" --description "Copied dashboard for personal use"
```

### Gadget Examples

> **Note:** Gadget management is primarily supported on Jira Cloud. Some features may not be available on Jira Server/Data Center.

#### Listing Gadgets

```bash
# List all gadgets on a dashboard
python jira_utils.py --gadgets 10001
```

#### Adding Gadgets

```bash
# Add a filter results gadget
python jira_utils.py --dashboard 10001 --add-gadget "com.atlassian.jira.gadgets:filter-results-gadget"

# Add a gadget at a specific position
python jira_utils.py --dashboard 10001 --add-gadget "com.atlassian.jira.gadgets:pie-chart-gadget" --position 0,1

# Add a gadget with a specific color
python jira_utils.py --dashboard 10001 --add-gadget "com.atlassian.jira.gadgets:assigned-to-me-gadget" --color blue

# Add a gadget with properties
python jira_utils.py --dashboard 10001 --add-gadget "com.atlassian.jira.gadgets:filter-results-gadget" --gadget-properties '{"filterId": "10000", "num": "10"}'
```

#### Updating Gadgets

```bash
# Update gadget position
python jira_utils.py --dashboard 10001 --update-gadget 10050 --position 1,0

# Update gadget color
python jira_utils.py --dashboard 10001 --update-gadget 10050 --color green

# Update gadget properties
python jira_utils.py --dashboard 10001 --update-gadget 10050 --gadget-properties '{"num": "20"}'
```

#### Removing Gadgets

```bash
# Remove a gadget from a dashboard
python jira_utils.py --dashboard 10001 --remove-gadget 10050
```

### Common Gadget Module Keys

| Module Key | Description |
|------------|-------------|
| `com.atlassian.jira.gadgets:filter-results-gadget` | Displays issues from a saved filter |
| `com.atlassian.jira.gadgets:pie-chart-gadget` | Pie chart visualization of issues |
| `com.atlassian.jira.gadgets:created-vs-resolved-gadget` | Chart comparing created vs resolved issues |
| `com.atlassian.jira.gadgets:assigned-to-me-gadget` | Shows issues assigned to current user |
| `com.atlassian.jira.gadgets:activity-stream-gadget` | Activity stream showing recent updates |

### Share Permissions Format

Share permissions are specified as a JSON array. Common permission types:

```json
[
  {"type": "user", "user": {"accountId": "user-account-id"}},
  {"type": "project", "projectId": "10000"},
  {"type": "group", "group": {"name": "jira-users"}},
  {"type": "loggedin"},
  {"type": "global"}
]
```

| Type | Description |
|------|-------------|
| `user` | Share with a specific user |
| `project` | Share with all users in a project |
| `group` | Share with a Jira group |
| `loggedin` | Share with all logged-in users |
| `global` | Share with everyone (public) |

## L1 Dashboard

The `l1_dashboard.py` script shows issues filtered by **Support Level L1**, using the same Jira credentials as `jira_utils.py`. It displays the columns: **Reporter**, **Time to resolution**, **Time to first response**, **Assignee**, **Request Type**.

### JQL used by default

```text
(component not in (gadgets-devices) OR component IS EMPTY) AND "Support Level - ITOPS" = "L1" ORDER BY "Time to resolution" ASC
```

### Usage

```bash
# Run with default JQL (L1 filter, ordered by Time to resolution)
python l1_dashboard.py

# Limit number of issues
python l1_dashboard.py --limit 50

# Export to HTML dashboard
python l1_dashboard.py --html dashboard.html

# Custom JQL
python l1_dashboard.py --jql 'project = PROJ AND "Support Level - ITOPS" = "L1"'
```

### Options

| Option | Description |
|--------|-------------|
| `--jql QUERY` | Custom JQL (default: L1 filter above) |
| `--limit N` | Maximum issues to fetch |
| `--html FILE` | Write HTML dashboard to FILE |
| `--no-key` | Hide Key column in text output |

### App com interface gráfica (Tkinter)

Para usar o dashboard como aplicativo com janela e tabela:

```bash
source venv/bin/activate
python l1_dashboard_app.py
```

- **JQL**: edite a consulta na caixa de texto e clique em **Buscar**.
- **Limite**: número máximo de issues (ex.: 50).
- **Duplo clique** em uma linha abre o issue no navegador (Jira).
- **Exportar HTML**: salva a tabela atual em um arquivo HTML.

Requer as mesmas credenciais (`.env`: `JIRA_EMAIL`, `JIRA_API_TOKEN`). Tkinter vem com o Python; não é necessário instalar nada extra.

**Se o app Tkinter falhar no macOS** (por exemplo: "macOS 15 (1507) or later required" ou "Tkinter não encontrado"), use a **versão web**:

### Versão web (Flask) – sem Tkinter

Roda no navegador e evita problemas de Tk no macOS.

```bash
source venv/bin/activate
pip install flask
python l1_dashboard_web.py
```

O navegador abre em **http://127.0.0.1:5000/**. Use a mesma JQL, **Carregar meus filtros**, **Projeto** / **Status** e **Buscar**; **Exportar HTML** baixa o arquivo.

## Draw.io Diagram Generation

The `drawio_utilities.py` script generates draw.io diagrams from Jira hierarchy CSV exports. This creates visual dependency maps that can be opened in draw.io desktop, VS Code, or the online editor.

### Installation

No additional dependencies required - uses Python standard library only.

### Usage

```bash
python drawio_utilities.py --create-map INPUT_CSV [--output FILE] [--title TITLE]
```

### Options

| Option | Description |
|--------|-------------|
| `--create-map CSV_FILE` | Create a draw.io diagram from a Jira hierarchy CSV file |
| `--output FILE`, `-o` | Output filename for the .drawio file (default: input file with .drawio extension) |
| `--title TITLE`, `-t` | Title for the diagram (default: derived from root ticket) |
| `-v`, `--verbose` | Enable verbose output |
| `-q`, `--quiet` | Minimal output |

### Workflow

1. **Export hierarchy from Jira:**
   ```bash
   python jira_utils.py --get-related STL-74071 --hierarchy --dump-file tickets
   ```

2. **Generate draw.io diagram:**
   ```bash
   python drawio_utilities.py --create-map tickets.csv
   ```

3. **Open the diagram:**
   - draw.io desktop app: https://www.diagrams.net/
   - VS Code with Draw.io Integration extension
   - Online at https://app.diagrams.net/

### Examples

```bash
# Basic usage - creates tickets.drawio from tickets.csv
python drawio_utilities.py --create-map tickets.csv

# Custom output filename
python drawio_utilities.py --create-map tickets.csv --output release-deps.drawio

# Custom title
python drawio_utilities.py --create-map tickets.csv --title "Release 12.2 Dependencies"

# Full workflow example
python jira_utils.py --get-related STL-74071 --hierarchy 2 --dump-file deps
python drawio_utilities.py --create-map deps.csv --output release-12.2-map.drawio
```

### Color Coding

The diagram uses color coding to indicate relationship types:

| Link Type | Box Fill | Border Color |
|-----------|----------|--------------|
| Root ticket (depth 0) | Light green | Gray |
| `is blocked by` / `blocks` | Light red | Red |
| `relates to` | Light blue | Blue |
| `is caused by` / `causes` | Light yellow | Yellow |
| `is cloned by` / `clones` | Light orange | Orange |
| Other link types | White | Gray |

### Diagram Layout

- **Pyramid structure**: Root ticket at top, children arranged by depth level below
- **Clickable links**: Each ticket box links to the Jira ticket URL
- **Connection lines**: Lines between boxes show the relationship type
- **Labels**: Each box shows the ticket key and truncated summary

## Author

John Macdonald

## License

Internal use - Cornelis Networks