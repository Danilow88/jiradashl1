##########################################################################################
#
# Script name: drawio_utilities.py
#
# Description: Utilities for generating draw.io diagrams from Jira hierarchy CSV exports.
#
# Author: John Macdonald
#
# Usage:
#   python drawio_utilities.py --create-map input.csv --output diagram.drawio
#
##########################################################################################

import argparse
import logging
import sys
import os
import csv
import xml.etree.ElementTree as ET
from datetime import date
from urllib.parse import quote

# ****************************************************************************************
# Global data and configuration
# ****************************************************************************************

# Jira configuration - used for building ticket URLs
JIRA_URL = 'https://cornelisnetworks.atlassian.net'

# Logging config
log = logging.getLogger(os.path.basename(sys.argv[0]))
log.setLevel(logging.DEBUG)

# File handler for logging
fh = logging.FileHandler('drawio_utilities.log', mode='w')
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)-15s [%(funcName)25s:%(lineno)-5s] %(levelname)-8s %(message)s')
fh.setFormatter(formatter)
log.addHandler(fh)

log.debug(f'Global data and configuration for this script...')
log.debug(f'JIRA_URL: {JIRA_URL}')

# Output control - set by handle_args()
_quiet_mode = False


def output(message=''):
    '''
    Print user-facing output, respecting quiet mode.
    Always logs to file regardless of quiet mode.

    Input:
        message: String to output (default empty for blank line).

    Output:
        None; prints to stdout unless in quiet mode.

    Side Effects:
        Always logs message to log file at INFO level.
    '''
    # Log to file only (bypass stdout handler by writing directly to file handler)
    if message:
        record = logging.LogRecord(
            name=log.name,
            level=logging.INFO,
            pathname=__file__,
            lineno=0,
            msg=f'OUTPUT: {message}',
            args=(),
            exc_info=None,
            func='output'
        )
        fh.emit(record)

    # Print to stdout unless quiet mode
    if not _quiet_mode:
        print(message)


# ****************************************************************************************
# Color definitions for link types
# ****************************************************************************************

# Color mapping for link types (draw.io uses hex colors without #)
LINK_COLORS = {
    'is blocked by': 'FF0000',      # Red for blocking relationships
    'blocks': 'FF0000',              # Red for blocking relationships
    'is cloned by': 'FFA500',        # Orange for clones
    'clones': 'FFA500',              # Orange for clones
    'is duplicated by': '808080',    # Gray for duplicates
    'duplicates': '808080',          # Gray for duplicates
    'relates to': '0000FF',          # Blue for related
    'is related to': '0000FF',       # Blue for related
    'is caused by': 'FFCC00',        # Yellow for causes
    'causes': 'FFCC00',              # Yellow for causes
    'is child of': '00AA00',         # Green for parent/child
    'is parent of': '00AA00',        # Green for parent/child
}

# Default color for unknown link types
DEFAULT_LINK_COLOR = '666666'  # Gray

# Box fill colors based on link type (lighter versions)
BOX_FILL_COLORS = {
    'is blocked by': 'FFCCCC',      # Light red
    'blocks': 'FFCCCC',              # Light red
    'relates to': 'CCE5FF',          # Light blue
    'is related to': 'CCE5FF',       # Light blue
    'is cloned by': 'FFE5CC',        # Light orange
    'clones': 'FFE5CC',              # Light orange
    'is caused by': 'FFFFCC',        # Light yellow
    'causes': 'FFFFCC',              # Light yellow
}

# Default box fill color
DEFAULT_BOX_FILL = 'FFFFFF'  # White

# Root node color (depth 0)
ROOT_BOX_FILL = 'E6FFE6'  # Light green


# ****************************************************************************************
# Draw.io XML generation functions
# ****************************************************************************************

def load_tickets_from_csv(input_file):
    '''
    Load tickets from a CSV file exported by jira_utils.py --get-related --hierarchy.

    Input:
        input_file: Path to the CSV file.

    Output:
        List of dictionaries, each containing ticket data with 'key', 'depth', 'link_via', etc.

    Raises:
        FileNotFoundError: If the input file doesn't exist.
        ValueError: If the CSV doesn't have required columns.
    '''
    log.debug(f'Entering load_tickets_from_csv(input_file={input_file})')

    if not os.path.exists(input_file):
        # Try adding .csv extension if not present
        if not input_file.endswith('.csv') and os.path.exists(f'{input_file}.csv'):
            input_file = f'{input_file}.csv'
            log.debug(f'Added .csv extension: {input_file}')
        else:
            raise FileNotFoundError(f'Input file not found: {input_file}')

    tickets = []
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        # Verify required columns exist
        required_cols = ['key', 'depth']
        for col in required_cols:
            if col not in reader.fieldnames:
                raise ValueError(f'CSV file must have a "{col}" column. Found columns: {reader.fieldnames}')

        for row in reader:
            # Convert depth to integer
            try:
                row['depth'] = int(row.get('depth', 0))
            except (ValueError, TypeError):
                row['depth'] = 0

            tickets.append(row)

    log.debug(f'Loaded {len(tickets)} tickets from CSV')
    return tickets


def get_box_color(link_via, depth):
    '''
    Determine the fill color for a ticket box based on link type and depth.

    Input:
        link_via: The link type string (e.g., 'is blocked by', 'relates to').
        depth: The depth level of the ticket in the hierarchy.

    Output:
        Hex color string without # prefix.
    '''
    # Root node gets special color
    if depth == 0:
        return ROOT_BOX_FILL

    # Look up color based on link type
    if link_via:
        link_lower = link_via.lower().strip()
        return BOX_FILL_COLORS.get(link_lower, DEFAULT_BOX_FILL)

    return DEFAULT_BOX_FILL


def get_stroke_color(link_via):
    '''
    Determine the stroke/border color for a ticket box based on link type.

    Input:
        link_via: The link type string (e.g., 'is blocked by', 'relates to').

    Output:
        Hex color string without # prefix.
    '''
    if link_via:
        link_lower = link_via.lower().strip()
        return LINK_COLORS.get(link_lower, DEFAULT_LINK_COLOR)

    return DEFAULT_LINK_COLOR


def create_drawio_xml(tickets, title='Jira Dependency Map'):
    '''
    Generate draw.io XML content from a list of tickets.

    The diagram is laid out as a pyramid with the root ticket at the top,
    and child tickets arranged by depth level below.

    Input:
        tickets: List of ticket dictionaries with 'key', 'depth', 'link_via', 'summary', etc.
        title: Title for the diagram.

    Output:
        String containing the complete draw.io XML.
    '''
    log.debug(f'Entering create_drawio_xml(tickets_count={len(tickets)}, title={title})')

    # Group tickets by depth
    by_depth = {}
    for ticket in tickets:
        depth = ticket.get('depth', 0)
        by_depth.setdefault(depth, []).append(ticket)

    # Calculate layout dimensions
    box_width = 180
    box_height = 60
    h_spacing = 40  # Horizontal spacing between boxes
    v_spacing = 80  # Vertical spacing between depth levels

    # Calculate positions for each ticket
    positions = {}  # key -> (x, y)
    max_width = 0

    for depth in sorted(by_depth.keys()):
        tickets_at_depth = by_depth[depth]
        count = len(tickets_at_depth)

        # Calculate total width needed for this row
        row_width = count * box_width + (count - 1) * h_spacing

        # Track max width for centering
        if row_width > max_width:
            max_width = row_width

    # Now position each ticket, centering each row
    for depth in sorted(by_depth.keys()):
        tickets_at_depth = by_depth[depth]
        count = len(tickets_at_depth)

        row_width = count * box_width + (count - 1) * h_spacing
        start_x = (max_width - row_width) / 2 + 50  # 50px margin

        y = 50 + depth * (box_height + v_spacing)

        for i, ticket in enumerate(tickets_at_depth):
            x = start_x + i * (box_width + h_spacing)
            positions[ticket['key']] = (x, y)

    # Build the XML structure
    # draw.io uses mxGraphModel format
    mxfile = ET.Element('mxfile')
    mxfile.set('host', 'app.diagrams.net')
    mxfile.set('modified', date.today().isoformat())
    mxfile.set('agent', 'drawio_utilities.py')
    mxfile.set('version', '1.0')
    mxfile.set('type', 'device')

    diagram = ET.SubElement(mxfile, 'diagram')
    diagram.set('name', title)
    diagram.set('id', 'jira-dependency-map')

    mxGraphModel = ET.SubElement(diagram, 'mxGraphModel')
    mxGraphModel.set('dx', '0')
    mxGraphModel.set('dy', '0')
    mxGraphModel.set('grid', '1')
    mxGraphModel.set('gridSize', '10')
    mxGraphModel.set('guides', '1')
    mxGraphModel.set('tooltips', '1')
    mxGraphModel.set('connect', '1')
    mxGraphModel.set('arrows', '1')
    mxGraphModel.set('fold', '1')
    mxGraphModel.set('page', '1')
    mxGraphModel.set('pageScale', '1')
    mxGraphModel.set('pageWidth', str(int(max_width + 100)))
    mxGraphModel.set('pageHeight', str(int(50 + (max(by_depth.keys()) + 1) * (box_height + v_spacing) + 50)))
    mxGraphModel.set('math', '0')
    mxGraphModel.set('shadow', '0')

    root = ET.SubElement(mxGraphModel, 'root')

    # Add required root cells
    cell0 = ET.SubElement(root, 'mxCell')
    cell0.set('id', '0')

    cell1 = ET.SubElement(root, 'mxCell')
    cell1.set('id', '1')
    cell1.set('parent', '0')

    # Create cells for each ticket
    cell_ids = {}  # key -> cell_id
    cell_counter = 2

    for ticket in tickets:
        key = ticket['key']
        depth = ticket.get('depth', 0)
        link_via = ticket.get('link_via', '')
        summary = ticket.get('summary', '').strip()

        # Clean up summary - remove leading spaces and "(via ...)" suffix
        if summary.startswith('  '):
            summary = summary.strip()
        if '(via ' in summary:
            summary = summary.split('(via ')[0].strip()

        # Truncate summary for display
        if len(summary) > 40:
            summary = summary[:37] + '...'

        x, y = positions[key]
        cell_id = str(cell_counter)
        cell_ids[key] = cell_id
        cell_counter += 1

        # Determine colors
        fill_color = get_box_color(link_via, depth)
        stroke_color = get_stroke_color(link_via)

        # Build the ticket URL
        ticket_url = f'{JIRA_URL}/browse/{key}'

        # Create the cell
        cell = ET.SubElement(root, 'mxCell')
        cell.set('id', cell_id)
        # Value contains the label with link - use HTML format
        label = f'<a href="{ticket_url}" target="_blank">{key}</a><br/><font style="font-size:10px">{summary}</font>'
        cell.set('value', label)
        cell.set('style', f'rounded=1;whiteSpace=wrap;html=1;fillColor=#{fill_color};strokeColor=#{stroke_color};strokeWidth=2;')
        cell.set('vertex', '1')
        cell.set('parent', '1')

        geometry = ET.SubElement(cell, 'mxGeometry')
        geometry.set('x', str(int(x)))
        geometry.set('y', str(int(y)))
        geometry.set('width', str(box_width))
        geometry.set('height', str(box_height))
        geometry.set('as', 'geometry')

    # Create edges (connections) between parent and child tickets
    # We connect each non-root ticket to its parent (the root or previous depth level)
    # For simplicity, connect each ticket at depth N to the first ticket at depth N-1
    # A more sophisticated approach would track actual parent relationships

    # Build parent mapping: for each depth level, connect to tickets at previous level
    # Since we don't have explicit parent info, we'll connect based on depth ordering
    for depth in sorted(by_depth.keys()):
        if depth == 0:
            continue  # Root has no parent

        tickets_at_depth = by_depth[depth]
        parent_tickets = by_depth.get(depth - 1, [])

        if not parent_tickets:
            continue

        # For now, connect all tickets at this depth to the first parent
        # (In a real scenario, you'd track the actual parent relationship)
        parent_key = parent_tickets[0]['key']
        parent_id = cell_ids.get(parent_key)

        if not parent_id:
            continue

        for ticket in tickets_at_depth:
            child_key = ticket['key']
            child_id = cell_ids.get(child_key)
            link_via = ticket.get('link_via', '')

            if not child_id:
                continue

            edge_id = str(cell_counter)
            cell_counter += 1

            # Get edge color based on link type
            edge_color = get_stroke_color(link_via)

            edge = ET.SubElement(root, 'mxCell')
            edge.set('id', edge_id)
            edge.set('value', link_via if link_via else '')
            edge.set('style', f'edgeStyle=none;rounded=0;html=1;strokeColor=#{edge_color};strokeWidth=2;endArrow=classic;endFill=1;')
            edge.set('edge', '1')
            edge.set('parent', '1')
            edge.set('source', parent_id)
            edge.set('target', child_id)

            geometry = ET.SubElement(edge, 'mxGeometry')
            geometry.set('relative', '1')
            geometry.set('as', 'geometry')

    # Convert to string
    xml_str = ET.tostring(mxfile, encoding='unicode')

    # Add XML declaration
    xml_str = '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_str

    log.debug(f'Generated draw.io XML with {len(tickets)} nodes')
    return xml_str


def create_map(input_file, output_file=None, title=None):
    '''
    Create a draw.io diagram from a Jira hierarchy CSV file.

    Input:
        input_file: Path to the CSV file exported by jira_utils.py.
        output_file: Path for the output .drawio file (default: input_file with .drawio extension).
        title: Title for the diagram (default: derived from input filename).

    Output:
        None; writes the .drawio file.

    Side Effects:
        Creates or overwrites the output file.
    '''
    log.debug(f'Entering create_map(input_file={input_file}, output_file={output_file}, title={title})')

    log.info(f'Loading tickets from {input_file}...')

    # Load tickets from CSV
    tickets = load_tickets_from_csv(input_file)

    if not tickets:
        output('ERROR: No tickets found in input file.')
        return

    log.info(f'Loaded {len(tickets)} tickets from CSV')

    # Determine output filename
    if not output_file:
        # Replace .csv extension with .drawio, or append .drawio
        if input_file.endswith('.csv'):
            output_file = input_file[:-4] + '.drawio'
        else:
            output_file = input_file + '.drawio'

    # Determine title
    if not title:
        # Use the root ticket key if available
        root_tickets = [t for t in tickets if t.get('depth', 0) == 0]
        if root_tickets:
            title = f'Dependency Map: {root_tickets[0]["key"]}'
        else:
            title = 'Jira Dependency Map'

    log.info(f'Generating draw.io diagram: {title}')

    # Generate the draw.io XML
    xml_content = create_drawio_xml(tickets, title)

    log.info(f'Writing diagram to {output_file}...')

    # Write to file
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(xml_content)

    output('')
    output('=' * 80)
    output('Draw.io Diagram Created Successfully')
    output('=' * 80)
    output(f'Input file:  {input_file}')
    output(f'Output file: {output_file}')
    output(f'Tickets:     {len(tickets)}')
    output(f'Title:       {title}')
    output('=' * 80)
    output('')
    output('Open the .drawio file with:')
    output('  - draw.io desktop app (https://www.diagrams.net/)')
    output('  - VS Code with Draw.io Integration extension')
    output('  - Online at https://app.diagrams.net/')
    output('')

    log.info(f'Created draw.io diagram: {output_file}')


# ****************************************************************************************
# Handle the arguments
# ****************************************************************************************

def handle_args():
    '''
    Parse CLI arguments and configure console logging handlers.

    Input:
        None directly; reads flags from sys.argv.

    Output:
        argparse.Namespace containing parsed arguments.

    Side Effects:
        Attaches a stream handler to the module logger with formatting and
        level derived from the parsed arguments.
    '''
    log.debug('Entering handle_args()')

    parser = argparse.ArgumentParser(
        description='Draw.io diagram utilities for Jira hierarchy visualization',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  %(prog)s --create-map tickets.csv
      Create a draw.io diagram from tickets.csv, output to tickets.drawio

  %(prog)s --create-map tickets.csv --output diagram.drawio
      Create a draw.io diagram with custom output filename

  %(prog)s --create-map tickets.csv --title "Release 12.2 Dependencies"
      Create a diagram with a custom title

Workflow:
  1. Export hierarchy from Jira:
     python jira_utils.py --get-related STL-74071 --hierarchy --dump-file tickets

  2. Generate draw.io diagram:
     python drawio_utilities.py --create-map tickets.csv

  3. Open the .drawio file in draw.io or VS Code

Color Coding:
  - Root ticket: Light green background
  - "is blocked by" / "blocks": Red border, light red fill
  - "relates to": Blue border, light blue fill
  - Other link types: Gray border, white fill
        ''')

    parser.add_argument(
        '-v',
        '--verbose',
        action='store_true',
        help='Enable verbose output to stdout.')

    parser.add_argument(
        '-q',
        '--quiet',
        action='store_true',
        help='Minimal stdout.')

    parser.add_argument(
        '--create-map',
        type=str,
        metavar='CSV_FILE',
        dest='create_map',
        help='Create a draw.io diagram from a Jira hierarchy CSV file.')

    parser.add_argument(
        '--output',
        '-o',
        type=str,
        metavar='FILE',
        dest='output_file',
        help='Output filename for the .drawio file (default: input file with .drawio extension).')

    parser.add_argument(
        '--title',
        '-t',
        type=str,
        metavar='TITLE',
        dest='title',
        help='Title for the diagram (default: derived from root ticket).')

    args = parser.parse_args()

    # Configure stdout logging based on arguments (always add handler, level varies)
    ch = logging.StreamHandler(sys.stdout)
    if args.verbose:
        ch.setLevel(logging.DEBUG)
    elif args.quiet:
        ch.setLevel(logging.ERROR)
    else:
        ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    log.addHandler(ch)

    # Set quiet mode for output function
    global _quiet_mode
    _quiet_mode = args.quiet

    log.debug('Checking script requirements...')
    # Check requirements to execute the script here
    if not args.verbose and not args.quiet:
        log.debug('No output level specified. Defaulting to INFO.')

    # Validate arguments
    if not args.create_map:
        parser.print_help()
        sys.exit(1)

    # Validate --output requires --create-map (already implied by above check)
    if args.output_file and not args.create_map:
        parser.error('--output requires --create-map')

    log.info('++++++++++++++++++++++++++++++++++++++++++++++')
    log.info(f'+  {os.path.basename(sys.argv[0])}')
    log.info(f'+  Python Version: {sys.version.split()[0]}')
    log.info(f'+  Today is: {date.today()}')
    log.info('++++++++++++++++++++++++++++++++++++++++++++++')

    return args


# ****************************************************************************************
# Main
# ****************************************************************************************

def main():
    '''
    Entrypoint that wires together dependencies and launches the CLI.

    Sequence:
        1. Parse command line arguments
        2. Execute requested action(s)

    Output:
        Exit code 0 on success, 1 on failure.
    '''
    args = handle_args()
    log.debug('Entering main()')

    try:
        if args.create_map:
            create_map(args.create_map, args.output_file, args.title)

    except FileNotFoundError as e:
        log.error(str(e))
        output('')
        output(f'ERROR: {e}')
        output('')
        sys.exit(1)
    except ValueError as e:
        log.error(str(e))
        output('')
        output(f'ERROR: {e}')
        output('')
        sys.exit(1)
    except Exception as e:
        log.error(f'Unexpected error: {e}')
        output(f'ERROR: {e}')
        sys.exit(1)

    log.info('Operation complete.')


if __name__ == '__main__':
    main()
