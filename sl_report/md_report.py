from sl_report.report import AbstractReport
import base64
from pathlib import Path
import logging
mdreports_logging = logging.getLogger("md_reports")
mdreports_logging.setLevel(logging.DEBUG)
def get_nested_value(data, key):
    keys = key.split('.')
    value = data
    for k in keys:
        if isinstance(value, dict):
            value = value.get(k, '')
        else:
            return ''
    return value

class MDReport(AbstractReport):

    def __init__(self, header_txt):
        self.content = []
        self.header_txt = header_txt
        self.toc = []
        self.counter=0
        self.header()

    def addpage(self):
        """Simulate adding a new 'page' with a separator."""
        # Markdown doesn't support pagination; you can visually separate sections with a line.
        self.content.append("\n---\n")

    def chapter_body(self, body):
        """Equivalent to add a chapter body in Markdown"""
        # Simply add the text as paragraph content
        self.add_paragraph(body.strip())




    def header(self):
        anchor=f"lnk{self.counter}"
        self.toc.append(f"# {self.header_txt} <div id=\"#{anchor}\"></div>")
        self.counter+=1

    def chapter_title(self, title):
        self.add_heading(title, level=2)

    def subChapter_title(self, subchapter):
        self.add_heading(subchapter, level=3)

    def sub2Chapter_title(self, subchapter):
        self.add_heading(subchapter, level=4)

    def sub3Chapter_title(self, subchapter):
        self.add_heading(subchapter, level=5)

    def sub4Chapter_title(self, subchapter):
        self.add_heading(subchapter, level=6)

    def add_heading(self, text, level=1):
        """Helper method to add a heading at the specified level."""
        if not (1 <= level <= 6):
            raise ValueError("Heading level must be between 1 and 6.")
        anchor=f"lnk{self.counter}"
        self.toc.append(f"{'.' * (level-1)}[{text}](#{anchor})")
        self.content.append(f"{'#' * level} {text} <div id=\"{anchor}\"></div>\n")
        self.counter+=1

    def add_text(self, text):
        """Add plain text to Markdown."""
        self.content.append(text)
    def add_section(self, title, level=1):
        """Add a section with appropriate Markdown heading level."""
        heading = f"{'#' * level} {title}"
        self.content.append(heading)
    def add_toc(self, sections):
        """Generate a Table of Contents."""
        self.add_text("## Table of Contents")
        for section in sections:
            toc_entry = f"{'  ' * section['level']}- [{section['name']}](#{section['name'].replace(' ', '-').lower()})"
            self.content.append(toc_entry)
    def add_paragraph(self, text):
        """Add a paragraph."""
        self.content.append(text)

    def table(self, row, columns):
        """Add a dynamic table that aggregates and processes advanced columns."""
        self.content.append("### Table Summary")
        # Prepare aggregated values
        aggregated_values = {}
        for col in columns:
            if col.endswith(('_min', '_max', '_avg', '_total', '_count')):
                base_name = col.rsplit('_', 1)[0]
                if base_name not in aggregated_values:
                    aggregated_values[base_name] = {}
                summary_type = col.rsplit('_', 1)[1]
                value = row.get(col, None)
                if value:
                    aggregated_values[base_name][summary_type] = value
        # Render unique/non-aggregated values
        self.sub2Chapter_title("Non-Aggregated Values")
        unique_columns = [col for col in columns if not col.endswith(('_min', '_max', '_avg', '_total', '_count'))]
        for col in unique_columns:
            value = row.get(col, '')
            if isinstance(value, list) and len(value) <= 1:  # Handle lists with one or zero elements
                value = value[0] if value else ''
            if not value:  # Skip empty fields
                continue
            self.content.append(f"- **{col}**: {value}")
        # Render aggregated values in table form
        if aggregated_values:
            self.sub2Chapter_title("Aggregated Values")

            # Header and separator
            headers = ["Metric", "Min", "Max", "Avg", "Total", "Count"]
            table ="| " + " | ".join(headers) + " |\n"
            table +="|" + "|".join(['---' for _ in headers]) + "|\n"

            # Data rows
            for name, metrics in aggregated_values.items():
                row_data = [
                    name,
                    metrics.get('min', ''),
                    metrics.get('max', ''),
                    metrics.get('avg', ''),
                    metrics.get('total', ''),
                    metrics.get('count', '')
                ]
                # Ensure that all entries are strings for Markdown
                table +="| " + " | ".join(map(str, row_data)) + " |\n"
            self.content.append(table)

    def display_cluster_table(self, cluster):
        """Display cluster information."""
        # General Cluster Information
        self.add_section(f"Cluster {cluster.get('name', 'N/A')} Report", level=2)
        general_info = [
            ("Cluster Name", cluster.get('name')),
            ("Cluster Type", cluster.get('clusterType')),
            ("Create Date", cluster.get('createDate')),
            ("MongoDB Version", cluster.get('mongoDBVersion')),
            ("Version Release System", cluster.get('versionReleaseSystem')),
            ("Group/Project Id", cluster.get('groupId')),
            ("Cluster Id", cluster.get('id')),
            ("Backup Enabled", cluster.get('backupEnabled')),
            ("Paused", cluster.get('paused')),
        ]
        self.subChapter_title("General Cluster Information")
        for label, value in general_info:
            if value not in [None, '', '0', False]:
                self.content.append(f"- **{label}**: {value}")
        # Backup and Performance Metrics
        metrics_info = [
            {"Metric": "Backup Compliance Configured", "Value": cluster.get("backupCompliance_configured")},
            {"Metric": "Backup Snapshot Count", "Value": cluster.get("backup_snapshot_count")},
            {"Metric": "Online Archive Count", "Value": cluster.get("onlineArchiveForOneCluster_count")},
            {"Metric": "Suggested Index Count", "Value": cluster.get("performanceAdvisorSuggestedIndexes_count")}
        ]

        self.add_paragraph("\n### Metrics")
        if any(metrics_info):
            self.add_table(metrics_info, ["Metric", "Value"])
        # Advanced Configuration
        self.add_paragraph("\n### Advanced Configuration")
        advanced_config = cluster.get('advancedConfiguration', {})
        for key, value in advanced_config.items():
            self.content.append(f"- **{key}**: {value}")
        # Replication Specs
        replication_specs = cluster.get('replicationSpecs', [])
        if replication_specs:
            self.add_paragraph("\n### Replication Specifications")
            for i, spec in enumerate(replication_specs):
                self.add_section(f"Replication Spec {i} Zone: {spec.get('zoneName', 'N/A')}", level=3)
                region_configs = spec.get('regionConfigs', [])
                for j, region in enumerate(region_configs):
                    self.add_section(f"Region {j} Name: {region.get('regionName', 'N/A')}", level=4)
                    self.content.append(f"- **Provider**: {region.get('providerName', 'N/A')}")
                    self.content.append(f"- **Priority**: {region.get('priority', 'N/A')}")
                    # Additional region configurations (e.g., specs)
                    for spec_type in ['electableSpecs', 'readOnlySpecs', 'analyticsSpecs']:
                        specs = region.get(spec_type, {})
                        if specs:
                            self.content.append(f"- **{spec_type}**: {specs}")
        scaling=cluster.get("scaling",[])
        if len(scaling)>0:
            self.subChapter_title("Scaling information")
            self.add_table(scaling,
                           ['id','created', 'clusterName','computeAutoScalingTriggers','eventTypeName','raw.computeAutoScaleTriggers','raw.originalCostPerHour','raw.newCostPerHour', 'raw.originalDiskSizeGB','raw.newDiskSizeGB','raw.originalInstanceSize', 'raw.newInstanceSize','raw.isAtMaxCapacityAfterAutoScale'],
                           ['id','time', 'cluster','triggers','event type','computeAutoScaleTriggers','orig Cost/Hour','new Cost/Hour', 'orig DiskSizeGB','new DiskSizeGB','orig instance', 'new instance','isAtMaxCapacityAfterAutoScale']
                           )


    def add_image(self, image_path, move_cursor_down,aspect_ratio=1):
        self.add_image_md(image_path,"","")


    def add_table(self, data_list, columns,columns_name=None):
        """Add a markdown table."""
        # Add column headers
        if columns_name is None:
            headers = "| " + " | ".join(columns) + " |"
        else:
            headers = "| " + " | ".join(columns_name) + " |"

        separator = "| " + " | ".join(['----' for _ in columns]) + " |"
        self.content += [headers, separator]
        # Add each data row
        for data in data_list:
            row_content = "| " + " | ".join(str(get_nested_value(data,col)) for col in columns) + " |"
            self.content.append(row_content)
        self.content.append("\n")

    def add_image_md(self, image_path, alt_text, title):
        """Embed an image as base64 within Markdown."""
        # Ensure image file exists
        image_path = Path(image_path)
        if not image_path.is_file():
            print(f"Error: Image file {image_path} not found.")
            return
        # Read the image and encode it to base64
        with open(image_path, "rb") as img_file:
            encoded_image = base64.b64encode(img_file.read()).decode('utf-8')
        # Create data URI based on image type
        image_format = image_path.suffix[1:]  # Get image format from file suffix
        if image_format=="svg":
            image_format="svg+xml"
        data_uri = f"data:image/{image_format};base64,{encoded_image}"
        # Append Markdown image syntax with base64 data URI
        self.content.append(f"![{alt_text}]({data_uri} \"{title}\")")

    def add_code_block(self, code, language=""):
        """Add a code block in Markdown."""
        self.content.append(f"```{language}")
        self.content.append(code.decode('utf-8'))
        self.content.append("```")

    def add_code_box(self, code):
        self.add_code_block(code, language="json")

    def add_json(self, json_data):
        """Render JSON as a block of formatted code."""
        #formatted_json = json.dumps(json_data, indent=4)
        self.add_code_block(json_data, language="json")
    def write(self, filename):
        with open(f"{filename}.md", "w") as f:
            # Write the content to the file
            f.write("\n\n".join(self.toc))
            f.write("\n")

            # Check content types and print if there are issues.
            for idx, item in enumerate(self.content):
                if not isinstance(item, str):
                    # Log the error with the index and type
                    mdreports_logging.warning(f"Item at index {idx} is not a string: {type(item).__name__}; content:{item}")
                    # Convert item to string if possible
                    self.content[idx] = str(item)

            # Write the content to the file
            f.write("\n".join(self.content))
        print(f"Markdown report written to {filename}.md.")