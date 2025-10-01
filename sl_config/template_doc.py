import json
import argparse
import os

def render_fields(fields):
    md = "\n"   # <-- Ensures table starts after a blank line
    md += "| Field | Title | Path | Included |\n"
    md += "|---|---|---|---|\n"
    for key, value in fields.items():
        title = value.get("title", "")
        path = value.get("path", "")
        include = value.get("include", "")
        md += f'| `{key}` | {title} | `{path}` | {include} |\n'
    return md

def document_section(section, name="", level=2):
    md = ""
    header = "#" * level
    section_title = section.get("title", name if name else "")
    if section_title:
        md += f"\n{header} {section_title}\n"
    if "include" in section:
        md += f"\n**Included:** `{section['include']}`\n"
    if "fields" in section:
        md += render_fields(section["fields"])
    for k, v in section.items():
        if isinstance(v, dict) and k not in {"fields", "title", "include"}:
            md += document_section(v, name=k, level=level+1)
    return md

def generate_markdown_doc(template):
    md = f"# JSON Template Documentation\n"
    md += f"\n**Title:** {template.get('title','')}\n"
    md += "\n## Top-Level Config\n"
    for k in template:
        if k in ["title", "sections", "format"]:
            continue
        md += f"- **{k}**: {template[k]}\n"
    if "format" in template:
        md += "\n## Format\n"
        md += "```json\n" + json.dumps(template["format"], indent=2) + "\n```\n"
    if "sections" in template:
        md += "\n## Sections\n"
        for section_name, section_conf in template["sections"].items():
            md += document_section(section_conf, name=section_name, level=2)
    return md

if __name__ == "__main__":
    DEFAULT_TEMPLATE = "../config/default/template.json"
    DEFAULT_OUTPUT = "../template_doc.md"
    parser = argparse.ArgumentParser(description="Generate Markdown documentation for a JSON template.")
    parser.add_argument("--input", "-i", help="Input JSON template file", default=DEFAULT_TEMPLATE)
    parser.add_argument("--output", "-o", help="Output Markdown documentation file", default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    if not os.path.exists(args.input):
        print(f"Template file not found: {args.input}")
        exit(1)
    with open(args.input, "r") as f:
        template = json.load(f)
    md_doc = generate_markdown_doc(template)
    with open(args.output, "w") as f:
        f.write(md_doc)
    print(f"Documentation written to {args.output}")
