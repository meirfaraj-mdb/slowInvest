from fpdf import FPDF, TextStyle
from fpdf.enums import XPos,YPos
import json
from utils import *

#------------------------------------------------------------------------------------
# Pdf related
def render_toc(pdf, outline):
    pdf.y += 10
    pdf.set_font("Helvetica", size=16)
    pdf.underline = True
    pdf.x =0
    pdf.p("Table of contents:")
    pdf.underline = False
    pdf.y += 2
    pdf.set_font("Courier", size=5)
    for section in outline:
        pdf.x =0
        link = pdf.add_link(page=section.page_number)
        pdf.p(
            f'{" " * section.level} {section.name} {"." * (150 - section.level - len(section.name))} {section.page_number}',
            align="L",
            link=link,
        )



class PDF(FPDF):
    def __init__(self, header_txt):
        super().__init__(orientation='P',unit='mm',format='A4')
        self.isCover = False
        self.headerTxt = header_txt
        self.add_page()
        self.insert_toc_placeholder(render_toc, allow_extra_pages=True)
        self.set_section_title_styles(
            # Level 0 titles:
            TextStyle(
                font_family="Times",
                font_style="B",
                font_size_pt=16,
                color=128,
                underline=True,
                t_margin=10,
                l_margin=2,
                b_margin=0,
            ),
            # Level 1 subtitles:
            TextStyle(
                font_family="Times",
                font_style="B",
                font_size_pt=14,
                color=128,
                underline=True,
                t_margin=10,
                l_margin=10,
                b_margin=5,
            ),
            # Level 2 subtitles:
            TextStyle(
                font_family="Times",
                font_style="B",
                font_size_pt=12,
                color=128,
                underline=True,
                t_margin=10,
                l_margin=4,
                b_margin=10,
            ),
            # Level 3 subtitles:
            TextStyle(
                font_family="Times",
                font_style="B",
                font_size_pt=8,
                color=128,
                underline=True,
                t_margin=10,
                l_margin=6,
                b_margin=10,
            ),
            # Level 4 subtitles:
            TextStyle(
                font_family="Times",
                font_style="",
                font_size_pt=4,
                color=128,
                underline=True,
                t_margin=10,
                l_margin=8,
                b_margin=10,
            ),
        )

    # Override footer method
    def footer(self):
        # Page number with condition isCover
        self.set_y(-15) # Position at 1.5 cm from bottom
        self.cell(0, 10, 'Page  ' + str(self.page_no()) + '  |  {nb}', 0, new_x=XPos.RIGHT, new_y=YPos.TOP, align='C')

    def p(self, text, **kwargs):
        "Inserts a paragraph"
        self.multi_cell(
            w=self.epw,
            h=self.font_size,
            text=text,
            new_x="LMARGIN",
            new_y="NEXT",
            **kwargs,
        )

    def header(self):
        self.set_font("helvetica", "B", 12)
        self.set_text_color(0, 0, 0)  # Black text
        self.cell(0, 10, self.headerTxt, 0, align= "C",new_x="LMARGIN", new_y="NEXT")

    def chapter_title(self, title):
        self.start_section(title)
        self.ln(10)

    def subChapter_title(self, subchapter):
        self.start_section(subchapter,level=1)
        self.ln(10)

    def sub2Chapter_title(self, subchapter):
        self.start_section(subchapter,level=2)
        self.ln(10)
    def sub3Chapter_title(self, subchapter):
        self.start_section(subchapter,level=3)
        self.ln(10)
    def sub4Chapter_title(self, subchapter):
        self.start_section(subchapter,level=4)
        self.ln(10)


    def clean_name(self,name):
        """
        Remove specific substrings from the name string.

        - Remove 'bytes' if it appears anywhere in the name.
        - Remove 'Micros' if the name ends with it.
        - Remove 'Millis' if the name ends with it.

        Parameters:
        - name: str, the original name string.

        Returns:
        - str, the cleaned name string.
        """
        # Remove 'bytes' from anywhere in the name
        name = name.replace('bytes', '')
        # Remove 'Micros' if it ends with it
        if name.endswith('Micros'):
            name = name[:-6]  # Remove the last 6 characters
        # Remove 'Millis' if it ends with it
        if name.endswith('Millis'):
            name = name[:-6]  # Remove the last 6 characters
        # Strip any leading or trailing whitespace
        return name.strip()

    def table(self, row, columns):
        # Calculate column width based on the number of columns
        num_columns = len(columns)
        col_width = self.epw / 2
        row_height = self.font_size * 1.5
        # Dictionary to hold aggregated values
        aggregated_values = {}
        # Process columns to aggregate min, max, avg, total
        for col in columns:
            if col.endswith(('_min', '_max', '_avg', '_total')):
                base_name = col.rsplit('_', 1)[0]
                if base_name not in aggregated_values:
                    aggregated_values[base_name] = {}
                # Store the value in the appropriate category
                category = col.rsplit('_', 1)[1]
                if row.get(col, 0)!=0 :
                    aggregated_values[base_name][category] = convertToHumanReadable(base_name,row.get(col, 0))

        # Process unique columns
        for col in columns:
            if not col.endswith(('_min', '_max', '_avg', '_total')):
                value = row.get(col, 0)
                if value == 0 or value == '' or value == '0':
                    continue
                # Add header for the column
                self.set_font('helvetica', 'B', 9)
                self.cell(col_width-40, row_height, self.clean_name(col), border=1)
                # Add the value
                self.set_font('helvetica', '', 8)
                self.cell(col_width+40, row_height, convertToHumanReadable(col,value), border=1)
                self.ln(row_height)
        # Add table header and rows
        for base_name, values in aggregated_values.items():
            # Check if all values are zero
            if len(values)==0 or all(v == 0 for v in values.values()):
                continue
            # Add header for the base name
            self.set_font('helvetica', 'B', 9)
            self.cell(col_width-40, row_height, self.clean_name(base_name), border=1)
            # Construct the human-readable value string
            value_str = ', '.join(f"{k}: {v}" for k, v in values.items() if v != 0)
            self.set_font('helvetica', '', 8)
            self.cell(col_width+40, row_height, value_str, border=1)
            self.ln(row_height)


    def chapter_body(self, body):
        self.set_font("helvetica", "", 12)
        self.set_text_color(0, 0, 0)  # Black text
        self.multi_cell(0, 10, body)
        self.ln()

    def add_code_box(self, code):
        self.set_font("Courier", "B", 10)
        self.set_text_color(255, 255, 255)  # White text
        self.set_fill_color(0, 0, 0)  # Black background
        self.multi_cell(0, 5, code, 0, 'L', True)
        self.ln()

    def add_image(self, image_path):
        self.image(image_path,
                   x=2,
                   w=self.epw,
                   keep_aspect_ratio=True)
        #self.ln(10)
    def add_colored_json(self, json_data, x, y, w, h):
        self.set_font("Courier", "", 7)
        self.set_xy(x, y)
        self.set_fill_color(240, 240, 240)  # Light gray background
        self.rect(x, y, w, h, 'F')  # Draw the box
        self.set_xy(x + 2, y + 2)  # Add some padding
        self.write_html(self.json_to_html(json.loads(json_data)))

    def json_to_html(self, json_data, indent=0):
        # Ensure indent is an integer
        if not isinstance(indent, int):
            raise ValueError("Indent must be an integer.")
        html_output = ""
        if indent==0 :
            indent_space = ""
        else:
            indent_space = "&nbsp;" * (indent * 4)  # Create indentation using &nbsp;
        if isinstance(json_data, dict):
            html_output += f"{indent_space}<font color='black'>{{</font><br>"
            for key, value in json_data.items():
                html_output += f"{indent_space}  <font color='darkred'>{key}:</font> "
                # Format the value based on its type
                if isinstance(value, str):
                    html_output += f"<font color='darkgreen'>{value}</font>"
                elif isinstance(value, (int, float)):
                    html_output += f"<font color='lightcoral'>{value}</font>"
                elif isinstance(value, dict):
                    html_output += "<br>" + self.json_to_html(value, indent + 1)  # Recursive call for nested dicts
                elif isinstance(value, list):
                    html_output += "<br>" + self.json_to_html(value, indent + 1)  # Handle lists
                else:
                    html_output += f"<font  color='black'>{value}</font>"  # Fallback for other types
                html_output += "<br>"
            html_output += f"{indent_space}<font color='black'>}}</font><br>"
        elif isinstance(json_data, list):
            html_output += f"{indent_space}<font color='darkred'>[</font><br>"
            for item in json_data:
                html_output += f"{indent_space}  "
                if isinstance(item, dict):
                    html_output += self.json_to_html(item, indent + 1)
                elif isinstance(item, str):
                    html_output += f"<font color='darkgreen'>{item}</font>"
                elif isinstance(item, (int, float)):
                    html_output += f"<font color='lightcoral'>{item}</font>"
                else:
                    html_output += f"<font>{item}</font>"  # Fallback for other types
                html_output += "<br>"
            html_output += f"{indent_space}<font color='black'>]</font><br>"
        return html_output

    def add_json(self,json_data):
        self.add_colored_json(json_data, x=10, y=30, w=190, h=250)

#----------------------------------------------------------------
