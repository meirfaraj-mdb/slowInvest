# python.exe -m pip install --upgrade pip
# pip install requests
# pip install pandas
# pip install seaborn
# pip install matplotlib
# pip install tabulate
# pip install PdfReader
# pip install PdfWriter
#pip uninstall fpdf2
#pip install git+https://github.com/andersonhc/fpdf2.git@page-number

import requests
from requests.auth import HTTPDigestAuth
import json
from collections import defaultdict
from fpdf import FPDF, TextStyle, HTML2FPDF
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime
from tabulate import tabulate


# Global variables from context values
USING_API = False

PUBLIC_KEY = 'fbbfplog'
PRIVATE_KEY = '14024f30-42aa-461a-b654-74143309354b'
GROUP_ID = "663bef88730dee3831c03033"
PROCESSES_ID = ["cluster0-shard-00-00.kaffo.mongodb.net:27017","cluster0-shard-00-01.kaffo.mongodb.net:27017","cluster0-shard-00-02.kaffo.mongodb.net:27017"]

# Using log (in case USING_API = False)
LOG_FILE_PATH = 'mongod.log'  # Path to your mongod.log file

# Related to output
OUTPUT_FILE_PATH = 'slow_queries.log'  # Path to the output file where slow queries will be saved
#To add :
# Generate cluster structure
# Generate cluster metrics
#   iops,disk,scanAndOrder, cache (dirty)
#   opcount, cpu, oplog...
# log collection and index infos.
# get advise load properties from file instead of code
# Generate cluster
GENERATE_SLOW_QUERY_LOG = True
GENERATE_PDF_REPORT = True


#------------------------------------------------------------------------------------
# Pdf related
def render_toc(pdf, outline):
    pdf.y += 50
    pdf.set_font("Helvetica", size=16)
    pdf.underline = True
    pdf.p("Table of contents:")
    pdf.underline = False
    pdf.y += 20
    pdf.set_font("Courier", size=12)
    for section in outline:
        link = pdf.add_link(page=section.page_number)
        pdf.p(
            f'{" " * section.level * 2} {section.name} {"." * (60 - section.level*2 - len(section.name))} {section.page_number}',
            align="C",
            link=link,
        )


class PDF(FPDF):
    def __init__(self, header_txt):
        super().__init__()
        self.headerTxt = header_txt
        self.add_page()
        self.insert_toc_placeholder(render_toc, allow_extra_pages=True)
        self.set_section_title_styles(
            # Level 0 titles:
            TextStyle(
                font_family="Times",
                font_style="B",
                font_size_pt=24,
                color=128,
                underline=True,
                t_margin=10,
                l_margin=10,
                b_margin=0,
            ),
            # Level 1 subtitles:
            TextStyle(
                font_family="Times",
                font_style="B",
                font_size_pt=20,
                color=128,
                underline=True,
                t_margin=10,
                l_margin=20,
                b_margin=5,
            ),
            # Level 2 subtitles:
            TextStyle(
                font_family="Times",
                font_style="B",
                font_size_pt=18,
                color=128,
                underline=True,
                t_margin=10,
                l_margin=30,
                b_margin=10,
            ),
            # Level 3 subtitles:
            TextStyle(
                font_family="Times",
                font_style="B",
                font_size_pt=14,
                color=128,
                underline=True,
                t_margin=10,
                l_margin=50,
                b_margin=10,
            ),
        )


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


    def table(self, row, columns):
        col_width = self.epw / 2  # Calculate column width
        row_height = self.font_size * 1.5
        # Add table header
        for col in columns:
            self.set_font('helvetica', 'B', 10)
            self.cell(col_width, row_height, col, border=1)
            self.set_font('helvetica', '', 8)
            self.cell(col_width, row_height, str(row[col]), border=1)
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

class Report():
    def __init__(self):
        if GENERATE_PDF_REPORT :
            self.lpdf = PDF("Slow Query Report")

    def header(self):
        if GENERATE_PDF_REPORT :
           self.lpdf.header()

    def chapter_title(self, title):
        if GENERATE_PDF_REPORT :
            self.lpdf.chapter_title(title)

    def subChapter_title(self, subchapter):
        if GENERATE_PDF_REPORT :
            self.lpdf.subChapter_title(subchapter)

    def sub2Chapter_title(self, subchapter):
        if GENERATE_PDF_REPORT :
            self.lpdf.sub2Chapter_title(subchapter)

    def sub3Chapter_title(self, subchapter):
       if GENERATE_PDF_REPORT :
           self.lpdf.sub3Chapter_title(subchapter)

    def chapter_body(self, body):
        if GENERATE_PDF_REPORT :
            self.lpdf.chapter_body(body)

    def add_code_box(self, code):
        if GENERATE_PDF_REPORT :
            self.lpdf.add_code_box(code)

    def add_image(self, image_path):
        if GENERATE_PDF_REPORT :
            self.lpdf.add_image(image_path)

    def add_page(self):
        if GENERATE_PDF_REPORT :
            self.lpdf.add_page()

    def add_json(self,json):
        if GENERATE_PDF_REPORT :
            self.lpdf.add_json(json)

    def table(self, df, column):
        if GENERATE_PDF_REPORT :
            self.lpdf.table(df,column)

    def write(self,name):
        if GENERATE_PDF_REPORT :
            print(f"Writing {name}.pdf")
            self.lpdf.output(f"{name}.pdf")



#------------------------------------------------------------------------------------
# Function for extracting from Api
def atlas_request(op, fpath, fdate, arg):

    apiBaseURL = '/api/atlas/v2'
    url = f"https://cloud.mongodb.com{apiBaseURL}{fpath}"

    headers = {
        'Accept': f"application/vnd.atlas.{fdate}+json",
        'Content-Type': f"application/vnd.atlas.{fdate}+json"
    }

    response = requests.get(
        url,
        params=arg,
        auth=HTTPDigestAuth(PUBLIC_KEY, PRIVATE_KEY),
        headers=headers
    )
    #print(f"{op} response: {response.text}")
    return json.loads(response.text)

# retrieve slow queries
def retrieveLast24HSlowQueriesFromCluster(groupId,processId, output_file_path):
    path=f"/groups/{groupId}/processes/{processId}/performanceAdvisor/slowQueryLogs"
    resp=atlas_request('SlowQueries', path, '2023-01-01', {})
    slow_queries = []
    data = []
    for entry in resp['slowQueries']:
        try:
            line = entry['line']
            log_entry = json.loads(line)
            if log_entry.get("msg") == "Slow query":
                extractSlowQueryInfos(data, line, log_entry, slow_queries)
        except json.JSONDecodeError:
            # Skip lines that are not valid JSON
            continue
    print(f"Extracted slow queries have been saved to {output_file_path}")
    return pd.DataFrame(data, columns=['hour', 'namespace', 'slow_query_count', 'total_duration', 'has_sort_stage', 'query_targeting', 'plan_summary', 'command_shape', 'writeConflicts','skip','limit','appName','changestream','keys_examined','docs_examined','nreturned','cursorid','nBatches','numYields','totalOplogSlotDurationMicros','waitForWriteConcernDurationMillis'])

#------------------------------------------------------------------------------------
# Function for extracting from File :
def extract_slow_queries(log_file_path, output_file_path):
    slow_queries = []
    data = []

    with open(log_file_path, 'r') as log_file:
        for line in log_file:
            try:
                log_entry = json.loads(line)
                if log_entry.get("msg") == "Slow query":
                    extractSlowQueryInfos(data, line, log_entry, slow_queries)
            except json.JSONDecodeError:
                # Skip lines that are not valid JSON
                continue

    with open(output_file_path, 'w') as output_file:
        for query in slow_queries:
            output_file.write(query)

    print(f"Extracted slow queries have been saved to {output_file_path}")
    return pd.DataFrame(data, columns=['hour', 'namespace', 'slow_query_count', 'total_duration', 'has_sort_stage', 'query_targeting', 'plan_summary', 'command_shape', 'writeConflicts','skip','limit','appName','changestream','keys_examined','docs_examined','nreturned','cursorid','nBatches','numYields','totalOplogSlotDurationMicros','waitForWriteConcernDurationMillis'])

def check_change_stream(document):
    changestream = False
    def search_for_change_stream(subdoc):
        if isinstance(subdoc, dict):
            for key, value in subdoc.items():
                if key == "$changeStream":
                    return True
                if isinstance(value, dict) or isinstance(value, list):
                    if search_for_change_stream(value):
                        return True
        elif isinstance(subdoc, list):
            for item in subdoc:
                if search_for_change_stream(item):
                    return True
        return False
    if search_for_change_stream(document):
        changestream = True
    return changestream

def extractSlowQueryInfos(data, line, log_entry, slow_queries):
    slow_queries.append(line)
    # Extract relevant fields
    namespace = log_entry.get("attr", {}).get("ns", "unknown")
    totalOplogSlotDurationMicros = log_entry.get("attr", {}).get("totalOplogSlotDurationMicros", 0)
    waitForWriteConcernDurationMillis = log_entry.get("attr", {}).get("waitForWriteConcernDurationMillis", 0)

    duration = log_entry.get("attr", {}).get("durationMillis", 0)
    has_sort_stage = 1 if log_entry.get("attr", {}).get("hasSortStage", False) else 0
    keys_examined = log_entry.get("attr", {}).get("keysExamined", 0)
    docs_examined = log_entry.get("attr", {}).get("docsExamined", 0)
    nreturned = log_entry.get("attr", {}).get("nreturned", 0)  # Default to 0 if not defined
    appName = log_entry.get("attr", {}).get("appName", "n")
    query_targeting = max(keys_examined, docs_examined) / nreturned if nreturned > 0 else 0
    #if(query_targeting>1):
    #    print(f" query_targeting={query_targeting}, nreturned={nreturned} , keys_examined={keys_examined}, docs_examined={docs_examined}")
    plan_summary = log_entry.get("attr", {}).get("planSummary", "n")
    cursorid = log_entry.get("attr", {}).get("cursorid", 0)
    nBatches = log_entry.get("attr", {}).get("nBatches", 0)
    numYields = log_entry.get("attr", {}).get("numYields", 0)

    writeConflicts = log_entry.get("attr", {}).get("writeConflicts", 0)
    command = log_entry.get("attr", {}).get("command", {})
    skip = command.get("skip", 0)
    limit = command.get("limit", 0)
    changestream = check_change_stream(log_entry)
    command_shape = get_command_shape(command)
    if command_shape == 0:
        command_shape = "no_command"
    timestamp = log_entry.get("t", {}).get("$date")
    if timestamp:
        hour = datetime.fromisoformat(timestamp).strftime('%Y-%m-%d %H:00:00')
        data.append([hour, namespace, 1, duration, has_sort_stage, query_targeting, plan_summary, command_shape,writeConflicts,skip,limit,appName,changestream,keys_examined,docs_examined,nreturned,cursorid,nBatches,numYields,totalOplogSlotDurationMicros,waitForWriteConcernDurationMillis])


#----------------------------------------------------------------------------------------
# Common for extracting
# def get_command_shape(command):
#     def replace_values(obj):
#         if isinstance(obj, dict):
#             return {k: replace_values(v) for k, v in obj.items()}
#         elif isinstance(obj, list):
#             return [replace_values(i) for i in obj]
#         elif isinstance(obj, int):
#             return "int"
#         elif isinstance(obj, float):
#             return "float"
#         elif isinstance(obj, str):
#             return "string"
#         else:
#             return "other"
#
#     command_shape = replace_values(command)
#     keys = list(command_shape.keys())
#     if ("insert" in command_shape) and (keys.index("insert")==0) :
#         command_shape["insert"] = command["insert"]
#     if ("findAndModify" in command_shape) and (keys.index("findAndModify")==0):
#         command_shape["findAndModify"] = command["findAndModify"]
#     if ("update" in command_shape) and (keys.index("update")==0):
#         command_shape["update"] = command["update"]
#     if "collection" in command_shape:
#         command_shape["collection"] = command["collection"]
#     if "aggregate" in command_shape:
#         command_shape["aggregate"] = command["aggregate"]
#     if "find" in command_shape:
#         command_shape["find"] = command["find"]
#     if "ordered" in command_shape:
#         command_shape["ordered"] = command["ordered"]
#     if "$db" in command_shape:
#         command_shape["$db"] = command["$db"]
#     return json.dumps(command_shape)


def get_command_shape(command):
    def replace_values(obj):
        if isinstance(obj, dict):
            return {k: replace_values(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [replace_values(i) for i in obj]
        elif isinstance(obj, int):
            return "int"
        elif isinstance(obj, float):
            return "float"
        elif isinstance(obj, str):
            return "string"
        else:
            return "other"
    def handle_pipeline(pipeline):
        new_pipeline = []
        for stage in pipeline:
            if isinstance(stage, dict):
                new_stage = {}
                for k, v in stage.items():
                    if k == "$group" and isinstance(v, dict):
                        new_stage[k] = {sub_k: replace_values(sub_v) if sub_k != "_id" else sub_v for sub_k, sub_v in v.items()}
                    elif k == "$lookup" and isinstance(v, dict):
                        new_stage[k] = {sub_k: replace_values(sub_v) if sub_k not in ["from", "localField", "foreignField", "as"] else sub_v for sub_k, sub_v in v.items()}
                    else:
                        new_stage[k] = replace_values(v)
                new_pipeline.append(new_stage)
            else:
                new_pipeline.append(replace_values(stage))
        return new_pipeline
    command_shape = replace_values(command)
    keys = list(command_shape.keys())
    # Preserve specific keys in their original form if they are the first key
    for key in ["insert", "findAndModify", "update"]:
        if key in command_shape and keys.index(key) == 0:
            command_shape[key] = command[key]
    # Preserve other specific keys in their original form
    for key in ["collection", "aggregate", "find", "ordered", "$db"]:
        if key in command_shape:
            command_shape[key] = command[key]
    # Handle the pipeline separately
    if "pipeline" in command:
        command_shape["pipeline"] = handle_pipeline(command["pipeline"])
    return json.dumps(command_shape)


#---------------------------------------------------------------------------
# Plot Util :
def plot_stats(df, value_col, title, ylabel, xlabel='Time', output_file=None, columns = ['namespace'],report=None):
    if df.empty:
        if report:
            report.chapter_body(f"No {title} to present as graph")
        return
    # Create a pivot table with hour and namespace
    pivot_df = df.pivot_table(index='hour', columns= columns, values=value_col, aggfunc='sum', fill_value=0)

    # Plot the data
    fig, ax = plt.subplots(figsize=(12, 8))
    pivot_df.plot(kind='line', ax=ax)

    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend(title='Namespace', loc='upper left', bbox_to_anchor=(1, 1))
    plt.xticks(rotation=45)
    plt.tight_layout()

    if output_file:
        plt.savefig(output_file, bbox_inches='tight')
        plt.savefig(f"{output_file}.svg", format="svg")
        print(f"Graph saved to {output_file}")
        if report:
           report.add_image(f"{output_file}.svg")
    else:
        plt.show()

#-----------------------------------------------------------------------------------------
# markdown utility :

def save_markdown(df,fileName,comment):
    # Convert the DataFrame to a markdown table
    markdown_table = tabulate(df, headers='keys', tablefmt='pipe')

    # Save the markdown table to a file
    with open(fileName, 'w') as f:
        f.write(markdown_table)

    print("\nStatistics by Command "+comment+" Shape (sorted by average duration) have been saved to '"+fileName+"'")

#----------------------------------------------------------------------------------------
# Other utilities
def distinct_values(series):
    return ', '.join(sorted(set(series)))


#----------------------------------------------------------------------------------------
# display slow query
def process_row(index, row,report,columns):
    # Define a function to apply to each row
    report.add_page()
    report.sub3Chapter_title(f"Index: {index}")
    report.add_json(row['command_shape'])
    report.add_page()
    report.table(row.drop(columns=['command_shape']), [col for col in columns if col != 'command_shape'])


# def display_queries(report, df):
#     for index, row in df.iterrows():
#        process_row(index,row, report,df.columns)

def display_queries(report, df):
    # Group the DataFrame by appName
    grouped_by_app = df.groupby('app_name')

    # Iterate over each appName group
    for app_name, app_group in grouped_by_app:
        # Call report.sub3Chapter_title with the appName
        report.sub3Chapter_title(f"appName: {app_name}")

        # Sort the grouped DataFrame by slow_query_count and average_duration in descending order
        sorted_grouped_df = app_group.sort_values(by=['slow_query_count', 'average_duration'], ascending=[False, False])

        # Iterate over each row in the sorted grouped DataFrame
        for index, row in sorted_grouped_df.iterrows():
            process_row(index, row, report, sorted_grouped_df.columns)



#----------------------------------------------------------------------------------------
# report
def groupbyCommandShape(df):
    return df.groupby('command_shape').agg(
        slow_query_count=('slow_query_count', 'sum'),
        total_duration=('total_duration', 'sum'),
        average_duration=('total_duration', 'mean'),
        writeConflictsTotal=('writeConflicts', 'sum'),
        writeConflictsMax=('writeConflicts', 'max'),
        has_sort_stage=('has_sort_stage', lambda x: x.mode().iloc[0] if not x.mode().empty else False),
        max_query_targeting=('query_targeting', 'max'),
        sum_skip=('skip', 'sum'),
        min_limit=('limit', 'min'),
        max_limit=('limit', 'max'),
        plan_summary=('plan_summary', distinct_values),
        app_name=('appName', distinct_values),
        keys_examined_min=('keys_examined', 'min'),
        keys_examined_max=('keys_examined', 'max'),
        keys_examined_avg=('keys_examined', 'mean'),
        keys_examined_total=('keys_examined', 'sum'),
        docs_examined_min=('docs_examined', 'min'),
        docs_examined_max=('docs_examined', 'max'),
        docs_examined_avg=('docs_examined', 'mean'),
        docs_examined_total=('docs_examined', 'sum'),
        nreturned_min=('nreturned', 'min'),
        nreturned_max=('nreturned', 'max'),
        nreturned_avg=('nreturned', 'mean'),
        nreturned_total=('nreturned', 'sum'),
        nBatches_min=('nBatches', 'min'),
        nBatches_max=('nBatches', 'max'),
        nBatches_avg=('nBatches', 'mean'),
        nBatches_total=('nBatches', 'sum'),
        numYields_min=('numYields', 'min'),
        numYields_max=('numYields', 'max'),
        numYields_avg=('numYields', 'mean'),
        numYields_total=('numYields', 'sum'),
        waitForWriteConcernDurationMillis_min=('waitForWriteConcernDurationMillis', 'min'),
        waitForWriteConcernDurationMillis_max=('waitForWriteConcernDurationMillis', 'max'),
        waitForWriteConcernDurationMillis_avg=('waitForWriteConcernDurationMillis', 'mean'),
        waitForWriteConcernDurationMillis_total=('waitForWriteConcernDurationMillis', 'sum')

    ).reset_index()


def addToReport(df,prefix,report):
    report.chapter_title(f"Slow Query Report Summary : {prefix}")
    report.subChapter_title("General")
    report.chapter_body(f"Instance : {prefix}")
    df = df[~df['namespace'].str.startswith(('admin', 'local', 'config'))]
    df_orig = df
    # Aggregate the data to ensure unique hour-namespace combinations
    df = df.groupby(['hour', 'namespace']).agg(
        slow_query_count=('slow_query_count', 'sum'),
        total_duration=('total_duration', 'sum'),
        writeConflicts=('writeConflicts', 'sum'),
        has_sort_stage=('has_sort_stage', 'sum'),
        sum_skip=('skip', 'sum'),
        query_targeting=('query_targeting', 'max')).reset_index()
    all_namespaces = df['namespace'].unique()
    report.chapter_body(f"Namespace List : {all_namespaces}")
    if df.empty :
        report.chapter_body("No slow query!")
        report.add_page()
        return

    report.subChapter_title("Graphics")
    df_changestream = df_orig[df_orig['changestream'] == True]
    df_withoutChangestream = df_orig[df_orig['changestream'] == False]
    df_plan_summary = df_withoutChangestream.groupby(['hour', 'namespace', 'plan_summary']).agg(
    slow_query_count=('slow_query_count', 'sum'),
    writeConflicts=('writeConflicts', 'sum'),
    total_duration=('total_duration', 'sum'),
    has_sort_stage=('has_sort_stage', 'sum'),
    sum_skip=('skip', 'sum'),
    query_targeting=('query_targeting', 'max')).reset_index()

    # Plot number of slow queries per hour per namespace
    report.add_page()
    report.sub2Chapter_title("Number of Slow Queries per Hour per Namespace")
    file_name = f"{prefix}_slow_queries_per_hour.png"
    plot_stats(df, 'slow_query_count', 'Number of Slow Queries per Hour per Namespace', 'Number of Slow Queries',
               output_file=file_name,report=report)

    # Plot total duration of slow queries per hour per namespace
    report.add_page()
    report.sub2Chapter_title("Total Duration of Slow Queries per Hour per Namespace")
    file_name = f"{prefix}_total_duration_per_hour.png"
    plot_stats(df, 'total_duration', 'Total Duration of Slow Queries per Hour per Namespace', 'Total Duration (ms)',
               output_file=file_name,report=report)

#    tmp = df[df['plan_summary'].str.contains('COLLSCAN')]
    report.add_page()
    report.sub2Chapter_title("COLLSCAN Count per Hour per Namespace")
    file_name = f"{prefix}_COLLSCAN_per_hour.png"
    plot_stats(df_plan_summary[df_plan_summary['plan_summary'].str.contains('COLLSCAN')], 'slow_query_count', 'COLLSCAN Count per Hour per Namespace', 'COLLSCAN Count',
           output_file=file_name,columns = ['namespace','plan_summary'],report=report)

    # Plot hasSortStage count per hour per namespace
    report.add_page()
    report.sub2Chapter_title("Has Sort Stage Count per Hour per Namespace")
    file_name = f"{prefix}_has_sort_stage_per_hour.png"
    plot_stats(df, 'has_sort_stage', 'Has Sort Stage Count per Hour per Namespace', 'Has Sort Stage Count',
               output_file=file_name,report=report)

    # Plot writeConflicts count per hour per namespace
    report.add_page()
    report.sub2Chapter_title("write conflicts Count per Hour per Namespace")
    file_name = f"{prefix}_writeConflicts_per_hour.png"
    plot_stats(df, 'writeConflicts', 'write conflicts Count per Hour per Namespace', 'write conflicts Count',
               output_file=file_name,report=report)

    # Plot query targeting per hour per namespace
    report.add_page()
    report.sub2Chapter_title("Query Targeting per Hour per Namespace")
    file_name = f"{prefix}_query_targeting_per_hour.png"
    plot_stats(df, 'query_targeting', 'Query Targeting per Hour per Namespace', 'Query Targeting',
               output_file=file_name,report=report)


    # Plot query targeting per hour per namespace
    report.add_page()
    report.sub2Chapter_title("Skip per Hour per Namespace")
    file_name = f"{prefix}_skip_per_hour.png"
    plot_stats(df, 'sum_skip', 'total skip per Hour per Namespace', 'total skip',
               output_file=file_name,report=report)


    # Group by command shape and calculate statistics
    command_shape_stats = groupbyCommandShape(df_withoutChangestream)
    # Sort by average duration
    command_shape_stats = command_shape_stats.sort_values(by='average_duration', ascending=False)
    filtered_df = command_shape_stats[command_shape_stats['plan_summary'].str.contains("COLLSCAN", na=False)]
    # Create DataFrame excluding filtered rows
    remaining_df = command_shape_stats[~command_shape_stats['plan_summary'].str.contains("COLLSCAN", na=False)]
    # Further split remaining_df
    sort_stage_df = remaining_df[remaining_df['has_sort_stage'] == True]
    no_sort_stage_df = remaining_df[remaining_df['has_sort_stage'] == False]

    with_conflict = no_sort_stage_df[no_sort_stage_df['writeConflictsTotal'] >0]
    without_conflict = no_sort_stage_df[no_sort_stage_df['writeConflictsTotal'] == 0]

    has_skip = without_conflict[without_conflict['sum_skip']>0]
    without_skip = without_conflict[without_conflict['sum_skip']==0]

    save_markdown(filtered_df, 'command_shape_collscan_stats.md', "collscan")
    report.add_page()
    report.subChapter_title("List of slow query shape")
    report.sub2Chapter_title("List of Collscan query shape")
    display_queries(report,filtered_df)

    save_markdown(sort_stage_df, 'command_shape_remaining_hasSort_stats.md', "remainingHasSort")
    report.add_page()
    report.sub2Chapter_title("List of remain hasSortStage query shape")
    display_queries(report,sort_stage_df)

    save_markdown(with_conflict, 'withConflict_stats.md', "withConflict")
    report.add_page()
    report.sub2Chapter_title("List of other query withConflict")
    display_queries(report,with_conflict)

###################SKIP
    save_markdown(has_skip, 'has_skip_stats.md', "has_skip")
    report.add_page()
    report.sub2Chapter_title("List of other query with skip")
    display_queries(report,has_skip)

###################$Regex
###################disk
    save_markdown(without_skip, 'command_shape_others_stats.md', "others")
    report.add_page()
    report.sub2Chapter_title("List of other query shape")
    display_queries(report,without_skip)


    command_shape_cs_stats = groupbyCommandShape(df_changestream)
    save_markdown(command_shape_cs_stats, 'command_shape_cs_stats.md', "changestream")
    report.add_page()
    report.sub2Chapter_title("List of changestream")
    display_queries(report,command_shape_cs_stats)

#----------------------------------------------------------------------------------------
#  Main :
if __name__ == "__main__":
    report = Report()
    report.add_page()
    if USING_API :
       for process in PROCESSES_ID:
           addToReport(retrieveLast24HSlowQueriesFromCluster(GROUP_ID,process,OUTPUT_FILE_PATH),process,report)
    else :
       addToReport(extract_slow_queries(LOG_FILE_PATH, OUTPUT_FILE_PATH),LOG_FILE_PATH,report)

    report.write("slow_report")


