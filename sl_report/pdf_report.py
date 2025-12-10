import logging

from fpdf import *
from fpdf.enums import XPos,YPos

from sl_report.report import AbstractReport
from sl_utils.utils import *
import msgspec

from datetime import datetime
import html
pdf_reports_logging = logging.getLogger("pdf_reports")
pdf_reports_logging.setLevel(logging.DEBUG)

decoder = msgspec.json.Decoder()
def get_nested_value(d, key):
    """
    d : dict
    key : string, ex 'prop.subprop'
    """
    keys = key.split('.')
    for k in keys:
        if isinstance(d, dict) and k in d:
            d = d[k]
        else:
            return None
    return d

def create_instance_size_timeline(events):
    # Sort events by their creation time
    events_sorted = sorted(events, key=lambda x: x['created'])

    # Initialize the timeline
    timeline = []

    # Track previous instance size and the start time of the current segment
    previous_instance_size = None
    current_start_time = None

    for event in events_sorted:
        event_time = event['created']
        raw_data = event.get("raw", {})
        instance_size = raw_data.get('newInstanceSize')
        original_instance_size = raw_data.get('originalInstanceSize')
        baseBoundsUpdates = raw_data.get("baseBoundsUpdates",{})
        if baseBoundsUpdates is None:
            boundsUpdates = raw_data.get("boundsUpdates",{})
            if boundsUpdates is None:
                new_min_instance_size = None
                new_max_instance_size = None
            else:
                new_min_instance_size = boundsUpdates.get('newMinInstanceSize',None)
                new_max_instance_size = boundsUpdates.get('newMaxInstanceSize',None)

        else:
            new_min_instance_size = baseBoundsUpdates.get('newMinInstanceSize',None)
            new_max_instance_size = baseBoundsUpdates.get('newMaxInstanceSize',None)

        # Convert creation time to a datetime object
        event_datetime = datetime.strptime(event_time, "%Y-%m-%dT%H:%M:%SZ")

        # If instance size is None, assume continuous size from previous timeline
        if instance_size is None:
            if previous_instance_size is not None and current_start_time is not None:
                end_time = event_datetime
                timeline.append({"start":current_start_time,
                                 "end":end_time,
                                 "instanceSize":previous_instance_size,
                                 "is_min_instance":False,
                                 "is_max_instance":False})
                current_start_time = end_time
            continue

        # Check for size discrepancy
        if previous_instance_size is not None and original_instance_size != previous_instance_size:
            # Insert a transition entry indicating unknown change due to manual or missing event
            unknown_instance = f"unknown_{previous_instance_size}_{original_instance_size}"
            timeline.append({"start":current_start_time,
                             "end":event_datetime,
                             "instanceSize":unknown_instance,
                             "is_min_instance":False,
                             "is_max_instance":False})
            current_start_time = event_datetime
        # Determine if the new instance size matches the min/max thresholds
        is_min_instance = instance_size == new_min_instance_size
        is_max_instance = instance_size == new_max_instance_size

        # Add actual instance change to the timeline
        if current_start_time is not None:
            timeline.append({"start":current_start_time,
                             "end":event_datetime,
                             "instanceSize":instance_size,
                             "is_min_instance":is_min_instance,
                             "is_max_instance":is_max_instance})

        # Update state for next iteration
        previous_instance_size = instance_size
        current_start_time = event_datetime

    # Ensure to close the last segment
    if current_start_time is not None and previous_instance_size is not None:
        timeline.append({"start":current_start_time,
                         "end":events_sorted[-1]['created'],
                         "instanceSize":previous_instance_size,
                         "is_min_instance":False,
                         "is_max_instance":False})

    return timeline

def get_nested_value(data, key):
    keys = key.split('.')
    value = data
    for k in keys:
        if isinstance(value, dict):
            value = value.get(k, '')
        else:
            return ''
    return value

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



class PDFReport(FPDF,AbstractReport):
    def __init__(self, config):
        super().__init__(orientation='P',unit='mm',format='A4')
        self.config = config
        self.isCover = False
        self.headerTxt = config.get_template("title","Slow Query Report")
        self.add_page()
        if config.get_template("include_toc",True) :
            self.insert_toc_placeholder(render_toc, allow_extra_pages=True)
        self.set_section_title_styles(
            # Level 0 titles:
            TextStyle(
                font_family=config.get_template("format.title.level_0.font_family","Times") ,
                font_style=config.get_template("format.title.level_0.font_style","B") ,
                font_size_pt=config.get_template("format.title.level_0.font_size_pt",16),
                color=config.get_template("format.title.level_0.color",128),
                underline=config.get_template("format.title.level_0.underline",True),
                t_margin=config.get_template("format.title.level_0.t_margin",0),
                l_margin=config.get_template("format.title.level_0.l_margin",2),
                b_margin=config.get_template("format.title.level_0.b_margin",0),
            ),
            # Level 1 subtitles:
            TextStyle(
                font_family=config.get_template("format.title.level_1.font_family","Times") ,
                font_style=config.get_template("format.title.level_1.font_style","B") ,
                font_size_pt=config.get_template("format.title.level_1.font_size_pt",14),
                color=config.get_template("format.title.level_1.color",128),
                underline=config.get_template("format.title.level_1.underline",True),
                t_margin=config.get_template("format.title.level_1.t_margin",0),
                l_margin=config.get_template("format.title.level_1.l_margin",10),
                b_margin=config.get_template("format.title.level_1.b_margin",0),
            ),
            # Level 2 subtitles:
            TextStyle(
                font_family=config.get_template("format.title.level_2.font_family","Times") ,
                font_style=config.get_template("format.title.level_2.font_style","B") ,
                font_size_pt=config.get_template("format.title.level_2.font_size_pt",12),
                color=config.get_template("format.title.level_2.color",128),
                underline=config.get_template("format.title.level_2.underline",True),
                t_margin=config.get_template("format.title.level_2.t_margin",0),
                l_margin=config.get_template("format.title.level_2.l_margin",4),
                b_margin=config.get_template("format.title.level_2.b_margin",0),
            ),
            # Level 3 subtitles:
            TextStyle(
                font_family=config.get_template("format.title.level_3.font_family","Times") ,
                font_style=config.get_template("format.title.level_3.font_style","B") ,
                font_size_pt=config.get_template("format.title.level_3.font_size_pt",8),
                color=config.get_template("format.title.level_3.color",128),
                underline=config.get_template("format.title.level_3.underline",True),
                t_margin=config.get_template("format.title.level_3.t_margin",0),
                l_margin=config.get_template("format.title.level_3.l_margin",6),
                b_margin=config.get_template("format.title.level_3.b_margin",0),
            ),
            # Level 4 subtitles:
            TextStyle(
                font_family=config.get_template("format.title.level_4.font_family","Times") ,
                font_style=config.get_template("format.title.level_4.font_style","") ,
                font_size_pt=config.get_template("format.title.level_4.font_size_pt",4),
                color=config.get_template("format.title.level_4.color",128),
                underline=config.get_template("format.title.level_4.underline",True),
                t_margin=config.get_template("format.title.level_4.t_margin",0),
                l_margin=config.get_template("format.title.level_4.l_margin",8),
                b_margin=config.get_template("format.title.level_4.b_margin",0),
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
        self.ln(1)

    def subChapter_title(self, subchapter):
        self.start_section(subchapter,level=1)
        self.ln(1)

    def sub2Chapter_title(self, subchapter):
        self.start_section(subchapter,level=2)
        self.ln(1)
    def sub3Chapter_title(self, subchapter):
        self.start_section(subchapter,level=3)
        self.ln(1)

    def sub4Chapter_title(self, subchapter):
        self.start_section(subchapter,level=4)
        self.ln(1)


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

    def display_line(self,col_width,row_height,dispName,value):
        # Add header for the base name
        if (value is None or value == False or value == 0 or value == '' or value == '0' or
                (isinstance(value, (list, tuple, dict)) and len(value) == 0)):
            return
        self.set_font('helvetica', 'B', 7)
        self.cell(col_width-55, row_height, dispName , border=1)
        self.set_font('helvetica', '', 7)
        self.cell(col_width+55, row_height, convertToHumanReadable(dispName,value), border=1)
        self.ln(row_height)

    def display_cluster_table_general(self,cluster):
        if not self.config.get_template("sections.config.general.include",True) :
            return
        self.subChapter_title(self.config.get_template("sections.config.general.title","General"))
        self.set_font('helvetica', 'B', 8)
        col_width = self.epw / 2
        row_height = self.font_size * 1.5
        displayCluster =self.config.get_fields_array("sections.config.general.fields")

        for line in displayCluster:
            self.display_line(col_width,row_height,line[0],cluster.get(line[1],None))


    def display_cluster_table_advanced(self,cluster):
        if not self.config.get_template("sections.config.advanced.include",True) :
            return
        self.set_font('helvetica', 'B', 8)
        col_width = self.epw / 2
        row_height = self.font_size * 1.5

        self.subChapter_title(self.config.get_template("sections.config.advanced.title","Advanced configuration"))

        advancedConfiguration = cluster.get('advancedConfiguration', {})
        displayCluster =self.config.get_fields_array("sections.config.advanced.fields")

        for line in displayCluster:
            self.display_line(col_width,row_height,line[0],advancedConfiguration.get(line[1],None))

    def display_cluster_table_backupCompliance(self,cluster):
        if not self.config.get_template("sections.config.backup_compliance.include",True) :
            return
        self.set_font('helvetica', 'B', 8)
        col_width = self.epw / 2
        row_height = self.font_size * 1.5

        if cluster["backupCompliance_configured"] == "True":
            self.subChapter_title(self.config.get_template("sections.config.backup_compliance.title","Backup Compliance"))
            for k,v in cluster["backupCompliance"].items():
                self.display_line(col_width,row_height,k,v)
        #todo fields

    def display_cluster_table_backupSnapshot(self,cluster):
        if not self.config.get_template("sections.config.backup_compliance.include",True) :
            return
        self.set_font('helvetica', 'B', 8)
        col_width = self.epw / 2
        row_height = self.font_size * 1.5

        if cluster["backupCompliance_configured"] == "True":
            self.subChapter_title(self.config.get_template("sections.config.backup_compliance.title","Backup Compliance"))
            for k,v in cluster["backupCompliance"].items():
                self.display_line(col_width,row_height,k,v)
        #todo fields


    def display_cluster_table_replication_prc_specs(self,type,region_elect,autoscaling):
        if not self.config.get_template(f"sections.config.replication_specs.{type}.include",True) :
            return
        self.sub4Chapter_title(self.config.get_template(f"sections.config.replication_specs.{type}.title","Prc"))
        node_count = region_elect.get('nodeCount',0)
        if node_count > 0:
            self.set_font('helvetica', 'B', 8)
            col_width = self.epw / 2
            row_height = self.font_size * 1.5
            display_cluster =self.config.get_fields_array(f"sections.config.replication_specs.{type}.fields")
            for line in display_cluster:
                self.display_line(col_width,row_height,line[0],region_elect.get(line[1],None))
            if autoscaling :
                display_cluster =self.config.get_fields_array(f"sections.config.replication_specs.{type}.autoscaling.fields")
                for line in display_cluster:
                    self.display_line(col_width,row_height,line[0],get_nested_value(autoscaling, line[1]))

    def display_cluster_table_replication_electable_specs(self,region_elect,autoscaling):
        self.display_cluster_table_replication_prc_specs("electable_specs",region_elect,autoscaling)

    def display_cluster_table_replication_read_only_specs(self,region_elect):
        self.display_cluster_table_replication_prc_specs("read_only_specs",region_elect,None)

    def display_cluster_table_replication_analytic_specs(self,region_elect,autoscaling):
        self.display_cluster_table_replication_prc_specs("analytic_specs",region_elect,autoscaling)



    def display_cluster_table_replication_specs(self,cluster):
        if not self.config.get_template("sections.config.replication_specs.include",True) :
            return

        self.set_font('helvetica', 'B', 8)
        col_width = self.epw / 2
        row_height = self.font_size * 1.5

        replicationSpecs = cluster.get('replicationSpecs',None)

        if replicationSpecs is not None:
            self.subChapter_title(f"Replication/cluster Spec")
            idNum=0
            for spec in replicationSpecs:
                ext= "(Shard) " if cluster.get('clusterType') in ["SHARDED", "GEOSHARDED"] else ""
                self.sub2Chapter_title(f"Replication Spec {ext}No {idNum} Id: {spec.get('id','')} Zone:{spec.get('zoneName','')}({spec.get('zoneId','')})")
                regionConfigs=spec.get('regionConfigs',[])
                regionNum=0
                for regionConfig in regionConfigs:
                    self.sub3Chapter_title(f"Region No {regionNum} {regionConfig.get('providerName','')} : Name: {regionConfig.get('regionName','')} Priority:{regionConfig.get('priority','')}")
                    self.display_cluster_table_replication_electable_specs(regionConfig.get("electableSpecs",{}),
                                                                           regionConfig.get("autoScaling",None))
                    self.display_cluster_table_replication_read_only_specs(regionConfig.get('readOnlySpecs',{}))
                    self.display_cluster_table_replication_analytic_specs(regionConfig.get('analyticsSpecs',{}),
                                                                          regionConfig.get("analyticsAutoScaling",None))

                    regionNum = regionNum + 1
                processes = cluster.get('processes', None)
                if processes is not None:
                    shard_process=processes.get(idNum,{})
                    primary = shard_process.get("primary",{})
                    self.sub3Chapter_title(f"Primary Process")
                    for k,v in primary.items():
                        self.display_line(col_width,row_height,k,v)
                    self.sub3Chapter_title(f"Others Process")
                    others = shard_process.get("others",{})
                    for proc in others:
                        for k,v in proc.items():
                            self.display_line(col_width,row_height,k,v)
                        self.ln(4)
                idNum=idNum+1
        connectionStrings = cluster.get('connectionStrings',None)
        if connectionStrings is not None:
            self.subChapter_title(f"Connection Strings")
            displayCluster = [
                ["AWS Private Link SRV",'awsPrivateLinkSrv'],
                ["Private Endpoint",'privateEndpoint'],
                ["Standard",'standard'],
                ["Standard SRV",'standardSrv']
            ]
            for line in displayCluster:
                self.display_line(col_width,row_height,line[0],connectionStrings.get(line[1],None))


    def display_cluster_table(self,cluster):
        if not self.config.get_template("sections.config.include",True) :
            return
        if self.config.get_template("initial_empty_page",True) :
            self.add_page()
        name=cluster.get('name')
        self.chapter_title(f"Cluster {name} report")
        cluster["performanceAdvisorSuggestedIndexes_count"] = len(cluster["performanceAdvisorSuggestedIndexes"]
                                                                  .get('content',{}).get('suggestedIndexes',[]))

        self.display_cluster_table_general(cluster)

        self.display_cluster_table_advanced(cluster)

        self.display_cluster_table_backupCompliance(cluster)
        #backup_snapshot
        self.display_cluster_table_replication_specs(cluster)

        scaling=cluster.get("scaling",[])
        if len(scaling)>0:
            if self.config.get_template("initial_empty_page",True) :
                self.add_page()
            self.subChapter_title("Scaling information "+cluster.get("name",""))
            col_diff={'id':15,'created':8,'scal_type':-4,"scal_succeed":-8,"scal_fail_cause":5,'raw.originalCostPerHour':-4,'raw.newCostPerHour':-4,'raw.originalDiskSizeGB':-4,'raw.newDiskSizeGB':-4,'raw.originalInstanceSize':-10, 'raw.newInstanceSize':-10,'raw.isAtMaxCapacityAfterAutoScale':-5}
            self.add_table(scaling,
                   ['id','created','scal_type',"scal_succeed","scal_fail_cause",'raw.originalCostPerHour','raw.newCostPerHour', 'raw.originalDiskSizeGB','raw.newDiskSizeGB','raw.originalInstanceSize', 'raw.newInstanceSize','raw.isAtMaxCapacityAfterAutoScale'],
                   ['id','time','scale type',"success","fail cause",'orig Cost/H','new Cost/H', 'orig DiskGB','new DiskGB','orig T', 'new T','@MaxAfter'],
                           col_diff,5)
            self.subChapter_title("Scaling Details"+cluster.get("name",""))


            col_diff={'id':-75,"compute_auto_scaling_triggers":75}
            self.add_table(scaling,
                           ['id', 'compute_auto_scaling_triggers'],
                           ['id','compute auto scale triggers'],
                           col_diff,4,5,True)
            timeline = create_instance_size_timeline(scaling)
            # Output the timeline
            minTable = [dp for dp in timeline if dp.get('is_min_instance',False)]
            if len(minTable)>0:
                self.sub2Chapter_title("Scaling At minimum for "+cluster.get("name",""))
                self.add_table(minTable,
                           ['start', 'end','instanceSize'],size=8)

            maxTable = [dp for dp in timeline if dp.get('is_max_instance',False)]
            if len(minTable)>0:
                self.sub2Chapter_title("Scaling At maximum for "+cluster.get("name",""))
                self.add_table(maxTable,
                           ['start', 'end','instanceSize'])

            for dp in timeline:
                start_str=dp.get("start","")
                end_str=dp.get("end","")
                instanceSize_str = dp.get("instanceSize","")
                is_min_instance = dp.get("is_min_instance","")
                is_max_instance=dp.get("is_max_instance","")
                print(f"Start: {start_str}, End: {end_str}, Instance Size: {instanceSize_str}, isMinInstance: {is_min_instance}, isMaxInstance: {is_max_instance}")

        custom_alert=cluster.get("custom_alert", {})
        if len(custom_alert) > 0:
            if self.config.get_template("initial_empty_page",True) :
                self.add_page()
            self.subChapter_title("Custom alert for "+cluster.get("name",""))
            scaling_alert=custom_alert.get("scaling",{})
            if len(scaling_alert) > 0:
                self.sub2Chapter_title("Scaling alert for "+cluster.get("name",""))
                alerts=[]

                keys=scaling_alert.keys()
                for key in keys:
                    value = scaling_alert.get(key,{})
                    al={}
                    al["type"]=key
                    al["details"]=value
                    alerts.append(al)
                self.add_table(alerts,
                           ['type', 'details.count'],
                           ['type','count'],
                               {},8)
        self.add_page()


    def add_table(self, data_list, columns,columns_name=None,col_size_diff={},size=4,line=1,skipIfColumnEmpty=False):
        """Add a pdf table."""
        # Add column headers
        num_columns = len(columns)
        self.set_font('helvetica', 'B', size)
        col_width = self.epw / num_columns
        row_height = self.font_size * 1.5

        diff_col = []
        for col in columns:
            diff_col.append(col_size_diff.get(col,0))
        if columns_name is None:
            i=0
            for col in columns:
                self.cell(col_width+diff_col[i], row_height, self.clean_name(col), border=1)
                i+=1
        else:
            i=0
            for col in columns_name:
                self.cell(col_width+diff_col[i], row_height, str(col), border=1)
                i+=1
        self.ln(row_height)
        self.set_font('helvetica', '', size)
        # Add each data row
        for data in data_list:
            i=0
            if skipIfColumnEmpty :
                containsEmpty = False
                for col in columns:
                    if len(str(get_nested_value(data, col)).strip())==0:
                        containsEmpty = True
                if containsEmpty :
                    continue
            for col in columns:
                if(i<len(columns)-1):
                    self.cell(col_width+diff_col[i], line*row_height, str(get_nested_value(data,col)),
                                    new_y=YPos.TOP, border=1)
                else:
                    self.multi_cell(col_width+diff_col[i], row_height, str(get_nested_value(data,col)),
                                new_y=YPos.TOP, border=1)
                i+=1
            self.ln(row_height*line)



    def table(self, row, columns):
        # Calculate column width based on the number of columns
        num_columns = len(columns)
        col_width = self.epw / 2
        row_height = self.font_size * 1.5
        # Dictionary to hold aggregated values
        aggregated_values = {}
        # Process columns to aggregate min, max, avg, total
        for col in columns:
            if col.endswith(('_min', '_max', '_avg', '_total',"_count")):
                base_name = col.rsplit('_', 1)[0]
                if base_name not in aggregated_values:
                    aggregated_values[base_name] = {}
                # Store the value in the appropriate category
                category = col.rsplit('_', 1)[1]
                if row.get(col, 0)!=0 :
                    if col.endswith("_count"):
                        aggregated_values[base_name][category] = convertToHumanReadable(col,row.get(col, 0))
                    else:
                        aggregated_values[base_name][category] = convertToHumanReadable(base_name,row.get(col, 0))

        # Process unique columns
        for col in columns:
            if not col.endswith(('_min', '_max', '_avg', '_total',"_count")):
                value = row.get(col, 0)
                if isinstance(value, list) and len(value) <= 1:
                    if len(value) == 0:
                        value=''
                    else:
                        value=value[0]
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
            if len(values)<=1 or all(v == 0 for v in values.values()):
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

    def add_image(self, image_path, img_width=None, padding=2):
        """Add an image to the PDF. If enough space remains in the row, place it next to the current image."""
        try:
            self.set_text_color(0, 0, 0)  # Black text
            # Width for image: default to half of effective page width
            if img_width is None:
                img_width = self.epw / 2.5

            current_x = self.get_x()
            current_y = self.get_y()

            # Does the image fit in remaining width?
            remaining_width = self.epw - (current_x - self.l_margin)
            if img_width <= remaining_width:
                # Fits, place it here
                self.image(image_path, x=current_x, y=current_y, w=img_width, keep_aspect_ratio=True)
                # Move X for next image, with padding
                self.set_x(current_x + img_width + padding)
            else:
                # Not enough space, move to next line
                # Find tallest image height in row? Here we just jump down by img_width ratio
                img_height = img_width * 0.75  # approximate height (depends on aspect ratio)
                self.set_xy(self.l_margin, current_y + img_height + padding)
                self.image(image_path, x=self.get_x(), y=self.get_y(), w=img_width, keep_aspect_ratio=True)
                self.set_x(self.get_x() + img_width + padding)

        except Exception:
            pdf_reports_logging.exception(f"Fail to add image: {image_path}")
            #self.ln(10)


    def add_colored_json(self, json_data, w=None):
        """
        Render colored JSON in a light gray box at the current position,
        with correct multi-page handling. Draws rect first, writes content once.
        """
        # Save original graphic state
        orig_font_family = self.font_family
        orig_font_style = self.font_style
        orig_font_size_pt = self.font_size_pt
        orig_fill_color = self.fill_color
        if not (
                isinstance(orig_fill_color, (tuple, list)) and
                len(orig_fill_color) == 3 and
                all(isinstance(c, (int, float)) for c in orig_fill_color)
        ):
            orig_fill_color = (0, 0, 0)  # default black

        # Default width = remaining space
        if w is None:
            left_margin = self.l_margin
            right_margin = self.r_margin
            page_width = self.w
            w = page_width - self.get_x() - right_margin

        try:
            # Parse JSON if needed
            if isinstance(json_data, (str, bytes)):
                parsed = decoder.decode(json_data)
            else:
                parsed = json_data

            html_content = self.json_to_html(parsed)

            # Set font for JSON
            self.set_font("Courier", "", 7)

            start_x = self.get_x()
            start_y = self.get_y()

            # --- Height estimation from <br> ---
            lines = html_content.split("<br>")
            line_height = self.font_size_pt * 0.3528  # pt to mm
            estimated_height = (len(lines) * line_height) + 4  # +4mm padding

            # If we don't have enough space left, add a page before drawing
            if start_y + estimated_height > self.page_break_trigger:
                self.add_page()
                start_x = self.get_x()
                start_y = self.get_y()

                # --- Draw rectangle first ---
            self.set_fill_color(240, 240, 240)
            self.rect(start_x, start_y, w, estimated_height, 'F')

            # --- Write content only once on top of rectangle ---
            self.set_xy(start_x + 2, start_y + 2)  # padding
            self.write_html(html_content)

            # Move cursor below box after content
            self.set_y(start_y + estimated_height)

        except Exception as e:
            logging.exception(f"Error rendering JSON in PDF: {e}")
            self.write_html(f"<font color='red'>Invalid JSON: {html.escape(str(e))}</font>")
            self.ln(5)

        finally:
            # Restore original font/fill
            self.set_font(orig_font_family, orig_font_style, orig_font_size_pt)
            self.set_fill_color(*orig_fill_color)

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
        self.add_colored_json(json_data, w=190)

    def write(self,name):
        print(f"Writing {name}.pdf")
        self.output(f"{name}.pdf")


    def addpage(self):
        super().add_page()

#----------------------------------------------------------------
