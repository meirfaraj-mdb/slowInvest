import importlib
from abc import ABC, abstractmethod
import logging
reports_logging = logging.getLogger("reports")
reports_logging.setLevel(logging.DEBUG)

class AbstractReport(ABC):
    @abstractmethod
    def header(self):
        pass

    @abstractmethod
    def chapter_title(self, title):
        pass

    @abstractmethod
    def subChapter_title(self, subchapter):
        pass

    @abstractmethod
    def sub2Chapter_title(self, subchapter):
        pass

    @abstractmethod
    def sub3Chapter_title(self, subchapter):
        pass

    @abstractmethod
    def sub4Chapter_title(self, subchapter):
        pass

    @abstractmethod
    def chapter_body(self, body):
        pass

    @abstractmethod
    def add_code_box(self, code):
        pass

    @abstractmethod
    def add_image(self, image_path):
        pass

    @abstractmethod
    def addpage(self):
        pass

    @abstractmethod
    def add_json(self,json):
        pass

    @abstractmethod
    def display_cluster_table(self,cluster):
        pass

    @abstractmethod
    def table(self, df, column):
        pass

    @abstractmethod
    def write(self,name):
        pass



class Report(AbstractReport):

    def __init__(self,reports):
        self.reports=[]
        for report in reports:
            module_name = f"sl_report.{report}_report"
            report_up=report.upper()
            class_name = f"{report_up}Report"
            reports_logging.debug(f"adding report type {report} about to load module {module_name} and class {class_name}")
            try:
                # Dynamically import the module and class
                module = importlib.import_module(module_name)
                report_class = getattr(module, class_name)
                self.reports.append(report_class("Slow Query Report"))
            except (ModuleNotFoundError, AttributeError) as e:
                reports_logging.error(f"Error: {report} report implementation is not available.",e)


    def header(self):
        for report in self.reports:
            report.header()

    def chapter_title(self, title):
        for report in self.reports:
            report.chapter_title(title)

    def subChapter_title(self, subchapter):
        for report in self.reports:
            report.subChapter_title(subchapter)

    def sub2Chapter_title(self, subchapter):
        for report in self.reports:
            report.sub2Chapter_title(subchapter)

    def sub3Chapter_title(self, subchapter):
        for report in self.reports:
            report.sub3Chapter_title(subchapter)

    def sub4Chapter_title(self, subchapter):
        for report in self.reports:
            report.sub4Chapter_title(subchapter)

    def chapter_body(self, body):
        for report in self.reports:
            report.chapter_body(body)


    def add_code_box(self, code):
        for report in self.reports:
            report.add_code_box(code)

    def add_image(self, image_path):
        for report in self.reports:
            report.add_image(image_path)

    def addpage(self):
        for report in self.reports:
            report.addpage()

    def add_json(self,json):
        for report in self.reports:
            report.add_json(json)

    def display_cluster_table(self,cluster):
        for report in self.reports:
            report.display_cluster_table(cluster)

    def table(self, df, column):
        for report in self.reports:
            report.table(df,column)

    def write(self,name):
        for report in self.reports:
            report.write(name)


