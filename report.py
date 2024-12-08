from pdf import PDF


class Report():
    def __init__(self,config):
        self.config=config
        if self.config.GENERATE_PDF_REPORT :
            self.lpdf = PDF("Slow Query Report")

    def header(self):
        if self.config.GENERATE_PDF_REPORT :
            self.lpdf.header()

    def chapter_title(self, title):
        if self.config.GENERATE_PDF_REPORT :
            self.lpdf.chapter_title(title)

    def subChapter_title(self, subchapter):
        if self.config.GENERATE_PDF_REPORT :
            self.lpdf.subChapter_title(subchapter)

    def sub2Chapter_title(self, subchapter):
        if self.config.GENERATE_PDF_REPORT :
            self.lpdf.sub2Chapter_title(subchapter)

    def sub3Chapter_title(self, subchapter):
        if self.config.GENERATE_PDF_REPORT :
            self.lpdf.sub3Chapter_title(subchapter)

    def sub4Chapter_title(self, subchapter):
        if self.config.GENERATE_PDF_REPORT :
            self.lpdf.sub4Chapter_title(subchapter)

    def chapter_body(self, body):
        if self.config.GENERATE_PDF_REPORT :
            self.lpdf.chapter_body(body)

    def add_code_box(self, code):
        if self.config.GENERATE_PDF_REPORT :
            self.lpdf.add_code_box(code)

    def add_image(self, image_path):
        if self.config.GENERATE_PDF_REPORT :
            self.lpdf.add_image(image_path)

    def add_page(self):
        if self.config.GENERATE_PDF_REPORT :
            self.lpdf.add_page()

    def add_json(self,json):
        if self.config.GENERATE_PDF_REPORT :
            self.lpdf.add_json(json)

    def table(self, df, column):
        if self.config.GENERATE_PDF_REPORT :
            self.lpdf.table(df,column)

    def write(self,name):
        if self.config.GENERATE_PDF_REPORT :
            print(f"Writing {name}.pdf")
            self.lpdf.output(f"{name}.pdf")


