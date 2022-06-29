from PyPDF2 import PdfFileMerger
from fpdf import FPDF

def write_text(stringa):
    pdf=FPDF()
    pdf.add_page()
    pdf.set_font('Times')
    pdf.cell(0,5,stringa,1,1)
    pdf.output('text.pdf')


def merge_pdf(paths):
    merger=PdfFileMerger()
    for path in paths:
        merger.append(path)
    merger.write("report.pdf")
    merger.close()