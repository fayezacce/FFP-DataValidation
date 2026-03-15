"""
PDF Report Generator for FFP Data Validator
Author: Fayez Ahmed
"""
import os
import uuid
import pandas as pd
from fpdf import FPDF

class ReportPDF(FPDF):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.show_cols = []
        self.col_widths = []
        self.print_table_headers = False
        self.report_title = "Food Friendly Program Data Validation Report"

    def header(self):
        self.set_font("Nikosh", '', 16)
        self.cell(0, 10, self.report_title, align='C', new_x="LMARGIN", new_y="NEXT")
        self.ln(5)
        if self.print_table_headers and self.show_cols and self.col_widths:
            self.set_font("Nikosh", '', 9)
            for col, width in zip(self.show_cols, self.col_widths):
                self.cell(width, 10, str(col), border=1, align='C')
            self.ln()

    def footer(self):
        self.set_y(-15)
        self.set_font("Nikosh", '', 8)
        self.cell(0, 10, f"Page {self.page_no()}  |  Computer Network Unit, Directorate General of Food", align='C')


def generate_pdf_report(
    df: pd.DataFrame,
    stats: dict,
    additional_columns: list = None,
    output_dir: str = "downloads",
    original_filename: str = "",
    geo: dict = None,
    invalid_only: bool = False,
    custom_title: str = None,
    issues_label: str = "Invalid Rows",
    status_filter: str = "error",
) -> str:
    """Generate a PDF validation report.

    Args:
        invalid_only: When True, only error rows are included and the filename/title
                      reflect an "invalid records" report.
    """
    if additional_columns is None:
        additional_columns = []
    if geo is None:
        geo = {"division": "Unknown", "district": "Unknown", "upazila": "Unknown"}

    os.makedirs(output_dir, exist_ok=True)

    if invalid_only:
        suffix = "_invalid_Report.pdf"
        title = custom_title or "Food Friendly Program — Invalid Records Report"
        if status_filter:
            df = df[df["Status"] == status_filter].copy()
    else:
        suffix = "_validation_Report.pdf"
        title = custom_title or "Food Friendly Program Data Validation Report"

    filename = (
        f"{original_filename}{suffix}"
        if original_filename
        else f"report_{uuid.uuid4().hex[:8]}.pdf"
    )
    filepath = os.path.join(output_dir, filename)

    pdf = ReportPDF("L", "mm", "A4")  # Landscape
    pdf.report_title = title

    # Register Nikosh Font (supports Bengali)
    font_path = os.path.join(os.path.dirname(__file__), "..", "Nikosh.ttf")
    if os.path.exists(font_path):
        pdf.add_font("Nikosh", "", font_path, uni=True)
    else:
        pdf.add_font("Nikosh", "", "C:\\Windows\\Fonts\\arial.ttf", uni=True)

    if hasattr(pdf, "set_text_shaping"):
        pdf.set_text_shaping(True)

    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    # ── Summary Section ────────────────────────────────────────────────────────
    pdf.set_font("Nikosh", "", 12)
    pdf.cell(0, 10, "Summary", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Nikosh", "", 11)
    if original_filename:
        pdf.cell(0, 8, f"File Name:      {original_filename}", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, f"Division:       {geo.get('division', 'Unknown')}", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, f"District:       {geo.get('district', 'Unknown')}", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, f"Upazila:        {geo.get('upazila', 'Unknown')}", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, f"Total Rows:     {stats['total_rows']}", new_x="LMARGIN", new_y="NEXT")

    if invalid_only:
        pdf.cell(0, 8, f"{issues_label}:   {stats['issues']}", new_x="LMARGIN", new_y="NEXT")
    else:
        valid_count = stats["total_rows"] - stats["issues"]
        pdf.cell(0, 8, f"Valid Rows:     {valid_count}", new_x="LMARGIN", new_y="NEXT")
        pdf.cell(0, 8, f"Invalid Rows:   {stats['issues']}", new_x="LMARGIN", new_y="NEXT")
        pdf.cell(0, 8, f"NIDs Converted: {stats['converted_nid']}", new_x="LMARGIN", new_y="NEXT")

    pdf.ln(10)

    # ── Empty state ────────────────────────────────────────────────────────────
    if df.empty:
        pdf.set_font("Nikosh", "", 12)
        pdf.cell(0, 10, "No invalid records found in this file.", new_x="LMARGIN", new_y="NEXT")
        pdf.output(filepath)
        return filepath

    # ── Table ──────────────────────────────────────────────────────────────────
    pdf.set_font("Nikosh", "", 9)

    show_cols = ["Excel_Row", "Cleaned_DOB", "Cleaned_NID", "Status", "Message"]
    total_usable_width = 277

    valid_additional = [c for c in additional_columns if c in df.columns and c not in show_cols]
    show_cols.extend(valid_additional)

    core_widths = [15, 25, 35, 20, 60]
    col_widths = list(core_widths)

    if valid_additional:
        extra_width_per_col = max(15, min(40, 117 // len(valid_additional)))
        col_widths.extend([extra_width_per_col] * len(valid_additional))

    total_w = sum(col_widths)
    scale = total_usable_width / total_w
    col_widths = [w * scale for w in col_widths]

    pdf.show_cols = show_cols
    pdf.col_widths = col_widths

    def print_headers():
        pdf.set_font("Nikosh", "", 9)
        for col, width in zip(show_cols, col_widths):
            pdf.cell(width, 10, str(col), border=1, align="C")
        pdf.ln()

    print_headers()
    pdf.print_table_headers = True
    pdf.set_font("Nikosh", "", 9)

    for _, row in df.iterrows():
        status = row.get("Status", "")

        if status == "error":
            pdf.set_fill_color(255, 200, 200)
        elif status == "warning":
            pdf.set_fill_color(255, 240, 200)
        else:
            pdf.set_fill_color(255, 255, 255)

        for col, width in zip(show_cols, col_widths):
            val = str(row.get(col, ""))
            if len(val) > int(width):
                val = val[: int(width) - 1] + ".."
            pdf.cell(width, 8, val, border=1, align="L", fill=True)
        pdf.ln()

    pdf.output(filepath)
    return filepath
