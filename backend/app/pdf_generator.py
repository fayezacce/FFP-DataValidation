"""
PDF Report Generator for FFP Data Validator
Author: Fayez Ahmed
"""
import os
import uuid
import pandas as pd
from fpdf import FPDF

# Module-level font path cache — resolved once, used for all PDF calls
_NIKOSH_FONT_PATH_CACHE = None

def _get_nikosh_font_path():
    """Return the cached path to Nikosh.ttf. Resolved once on first call."""
    global _NIKOSH_FONT_PATH_CACHE
    if _NIKOSH_FONT_PATH_CACHE is None:
        candidate = os.path.join(os.path.dirname(__file__), "..", "Nikosh.ttf")
        if os.path.exists(candidate):
            _NIKOSH_FONT_PATH_CACHE = os.path.abspath(candidate)
        else:
            _NIKOSH_FONT_PATH_CACHE = ""  # Empty = not found
    return _NIKOSH_FONT_PATH_CACHE or None

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
                display_col = "DOB" if col == "Cleaned_DOB" else ("NID" if col == "Cleaned_NID" else str(col))
                self.cell(width, 10, display_col, border=1, align='C')
            self.ln()

    def footer(self):
        self.set_y(-15)
        self.set_font("Nikosh", '', 8)
        self.cell(0, 10, f"Page {self.page_no()}  |  Computer Network Unit, Directorate General of Food", align='C')


# Performance threshold: Reports larger than this will only contain the summary
# to prevent memory exhaustion and hangs during national-scale exports.
SUMMARY_THRESHOLD = 10000

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
    """Generate a PDF validation report with automatic large-dataset protection."""
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
    font_path = _get_nikosh_font_path()
    if font_path:
        pdf.add_font("Nikosh", "", font_path, uni=True)
    else:
        # Fallback for local development environments
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

    # ── Protection Logic for Large Datasets ────────────────────────────────────
    row_count = len(df)
    if row_count > SUMMARY_THRESHOLD:
        pdf.set_font("Nikosh", "", 12)
        pdf.set_text_color(200, 0, 0)
        warning_msg = f"NOTE: This report contains {row_count} records. Detail rows are omitted to ensure system stability."
        pdf.cell(0, 10, warning_msg, new_x="LMARGIN", new_y="NEXT")
        pdf.set_text_color(0, 0, 0)
        pdf.set_font("Nikosh", "", 11)
        pdf.cell(0, 10, "Please use the exported Excel/CSV file for full record details.", new_x="LMARGIN", new_y="NEXT")
        pdf.output(filepath)
        return filepath

    # ── Empty state ────────────────────────────────────────────────────────────
    if df.empty:
        pdf.set_font("Nikosh", "", 12)
        pdf.cell(0, 10, "No invalid records found in this file.", new_x="LMARGIN", new_y="NEXT")
        pdf.output(filepath)
        return filepath

    # ── Table (Only for manageable sizes) ──────────────────────────────────────
    pdf.set_font("Nikosh", "", 9)

    # Fixed 5-column layout — full usable width on A4 Landscape (277mm)
    # No extra columns appended regardless of additional_columns argument
    show_cols = []
    col_labels = []
    col_widths = []

    # Map available column names (data may store DOB/NID with different aliases)
    dob_col  = next((c for c in ["DOB", "Cleaned_DOB"] if c in df.columns), None)
    nid_col  = next((c for c in ["NID", "Cleaned_NID"] if c in df.columns), None)
    row_col  = "Excel_Row" if "Excel_Row" in df.columns else None
    stat_col = "Status" if "Status" in df.columns else None
    msg_col  = "Message" if "Message" in df.columns else None

    # Build column list in fixed order: Row | DOB | NID | Status | Message
    if row_col:  show_cols.append(row_col);  col_labels.append("Row")
    if dob_col:  show_cols.append(dob_col);  col_labels.append("DOB")
    if nid_col:  show_cols.append(nid_col);  col_labels.append("NID")
    if stat_col: show_cols.append(stat_col); col_labels.append("Status")
    if msg_col:  show_cols.append(msg_col);  col_labels.append("Message")

    # Raw proportional widths (will be scaled to exactly fill 277mm)
    raw_widths_map = {"Row": 12, "DOB": 28, "NID": 42, "Status": 22, "Message": 173}
    raw_widths = [raw_widths_map.get(lbl, 30) for lbl in col_labels]
    total_usable_width = 277
    scale = total_usable_width / sum(raw_widths)
    col_widths = [w * scale for w in raw_widths]

    # Sort by NID so duplicate NIDs appear consecutively in the list
    sort_col = nid_col or dob_col
    if sort_col and sort_col in df.columns:
        df = df.sort_values(by=sort_col, kind="mergesort", na_position="last").reset_index(drop=True)

    pdf.show_cols = show_cols
    pdf.col_widths = col_widths

    def get_display_name(c):
        if c in ("Cleaned_DOB", "DOB"): return "DOB"
        if c in ("Cleaned_NID", "NID"): return "NID"
        if c == "Excel_Row": return "Row"
        return str(c)

    def print_headers():
        pdf.set_font("Nikosh", "", 9)
        for col, width in zip(show_cols, col_widths):
            pdf.cell(width, 10, get_display_name(col), border=1, align="C")
        pdf.ln()

    print_headers()
    pdf.print_table_headers = True
    pdf.set_font("Nikosh", "", 9)

    for _, row in df.iterrows():
        status = str(row.get("Status", ""))

        if status == "error":
            pdf.set_fill_color(255, 200, 200)
        elif status == "warning":
            pdf.set_fill_color(255, 240, 200)
        else:
            pdf.set_fill_color(255, 255, 255)

        for col, width in zip(show_cols, col_widths):
            val = str(row.get(col, "") or "")
            max_chars = max(int(width * 1.6), 10)  # Approximate chars that fit
            if len(val) > max_chars:
                val = val[:max_chars - 2] + ".."
            pdf.cell(width, 8, val, border=1, align="L", fill=True)
        pdf.ln()

    pdf.output(filepath)
    return filepath
