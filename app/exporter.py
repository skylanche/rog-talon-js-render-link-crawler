"""Convert the crawl's raw edges CSV into a formatted XLSX, streaming row by
row (openpyxl write-only mode) so this stays fast and low-memory even when
the CSV has millions of rows."""

import csv
from pathlib import Path

from openpyxl import Workbook
from openpyxl.cell import WriteOnlyCell
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter

HEADER_FILL = PatternFill(start_color="1F2937", end_color="1F2937", fill_type="solid")
HEADER_FONT = Font(color="FFFFFF", bold=True)

COLUMN_WIDTHS = {
    "from_url": 55,
    "to_url": 55,
    "link_text": 30,
    "is_internal": 10,
    "status_code": 11,
    "content_type": 22,
    "error": 30,
    "rendered_with_js": 14,
    "depth": 8,
}


def csv_to_xlsx(csv_path: Path, xlsx_path: Path) -> Path:
    wb = Workbook(write_only=True)
    ws = wb.create_sheet("Links")

    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        header_cells = []
        for col_name in header:
            cell = WriteOnlyCell(ws, value=col_name)
            cell.font = HEADER_FONT
            cell.fill = HEADER_FILL
            header_cells.append(cell)
        ws.append(header_cells)
        for row in reader:
            # keep booleans/ints as real types where possible for nicer filtering in Excel
            converted = []
            for col_name, value in zip(header, row):
                if col_name == "status_code" and value:
                    try:
                        converted.append(int(value))
                        continue
                    except ValueError:
                        pass
                if col_name in ("is_internal", "rendered_with_js"):
                    converted.append(value == "True")
                    continue
                if col_name == "depth" and value != "":
                    try:
                        converted.append(int(value))
                        continue
                    except ValueError:
                        pass
                converted.append(value)
            ws.append(converted)

    for idx, col_name in enumerate(header, start=1):
        ws.column_dimensions[get_column_letter(idx)].width = COLUMN_WIDTHS.get(col_name, 18)

    wb.save(xlsx_path)
    return xlsx_path
