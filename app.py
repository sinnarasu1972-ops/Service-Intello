# app.py (Render-ready)

import os
import io
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

from flask import Flask, jsonify, request, send_file, render_template_string
from openpyxl import load_workbook, Workbook

app = Flask(__name__)

# =============================================================================
# 1) CONFIGURATION (RENDER SAFE)
# =============================================================================
HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "5000"))

# Put Excel files in repo folder: ./data/
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

PERSONAL_XLSX = os.environ.get("PERSONAL_XLSX", os.path.join(DATA_DIR, "Link Triggered.xlsx"))
MEAL_XLSX = os.environ.get("MEAL_XLSX", os.path.join(DATA_DIR, "Link Triggered MEAL.xlsx"))
BODYSHOP_XLSX = os.environ.get("BODYSHOP_XLSX", os.path.join(DATA_DIR, "Link Triggered BP.xlsx"))
COMMERCIAL_XLSX = os.environ.get("COMMERCIAL_XLSX", os.path.join(DATA_DIR, "Link Triggered Commercial.xlsx"))

MONTH_ORDER = ["Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "Jan", "Feb", "Mar"]

# =============================================================================
# 2) HELPERS / UTILITIES (same as your file)
# =============================================================================
def s(v: Any) -> Optional[str]:
    if v is None:
        return None
    t = str(v).strip()
    if not t or t == "-":
        return None
    return t

def to_float(v: Any) -> Optional[float]:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    t = str(v).strip()
    if not t or t.lower() in {"na", "n/a", "none", "-"}:
        return None
    t = t.replace(",", "").replace("₹", "").strip()
    if t.endswith("%"):
        t = t[:-1].strip()
    try:
        return float(t)
    except Exception:
        return None

def r2(v: Optional[float]) -> Optional[float]:
    if v is None:
        return None
    try:
        return round(float(v), 2)
    except Exception:
        return None

def safe_div(num: Optional[float], den: Optional[float]) -> Optional[float]:
    if num is None or den is None or den == 0:
        return None
    return num / den

def normalize_header(h: Any) -> str:
    if h is None:
        return ""
    t = str(h).strip()
    if not t:
        return ""
    t = " ".join(t.split())
    if t.lower() == "diviosion":
        return "Division"
    return t

def detect_month(sheet_name: str) -> Optional[str]:
    if not sheet_name:
        return None
    key = sheet_name.strip()[:3].lower()
    for m in MONTH_ORDER:
        if m.lower() == key:
            return m
    return None

def avg(values: List[Optional[float]]) -> Optional[float]:
    nums = [v for v in values if isinstance(v, (int, float)) and v is not None]
    return (sum(nums) / len(nums)) if nums else None

def export_xlsx(columns: List[str], rows: List[Dict[str, Any]]) -> io.BytesIO:
    wb = Workbook()
    ws = wb.active
    ws.title = "Export"
    ws.append(columns)
    for row in rows:
        ws.append(["" if row.get(c) is None else row.get(c) for c in columns])
    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio

def key_norm(x: Optional[str]) -> str:
    return (x or "All").strip().lower()

# =============================================================================
# 3) COLUMN DETECTION (same as your file)
# =============================================================================
def build_header_map(header_row: List[Any]) -> Dict[str, int]:
    hmap: Dict[str, int] = {}
    for i, h in enumerate(header_row):
        nh = normalize_header(h)
        if nh:
            hmap[nh.lower()] = i
    return hmap

def find_col_index_any(hmap: Dict[str, int], tokens: List[str]) -> Optional[int]:
    for k, idx in hmap.items():
        kk = (k or "").lower()
        for t in tokens:
            if t in kk:
                return idx
    return None

def find_col_index_priority(hmap: Dict[str, int], token_groups: List[List[str]]) -> Optional[int]:
    for tokens in token_groups:
        idx = find_col_index_any(hmap, tokens)
        if idx is not None:
            return idx
    return None

# =============================================================================
# 4) DATASET (same logic as your file)
# =============================================================================
class Dataset:
    def __init__(self, name: str, excel_path: str):
        self.name = name
        self.excel_path = excel_path
        self.data_rows: List[Dict[str, Any]] = []
        self.available_months: List[str] = []
        self.load_error: Optional[str] = None
        self.index: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = defaultdict(list)
        self.div_by_month: Dict[str, set] = defaultdict(set)
        self.sa_by_month_div: Dict[Tuple[str, str], set] = defaultdict(set)
        self._load_and_index()

    def _load_and_index(self) -> None:
        try:
            self.data_rows, self.available_months = self._load_excel()
            self._build_indexes(self.data_rows)
            self.load_error = None
        except Exception as e:
            self.load_error = str(e)
            self.data_rows = []
            self.available_months = []
            self._build_indexes([])

    def _load_excel(self) -> Tuple[List[Dict[str, Any]], List[str]]:
        if not os.path.exists(self.excel_path):
            raise FileNotFoundError(f"Excel file not found: {self.excel_path}")

        wb = load_workbook(self.excel_path, data_only=True)
        all_rows: List[Dict[str, Any]] = []
        months_present = set()

        for sheet in wb.sheetnames:
            month = detect_month(sheet)
            if not month:
                continue
            months_present.add(month)

            ws = wb[sheet]
            it = ws.iter_rows(values_only=True)
            try:
                header = list(next(it))
            except StopIteration:
                continue

            hmap = build_header_map(header)

            c_mile = find_col_index_priority(hmap, [["mile id"], ["mileid"], ["mile"], ["mile_id"]])
            c_links = find_col_index_priority(hmap, [["links triggered"], ["links trig"], ["link triggered"], ["links"], ["trigger"]])
            c_resp = find_col_index_priority(hmap, [["total response"], ["responses"], ["response"], ["respon"], ["reply"]])
            c_nps = find_col_index_priority(hmap, [["nps"], ["np score"], ["np"]])
            c_concern = find_col_index_priority(hmap, [["concern count"], ["concern"], ["complaint"]])
            c_cc = find_col_index_priority(hmap, [["cc/1000"], ["cc/10"], ["cc per"], ["cc per 1000"]])
            c_osat = find_col_index_priority(hmap, [["osat"], ["overall satisfaction"], ["overall sat"], ["osat%"], ["os"]])
            c_sa = find_col_index_priority(hmap, [["sa name"], ["service advisor name"], ["service advisor"], ["advisor name"], ["advisor"], ["sa  "], ["sa_"], ["sa"]])
            c_div = find_col_index_priority(hmap, [["division name"], ["division"], ["diviosion"], ["divn"], ["div."], ["div "], ["div"],
                                                  ["branch name"], ["branch"], ["outlet"], ["workshop"], ["dealer location"], ["location"]])

            def cell(row: Tuple[Any, ...], idx: Optional[int]) -> Any:
                if idx is None or idx >= len(row):
                    return None
                return row[idx]

            for row in it:
                if not row:
                    continue

                links = to_float(cell(row, c_links))
                resp = to_float(cell(row, c_resp))
                nps = to_float(cell(row, c_nps))
                concern = to_float(cell(row, c_concern))
                cc = to_float(cell(row, c_cc))

                osat = to_float(cell(row, c_osat))
                if osat is not None and 0 <= osat <= 1:
                    osat = r2(osat * 100.0)
                else:
                    osat = r2(osat)

                pct = None
                ratio = safe_div(resp, links)
                if ratio is not None:
                    pct = r2(ratio * 100.0)

                rec = {
                    "Month": month,
                    "SA Name": s(cell(row, c_sa)),
                    "Division": s(cell(row, c_div)),
                    "Mile id": s(cell(row, c_mile)),
                    "Links Triggered": links,
                    "Response": resp,
                    "NPS": nps,
                    "% of Response": pct,
                    "Concern Count": concern,
                    "CC/1000": cc,
                    "OSAT": osat,
                }

                if (not rec["SA Name"] and not rec["Division"]
                    and rec["Links Triggered"] is None
                    and rec["Response"] is None
                    and rec["NPS"] is None
                    and rec["OSAT"] is None):
                    continue

                all_rows.append(rec)

        available_months = [m for m in MONTH_ORDER if m in months_present]
        return all_rows, available_months

    def _build_indexes(self, rows: List[Dict[str, Any]]) -> None:
        self.index.clear()
        self.div_by_month.clear()
        self.sa_by_month_div.clear()

        rows.sort(key=lambda r: ((r.get("Division") or ""), (r.get("SA Name") or ""), (r.get("Month") or "")))

        for r in rows:
            m = key_norm(r.get("Month"))
            d = key_norm(r.get("Division"))
            a = key_norm(r.get("SA Name"))

            m_txt = (r.get("Month") or "").strip()
            d_txt = (r.get("Division") or "").strip()
            a_txt = (r.get("SA Name") or "").strip()

            if m_txt and d_txt:
                self.div_by_month[m].add(d_txt)
            if m_txt and d_txt and a_txt:
                self.sa_by_month_div[(m, d)].add(a_txt)

            self.index[(m, d, a)].append(r)
            self.index[(m, d, "all")].append(r)
            self.index[(m, "all", a)].append(r)
            self.index[(m, "all", "all")].append(r)
            self.index[("all", d, a)].append(r)
            self.index[("all", d, "all")].append(r)
            self.index[("all", "all", a)].append(r)
            self.index[("all", "all", "all")].append(r)

    def apply_filters(self, month: str, division: str, sa_name: str) -> List[Dict[str, Any]]:
        return self.index.get((key_norm(month), key_norm(division), key_norm(sa_name)), [])

    def compute_kpis(self, rows: List[Dict[str, Any]], include_osat: bool = True) -> Dict[str, Any]:
        total_links = sum((r.get("Links Triggered") or 0) for r in rows if r.get("Links Triggered") is not None)
        total_resp = sum((r.get("Response") or 0) for r in rows if r.get("Response") is not None)
        total_concern = sum((r.get("Concern Count") or 0) for r in rows if r.get("Concern Count") is not None)

        avg_pct = r2((total_resp / total_links) * 100.0) if total_links else None
        cc_1000 = r2((total_concern / total_links) * 1000.0) if total_links else None

        out = {
            "total_links_triggered": int(total_links),
            "total_responses": int(total_resp),
            "avg_percent_response": avg_pct,
            "total_concern_count": int(total_concern),
            "avg_cc_per_1000": cc_1000,
            "record_count": len(rows),
        }
        if include_osat:
            out["avg_osat"] = r2(avg([r.get("OSAT") for r in rows]))
        return out

    def get_filters(self, sel_month: str, sel_div: str) -> Dict[str, List[str]]:
        if self.load_error:
            raise RuntimeError(self.load_error)

        m = key_norm(sel_month or "All")
        d = key_norm(sel_div or "All")

        if m != "all":
            divisions = sorted(self.div_by_month.get(m, set()))
        else:
            all_divs = set()
            for ds in self.div_by_month.values():
                all_divs |= set(ds)
            divisions = sorted(all_divs)

        if m == "all" and d == "all":
            sa_names = sorted({(r.get("SA Name") or "").strip() for r in self.data_rows if (r.get("SA Name") or "").strip()})
        elif m == "all" and d != "all":
            sa_names = sorted({
                (r.get("SA Name") or "").strip()
                for r in self.data_rows
                if key_norm(r.get("Division")) == d and (r.get("SA Name") or "").strip()
            })
        elif m != "all" and d == "all":
            sa_set = set()
            for (mm, dd), names in self.sa_by_month_div.items():
                if mm == m:
                    sa_set |= set(names)
            sa_names = sorted(sa_set)
        else:
            sa_names = sorted(self.sa_by_month_div.get((m, d), set()))

        return {"months": self.available_months or MONTH_ORDER, "divisions": divisions, "sa_names": sa_names}

# =============================================================================
# DATASETS INIT
# =============================================================================
PERSONAL = Dataset("Personal", PERSONAL_XLSX)
MEAL = Dataset("MEAL", MEAL_XLSX)
BODYSHOP = Dataset("Body Shop", BODYSHOP_XLSX)
COMMERCIAL = Dataset("Commercial", COMMERCIAL_XLSX)

DATASETS: Dict[str, Dataset] = {"personal": PERSONAL, "meal": MEAL, "bodyshop": BODYSHOP, "commercial": COMMERCIAL}

def get_dataset(key: str) -> Dataset:
    return DATASETS.get((key or "").strip().lower(), PERSONAL)

# Your DATASET_META + INDEX_HTML + routes/APIs remain same as your file.
# Keep them unchanged.

# IMPORTANT:
# Do NOT use app.run() on Render. Gunicorn will serve `app`.
