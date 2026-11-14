import asyncio
import glob
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import duckdb
import gradio as gr
import httpx
from fastapi import FastAPI, HTTPException, Query, Response
from pydantic import BaseModel

# ── Config ──────────────────────────────────────────────────────────────────
BASE = os.path.dirname(os.path.abspath(__file__))

# ✅ Updated bucket — shadowfolks/Icmr (227 GB, 7 phone + 7 aadhar parts)
HF_INDEX_BASE = os.environ.get(
    "ICMR_HF_INDEX_BASE",
    "https://huggingface.co/buckets/MRSHREY197/4444/resolve",
).rstrip("/")

INDEX_SOURCE       = os.environ.get("ICMR_INDEX_SOURCE", "remote").lower()
PARALLELISM        = int(os.environ.get("ICMR_PARALLEL", "2"))
THREADS_PER_CONN   = int(os.environ.get("ICMR_THREADS_PER_CONN", "2"))
DUPLICATE_CAP      = 2

PHONE_PARTS  = int(os.environ.get("ICMR_PHONE_PARTS",  "7"))   # 0..6
AADHAR_PARTS = int(os.environ.get("ICMR_AADHAR_PARTS", "7"))   # 0..6

SEARCH_FIELDS = [
    "name", "fathersName", "phoneNumber", "aadharNumber", "otherNumber",
    "address", "district", "pincode", "state", "town", "source",
]
NUMBER_FIELDS = ["phoneNumber", "aadharNumber", "otherNumber"]

FAST_FIELDS = {"phoneNumber", "aadharNumber"}

REMOTE_INDEXES = {
    "phone":  [f"{HF_INDEX_BASE}/idx_phone.{i}.parquet"  for i in range(PHONE_PARTS)],
    "aadhar": [f"{HF_INDEX_BASE}/idx_aadhar.{i}.parquet" for i in range(AADHAR_PARTS)],
}

# ── DuckDB Connection Pool ──────────────────────────────────────────────────
_conns: list[duckdb.DuckDBPyConnection] = []
_conns_lock = threading.Lock()
_thread_local = threading.local()
pool = ThreadPoolExecutor(max_workers=PARALLELISM, thread_name_prefix="duck")


def _idx_ready(kind: str) -> bool:
    return kind in REMOTE_INDEXES


def _new_conn() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET home_directory='/tmp'")
    con.execute("SET extension_directory='/tmp/duckdb_extensions'")
    con.execute("INSTALL parquet; LOAD parquet;")
    con.execute("INSTALL httpfs;  LOAD httpfs;")

    for kind, urls in REMOTE_INDEXES.items():
        view = f"people_{kind}"
        lst  = ", ".join(f"'{u}'" for u in urls)
        con.execute(
            f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM read_parquet([{lst}])"
        )
    con.execute(f"SET threads = {THREADS_PER_CONN}")
    return con


def _thread_id() -> int:
    tid = getattr(_thread_local, "id", None)
    if tid is None:
        with _conns_lock:
            tid = len(_conns)
            _thread_local.id = tid
    return tid


def _get_conn() -> duckdb.DuckDBPyConnection:
    ident = _thread_id()
    with _conns_lock:
        while len(_conns) <= ident:
            _conns.append(_new_conn())
    return _conns[ident]


# ── Dedup & Connected Records ───────────────────────────────────────────────
def _person_key(row: dict) -> tuple:
    ph = (row.get("phoneNumber") or "").strip()
    ad = (row.get("aadharNumber") or "").strip()
    if ph or ad:
        return (ph, ad)
    return (row.get("name") or "").strip(), (row.get("fathersName") or "").strip()


def _connected_numbers(row: dict) -> list[dict]:
    connected, seen = [], set()
    for field in NUMBER_FIELDS:
        raw = row.get(field)
        if raw is None:
            continue
        value = str(raw).strip()
        if not value or value in seen:
            continue
        seen.add(value)
        connected.append({"field": field, "value": value})
    return connected


def _cap_duplicates(rows: list[dict]) -> list[dict]:
    seen: dict[tuple, int] = {}
    out = []
    for r in rows:
        k = _person_key(r)
        n = seen.get(k, 0)
        if n < DUPLICATE_CAP:
            seen[k] = n + 1
            record = dict(r)
            record["connected_numbers"] = _connected_numbers(record)
            out.append(record)
    return out


# ── Search Logic ────────────────────────────────────────────────────────────
def _pick_view(field: str, mode: str) -> str:
    if field == "phoneNumber" and mode == "exact":
        return "people_phone"
    if field == "aadharNumber" and mode == "exact":
        return "people_aadhar"
    return "people_phone"


def _run_field_search(field: str, value: str, mode: str, limit: int) -> dict:
    if field not in SEARCH_FIELDS:
        raise ValueError(f"Unknown field: {field}")
    if mode not in ("exact", "contains", "prefix"):
        raise ValueError(f"Unknown mode: {mode}")

    v = value.replace("'", "''")
    limit_sql = limit * DUPLICATE_CAP + 20
    view = _pick_view(field, mode)

    if mode == "exact":
        where = f"{field} = '{v}'"
    elif mode == "contains":
        v2 = v.replace("%", r"\%").replace("_", r"\_")
        where = f"{field} ILIKE '%{v2}%' ESCAPE '\\'"
    else:
        v2 = v.replace("%", r"\%").replace("_", r"\_")
        where = f"{field} ILIKE '{v2}%' ESCAPE '\\'"

    sql = f"SELECT * FROM {view} WHERE {where} LIMIT {limit_sql}"

    con = _get_conn()
    rows = con.execute(sql).fetchall()
    cols = [d[0] for d in con.description]
    results = _cap_duplicates([dict(zip(cols, r)) for r in rows])[:limit]
    return {
        "field": field, "value": value, "mode": mode,
        "count": len(results), "results": results,
        "fast": field in FAST_FIELDS and mode == "exact",
    }


def _unified_search(q: str, limit: int = 10) -> dict:
    q = q.strip()
    if not q:
        return {"query": q, "searched_fields": [], "count": 0, "results": []}

    searched = []
    candidates: list[tuple[str, str]] = []

    if q.isdigit():
        if len(q) == 10:
            candidates.append(("phoneNumber", "exact"))
        if len(q) == 12:
            candidates.append(("aadharNumber", "exact"))
        if len(q) == 6:
            candidates.append(("pincode", "exact"))
        candidates.append(("otherNumber", "exact"))
    else:
        candidates.append(("name", "contains"))

    for field, mode in candidates:
        searched.append(field)
        try:
            r = _run_field_search(field, q, mode, limit)
            if r["count"] > 0:
                return {
                    "query": q, "searched_fields": searched,
                    "count": r["count"], "results": r["results"],
                }
        except Exception as e:
            print(f"[search] {field}/{mode} failed: {e}")
            continue

    return {"query": q, "searched_fields": searched,
            "count": 0, "results": []}


# ── FastAPI ────────────────────────────────────────────────────────────────
fastapi_app = FastAPI(title="ICMR + HITEK Search API")


class BatchRequest(BaseModel):
    queries: list[dict[str, Any]]
    limit: int = 10


@fastapi_app.get("/")
def root():
    return {
        "app": "ICMR + HITEK Search API",
        "records": 2_504_793_870,
        "indexes": {"phone": _idx_ready("phone"), "aadhar": _idx_ready("aadhar")},
        "index_source": INDEX_SOURCE,
        "index_base": HF_INDEX_BASE,
        "phone_parts": PHONE_PARTS,
        "aadhar_parts": AADHAR_PARTS,
        "columns": SEARCH_FIELDS,
        "fast_fields": list(FAST_FIELDS),
        "modes": ["exact", "contains", "prefix"],
        "docs": "/docs",
        "developer": "@SRACyberTech | channel https://t.me/SRACyberTechPvtLtd",
    }


@fastapi_app.get("/health")
def health():
    return {
        "status": "ok",
        "indexes": {"phone": _idx_ready("phone"), "aadhar": _idx_ready("aadhar")},
        "index_source": INDEX_SOURCE,
        "index_base": HF_INDEX_BASE,
    }


@fastapi_app.get("/test-bucket")
def test_bucket():
    out = {}
    for kind, urls in REMOTE_INDEXES.items():
        item = {"sample_url": urls[0], "total_parts": len(urls)}
        try:
            con = duckdb.connect()
            con.execute("SET home_directory='/tmp'")
            con.execute("SET extension_directory='/tmp/duckdb_extensions'")
            con.execute("INSTALL parquet; LOAD parquet;")
            con.execute("INSTALL httpfs;  LOAD httpfs;")
            row = con.execute(
                f"SELECT * FROM read_parquet('{urls[0]}') LIMIT 1"
            ).fetchone()
            cols = [d[0] for d in con.description]
            con.close()
            item.update({"ok": True, "row_found": row is not None, "columns": cols})
        except Exception as e:
            item.update({"ok": False, "error": str(e)[:400]})
        out[kind] = item
    return {"base_url": HF_INDEX_BASE, "indexes": out}


@fastapi_app.get("/search")
async def search(
    q: str | None = Query(None),
    mobile: str | None = Query(None),
    field: str | None = Query(None),
    mode: str = Query("exact"),
    limit: int = Query(10, ge=1, le=1000),
    pretty: bool = Query(True),
):
    q_val = (q or mobile or "").strip()
    if not q_val:
        raise HTTPException(422, "Provide q or mobile")
    loop = asyncio.get_running_loop()
    if field and field != "auto":
        data = await loop.run_in_executor(
            pool, _run_field_search, field, q_val, mode, limit)
        data_out = {"query": q_val, "searched_fields": [field],
                    "count": data["count"], "results": data["results"]}
    else:
        data_out = await loop.run_in_executor(pool, _unified_search, q_val, limit)
    result = {"success": bool(data_out["count"]), **data_out,
              "number": q_val, "total": data_out["count"]}
    content = json.dumps(result, indent=2 if pretty else None, ensure_ascii=False)
    return Response(content=content, media_type="application/json")


@fastapi_app.post("/search/parallel")
async def search_parallel(req: BatchRequest):
    if not req.queries:
        raise HTTPException(400, "queries must not be empty")
    if len(req.queries) > 50:
        raise HTTPException(400, "max 50 queries per batch")
    loop = asyncio.get_running_loop()
    tasks = [
        loop.run_in_executor(
            pool, _run_field_search,
            item.get("field", "phoneNumber"),
            item.get("value", ""),
            item.get("mode", "exact"),
            int(item.get("limit", req.limit)),
        )
        for item in req.queries
    ]
    results = await asyncio.gather(*tasks)
    return Response(
        content=json.dumps({"searches": len(req.queries), "results": list(results)},
                           indent=2, ensure_ascii=False),
        media_type="application/json",
    )


# ── Pinger ────────────────────────────────────────────────────────────────
async def pinger():
    port = os.getenv("PORT", "7860")
    url = f"http://localhost:{port}/health"
    async with httpx.AsyncClient(timeout=10) as client:
        while True:
            await asyncio.sleep(120)
            try:
                resp = await client.get(url)
                print(f"[Pinger] {'OK' if resp.status_code == 200 else resp.status_code}")
            except Exception as e:
                print(f"[Pinger] Error: {e}")


@fastapi_app.on_event("startup")
async def startup_event():
    asyncio.create_task(pinger())


# ── Gradio UI ──────────────────────────────────────────────────────────────
def format_result(row: dict) -> str:
    lines = []
    for field in SEARCH_FIELDS:
        val = row.get(field, "")
        if val:
            lines.append(f"**{field}:** {val}")
    cn = row.get("connected_numbers", [])
    if cn:
        nums = ", ".join(f"{c['field']}={c['value']}" for c in cn)
        lines.append(f"**connected:** {nums}")
    return "\n\n".join(lines)


def search_ui(query: str, limit: int, field: str, mode: str) -> str:
    if not query or not query.strip():
        return "⚠️ Kuch toh search karo — phone, aadhar, name, address daalo."

    q = query.strip()

    warning = ""
    if field in SEARCH_FIELDS and field not in FAST_FIELDS:
        warning = ("\n\n> ⏳ **Slow search** — full scan of remote parquet. "
                   "Takes 30s–5min, may timeout.\n\n---\n\n")
    elif field == "Auto" and not q.isdigit():
        warning = ("\n\n> ⏳ **Text search** — full scan. May take long.\n\n---\n\n")

    try:
        if field == "Auto":
            data = _unified_search(q, int(limit))
        else:
            r = _run_field_search(field, q, mode, int(limit))
            data = {"query": q, "searched_fields": [f"{field} ({mode})"],
                    "count": r["count"], "results": r["results"]}
    except Exception as e:
        return f"❌ Error: {str(e)}"

    count = data["count"]
    results = data["results"]
    searched = ", ".join(data.get("searched_fields", []))

    if not results:
        return (f"🔍 **Query:** `{q}`\n**Searched:** {searched}\n\n"
                f"❌ **No data found**.")

    header = (f"🔍 **Query:** `{q}`  |  **Found:** {count} results  |  "
              f"**Searched:** {searched}\n\n---\n\n")
    parts = []
    for i, row in enumerate(results, 1):
        parts.append(f"### Result {i}\n{format_result(row)}")
    return header + warning + "\n\n---\n\n".join(parts)


def build_ui():
    with gr.Blocks(
        title="ICMR Search API",
        theme=gr.themes.Soft(),
        css="""
        .main-title { text-align: center; margin-bottom: 0; }
        .subtitle { text-align: center; color: #666; margin-top: 0; }
        .footer { text-align: center; color: #888; margin-top: 20px; }
        """
    ) as demo:
        gr.Markdown("# 🔍 ICMR + HITEK Search API", elem_classes="main-title")
        gr.Markdown(
            "Search **2.5 billion records** — all columns supported",
            elem_classes="subtitle")

        with gr.Row():
            with gr.Column(scale=3):
                query_input = gr.Textbox(
                    label="Search Query",
                    placeholder="Phone, Aadhaar, name, address, pincode...",
                    lines=1,
                )
            with gr.Column(scale=1):
                limit_slider = gr.Slider(1, 50, value=10, step=1, label="Max Results")

        with gr.Row():
            field_dropdown = gr.Dropdown(
                choices=["Auto"] + SEARCH_FIELDS,
                value="Auto",
                label="Field (Auto = smart detect)",
            )
            mode_dropdown = gr.Dropdown(
                choices=["exact", "contains", "prefix"],
                value="exact",
                label="Match mode",
            )

        search_btn = gr.Button("🔍 Search", variant="primary", size="lg")
        output = gr.Markdown(label="Results")

        search_btn.click(
            fn=search_ui,
            inputs=[query_input, limit_slider, field_dropdown, mode_dropdown],
            outputs=output,
        )
        query_input.submit(
            fn=search_ui,
            inputs=[query_input, limit_slider, field_dropdown, mode_dropdown],
            outputs=output,
        )

        gr.Markdown("---")
        with gr.Accordion("📡 API Info", open=False):
            gr.Markdown("""
**Endpoints:**
- `GET /search?q=<value>` — Auto search (phone → aadhar → pincode → other → name)
- `GET /search?q=<value>&field=name&mode=contains` — Search specific column
- `GET /search?q=<value>&field=phoneNumber&mode=exact` — Fast exact phone
- `GET /health`, `GET /test-bucket`, `GET /docs`

**Modes:** `exact`, `contains`, `prefix`

**⚡ Fast:** `phoneNumber` (10-digit), `aadharNumber` (12-digit) — sorted index
**🐢 Slow:** all other columns — full scan of remote parquet
            """)

        gr.Markdown(
            "<div class='footer'>"
            "👨‍💻 **Developer:** @Lejent36  |  "
            "📢 **Channel:** https://t.me/sufiyanr31"
            "</div>",
            elem_classes="footer"
        )

    return demo


demo = build_ui()
app = gr.mount_gradio_app(fastapi_app, demo, path="/")
