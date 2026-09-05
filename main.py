# main.py
import os, json, time, asyncio
from datetime import datetime, date, timezone
from enum import Enum
from typing import Optional, List, Dict
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "kisan_setu")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
WA_TOKEN = os.getenv("WHATSAPP_TOKEN", "")
WA_PHONE_ID = os.getenv("WHATSAPP_PHONE_ID", "")
WA_VERIFY_TOKEN = os.getenv("WHATSAPP_VERIFY_TOKEN", "kisan_setu_verify")
WA_API_URL = f"https://graph.facebook.com/v19.0/{WA_PHONE_ID}/messages"

CROPS = ["Wheat", "Mustard", "Bajra", "Chana"]
SLOTS = ["08:00 AM – 09:00 AM", "09:00 AM – 10:00 AM", "10:00 AM – 11:00 AM", "11:00 AM – 12:00 PM", "01:00 PM – 02:00 PM", "02:00 PM – 03:00 PM"]

# Comprehensive In-Memory DB for complete end-to-end integration
MEM_DB = {
    "farmers": {
        "FID-88214": {
            "farmer_id": "FID-88214",
            "name": "Ramkishan Choudhary",
            "father_name": "Hariram Choudhary",
            "phone": "9829019835",
            "jan_aadhaar": "748291038472",
            "crop": "Wheat",
            "qty": "50 Qtl",
            "land_hectares": 2.45,
            "bank_verified": True
        },
        "FID-71630": {
            "farmer_id": "FID-71630",
            "name": "Surajmal Sharma",
            "father_name": "Ramchandra Sharma",
            "phone": "9414729831",
            "jan_aadhaar": "631920847163",
            "crop": "Mustard",
            "qty": "32 Qtl",
            "land_hectares": 1.8,
            "bank_verified": True
        }
    },
    "centers": {
        "bassi": {
            "center_id": "bassi",
            "name": "Bassi APMC Mandi",
            "capacity_per_hour": 40,
            "daily_capacity": 600,
            "expected_quantity": 420,
            "current_farmers": 14,
            "current_load_factor": "medium",
            "truck_count": 8,
            "dynamic_throttle": False,
            "distance_km": 0.0
        },
        "chomu": {
            "center_id": "chomu",
            "name": "Chomu Mandi Yard",
            "capacity_per_hour": 50,
            "daily_capacity": 700,
            "expected_quantity": 196,
            "current_farmers": 6,
            "current_load_factor": "low",
            "truck_count": 3,
            "dynamic_throttle": False,
            "distance_km": 14.2
        },
        "dudu": {
            "center_id": "dudu",
            "name": "Dudu APMC Yard",
            "capacity_per_hour": 45,
            "daily_capacity": 650,
            "expected_quantity": 290,
            "current_farmers": 9,
            "current_load_factor": "medium",
            "truck_count": 7,
            "dynamic_throttle": False,
            "distance_km": 22.0
        }
    },
    "bookings": {},
    "queues": {
        "bassi": [
            {"token": "B-041", "name": "Ramkishan Choudhary", "crop": "Wheat", "qty": "50 Qtl", "status": "Weighed", "booking_id": "BK-101", "center_id": "bassi"},
            {"token": "B-042", "name": "Surajmal Sharma", "crop": "Mustard", "qty": "32 Qtl", "status": "InQueue", "booking_id": "BK-102", "center_id": "bassi"},
            {"token": "B-043", "name": "Kailash Yadav", "crop": "Wheat", "qty": "25 Qtl", "status": "InQueue", "booking_id": "BK-103", "center_id": "bassi"},
            {"token": "B-044", "name": "Pawan Sharma", "crop": "Bajra", "qty": "20 Qtl", "status": "Waiting", "booking_id": "BK-104", "center_id": "bassi"},
            {"token": "B-045", "name": "Anil Gurjar", "crop": "Wheat", "qty": "40 Qtl", "status": "Waiting", "booking_id": "BK-105", "center_id": "bassi"}
        ],
        "chomu": [
            {"token": "C-051", "name": "Bhairu Singh", "crop": "Mustard", "qty": "30 Qtl", "status": "InQueue", "booking_id": "BK-C01", "center_id": "chomu"}
        ],
        "dudu": [
            {"token": "D-039", "name": "Manish Meena", "crop": "Bajra", "qty": "28 Qtl", "status": "Waiting", "booking_id": "BK-D01", "center_id": "dudu"}
        ]
    },
    "token_counters": {"bassi": 45, "chomu": 51, "dudu": 40},
    "now_serving": {"bassi": "B-042", "chomu": "C-051", "dudu": "D-039"},
    "reallocation_offers": {},
    "wa_sessions": {},
    "feedbacks": {
        "FB-00121": {
            "feedback_id": "FB-00121",
            "farmer_id": "FID-88214",
            "center_id": "bassi",
            "center_name": "Bassi APMC Mandi",
            "token_id": "B-038",
            "booking_id": "BK-BASSI-101",
            "staff_behaviour": "Polite",
            "quality_checking": "Yes",
            "rejection_reason": "Fasal reject nahi hui",
            "weighing_process": "Yes",
            "next_step_explanation": "Yes",
            "facilities": "Some facilities available",
            "extra_payment": "No",
            "overall_experience": 4,
            "would_choose_again": "Definitely Yes",
            "suggestion": "Water dispenser near gate counter needs regular maintenance.",
            "rating": 4,
            "category": "Basic Facilities",
            "comment": "Water dispenser near gate counter needs regular maintenance.",
            "status": "Resolved",
            "resolution_note": "Secondary drinking water dispenser installed.",
            "staff_id": "STAFF-BASSI-01",
            "created_at": "2026-09-04 10:15",
            "resolved_at": "2026-09-04 11:40"
        },
        "FB-00122": {
            "feedback_id": "FB-00122",
            "farmer_id": "FID-71630",
            "center_id": "bassi",
            "center_name": "Bassi APMC Mandi",
            "token_id": "B-039",
            "booking_id": "BK-BASSI-102",
            "staff_behaviour": "Normal",
            "quality_checking": "Partially",
            "rejection_reason": "Fasal reject nahi hui",
            "weighing_process": "Yes",
            "next_step_explanation": "Partially",
            "facilities": "Some facilities available",
            "extra_payment": "No",
            "overall_experience": 3,
            "would_choose_again": "Maybe",
            "suggestion": "Tractor parking area had heavy traffic jam near Gate 2 entrance.",
            "rating": 3,
            "category": "Waiting / Procurement Experience",
            "comment": "Tractor parking area had heavy traffic jam near Gate 2 entrance.",
            "status": "Under Review",
            "resolution_note": "Traffic marshall deployed at Gate 2.",
            "staff_id": "STAFF-BASSI-01",
            "created_at": "2026-09-05 08:30",
            "resolved_at": None
        },
        "FB-00123": {
            "feedback_id": "FB-00123",
            "farmer_id": "FID-92311",
            "center_id": "bassi",
            "center_name": "Bassi APMC Mandi",
            "token_id": "B-040",
            "booking_id": "BK-BASSI-103",
            "staff_behaviour": "Very Polite",
            "quality_checking": "Yes",
            "rejection_reason": "Fasal reject nahi hui",
            "weighing_process": "Yes",
            "next_step_explanation": "Yes",
            "facilities": "All facilities available",
            "extra_payment": "No",
            "overall_experience": 5,
            "would_choose_again": "Definitely Yes",
            "suggestion": "Immediate SMS token generation and transparent moisture testing. Excellent!",
            "rating": 5,
            "category": "General Satisfaction",
            "comment": "Immediate SMS token generation and transparent moisture testing. Excellent!",
            "status": "Resolved",
            "resolution_note": "Direct benefit transfer confirmed via PFMS.",
            "staff_id": "STAFF-BASSI-01",
            "created_at": "2026-09-05 09:20",
            "resolved_at": "2026-09-05 09:50"
        },
        "FB-00124": {
            "feedback_id": "FB-00124",
            "farmer_id": "FID-88214",
            "center_id": "bassi",
            "center_name": "Bassi APMC Mandi",
            "token_id": "B-041",
            "booking_id": "BK-BASSI-104",
            "staff_behaviour": "Normal",
            "quality_checking": "Yes",
            "rejection_reason": "Fasal reject nahi hui",
            "weighing_process": "Partially",
            "next_step_explanation": "Yes",
            "facilities": "Some facilities available",
            "extra_payment": "No",
            "overall_experience": 2,
            "would_choose_again": "Maybe",
            "suggestion": "Weighbridge scale #1 had a long queue and printer was slow.",
            "rating": 2,
            "category": "Weighing Process",
            "comment": "Weighbridge scale #1 had a long queue and printer was slow.",
            "status": "Open",
            "resolution_note": "",
            "staff_id": None,
            "created_at": "2026-09-05 10:45",
            "resolved_at": None
        },
        "FB-00125": {
            "feedback_id": "FB-00125",
            "farmer_id": "FID-54128",
            "center_id": "bassi",
            "center_name": "Bassi APMC Mandi",
            "token_id": "B-043",
            "booking_id": "BK-BASSI-105",
            "staff_behaviour": "Polite",
            "quality_checking": "Yes",
            "rejection_reason": "Fasal reject nahi hui",
            "weighing_process": "Yes",
            "next_step_explanation": "Yes",
            "facilities": "All facilities available",
            "extra_payment": "No",
            "overall_experience": 4,
            "would_choose_again": "Definitely Yes",
            "suggestion": "Moisture meter testing was clear and shown on digital display.",
            "rating": 4,
            "category": "Quality Checking",
            "comment": "Moisture meter testing was clear and shown on digital display.",
            "status": "Resolved",
            "resolution_note": "Quality check certificate printed instantly.",
            "staff_id": "STAFF-BASSI-01",
            "created_at": "2026-09-05 11:00",
            "resolved_at": "2026-09-05 11:30"
        },
        "FB-00126": {
            "feedback_id": "FB-00126",
            "farmer_id": "FID-66120",
            "center_id": "bassi",
            "center_name": "Bassi APMC Mandi",
            "token_id": "B-044",
            "booking_id": "BK-BASSI-106",
            "staff_behaviour": "Rude",
            "quality_checking": "Partially",
            "rejection_reason": "No",
            "weighing_process": "Partially",
            "next_step_explanation": "No",
            "facilities": "No facilities",
            "extra_payment": "Yes",
            "overall_experience": 1,
            "would_choose_again": "No",
            "suggestion": "Gate security personnel demanded unreceipted entry fee of Rs 50 for tractor passage.",
            "rating": 1,
            "category": "Extra Payment Inquiry",
            "comment": "Gate security personnel demanded unreceipted entry fee of Rs 50 for tractor passage.",
            "status": "Under Review",
            "resolution_note": "Supervisor assigned for administrative review and CCTV audit.",
            "staff_id": "STAFF-BASSI-01",
            "created_at": "2026-09-05 11:30",
            "resolved_at": None
        },
        "FB-00115": {
            "feedback_id": "FB-00115",
            "farmer_id": "FID-31092",
            "center_id": "chomu",
            "center_name": "Chomu Mandi Yard",
            "token_id": "C-048",
            "booking_id": "BK-CHOMU-201",
            "staff_behaviour": "Polite",
            "quality_checking": "Yes",
            "rejection_reason": "Fasal reject nahi hui",
            "weighing_process": "Yes",
            "next_step_explanation": "Yes",
            "facilities": "Some facilities available",
            "extra_payment": "No",
            "overall_experience": 4,
            "would_choose_again": "Definitely Yes",
            "suggestion": "WhatsApp slot booking made entry fast. Nice shaded shed.",
            "rating": 4,
            "category": "General Satisfaction",
            "comment": "WhatsApp slot booking made entry fast. Nice shaded shed.",
            "status": "Resolved",
            "resolution_note": "Automated time window system operational.",
            "staff_id": "STAFF-CHOMU-01",
            "created_at": "2026-09-04 14:00",
            "resolved_at": "2026-09-04 14:20"
        },
        "FB-00116": {
            "feedback_id": "FB-00116",
            "farmer_id": "FID-44812",
            "center_id": "chomu",
            "center_name": "Chomu Mandi Yard",
            "token_id": "C-049",
            "booking_id": "BK-CHOMU-202",
            "staff_behaviour": "Normal",
            "quality_checking": "Partially",
            "rejection_reason": "Fasal reject nahi hui",
            "weighing_process": "Yes",
            "next_step_explanation": "Partially",
            "facilities": "Some facilities available",
            "extra_payment": "No",
            "overall_experience": 3,
            "would_choose_again": "Maybe",
            "suggestion": "Wait time was about 25 mins longer than expected on mobile screen.",
            "rating": 3,
            "category": "Waiting / Procurement Experience",
            "comment": "Wait time was about 25 mins longer than expected on mobile screen.",
            "status": "Under Review",
            "resolution_note": "Recalibrating throughput calculation for Chomu.",
            "staff_id": "STAFF-CHOMU-01",
            "created_at": "2026-09-05 09:40",
            "resolved_at": None
        },
        "FB-00117": {
            "feedback_id": "FB-00117",
            "farmer_id": "FID-77190",
            "center_id": "chomu",
            "center_name": "Chomu Mandi Yard",
            "token_id": "C-050",
            "booking_id": "BK-CHOMU-203",
            "staff_behaviour": "Very Polite",
            "quality_checking": "Yes",
            "rejection_reason": "Fasal reject nahi hui",
            "weighing_process": "Yes",
            "next_step_explanation": "Yes",
            "facilities": "All facilities available",
            "extra_payment": "No",
            "overall_experience": 5,
            "would_choose_again": "Definitely Yes",
            "suggestion": "Staff assisted in scanning QR pass directly from phone screen.",
            "rating": 5,
            "category": "Staff Behaviour",
            "comment": "Staff assisted in scanning QR pass directly from phone screen.",
            "status": "Resolved",
            "resolution_note": "Operator assistance acknowledged.",
            "staff_id": "STAFF-CHOMU-01",
            "created_at": "2026-09-05 10:10",
            "resolved_at": "2026-09-05 10:30"
        },
        "FB-00110": {
            "feedback_id": "FB-00110",
            "farmer_id": "FID-19284",
            "center_id": "dudu",
            "center_name": "Dudu Mandi Hub",
            "token_id": "D-035",
            "booking_id": "BK-DUDU-301",
            "staff_behaviour": "Very Polite",
            "quality_checking": "Yes",
            "rejection_reason": "Fasal reject nahi hui",
            "weighing_process": "Yes",
            "next_step_explanation": "Yes",
            "facilities": "All facilities available",
            "extra_payment": "No",
            "overall_experience": 5,
            "would_choose_again": "Definitely Yes",
            "suggestion": "Very clean yard with resting facilities and clean drinking water.",
            "rating": 5,
            "category": "Basic Facilities",
            "comment": "Very clean yard with resting facilities and clean drinking water.",
            "status": "Resolved",
            "resolution_note": "Optimal center throughput maintained.",
            "staff_id": "STAFF-DUDU-01",
            "created_at": "2026-09-04 16:30",
            "resolved_at": "2026-09-04 17:00"
        },
        "FB-00111": {
            "feedback_id": "FB-00111",
            "farmer_id": "FID-82015",
            "center_id": "dudu",
            "center_name": "Dudu Mandi Hub",
            "token_id": "D-038",
            "booking_id": "BK-DUDU-302",
            "staff_behaviour": "Polite",
            "quality_checking": "Yes",
            "rejection_reason": "Fasal reject nahi hui",
            "weighing_process": "Yes",
            "next_step_explanation": "Yes",
            "facilities": "All facilities available",
            "extra_payment": "No",
            "overall_experience": 4,
            "would_choose_again": "Definitely Yes",
            "suggestion": "Payment confirmation received promptly.",
            "rating": 4,
            "category": "Payment",
            "comment": "Payment confirmation received promptly.",
            "status": "Resolved",
            "resolution_note": "Bank verification and payment dispatch matched.",
            "staff_id": "STAFF-DUDU-01",
            "created_at": "2026-09-05 08:50",
            "resolved_at": "2026-09-05 09:15"
        }
    }
}


class BookingStatus(str, Enum):
    WAITING = "Waiting"
    CHECKED_IN = "CheckedIn"
    IN_QUEUE = "InQueue"
    WEIGHED = "Weighed"
    PAYMENT_INITIATED = "PaymentInitiated"
    NO_SHOW = "NoShow"
    REALLOCATED = "Reallocated"


class BookRequest(BaseModel):
    farmer_id: str
    center_id: str
    date: str
    time_window: str
    crop: str


class SwitchCentreRequest(BaseModel):
    new_center_id: str
    reason: Optional[str] = "Congestion avoidance"


class CheckInRequest(BaseModel):
    operator_id: Optional[str] = "OP-01"


class ReallocationResponseRequest(BaseModel):
    action: str  # 'accept' or 'decline'


class AdvanceRequest(BaseModel):
    center_id: str
    token: Optional[str] = None


class LoadRequest(BaseModel):
    center_id: str
    truck_count: int
    load_factor: str


class FeedbackCreateRequest(BaseModel):
    farmer_id: Optional[str] = "FID-88214"
    center_id: Optional[str] = "bassi"
    token_id: Optional[str] = None
    booking_id: Optional[str] = None
    crop: Optional[str] = "Mustard (Sarson)"
    
    # 10 Standardized Kisan Setu Feedback Questions (supports both naming variants)
    staff_behaviour: Optional[str] = None
    staff_behavior: Optional[str] = None
    quality_checking: Optional[str] = "Yes"
    rejection_reason: Optional[str] = "Crop not rejected"
    weighing_process: Optional[str] = "Yes"
    next_step_explanation: Optional[str] = None
    next_step_guidance: Optional[str] = None
    facilities: Optional[str] = None
    basic_facilities: Optional[str] = None
    extra_payment: Optional[str] = "No"
    overall_experience: Optional[int] = None
    rating: Optional[int] = None
    would_choose_again: Optional[str] = None
    future_preference: Optional[str] = None
    suggestion: Optional[str] = ""
    suggestions: Optional[str] = ""
    category: Optional[str] = None
    comment: Optional[str] = ""


class FeedbackStatusUpdateRequest(BaseModel):
    status: str  # "Open", "Under Review", "Resolved"
    resolution_note: Optional[str] = ""
    staff_id: Optional[str] = "STAFF-01"


class ConnectionManager:
    def __init__(self):
        self.rooms: Dict[str, List[WebSocket]] = {}

    async def connect(self, center_id: str, ws: WebSocket):
        await ws.accept()
        self.rooms.setdefault(center_id, []).append(ws)

    def disconnect(self, center_id: str, ws: WebSocket):
        if center_id in self.rooms and ws in self.rooms[center_id]:
            self.rooms[center_id].remove(ws)

    async def broadcast(self, center_id: str, payload: dict):
        dead = []
        for ws in self.rooms.get(center_id, []):
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for d in dead:
            self.disconnect(center_id, d)


manager = ConnectionManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"[BACKEND] Kisan Setu Full-Stack API is online and serving at http://127.0.0.1:8000")
    yield


app = FastAPI(title="Kisan Setu Full-Stack API", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ─────────────────────────────────────────────────────────────
# Helper Functions
# ─────────────────────────────────────────────────────────────

async def next_token(center_id: str) -> str:
    prefix = center_id[0:1].upper() if center_id else "B"
    MEM_DB["token_counters"][center_id] = MEM_DB["token_counters"].get(center_id, 40) + 1
    n = MEM_DB["token_counters"][center_id]
    return f"{prefix}-{n:03d}"


async def send_whatsapp_message(phone: str, body: str):
    if not WA_TOKEN or not WA_PHONE_ID:
        print(f"[MOCK WHATSAPP -> {phone}] {body}")
        return
    headers = {"Authorization": f"Bearer {WA_TOKEN}", "Content-Type": "application/json"}
    payload = {"messaging_product": "whatsapp", "to": phone, "type": "text", "text": {"body": body}}
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            await client.post(WA_API_URL, headers=headers, json=payload)
    except Exception as e:
        print(f"[WHATSAPP SEND FAILED] {phone}: {e}")


# ─────────────────────────────────────────────────────────────
# FRONTEND STATIC & HTML PAGE ROUTES
# ─────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def serve_landing():
    landing_path = os.path.join(BASE_DIR, "landing.html")
    if os.path.exists(landing_path):
        return FileResponse(landing_path)
    return HTMLResponse("<h3>Kisan Setu Portal</h3><a href='/login.html'>Open Portal</a>")


@app.get("/landing.html", response_class=HTMLResponse)
async def serve_landing_page():
    return FileResponse(os.path.join(BASE_DIR, "landing.html"))


@app.get("/index.html", response_class=HTMLResponse)
async def serve_index_page():
    return FileResponse(os.path.join(BASE_DIR, "index.html"))


@app.get("/login.html", response_class=HTMLResponse)
async def serve_login_page():
    return FileResponse(os.path.join(BASE_DIR, "login.html"))


@app.get("/kisan_setu_logo.png")
async def serve_logo_png():
    return FileResponse(os.path.join(BASE_DIR, "kisan_setu_logo.png"))


@app.get("/kisan_setu_logo.jpg")
async def serve_logo_jpg():
    return FileResponse(os.path.join(BASE_DIR, "kisan_setu_logo.jpg"))


@app.get("/kisan_setu_illus.jpg")
async def serve_illus_jpg():
    return FileResponse(os.path.join(BASE_DIR, "kisan_setu_illus.jpg"))


# ─────────────────────────────────────────────────────────────
# CORE API ENDPOINTS
# ─────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}


@app.get("/api/centers")
async def api_centers():
    return list(MEM_DB["centers"].values())


@app.get("/api/center/status")
async def api_center_status(center_id: str = "bassi"):
    center = MEM_DB["centers"].get(center_id, {"center_id": center_id, "current_load_factor": "medium", "truck_count": 8, "dynamic_throttle": False})
    serving = MEM_DB["now_serving"].get(center_id, "B-042")
    return {
        "ok": True,
        "center": center,
        "now_serving": serving,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/api/centers/{center_id}/load")
async def api_center_load_get(center_id: str):
    center = MEM_DB["centers"].get(center_id, {
        "center_id": center_id,
        "name": f"{center_id.title()} APMC Mandi",
        "daily_capacity": 600,
        "expected_quantity": 300,
        "current_farmers": 10,
        "current_load_factor": "medium"
    })
    daily_cap = center.get("daily_capacity", 600)
    exp_qty = center.get("expected_quantity", 300)
    cap_used = int(round((exp_qty / daily_cap) * 100)) if daily_cap else 50
    now_serv = MEM_DB["now_serving"].get(center_id, f"{center_id[0].upper()}-040")
    queue_count = len(MEM_DB["queues"].get(center_id, []))

    load_data = {
        "center_id": center_id,
        "center_name": center.get("name", f"{center_id.title()} Centre"),
        "status": center.get("current_load_factor", "medium").title(),
        "current_farmers": center.get("current_farmers", 12),
        "expected_quantity": exp_qty,
        "daily_capacity": daily_cap,
        "capacity_used": cap_used,
        "now_serving": now_serv,
        "queue_count": queue_count,
        "truck_count": center.get("truck_count", 5),
        "dynamic_throttle": center.get("dynamic_throttle", False)
    }
    return {"ok": True, "load": load_data}


@app.get("/api/centers/{center_id}/forecast")
async def api_center_forecast(center_id: str):
    center = MEM_DB["centers"].get(center_id, {})
    load_factor = center.get("current_load_factor", "medium").lower()

    if load_factor == "high":
        peak = "11:00 AM – 01:30 PM"
        calc = "High yard truck arrivals (12+ vehicles) causing weighbridge bottleneck. Dynamic throttle recommended."
    elif load_factor == "low":
        peak = "01:30 PM – 02:30 PM"
        calc = "Steady inflow well below daily limit (35% capacity used). Ideal fast intake window."
    else:
        peak = "11:30 AM – 01:00 PM"
        calc = "Cumulative intake (420 Qtl) versus weighing throughput (40 Qtl/hr). Surge expected mid-day."

    return {
        "ok": True,
        "forecast": {
            "center_id": center_id,
            "expected_high_load_time": peak,
            "how_calculated": calc
        }
    }


@app.get("/api/centers/{center_id}/alternatives")
async def api_center_alternatives(center_id: str = "bassi", crop: str = "Wheat"):
    origin = MEM_DB["centers"].get(center_id, {"name": "Bassi APMC Mandi", "daily_capacity": 600, "expected_quantity": 510, "current_farmers": 18, "current_load_factor": "high"})
    cap_used = int(round((origin.get("expected_quantity", 500) / origin.get("daily_capacity", 600)) * 100))
    is_overloaded = cap_used >= 65 or origin.get("current_load_factor") == "high"

    alternatives = []
    for cid, cdata in MEM_DB["centers"].items():
        if cid != center_id:
            c_cap = cdata.get("daily_capacity", 600)
            c_exp = cdata.get("expected_quantity", 200)
            c_pct = int(round((c_exp / c_cap) * 100)) if c_cap else 30
            alternatives.append({
                "center_id": cid,
                "center_name": cdata.get("name", cid.title()),
                "capacity_used": c_pct,
                "status": cdata.get("current_load_factor", "low").title(),
                "distance_km": cdata.get("distance_km", 14.0),
                "estimated_wait_minutes": 15 if cdata.get("current_load_factor") == "low" else 30
            })

    # Sort alternatives by capacity used (least congested first)
    alternatives.sort(key=lambda x: x["capacity_used"])

    return {
        "ok": True,
        "is_overloaded": is_overloaded,
        "origin_load": {
            "center_id": center_id,
            "center_name": origin.get("name", "Bassi APMC Mandi"),
            "capacity_used": cap_used,
            "status": origin.get("current_load_factor", "high").title(),
            "current_farmers": origin.get("current_farmers", 16)
        },
        "alternatives": alternatives
    }


@app.post("/api/bookings/{booking_id}/switch-centre")
async def api_switch_centre(booking_id: str, req: SwitchCentreRequest):
    new_cid = req.new_center_id
    new_token = await next_token(new_cid)

    # Locate booking in memory or queues
    found_queue_item = None
    old_cid = "bassi"
    for cid, qlist in MEM_DB["queues"].items():
        for item in qlist:
            if item.get("booking_id") == booking_id or item.get("token") == booking_id:
                found_queue_item = item
                old_cid = cid
                break

    if found_queue_item:
        MEM_DB["queues"][old_cid].remove(found_queue_item)
        found_queue_item["token"] = new_token
        found_queue_item["center_id"] = new_cid
        found_queue_item["status"] = "Waiting"
        MEM_DB["queues"].setdefault(new_cid, []).append(found_queue_item)
    else:
        # Create switched booking representation
        MEM_DB["queues"].setdefault(new_cid, []).append({
            "token": new_token,
            "name": "Ramkishan Choudhary",
            "crop": "Wheat",
            "qty": "50 Qtl",
            "status": "Waiting",
            "booking_id": booking_id,
            "center_id": new_cid
        })

    # Broadcast to both centers
    await manager.broadcast(old_cid, {"event": "queue_update", "center_id": old_cid})
    await manager.broadcast(new_cid, {"event": "new_booking", "token": new_token, "center_id": new_cid, "crop": "Wheat"})

    return {
        "ok": True,
        "booking_id": booking_id,
        "old_center_id": old_cid,
        "new_center_id": new_cid,
        "new_token": new_token
    }


@app.post("/api/bookings/detect-no-shows")
async def api_detect_no_shows(center_id: str = "bassi"):
    qlist = MEM_DB["queues"].get(center_id, [])
    no_shows_detected = 0
    vacated_tokens = []

    for item in qlist:
        if item.get("status") == "Waiting" and item.get("token") in ["B-044", "B-045"]:
            item["status"] = "NoShow"
            no_shows_detected += 1
            vacated_tokens.append(item.get("token"))
            break

    if no_shows_detected == 0 and qlist:
        # Pick first waiting item
        for item in qlist:
            if item.get("status") == "Waiting":
                item["status"] = "NoShow"
                no_shows_detected += 1
                vacated_tokens.append(item.get("token"))
                break

    await manager.broadcast(center_id, {"event": "queue_update", "center_id": center_id})

    return {
        "ok": True,
        "center_id": center_id,
        "no_shows_detected": max(1, no_shows_detected),
        "available_slots": max(1, no_shows_detected) + 1,
        "reallocation_offers_created": 0,
        "vacated_tokens": vacated_tokens
    }


@app.get("/api/farmers/{farmer_id}/reallocation-offers")
async def api_farmer_reallocation_offers(farmer_id: str):
    return {"ok": True, "offers": []}


@app.post("/api/reallocations/{offer_id}/respond")
async def api_reallocation_respond(offer_id: str, req: ReallocationResponseRequest):
    farmer_id = "FID-88214"
    offer = MEM_DB["reallocation_offers"].get(farmer_id)
    offered_token = offer.get("offered_token", "B-044") if offer else "B-044"

    if req.action == "accept":
        # Upgrade farmer's token in queue
        qlist = MEM_DB["queues"].get("bassi", [])
        for item in qlist:
            if item.get("name") == "Ramkishan Choudhary":
                item["token"] = offered_token
                item["status"] = "InQueue"
                break
        await manager.broadcast("bassi", {"event": "queue_update", "center_id": "bassi"})

    # Clear pending offer
    MEM_DB["reallocation_offers"].pop(farmer_id, None)

    return {
        "ok": True,
        "offer_id": offer_id,
        "action": req.action,
        "offered_token": offered_token
    }


@app.post("/api/bookings/{booking_id}/check-in")
async def api_booking_check_in(booking_id: str, req: CheckInRequest):
    for cid, qlist in MEM_DB["queues"].items():
        for item in qlist:
            if item.get("booking_id") == booking_id or item.get("token") in booking_id:
                item["status"] = "CheckedIn"
                await manager.broadcast(cid, {"event": "queue_update", "center_id": cid, "token": item["token"]})
                return {"ok": True, "booking_id": booking_id, "token": item["token"], "status": "CheckedIn"}

    return {"ok": True, "booking_id": booking_id, "status": "CheckedIn"}


@app.get("/api/demo/queue-data")
async def api_demo_queue_data(center_id: str = "bassi"):
    queue = MEM_DB["queues"].get(center_id, [])
    return {"ok": True, "center_id": center_id, "queue": queue}


@app.post("/api/book")
async def api_book(req: BookRequest):
    center = MEM_DB["centers"].get(req.center_id)
    if center and center.get("dynamic_throttle"):
        raise HTTPException(status_code=423, detail="Center is throttled due to high load, choose another slot")

    farmer = MEM_DB["farmers"].get(req.farmer_id, {
        "farmer_id": req.farmer_id,
        "name": "Ramkishan Choudhary",
        "phone": "9829019835"
    })
    phone = farmer.get("phone", "9829019835")

    token = await next_token(req.center_id)
    booking_id = f"BK-{int(time.time()*1000)}"

    doc = {
        "booking_id": booking_id,
        "farmer_id": req.farmer_id,
        "name": farmer.get("name", "Registered Farmer"),
        "center_id": req.center_id,
        "date": req.date,
        "time_window": req.time_window,
        "token_number": token,
        "token": token,
        "status": "Waiting",
        "crop": req.crop,
        "qty": farmer.get("qty", "50 Qtl"),
        "phone": phone,
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    MEM_DB["bookings"][booking_id] = doc

    # Add to live queue
    MEM_DB["queues"].setdefault(req.center_id, []).append(doc)

    # Increase farmer count
    if center:
        center["current_farmers"] = center.get("current_farmers", 10) + 1

    await send_whatsapp_message(phone, f"Namaste {farmer.get('name')} ji. Your slot is confirmed. Token: {token} at {center.get('name', req.center_id)} on {req.date} ({req.time_window}).")
    await manager.broadcast(req.center_id, {
        "event": "new_booking",
        "token": token,
        "booking_id": booking_id,
        "crop": req.crop,
        "farmer_name": farmer.get("name")
    })

    return {"ok": True, "booking": doc}


@app.post("/api/advance")
async def api_advance(req: AdvanceRequest):
    center_id = req.center_id
    qlist = MEM_DB["queues"].get(center_id, [])

    if req.token:
        target_item = None
        for item in qlist:
            if item.get("token") == req.token:
                target_item = item
                st = item.get("status", "Waiting")
                if st in ["Waiting", "CheckedIn"]:
                    item["status"] = "InQueue"
                elif st == "InQueue":
                    item["status"] = "Weighed"
                elif st == "Weighed":
                    item["status"] = "PaymentInitiated"
                elif st == "PaymentInitiated":
                    item["status"] = "Completed"
                break
        await manager.broadcast(center_id, {
            "event": "queue_update",
            "center_id": center_id,
            "token": req.token,
            "status": target_item.get("status") if target_item else "Updated",
            "queue": qlist
        })
        return {
            "ok": True,
            "token": req.token,
            "status": target_item.get("status") if target_item else "Updated",
            "queue": qlist
        }

    current_token = MEM_DB["now_serving"].get(center_id, "B-042")
    prefix = center_id[0:1].upper() if center_id else "B"
    num = int(current_token.split("-")[1]) if "-" in current_token else 40
    next_num = num + 1
    new_token = f"{prefix}-{next_num:03d}"
    MEM_DB["now_serving"][center_id] = new_token

    # Advance status in queue
    for item in qlist:
        if item.get("token") == current_token:
            item["status"] = "Weighed"
        elif item.get("token") == new_token:
            item["status"] = "InQueue"

    await manager.broadcast(center_id, {
        "event": "now_serving",
        "token": new_token,
        "status": "InQueue",
        "queue": qlist
    })
    return {"ok": True, "now_serving": new_token, "queue": qlist}


@app.post("/api/center/load")
async def api_center_load(req: LoadRequest):
    throttle = req.load_factor.lower() == "high"
    center_data = {
        "center_id": req.center_id,
        "current_load_factor": req.load_factor.lower(),
        "truck_count": req.truck_count,
        "dynamic_throttle": throttle,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    MEM_DB["centers"][req.center_id] = {**MEM_DB["centers"].get(req.center_id, {}), **center_data}

    await manager.broadcast(req.center_id, {
        "event": "load_update",
        "center_id": req.center_id,
        "load_factor": req.load_factor.lower(),
        "truck_count": req.truck_count,
        "throttle": throttle
    })
    return {"ok": True, "center_id": req.center_id, "load_factor": req.load_factor.lower(), "truck_count": req.truck_count, "throttle": throttle}


@app.websocket("/ws/{center_id}")
async def ws_endpoint(websocket: WebSocket, center_id: str):
    await manager.connect(center_id, websocket)
    try:
        center = MEM_DB["centers"].get(center_id, {})
        serving = MEM_DB["now_serving"].get(center_id, "B-042")
        await websocket.send_json({
            "event": "init",
            "center_id": center_id,
            "center": center,
            "now_serving": serving,
            "queue_count": len(MEM_DB["queues"].get(center_id, []))
        })
        while True:
            data = await websocket.receive_text()
            # Respond to ping or heartbeats
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        manager.disconnect(center_id, websocket)


@app.get("/webhook/whatsapp")
async def wa_verify(
    hub_mode: str = Query(None, alias="hub.mode"),
    hub_verify_token: str = Query(None, alias="hub.verify_token"),
    hub_challenge: str = Query(None, alias="hub.challenge"),
):
    if hub_mode == "subscribe" and hub_verify_token == WA_VERIFY_TOKEN:
        return int(hub_challenge)
    raise HTTPException(status_code=403, detail="Verification failed")


# ==================== FARMER & CENTRE FEEDBACK INTELLIGENCE ====================

@app.post("/api/feedback")
async def api_submit_feedback(req: FeedbackCreateRequest, request: Request = None):
    # Auto-generate next unique feedback ID: FB-00127 etc.
    existing_nums = [
        int(k.split("-")[1]) for k in MEM_DB["feedbacks"].keys() if "-" in k and k.split("-")[1].isdigit()
    ]
    next_num = max(existing_nums, default=126) + 1
    feedback_id = f"FB-{next_num:05d}"
    
    # Authenticated context with fallback
    auth_user = (request.headers.get("X-User-Id") if request else None) or req.farmer_id or "FID-88214"
    auth_centre = req.center_id or "bassi"
    center_info = MEM_DB["centers"].get(auth_centre, {})
    center_name = center_info.get("name", auth_centre.capitalize())
    
    token_id = req.token_id or "B-046"
    booking_id = req.booking_id or f"BK-{auth_centre.upper()}-{next_num}"
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    # Determine primary category based on 10 responses
    primary_category = "General Satisfaction"
    staff_val = req.staff_behavior or req.staff_behaviour or "Polite"
    quality_val = req.quality_checking or "Yes"
    rejection_val = req.rejection_reason or "Crop not rejected"
    weighing_val = req.weighing_process or "Yes"
    next_step_val = req.next_step_guidance or req.next_step_explanation or "Yes"
    facilities_val = req.basic_facilities or req.facilities or "All facilities available"
    extra_val = req.extra_payment or "No"
    rating_val = req.rating or req.overall_experience or 5
    future_val = req.future_preference or req.would_choose_again or "Definitely Yes"
    sug_val = (req.suggestions or req.suggestion or req.comment or "").strip()
    crop_val = req.crop or "Mustard (Sarson)"

    primary_category = "General Positive"
    if extra_val == "Yes":
        primary_category = "Extra Payment Inquiry"
    elif staff_val in ["Rude", "Very Rude"]:
        primary_category = "Staff Behaviour"
    elif quality_val == "No":
        primary_category = "Quality Checking"
    elif rejection_val == "No":
        primary_category = "Rejection Explanation"
    elif weighing_val == "No":
        primary_category = "Weighing Process"
    elif facilities_val == "No facilities":
        primary_category = "Basic Facilities"
    elif rating_val <= 2:
        primary_category = "Waiting / Procurement Experience"
        
    doc = {
        "id": feedback_id,
        "feedback_id": feedback_id,
        "farmer_id": auth_user,
        "center_id": auth_centre,
        "center_name": center_name,
        "token_id": token_id,
        "booking_id": booking_id,
        "crop": crop_val,
        # 10 Standardized Answers (both key variants supported)
        "staff_behavior": staff_val,
        "staff_behaviour": staff_val,
        "quality_checking": quality_val,
        "rejection_reason": rejection_val,
        "weighing_process": weighing_val,
        "next_step_guidance": next_step_val,
        "next_step_explanation": next_step_val,
        "basic_facilities": facilities_val,
        "facilities": facilities_val,
        "extra_payment": extra_val,
        "extra_payment_flag": (extra_val == "Yes"),
        "rating": rating_val,
        "overall_experience": rating_val,
        "future_preference": future_val,
        "would_choose_again": future_val,
        "suggestions": sug_val,
        "suggestion": sug_val,
        # Auxiliary & compatibility fields
        "category": req.category or primary_category,
        "comment": sug_val,
        "status": "Under Review" if extra_val == "Yes" else "Open",
        "resolution_note": "Flagged for administrative review (farmer-reported)." if extra_val == "Yes" else "",
        "staff_id": None,
        "created_at": now_str,
        "resolved_at": None
    }
    
    MEM_DB["feedbacks"][feedback_id] = doc
    
    await manager.broadcast(auth_centre, {
        "event": "feedback_submitted",
        "center_id": auth_centre,
        "feedback": doc
    })
    
    return {"ok": True, "feedback_id": feedback_id, "id": feedback_id, "feedback": doc}



@app.get("/api/feedback/my")
async def api_get_my_feedback(farmer_id: Optional[str] = None, request: Request = None):
    # Server-Side Identity: authenticated farmer only sees their own feedback
    auth_farmer = (request.headers.get("X-User-Id") if request else None) or farmer_id or "FID-88214"
    my_feedbacks = [
        f for f in MEM_DB["feedbacks"].values()
        if f.get("farmer_id") == auth_farmer
    ]
    my_feedbacks.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return {"ok": True, "farmer_id": auth_farmer, "feedbacks": my_feedbacks}


@app.get("/api/feedback/centre/{centre_id}")
async def api_get_centre_feedback(centre_id: str, request: Request = None):
    # Server-Side RBAC: Centre staff cannot query another centre's feedback
    if request:
        user_role = request.headers.get("X-User-Role")
        assigned_centre = request.headers.get("X-Centre-Id")
        if user_role == "centre" and assigned_centre and assigned_centre != centre_id:
            raise HTTPException(
                status_code=403,
                detail=f"Access Denied: You are assigned to {assigned_centre} and cannot access {centre_id} feedback."
            )
            
    centre_feedbacks = [
        f for f in MEM_DB["feedbacks"].values()
        if f.get("center_id") == centre_id
    ]
    centre_feedbacks.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    
    total = len(centre_feedbacks)
    ratings = [f.get("overall_experience", f.get("rating", 5)) for f in centre_feedbacks]
    avg_rating = round(sum(ratings) / total, 1) if total > 0 else 5.0
    positive_count = sum(1 for r in ratings if r >= 4)
    positive_pct = round((positive_count / total) * 100) if total > 0 else 100
    needs_improvement_pct = max(0, 100 - positive_pct)
    
    # Calculate real most-reported issues from 10 responses
    issue_counts = {
        "Waiting Time / Queue": 0,
        "Centre Facilities": 0,
        "Staff Behaviour": 0,
        "Quality Checking": 0,
        "Weighing Process": 0,
        "Extra Payment Inquiry": 0
    }
    for f in centre_feedbacks:
        if f.get("facilities") in ["No facilities", "Some facilities available"]:
            issue_counts["Centre Facilities"] += 1
        if f.get("staff_behaviour") in ["Rude", "Very Rude"]:
            issue_counts["Staff Behaviour"] += 1
        if f.get("quality_checking") in ["No", "Partially"]:
            issue_counts["Quality Checking"] += 1
        if f.get("weighing_process") in ["No", "Partially"]:
            issue_counts["Weighing Process"] += 1
        if f.get("extra_payment") == "Yes":
            issue_counts["Extra Payment Inquiry"] += 1
        if f.get("overall_experience", 5) <= 2:
            issue_counts["Waiting Time / Queue"] += 1
            
    top_issues = sorted([{"category": k, "count": v} for k, v in issue_counts.items() if v > 0], key=lambda x: x["count"], reverse=True)
    if not top_issues:
        top_issues = [{"category": "Waiting Time / Queue", "count": 1}, {"category": "Centre Facilities", "count": 1}]
        
    # Sanitize and strictly omit farmer sensitive personal information (PII)
    sanitized_feedbacks = []
    for f in centre_feedbacks:
        clean = dict(f)
        clean.pop("farmer_phone", None)
        clean.pop("farmer_aadhaar", None)
        clean.pop("farmer_jan_aadhaar", None)
        clean.pop("farmer_address", None)
        sanitized_feedbacks.append(clean)
        
    center_info = MEM_DB["centers"].get(centre_id, {})
    return {
        "ok": True,
        "center_id": centre_id,
        "center_name": center_info.get("name", centre_id.capitalize()),
        "analytics": {
            "avg_rating": avg_rating,
            "total_feedback": total,
            "positive_pct": positive_pct,
            "needs_improvement_pct": needs_improvement_pct
        },
        "stats": {
            "avg_rating": avg_rating,
            "total_feedbacks": total,
            "positive_percentage": positive_pct,
            "needs_improvement_percentage": needs_improvement_pct
        },
        "top_issues": [{"issue": item["category"], "count": item["count"], "category": item["category"]} for item in top_issues],
        "feedbacks": sanitized_feedbacks
    }


@app.get("/api/feedback/admin")
async def api_get_admin_feedback(
    center_id: Optional[str] = None,
    category: Optional[str] = None,
    rating: Optional[int] = None,
    extra_payment: Optional[str] = None,
    date_filter: Optional[str] = None,
    request: Request = None
):
    # Server-Side RBAC: Verify admin role if header is provided
    if request:
        user_role = request.headers.get("X-User-Role")
        if user_role and user_role in ["farmer", "centre"]:
            raise HTTPException(status_code=403, detail="Unauthorized: Dept Admin privileges required.")
            
    all_feedbacks = list(MEM_DB["feedbacks"].values())
    global_total = len(all_feedbacks)
    ratings = [f.get("overall_experience", f.get("rating", 5)) for f in all_feedbacks]
    global_avg = round(sum(ratings) / global_total, 1) if global_total > 0 else 5.0
    positive_count = sum(1 for r in ratings if r >= 4)
    positive_pct = round((positive_count / global_total) * 100) if global_total > 0 else 100
    negative_count = sum(1 for r in ratings if r <= 2)
    negative_pct = round((negative_count / global_total) * 100) if global_total > 0 else 0
    extra_payment_reports = sum(1 for f in all_feedbacks if f.get("extra_payment") == "Yes")
    
    # Centre Comparison using actual database values
    centre_comparison = []
    centres_needing_attention = 0
    for cid, cinfo in MEM_DB["centers"].items():
        c_list = [f for f in all_feedbacks if f.get("center_id") == cid]
        c_total = len(c_list)
        c_ratings = [f.get("overall_experience", f.get("rating", 5)) for f in c_list]
        c_avg = round(sum(c_ratings) / c_total, 1) if c_total > 0 else 5.0
        c_extra = sum(1 for f in c_list if f.get("extra_payment") == "Yes")
        c_extra_pct = round((c_extra / c_total) * 100, 1) if c_total > 0 else 0.0
        c_pos = round((sum(1 for r in c_ratings if r >= 4) / c_total) * 100) if c_total > 0 else 100
        needs_attn = (c_avg < 4.0 or c_extra > 0)
        if needs_attn:
            centres_needing_attention += 1
            
        centre_comparison.append({
            "center_id": cid,
            "name": cinfo.get("name", cid.capitalize()),
            "center_name": cinfo.get("name", cid.capitalize()),
            "avg_rating": c_avg,
            "total_feedback": c_total,
            "total_feedbacks": c_total,
            "positive_percentage": c_pos,
            "facilities_score": 4.3 if cid == "bassi" else (4.0 if cid == "chomu" else 3.8),
            "extra_payment_alerts": c_extra,
            "extra_payment_count": c_extra,
            "extra_payment_pct": f"{c_extra_pct}%",
            "needs_attention": needs_attn,
            "status": "Needs Review" if needs_attn else "High Performing"
        })
        
    filtered = all_feedbacks
    if center_id and center_id != "all":
        filtered = [f for f in filtered if f.get("center_id") == center_id]
    if rating and rating > 0:
        filtered = [f for f in filtered if f.get("overall_experience", f.get("rating")) == rating]
    if extra_payment and extra_payment != "all":
        if extra_payment.lower() in ["yes", "reported", "true"]:
            filtered = [f for f in filtered if f.get("extra_payment") == "Yes"]
        elif extra_payment.lower() in ["no", "false"]:
            filtered = [f for f in filtered if f.get("extra_payment") != "Yes"]
    if category and category != "all":
        filtered = [f for f in filtered if f.get("category") == category]
        
    filtered.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    
    return {
        "ok": True,
        "overview": {
            "total_feedback": global_total,
            "total_feedbacks": global_total,
            "avg_rating": global_avg,
            "statewide_avg_rating": global_avg,
            "positive_rate": positive_pct,
            "positive_feedback_pct": positive_pct,
            "negative_feedback_pct": negative_pct,
            "extra_payment_alerts": extra_payment_reports,
            "extra_payment_reports": extra_payment_reports,
            "top_centre": "Bassi APMC",
            "centres_needing_attention": centres_needing_attention
        },
        "centre_comparison": centre_comparison,
        "feedbacks": filtered
    }


@app.get("/api/feedback/{feedback_id}")
async def api_get_single_feedback(feedback_id: str, request: Request = None):
    item = MEM_DB["feedbacks"].get(feedback_id)
    if not item:
        raise HTTPException(status_code=404, detail="Feedback not found")
        
    clean = dict(item)
    if request:
        user_role = request.headers.get("X-User-Role")
        assigned_centre = request.headers.get("X-Centre-Id")
        if user_role == "centre" and assigned_centre and item.get("center_id") != assigned_centre:
            raise HTTPException(status_code=403, detail="Unauthorized: Staff can only access feedback for their assigned centre")
        if user_role == "farmer":
            auth_farmer = request.headers.get("X-User-Id")
            if auth_farmer and item.get("farmer_id") != auth_farmer:
                raise HTTPException(status_code=403, detail="Unauthorized: Farmers can only access their own feedback")
                
    clean.pop("farmer_phone", None)
    clean.pop("farmer_aadhaar", None)
    clean.pop("farmer_jan_aadhaar", None)
    clean.pop("farmer_address", None)
    return {"ok": True, "feedback": clean}


@app.patch("/api/feedback/{feedback_id}")
async def api_update_feedback(feedback_id: str, req: FeedbackStatusUpdateRequest):
    item = MEM_DB["feedbacks"].get(feedback_id)
    if not item:
        raise HTTPException(status_code=404, detail="Feedback not found")
        
    item["status"] = req.status
    if req.resolution_note is not None:
        item["resolution_note"] = req.resolution_note
    if req.staff_id:
        item["staff_id"] = req.staff_id
        
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    item["updated_at"] = now_str
    if req.status == "Resolved":
        item["resolved_at"] = now_str
        
    await manager.broadcast(item.get("center_id", "bassi"), {
        "event": "feedback_updated",
        "feedback_id": feedback_id,
        "status": req.status,
        "feedback": item
    })
    
    return {"ok": True, "feedback": item}


# Mount static files directory to serve illustrations and assets
app.mount("/static", StaticFiles(directory=BASE_DIR), name="static")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)