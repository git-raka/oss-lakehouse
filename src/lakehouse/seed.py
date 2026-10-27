"""Synthetic source-data generator for an industrial-estate scenario (all data is fictitious).

    python -m lakehouse.seed               # initial load: 12 months ERP, 30 days weighbridge/tickets, PDFs, photos,
                                           # 7 days of e-bus telemetry
    python -m lakehouse.seed --increment   # daily changes: ERP updates/inserts (CDC demo), new files & documents
"""
import argparse
import io
import json
import os
import random
from datetime import date, datetime, timedelta

from faker import Faker

from lakehouse.common import LANDING_BUCKET, SOURCES_DIR, ensure_buckets, pg, s3

fake = Faker("en_US")
CENTER = (-6.9000, 107.6000)  # arbitrary site centre
NOW = datetime.now().replace(microsecond=0)
MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August",
          "September", "October", "November", "December"]

TENANTS = [
    ("T001", "Prima Glass Works", "Glass & Ceramics"),
    ("T002", "GreenCell Battery", "EV Batteries"),
    ("T003", "Coastal Garments", "Textiles & Apparel"),
    ("T004", "Steelpipe Utama", "Basic Metals"),
    ("T005", "Pesisir Chemicals", "Chemicals"),
    ("T006", "Northline Logistics", "Logistics & Warehousing"),
    ("T007", "Solaris Panel", "Renewable Energy"),
    ("T008", "Stride Footwear", "Footwear"),
    ("T009", "Ciptadaya Electronics", "Electronics"),
    ("T010", "Lestari Foods", "Food & Beverage"),
    ("T011", "Raya Auto Components", "Automotive"),
    ("T012", "Medika Pharma", "Pharmaceuticals"),
]
ZONES = {"A": "Zone A - Heavy Industry", "B": "Zone B - Light Industry", "C": "Zone C - Logistics"}
ASSET_TYPES = ["Substation", "Water Treatment Plant", "Wastewater Plant", "Water Pump", "Estate Road",
               "Street Light", "Weighbridge", "Security Post"]
PO_CATEGORIES = ["Construction", "Electrical", "Water & Sanitation", "IT & Network", "Security",
                 "Cleaning", "Transport"]
BUSES = [f"BUS-{i:02d}" for i in range(1, 9)]


def jitter(scale=0.02):
    return CENTER[0] + random.uniform(-scale, scale), CENTER[1] + random.uniform(-scale, scale)


def long_date(d: date) -> str:
    return f"{d.day} {MONTHS[d.month - 1]} {d.year}"


def money(n: int) -> str:
    return f"{n:,}"


# ---------------------------------------------------------------- ERP / master data (Postgres)
DDL = """
CREATE TABLE IF NOT EXISTS tenant (
  tenant_id text PRIMARY KEY, name text, sector text, plot_id text, status text,
  pic_name text, pic_phone text, pic_email text, pic_national_id text, tax_id text,
  joined_date date, updated_at timestamp);
CREATE TABLE IF NOT EXISTS plot (
  plot_id text PRIMARY KEY, zone text, area_ha numeric(6,2), status text,
  lat double precision, lon double precision, updated_at timestamp);
CREATE TABLE IF NOT EXISTS asset (
  asset_id text PRIMARY KEY, name text, asset_type text, plot_id text, lat double precision,
  lon double precision, acquisition_year int, acquisition_value bigint, condition text, updated_at timestamp);
CREATE TABLE IF NOT EXISTS vendor (
  vendor_id text PRIMARY KEY, name text, category text, city text, email text, phone text,
  updated_at timestamp);
CREATE TABLE IF NOT EXISTS purchase_order (
  po_id text PRIMARY KEY, vendor_id text, category text, description text, amount bigint,
  status text, po_date date, updated_at timestamp);
CREATE TABLE IF NOT EXISTS employee (
  employee_id text PRIMARY KEY, national_id text, name text, division text, position text, email text,
  phone text, base_salary bigint, hire_date date, updated_at timestamp);
"""


def national_id():
    return "".join(random.choice("0123456789") for _ in range(16))


def seed_erp(conn):
    cur = conn.cursor()
    cur.execute(DDL)
    cur.execute("SELECT count(*) FROM tenant")
    if cur.fetchone()[0]:
        print("ERP already populated, skipping initial ERP seed")
        return

    plots = []
    for i, z in enumerate("AAAAAABBBBBBCCCC"):
        pid = f"P-{z}{i + 1:02d}"
        lat, lon = jitter()
        plots.append(pid)
        cur.execute("INSERT INTO plot VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    (pid, ZONES[z], round(random.uniform(2, 25), 2),
                     "OCCUPIED" if i < 12 else "AVAILABLE", lat, lon, NOW))
    for i, (tid, name, sector) in enumerate(TENANTS):
        cur.execute("INSERT INTO tenant VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (tid, name, sector, plots[i], "ACTIVE", fake.name(), fake.phone_number(),
                     fake.email(), national_id(), f"{random.randint(10, 99)}.{random.randint(100, 999)}."
                     f"{random.randint(100, 999)}.{random.randint(1, 9)}-{random.randint(100, 999)}.000",
                     date(2024, 1, 1) + timedelta(days=random.randint(0, 700)), NOW))
    n = 1
    for asset_type in ASSET_TYPES:
        for _ in range(4):
            lat, lon = jitter()
            cur.execute("INSERT INTO asset VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (f"AST-{n:04d}", f"{asset_type} {n:02d}", asset_type, random.choice(plots), lat, lon,
                         random.randint(2020, 2026), random.randint(50, 5000) * 1_000_000,
                         random.choice(["GOOD", "GOOD", "GOOD", "NEEDS MAINTENANCE", "MINOR DAMAGE"]), NOW))
            n += 1
    for b in BUSES:
        lat, lon = jitter()
        cur.execute("INSERT INTO asset VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (f"AST-{n:04d}", b, "Electric Bus", None, lat, lon, 2025, 3_200_000_000, "GOOD", NOW))
        n += 1
    vendors = []
    for i in range(1, 21):
        vid = f"V{i:03d}"
        vendors.append(vid)
        cur.execute("INSERT INTO vendor VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    (vid, fake.company(), random.choice(PO_CATEGORIES), fake.city(), fake.company_email(),
                     fake.phone_number(), NOW))
    start = date.today() - timedelta(days=365)
    for i in range(1, 601):
        d = start + timedelta(days=random.randint(0, 364))
        cat = random.choice(PO_CATEGORIES)
        status = random.choices(["DRAFT", "APPROVED", "DELIVERED", "PAID"], [1, 2, 3, 6])[0]
        cur.execute("INSERT INTO purchase_order VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    (f"PO-{d.year}-{i:05d}", random.choice(vendors), cat, f"{cat} procurement package {i}",
                     random.randint(5, 2500) * 1_000_000, status, d,
                     datetime.combine(d, datetime.min.time()) + timedelta(hours=random.randint(8, 17))))
    divisions = ["Operations", "Finance", "Marketing", "Engineering", "HR", "Legal", "IT", "HSE"]
    for i in range(1, 61):
        cur.execute("INSERT INTO employee VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (f"EMP-{i:04d}", national_id(), fake.name(), random.choice(divisions),
                     random.choice(["Staff", "Senior Staff", "Supervisor", "Manager"]), fake.email(),
                     fake.phone_number(), random.randint(6, 35) * 1_000_000,
                     date(2021, 1, 1) + timedelta(days=random.randint(0, 1800)), NOW))
    print("ERP: 16 plots, 12 tenants, 40 assets, 20 vendors, 600 POs, 60 employees")


def increment_erp(conn):
    cur = conn.cursor()
    cur.execute("SELECT po_id FROM purchase_order WHERE status <> 'PAID' ORDER BY random() LIMIT 20")
    ids = [r[0] for r in cur.fetchall()]
    nxt = {"DRAFT": "APPROVED", "APPROVED": "DELIVERED", "DELIVERED": "PAID"}
    for po in ids:
        cur.execute("UPDATE purchase_order SET status = CASE status "
                    + " ".join(f"WHEN '{a}' THEN '{b}'" for a, b in nxt.items())
                    + " ELSE status END, updated_at = %s WHERE po_id = %s", (NOW, po))
    cur.execute("SELECT vendor_id FROM vendor")
    vendors = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT count(*) FROM purchase_order")
    base = cur.fetchone()[0]
    for i in range(base + 1, base + 16):
        cat = random.choice(PO_CATEGORIES)
        cur.execute("INSERT INTO purchase_order VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    (f"PO-{date.today().year}-{i:05d}", random.choice(vendors), cat,
                     f"{cat} procurement package {i}", random.randint(5, 2500) * 1_000_000,
                     "DRAFT", date.today(), NOW))
    cur.execute("UPDATE tenant SET pic_phone = %s, updated_at = %s WHERE tenant_id = 'T003'",
                (fake.phone_number(), NOW))
    print(f"ERP increment: {len(ids)} PO status changes, 15 new POs, 1 tenant PIC update")


# ---------------------------------------------------------------- Files: weighbridge (CSV) & ticketing (JSONL)
def write_weighbridge(day: date, tag: str = ""):
    os.makedirs(f"{SOURCES_DIR}/weighbridge", exist_ok=True)
    path = f"{SOURCES_DIR}/weighbridge/weighbridge_{day:%Y%m%d}{tag}.csv"
    lines = ["ticket_no,time_in,time_out,plate_no,tenant_id,commodity,gross_kg,tare_kg,operator"]
    for i in range(random.randint(25, 60)):
        t_in = datetime.combine(day, datetime.min.time()) + timedelta(minutes=random.randint(360, 1260))
        tare = random.randint(7000, 15000)
        gross = tare + random.randint(2000, 28000)
        if random.random() < 0.03:  # dirty data: tare > gross (flagged in Silver)
            gross, tare = tare, gross
        row = (f"WB{day:%Y%m%d}{tag.replace('_inc', '-')}-{i:03d},{t_in:%Y-%m-%d %H:%M:%S},"
               f"{t_in + timedelta(minutes=random.randint(8, 40)):%Y-%m-%d %H:%M:%S},"
               f"{random.choice(['B', 'D', 'G', 'H'])} {random.randint(1000, 9999)} "
               f"{random.choice(['AB', 'BC', 'XY', 'KL'])},"
               f"{random.choice(TENANTS)[0]},"
               f"{random.choice(['Raw Material', 'Finished Goods', 'Waste', 'Construction Material'])},"
               f"{gross},{tare},{random.choice(['OPR-01', 'OPR-02', 'OPR-03'])}")
        lines.append(row)
        if random.random() < 0.02:  # duplicate delivery
            lines.append(row)
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")


def write_tickets(day: date, start_id: int, tag: str = "") -> int:
    os.makedirs(f"{SOURCES_DIR}/ticketing", exist_ok=True)
    categories = ["Electricity", "Clean Water", "Roads & Drainage", "Security", "Permits", "Cleaning"]
    with open(f"{SOURCES_DIR}/ticketing/tickets_{day:%Y%m%d}{tag}.jsonl", "w") as f:
        for _ in range(random.randint(2, 7)):
            created = datetime.combine(day, datetime.min.time()) + timedelta(minutes=random.randint(420, 1080))
            age_days = (date.today() - day).days
            status = "OPEN"
            resolved = None
            if age_days > 0 and random.random() < 0.85:
                status = "RESOLVED"
                resolved = created + timedelta(hours=random.randint(1, 96))
            elif random.random() < 0.5:
                status = "IN_PROGRESS"
            cat = random.choice(categories)
            f.write(json.dumps({
                "ticket_id": f"TKT-{start_id:05d}", "tenant_id": random.choice(TENANTS)[0], "category": cat,
                "priority": random.choice(["LOW", "MEDIUM", "HIGH"]), "status": status,
                "title": f"{cat} issue reported at tenant site",
                "created_at": created.isoformat(), "resolved_at": resolved.isoformat() if resolved else None,
                "updated_at": (resolved or created).isoformat(),
            }) + "\n")
            start_id += 1
    return start_id


# ---------------------------------------------------------------- PDFs: lease agreements (text) & scanned certificates
def lease_pdf(path, no, tenant, plot, zone, area, value, signed):
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas

    c = canvas.Canvas(path, pagesize=A4)
    w, h = A4
    y = h - 72
    c.setFont("Helvetica-Bold", 14)
    c.drawCentredString(w / 2, y, "INDUSTRIAL LAND LEASE AGREEMENT")
    y -= 20
    c.setFont("Helvetica", 11)
    c.drawCentredString(w / 2, y, f"Number: LSA/EST/{signed.year}/{no:03d}")
    body = [
        f"This agreement is made and signed on {long_date(signed)} by and between:",
        "1. The Estate Management Company, as the Estate Manager; and",
        f"2. {tenant[1]} (Tenant Code: {tenant[0]}), operating in {tenant[2]}, as the Lessee.",
        "",
        "Article 1 - Leased Object",
        f"The leased object is Plot {plot}, {zone}, with an area of {area} hectares.",
        "",
        "Article 2 - Value and Term",
        f"Contract Value: IDR {money(value)}",
        f"The lease term is {random.choice([20, 25, 30])} years from the signing date.",
        "",
        "Article 3 - Lessee Obligations",
        "The Lessee shall comply with estate regulations, waste management and HSE rules.",
        "The Lessee shall pay the monthly estate service charge.",
        "",
        "Article 4 - Dispute Resolution",
        "Disputes shall be settled amicably, or through the competent district court.",
    ]
    y -= 40
    for line in body:
        c.drawString(72, y, line)
        y -= 18
    y -= 30
    c.drawString(72, y, "Estate Manager")
    c.drawString(340, y, "Lessee")
    c.showPage()
    c.setFont("Helvetica", 11)
    c.drawString(72, h - 72, f"Appendix - Map of Plot {plot} and the list of provided utilities.")
    c.save()


def scanned_pdf(path, lines):
    """Simulates a scanned document: text rendered to an image (no text layer) -> needs OCR."""
    from PIL import Image, ImageDraw, ImageFilter, ImageFont

    img = Image.new("L", (1240, 1754), 248)
    d = ImageDraw.Draw(img)
    font_b = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 34)
    font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 26)
    y = 140
    d.text((620, y), lines[0], font=font_b, fill=20, anchor="mm")
    y += 80
    for line in lines[1:]:
        d.text((120, y), line, font=font, fill=25)
        y += 48
    for _ in range(4000):  # scanner speckle noise
        d.point((random.randint(0, 1239), random.randint(0, 1753)), fill=random.randint(120, 200))
    img = img.rotate(random.uniform(-1.2, 1.2), fillcolor=248, resample=Image.BICUBIC)
    img = img.filter(ImageFilter.GaussianBlur(0.6))
    img.save(path, "PDF", resolution=150)


def write_documents(conn, only_new=False):
    os.makedirs(f"{SOURCES_DIR}/documents", exist_ok=True)
    cur = conn.cursor()
    cur.execute("SELECT t.tenant_id, t.plot_id, p.zone, p.area_ha FROM tenant t JOIN plot p USING (plot_id) ORDER BY 1")
    rows = {r[0]: r[1:] for r in cur.fetchall()}
    tenants = TENANTS if not only_new else []
    for i, t in enumerate(tenants, 1):
        plot, zone, area = rows[t[0]]
        signed = date(2025, 1, 1) + timedelta(days=random.randint(0, 600))
        value = int(float(area) * random.randint(900, 1600)) * 1_000_000
        lease_pdf(f"{SOURCES_DIR}/documents/LEASE_{t[0]}.pdf", i, t, plot, zone, float(area), value, signed)
    picks = TENANTS[:8] if not only_new else random.sample(TENANTS, 2)
    for i, t in enumerate(picks, 1):
        plot, zone, area = rows[t[0]]
        issued = date.today() - timedelta(days=random.randint(1, 300))
        if only_new or i % 2:
            no = random.randint(100, 999) if only_new else i
            scanned_pdf(f"{SOURCES_DIR}/documents/HANDOVER_{t[0]}_{no:03d}.pdf", [
                "PLOT HANDOVER CERTIFICATE",
                f"Number: HOC/EST/{issued.year}/{no:03d}",
                f"Date: {long_date(issued)}",
                f"Tenant Name: {t[1]}",
                f"Tenant Code: {t[0]}",
                f"Plot: {plot} ({zone})",
                f"Area: {float(area)} ha",
                "Land condition: levelled and ready for construction.",
                "Utilities: electricity, clean water and road access available.",
                "",
                "First Party: Estate Manager",
                "Second Party: Lessee",
            ])
        else:
            scanned_pdf(f"{SOURCES_DIR}/documents/PERMIT_{t[0]}.pdf", [
                "BUILDING APPROVAL",
                f"Number: BPA/EST/{issued.year}/{i:03d}",
                f"Date: {long_date(issued)}",
                f"Applicant: {t[1]}",
                f"Tenant Code: {t[0]}",
                f"Location: Plot {plot}",
                f"Building use: {t[2]} factory",
                f"Investment Value: IDR {money(random.randint(50, 900) * 1_000_000_000)}",
                "This approval is valid while the building is used as approved.",
            ])
    print(f"PDF documents: {len(tenants)} lease agreements (text), {len(picks)} certificates (scanned)")


# ---------------------------------------------------------------- Field photos (JPEG + EXIF GPS)
def _gps(value):
    v = abs(value)
    deg = int(v)
    minutes = int((v - deg) * 60)
    sec = round(((v - deg) * 60 - minutes) * 60 * 100)
    return ((deg, 1), (minutes, 1), (sec, 100))


def write_photos(conn, n=30):
    import piexif
    from PIL import Image, ImageDraw, ImageFont

    os.makedirs(f"{SOURCES_DIR}/photos", exist_ok=True)
    cur = conn.cursor()
    cur.execute("SELECT asset_id, name, asset_type, lat, lon FROM asset WHERE asset_type <> 'Electric Bus'")
    assets = cur.fetchall()
    font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 30)
    colors = {"Substation": (200, 160, 40), "Wastewater Plant": (60, 140, 120), "Water Pump": (50, 110, 190)}
    for _ in range(n):
        asset_id, name, asset_type, lat, lon = random.choice(assets)
        taken = NOW - timedelta(days=random.randint(0, 60), minutes=random.randint(0, 600))
        img = Image.new("RGB", (1024, 768), (135, 180, 225))
        d = ImageDraw.Draw(img)
        d.rectangle([0, 470, 1024, 768], fill=(110, 140, 90))
        d.rectangle([300, 280, 720, 560], fill=colors.get(asset_type, (150, 150, 160)), outline=(40, 40, 40), width=4)
        d.rectangle([0, 0, 1024, 60], fill=(20, 30, 50))
        d.text((20, 14), f"{asset_id} | {name} | {taken:%Y-%m-%d %H:%M}", font=font, fill=(255, 255, 255))
        exif = {
            "0th": {piexif.ImageIFD.Make: b"FieldOps", piexif.ImageIFD.Model: b"FieldCam-01",
                    piexif.ImageIFD.ImageDescription:
                        f"{asset_id} {random.choice(['routine', 'post-rain', 'repair'])} inspection".encode()},
            "Exif": {piexif.ExifIFD.DateTimeOriginal: taken.strftime("%Y:%m:%d %H:%M:%S").encode()},
            "GPS": {piexif.GPSIFD.GPSLatitudeRef: b"S" if lat < 0 else b"N", piexif.GPSIFD.GPSLatitude: _gps(lat),
                    piexif.GPSIFD.GPSLongitudeRef: b"E", piexif.GPSIFD.GPSLongitude: _gps(lon)},
        }
        img.save(f"{SOURCES_DIR}/photos/PHOTO_{taken:%Y%m%d_%H%M%S}_{random.randint(100, 999)}.jpg",
                 "JPEG", quality=82, exif=piexif.dump(exif))
    print(f"Photos: {n} JPEGs with EXIF (timestamp, GPS, asset code)")


# ---------------------------------------------------------------- E-bus telemetry history (written straight to landing)
def write_bus_history(days=7):
    client = s3()
    total = 0
    for back in range(days, 0, -1):
        day = date.today() - timedelta(days=back)
        buf = io.StringIO()
        for bus in BUSES:
            soc, odo = 100.0, 10000 + BUSES.index(bus) * 3500 + (days - back) * 180
            lat, lon = jitter(0.015)
            for minute in range(6 * 60, 22 * 60):
                ts = datetime.combine(day, datetime.min.time()) + timedelta(minutes=minute)
                speed = max(0.0, random.gauss(22, 9)) if random.random() > 0.2 else 0.0
                km = speed / 60
                odo += km
                soc = max(8.0, soc - km * random.uniform(0.35, 0.55)) if minute != 13 * 60 else 95.0
                lat += random.uniform(-0.0004, 0.0004)
                lon += random.uniform(-0.0004, 0.0004)
                buf.write(json.dumps({
                    "bus_id": bus, "ts": ts.isoformat(), "lat": round(lat, 6), "lon": round(lon, 6),
                    "speed_kmh": round(speed, 1), "soc_pct": round(soc, 1), "odometer_km": round(odo, 2),
                    "energy_kwh": round(km * random.uniform(0.9, 1.3), 3),
                    "boarding": random.choices([0, 1, 2, 3], [70, 15, 10, 5])[0],
                }) + "\n")
                total += 1
        client.put_object(Bucket=LANDING_BUCKET, Key=f"iot_bus/history_{day:%Y%m%d}.jsonl",
                          Body=buf.getvalue().encode())
    print(f"E-bus telemetry history: {total} rows ({days} days)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--increment", action="store_true")
    args = ap.parse_args()
    random.seed(None if args.increment else 42)
    Faker.seed(None if args.increment else 42)
    ensure_buckets()
    conn = pg()
    if args.increment:
        increment_erp(conn)
        tag = f"_inc{NOW:%H%M%S}"
        write_weighbridge(date.today(), tag)
        write_tickets(date.today(), 90000 + random.randint(0, 9000), tag)
        write_documents(conn, only_new=True)
        write_photos(conn, n=3)
    else:
        seed_erp(conn)
        tid = 1
        for back in range(30, -1, -1):
            day = date.today() - timedelta(days=back)
            write_weighbridge(day)
            tid = write_tickets(day, tid)
        print("Weighbridge & ticketing: 31 daily files")
        write_documents(conn)
        write_photos(conn)
        write_bus_history()
    print("Done.")


if __name__ == "__main__":
    main()
