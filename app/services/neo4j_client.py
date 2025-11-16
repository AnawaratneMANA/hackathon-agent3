"""
Create Neo4j graph from suppliers_report.pdf
- Extracts supplier paragraphs from the PDF
- Parses fields (id, name, location/region, products, delayed?, incidents)
- Creates nodes: Supplier, Product, Region, Incident
- Creates relationships: (Supplier)-[:SUPPLIES]->(Product), (Supplier)-[:LOCATED_IN]->(Region), (Supplier)-[:HAS_INCIDENT]->(Incident)
"""

import re
import pdfplumber
from neo4j import GraphDatabase
import os
import logging
import uuid

# --------- CONFIG ----------
PDF_PATH = "suppliers_report.pdf"   # change if different
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "test")
# --------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------- helper text parsing functions ----------
def extract_paragraphs_from_pdf(pdf_path):
    text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            text += page_text + "\n\n"
    # Split paragraphs on lines that start with a number + dot (e.g., "1. LankaCoco...")
    parts = re.split(r'\n?\s*(?=\d+\.)', text)
    # Keep only non-empty parts that contain 'SUP' (your heuristic)
    paragraphs = [p.strip() for p in parts if p.strip() and "SUP" in p]
    return paragraphs

def parse_supplier_paragraph(p):
    """Return dict with parsed fields. Basic heuristic based parser."""
    out = {
        "supplier_id": None,
        "name": None,
        "location": None,
        "region": None,
        "products": [],
        "delayed": None,
        "incidents": [],
        "notes": p
    }

    # normalize spacing
    s = " ".join(p.split())

    # remove the leading number and dot
    s = re.sub(r'^\d+\.\s*', '', s)

    # Name and ID
    m = re.match(r'(?P<name>.*?)\s*\((?P<id>SUP\d{3})\)', s)
    if m:
        out["name"] = m.group("name").strip()
        out["supplier_id"] = m.group("id").strip()
    else:
        # fallback: try to find SUP### anywhere
        m2 = re.search(r'(SUP\d{3})', s)
        out["supplier_id"] = m2.group(1) if m2 else None
        # name as first few words up to comma
        out["name"] = s.split(",")[0][:80].strip()

    # Location/region extraction heuristics
    loc_match = re.search(r'(based in|located in)\s+([^,]+,\s*[^,\.]+)', s, flags=re.IGNORECASE)
    if loc_match:
        out["location"] = loc_match.group(2).strip()
    else:
        loc2 = re.search(r'(based in|located in)\s+([^,\.]+)', s, flags=re.IGNORECASE)
        if loc2:
            out["location"] = loc2.group(2).strip()

    # Try to infer region (country or province)
    region_match = re.search(r'(Province|District|region|hub|area|District|County)\s*,?\s*([A-Za-z ]+)', s, flags=re.IGNORECASE)
    if region_match:
        out["region"] = region_match.group(2).strip()
    else:
        country_match = re.search(r'(Sri Lanka|India|Bangladesh|China|Thailand|Vietnam)', s, flags=re.IGNORECASE)
        if country_match:
            out["region"] = country_match.group(1)

    # Products detection
    products = set()
    for prod_re in [r'coconut', r'coconuts', r'coconut oil', r'desiccated coconut', r'copra', r'king coconut',
                    r'coconut husk', r'coconut chips', r'virgin coconut oil',
                    r'rice', r'basmathi|basmati|jasmine|samba|nadu|white rice|parboiled', r'broken rice']:
        mprod = re.search(prod_re, s, flags=re.IGNORECASE)
        if mprod:
            prod_name = mprod.group(0).lower()
            if 'basmathi' in prod_name or 'basmati' in prod_name:
                products.add('Basmati Rice')
            elif 'jasmine' in prod_name:
                products.add('Jasmine Rice')
            elif 'samba' in prod_name:
                products.add('Samba Rice')
            elif 'nadu' in prod_name:
                products.add('Nadu Rice')
            elif 'parboiled' in prod_name:
                products.add('Parboiled Rice')
            elif 'broken' in prod_name:
                products.add('Broken Rice')
            elif 'coconut' in prod_name or 'copra' in prod_name:
                # check for specific coconut products
                if 'virgin' in s.lower():
                    products.add('Virgin Coconut Oil')
                elif 'desiccated' in s.lower():
                    products.add('Desiccated Coconut')
                elif 'copra' in s.lower():
                    products.add('Copra')
                elif 'king coconut' in s.lower():
                    products.add('King Coconut')
                else:
                    products.add('Coconut (general)')
            else:
                products.add(prod_name.title())
    out["products"] = list(products) if products else ["Unknown"]

    # Delays
    delay_match = re.search(r'\b(delay|delays|delayed|holdup|holdups|hold up|held up)\b', s, flags=re.IGNORECASE)
    out["delayed"] = bool(delay_match)

    # Incidents
    incidents = []
    for phrase in ['fuel shortage', 'machinery breakdown', 'customs holdup', 'flash floods', 'driver shortages', 'solar-powered processing', 'drought', 'harvest transitions']:
        if phrase.lower() in s.lower():
            incidents.append(phrase)
    out["incidents"] = incidents

    # safety: ensure strings for DB
    for k in ["name", "location", "region"]:
        if out[k] is None:
            out[k] = ""
    return out

# ---------- Neo4j ingestion ----------
class Neo4jIngestor:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_constraints(self):
        # Neo4j 5+ syntax: use FOR ... REQUIRE and name constraints
        # It's fine to run these DDL statements directly with session.run()
        with self.driver.session() as session:
            session.run("""
            CREATE CONSTRAINT supplier_id_unique IF NOT EXISTS
            FOR (s:Supplier)
            REQUIRE s.id IS UNIQUE
            """)
            session.run("""
            CREATE CONSTRAINT product_name_unique IF NOT EXISTS
            FOR (p:Product)
            REQUIRE p.name IS UNIQUE
            """)
            session.run("""
            CREATE CONSTRAINT region_name_unique IF NOT EXISTS
            FOR (r:Region)
            REQUIRE r.name IS UNIQUE
            """)
            logging.info("Constraints created (if not existing).")

    def ingest_supplier(self, info):
        """
        info: dict returned by parse_supplier_paragraph
        """
        # ensure supplier_id exists; generate short uuid if not present
        if not info.get("supplier_id"):
            info["supplier_id"] = f"SUP-{uuid.uuid4().hex[:8].upper()}"
        # use execute_write for transactional write operation
        with self.driver.session() as session:
            session.execute_write(self._merge_supplier_tx, info)

    @staticmethod
    def _merge_supplier_tx(tx, info):
        # Create supplier node with properties
        supplier_cypher = """
        MERGE (s:Supplier {id: $supplier_id})
        SET s.name = $name,
            s.location = $location,
            s.region = $region,
            s.delayed = $delayed,
            s.notes = $notes
        RETURN s
        """
        tx.run(supplier_cypher, supplier_id=info["supplier_id"],
               name=info["name"], location=info["location"], region=info["region"],
               delayed=info["delayed"], notes=info["notes"])

        # Merge region if present and relationship
        if info["region"]:
            tx.run("""
            MERGE (r:Region {name: $region})
            MERGE (s:Supplier {id: $supplier_id})
            MERGE (s)-[:LOCATED_IN]->(r)
            """, region=info["region"], supplier_id=info["supplier_id"])

        # Merge product nodes and relationships
        for prod in info["products"]:
            tx.run("""
            MERGE (p:Product {name: $prod})
            MERGE (s:Supplier {id: $supplier_id})
            MERGE (s)-[:SUPPLIES]->(p)
            """, prod=prod, supplier_id=info["supplier_id"])

        # Incidents
        for idx, inc in enumerate(info["incidents"], start=1):
            # create an incident node with small uid to avoid exact duplicates
            inc_node_name = f"{info['supplier_id']}_INC_{idx}"
            tx.run("""
            MERGE (inc:Incident {uid: $inc_uid})
            SET inc.description = $inc_desc
            MERGE (s:Supplier {id: $supplier_id})
            MERGE (s)-[:HAS_INCIDENT]->(inc)
            """, inc_uid=inc_node_name, inc_desc=inc, supplier_id=info["supplier_id"])

# ---------- main ----------
def main():
    logging.info("Extracting paragraphs from PDF...")
    paragraphs = extract_paragraphs_from_pdf(PDF_PATH)
    logging.info(f"Found {len(paragraphs)} supplier paragraph(s).")

    parsed = []
    for p in paragraphs:
        info = parse_supplier_paragraph(p)
        parsed.append(info)
        logging.info(f"Parsed supplier: {info['supplier_id']} - {info['name']} - products: {info['products']}")

    # Connect to Neo4j and ingest
    logging.info("Connecting to Neo4j...")
    ingestor = Neo4jIngestor(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    try:
        ingestor.create_constraints()
        for info in parsed:
            logging.info(f"Ingesting {info['supplier_id']}...")
            ingestor.ingest_supplier(info)
        logging.info("Ingestion completed.")
    except Exception as e:
        logging.exception("Failed during ingestion: %s", e)
    finally:
        ingestor.close()

if __name__ == "__main__":
    main()
