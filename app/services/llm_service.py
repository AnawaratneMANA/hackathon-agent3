"""
agentic_ensemble.py

Single-file implementation of:
- WeatherAgent: fetch weather and compute weather-based risk for supplier's location
- GraphAgent: query Neo4j for supplier info, incidents, compute dependency risk & distance
- OrchestratorAgent: combine factors into a composite risk score and produce a recommendation

Usage:
    python agentic_ensemble.py --supplier-id SUP001

Configuration:
- Edit NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, OPENWEATHER_APIKEY, ENTERPRISE_COORDS below.
"""

import os
import math
import argparse
from typing import Optional, Dict, Any, List

import requests
from neo4j import GraphDatabase
from geopy.geocoders import Nominatim
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("agentic-ensemble")

# ----------------------------
# CONFIG - change to your env
# ----------------------------
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "test")

# OpenWeatherMap API key (register at https://openweathermap.org/)
OPENWEATHER_APIKEY = "ba28f1ef121dd8626b239f712d141226"

# Enterprise coordinates (latitude, longitude) - used to compute distance to supplier
ENTERPRISE_COORDS = (7.8731, 80.7718)  # example: coordinates in Sri Lanka center

# Weights for final aggregation
WEIGHTS = {
    "dependency": 0.4,  # risk based on supplier reliability & incidents (0-10)
    "distance": 0.2,    # risk based on distance (0-10)
    "weather": 0.4      # risk based on weather (0-10)
}

# ----------------------------
# Utility: haversine
# ----------------------------
def haversine_km(lat1, lon1, lat2, lon2):
    """Return distance in kilometers between two lat/lon points."""
    R = 6371.0  # Earth radius in km
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

# ----------------------------
# WeatherAgent
# ----------------------------
class WeatherAgent:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()

    def fetch_weather(self, lat: float, lon: float) -> Dict[str, Any]:
        """Call OpenWeatherMap current weather API and return the JSON (or empty)."""
        if not self.api_key or self.api_key == "YOUR_OPENWEATHERMAP_KEY":
            logger.warning("No valid OpenWeather API key provided. Weather agent will return neutral risk.")
            return {}

        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {"lat": lat, "lon": lon, "appid": self.api_key, "units": "metric"}
        r = self.session.get(url, params=params, timeout=10)
        r.raise_for_status()
        return r.json()

    def weather_to_risk(self, weather_json: Dict[str, Any]) -> float:
        """
        Convert weather JSON to a [0,10] risk score.
        Heuristics:
          - Extreme conditions (hurricane, thunderstorm, heavy rain/snow) -> high risk
          - Moderate rain -> medium risk
          - Clear/sunny -> low risk
        """
        if not weather_json:
            return 3.0  # neutral-ish

        code = None
        weather = weather_json.get("weather")
        if isinstance(weather, list) and weather:
            code = weather[0].get("id")  # openweathermap weather condition id

        # Simple mapping based on weather codes (see OpenWeatherMap codes)
        # Thunderstorm: 200-232, Drizzle: 300-321, Rain: 500-531, Snow: 600-622, Atmosphere: 700-781, Clear: 800, Clouds: 801-804
        risk = 0.0
        if code is None:
            risk = 3.0
        elif 200 <= code <= 232:  # thunderstorm
            risk = 9.0
        elif 300 <= code <= 531:  # drizzle/rain
            # intensity: differentiate heavy vs light if available
            if code >= 520:  # heavy intensity rain
                risk = 8.0
            else:
                risk = 6.0
        elif 600 <= code <= 622:  # snow
            risk = 7.0
        elif 700 <= code <= 781:  # mist, smoke, etc.
            risk = 4.0
        elif code == 800:  # clear
            risk = 1.5
        elif 801 <= code <= 804:  # clouds
            risk = 2.5
        else:
            risk = 3.5
        # Further tune by wind speed
        wind = weather_json.get("wind", {})
        wind_speed = wind.get("speed", 0)
        if wind_speed > 15:  # m/s (~54 km/h)
            risk = max(risk, 7.0)
        return float(min(10.0, max(0.0, risk)))

# ----------------------------
# GraphAgent
# ----------------------------
class GraphAgent:
    def __init__(self, uri: str, user: str, password: str, geocode_user_agent="procurelens_geocoder"):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.geolocator = Nominatim(user_agent=geocode_user_agent, timeout=10)

    def close(self):
        self.driver.close()

    def get_supplier_node(self, supplier_id: str) -> Dict[str, Any]:
        """
        Query Neo4j to find supplier node and return properties.
        Expects Supplier nodes labeled :Supplier with property id.
        """
        cypher = """
        MATCH (s:Supplier {id:$supplier_id})
        OPTIONAL MATCH (s)-[:HAS_INCIDENT]->(inc:Incident)
        WITH s, collect(inc {.*}) AS incidents
        RETURN s {.*, incidents: incidents} AS supplier
        """
        with self.driver.session() as session:
            res = session.run(cypher, supplier_id=supplier_id)
            record = res.single()
            if record:
                return record["supplier"]
            return {}

    def geocode_location(self, location_text: str) -> Optional[Dict[str, float]]:
        """Use Nominatim to geocode a text location -> returns {'lat':..., 'lon':...} or None"""
        try:
            time.sleep(1)  # polite pause for Nominatim rate limiting
            loc = self.geolocator.geocode(location_text)
            if loc:
                return {"lat": float(loc.latitude), "lon": float(loc.longitude)}
        except Exception as e:
            logger.warning("Geocoding failed for '%s': %s", location_text, e)
        return None

    def compute_dependency_risk(self, supplier: Dict[str, Any]) -> float:
        """
        Compute supplier dependency risk (0-10) using:
          - supplier.reliability (0-1) -> lower reliability increases risk
          - incidents (each incident.severity 0-1) -> increases risk
          - delayed flag -> bump
        Formula (example):
          base = (1 - reliability) * 6  # if reliability 0.9 -> base ~0.6
          incident_score = avg_incident_severity * 4
          delayed_penalty = 1.5 if delayed else 0
          total = base + incident_score + delayed_penalty, clamped to 0-10
        """
        reliability = supplier.get("reliability")
        if reliability is None:
            # if not provided, assume medium reliability 0.7
            reliability = 0.7

        incidents = supplier.get("incidents", []) or []
        # incidents is list of dicts with severity maybe present
        sev_list = []
        for inc in incidents:
            if isinstance(inc, dict):
                if "severity" in inc and isinstance(inc["severity"], (int, float)):
                    sev_list.append(float(inc["severity"]))
        avg_inc = float(sum(sev_list) / len(sev_list)) if sev_list else 0.0

        delayed = bool(supplier.get("delayed"))

        base = (1.0 - float(reliability)) * 6.0
        incident_score = avg_inc * 4.0
        delayed_penalty = 1.5 if delayed else 0.0
        total = base + incident_score + delayed_penalty
        total = max(0.0, min(10.0, total))
        return float(total)

    def compute_distance_and_risk(self, supplier: Dict[str, Any], enterprise_coords: tuple) -> Dict[str, Any]:
        """
        Returns:
          {
            'supplier_coords': (lat, lon),
            'distance_km': float,
            'distance_risk': float  # 0-10
          }
        Strategy:
          - Use supplier.lat/lon if present.
          - Else try supplier.location text to geocode.
          - If neither available, return distance_km = None and distance_risk = 5 (neutral)
        Distance risk mapping (example):
          0-50 km -> low (1-2)
          50-300 km -> medium (3-5)
          300-1000 km -> higher (6-7)
          >1000 km -> high (8-10)
        You can change logic depending on your supply chain constraints.
        """
        lat = supplier.get("lat")
        lon = supplier.get("lon")
        supplier_coords = None

        if lat is not None and lon is not None:
            try:
                supplier_coords = (float(lat), float(lon))
            except Exception:
                supplier_coords = None

        if supplier_coords is None:
            # try geocode using location or name
            location_text = supplier.get("location") or supplier.get("name") or ""
            if location_text:
                geocoded = self.geocode_location(location_text)
                if geocoded:
                    supplier_coords = (geocoded["lat"], geocoded["lon"])

        if supplier_coords is None:
            # fallback neutral
            return {"supplier_coords": None, "distance_km": None, "distance_risk": 5.0}

        dist_km = haversine_km(supplier_coords[0], supplier_coords[1], enterprise_coords[0], enterprise_coords[1])

        # map distance to risk roughly
        if dist_km <= 50:
            dr = 1.5
        elif dist_km <= 300:
            dr = 3.5
        elif dist_km <= 1000:
            dr = 6.0
        else:
            dr = 8.5

        # small fine-tune by distance proportion
        dr = min(10.0, dr + (dist_km / 2000.0) * 2.0)  # slight scaling for very long distances
        return {"supplier_coords": supplier_coords, "distance_km": dist_km, "distance_risk": float(dr)}

# ----------------------------
# OrchestratorAgent
# ----------------------------
class OrchestratorAgent:
    def __init__(self, graph_agent: GraphAgent, weather_agent: WeatherAgent, enterprise_coords: tuple, weights: Dict[str, float]):
        self.graph_agent = graph_agent
        self.weather_agent = weather_agent
        self.enterprise_coords = enterprise_coords
        self.weights = weights

    def evaluate_supplier(self, supplier_id: str) -> Dict[str, Any]:
        # 1) Retrieve supplier from Neo4j
        supplier = self.graph_agent.get_supplier_node(supplier_id)
        if not supplier:
            raise ValueError(f"Supplier {supplier_id} not found in Neo4j")

        # 2) Dependency risk (from graph)
        dependency_risk = self.graph_agent.compute_dependency_risk(supplier)

        # 3) Distance risk
        dist_info = self.graph_agent.compute_distance_and_risk(supplier, self.enterprise_coords)
        distance_risk = dist_info.get("distance_risk", 5.0)

        # 4) Weather risk
        coords = dist_info.get("supplier_coords")
        if coords:
            try:
                weather_json = self.weather_agent.fetch_weather(coords[0], coords[1])
                weather_risk = self.weather_agent.weather_to_risk(weather_json)
            except Exception as e:
                logger.warning("Weather API failed: %s", e)
                weather_risk = 4.0
        else:
            # no coords -> neutral
            weather_risk = 4.0

        # 5) Combine via weighted average
        w_dep = self.weights.get("dependency", 0.4)
        w_dist = self.weights.get("distance", 0.2)
        w_wea = self.weights.get("weather", 0.4)

        composite_raw = (dependency_risk * w_dep) + (distance_risk * w_dist) + (weather_risk * w_wea)
        # ensure in 0-10
        composite = float(min(10.0, max(0.0, composite_raw)))

        # 6) Build explanation and recommendation
        recommendation = []
        if composite >= 7.5:
            recommendation.append("High risk: consider placing a smaller emergency order and sourcing an alternative vendor.")
        elif composite >= 5.0:
            recommendation.append("Medium risk: increase safety stock and monitor shipments.")
        else:
            recommendation.append("Low risk: continue normal procurement cadence.")

        # Additional advisories from components
        if dependency_risk >= 6.5:
            recommendation.append("Supplier reliability / incidents contribute significantly to risk.")
        if distance_risk >= 6.0:
            recommendation.append("Long distance increases transit risk — consider local suppliers.")
        if weather_risk >= 7.0:
            recommendation.append("Severe weather risk: expect potential delays.")

        result = {
            "supplier_id": supplier.get("id"),
            "supplier_name": supplier.get("name"),
            "dependency_risk": dependency_risk,
            "distance_km": dist_info.get("distance_km"),
            "distance_risk": distance_risk,
            "weather_risk": weather_risk,
            "composite_risk": composite,
            "recommendation": recommendation,
            "raw_supplier": supplier
        }
        return result

# ----------------------------
# CLI runner
# ----------------------------
def main_cli():
    parser = argparse.ArgumentParser(description="Agentic Ensemble Risk Evaluator")
    parser.add_argument("--supplier-id", "-s", required=True, help="Supplier ID to evaluate (e.g., SUP001)")
    parser.add_argument("--enterprise-lat", type=float, default=None, help="Enterprise latitude (optional override)")
    parser.add_argument("--enterprise-lon", type=float, default=None, help="Enterprise longitude (optional override)")
    args = parser.parse_args()

    enterprise_coords = ENTERPRISE_COORDS
    if args.enterprise_lat is not None and args.enterprise_lon is not None:
        enterprise_coords = (args.enterprise_lat, args.enterprise_lon)

    graph_agent = GraphAgent(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    weather_agent = WeatherAgent(OPENWEATHER_APIKEY)
    orchestrator = OrchestratorAgent(graph_agent, weather_agent, enterprise_coords, WEIGHTS)

    try:
        res = orchestrator.evaluate_supplier(args.supplier_id)
        print("===== Supplier Risk Evaluation =====")
        print(f"Supplier: {res['supplier_id']} - {res['supplier_name']}")
        print(f"Dependency risk (0-10): {res['dependency_risk']:.2f}")
        print(f"Distance (km): {res['distance_km']}")
        print(f"Distance risk (0-10): {res['distance_risk']:.2f}")
        print(f"Weather risk (0-10): {res['weather_risk']:.2f}")
        print(f"Composite risk (0-10): {res['composite_risk']:.2f}")
        print("Recommendation:")
        for r in res["recommendation"]:
            print(" -", r)
    finally:
        graph_agent.close()

if __name__ == "__main__":
    main_cli()
