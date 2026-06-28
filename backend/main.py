import json
import boto3
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from boto3.dynamodb.conditions import Key

app = FastAPI(title="BalticWatch API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Inicjalizacja AWS (pobiera z os.environ)
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
sqs = boto3.client('sqs', region_name='us-east-1')

TABLE_NAME = "baltic_watch_sensors"
QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/114538235433/baltic-watch-queue"

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception:
                pass

manager = ConnectionManager()

@app.get("/api/sensors")
def get_latest_sensors():
    table = dynamodb.Table(TABLE_NAME)
    response = table.scan(Limit=150)
    items = response.get('Items', [])
    
    # Słownik do grupowania według stacji/lokalizacji
    grouped_stations = {}
    
    for item in items:
        item['lat'] = float(item['lat'])
        item['lon'] = float(item['lon'])
        item['value'] = float(item['value'])
        if isinstance(item.get('extra'), str):
            item['extra'] = json.loads(item['extra'])
            
        # Wyciągamy unikalny klucz lokalizacji (dla wiatru id, dla GIOS station_id)
        station_id = item['extra'].get('station_id') or item['sensor_id']
        
        # Jeśli to nowa stacja lub mamy świeższy odczyt dla tego konkretnego parametru
        if station_id not in grouped_stations:
            grouped_stations[station_id] = {
                "station_id": station_id,
                "station_name": item['extra'].get('station_name') or "Centrum Zatoki",
                "lat": item['lat'],
                "lon": item['lon'],
                "sensor_type": item['sensor_type'], # do filtrowania warstw
                "readings": {}
            }
            
        # Pakujemy lub nadpisujemy najnowszy odczyt danego parametru w tej stacji
        current_param = item['extra'].get('parameter') or "Prędkość wiatru"
        existing_reading = grouped_stations[station_id]["readings"].get(current_param)
        
        if not existing_reading or item['timestamp'] > existing_reading['timestamp']:
            grouped_stations[station_id]["readings"][current_param] = {
                "sensor_id": item['sensor_id'],
                "value": item['value'],
                "unit": item['unit'],
                "timestamp": item['timestamp']
            }
            
    # Formatujemy słownik na listę zdatną dla Reacta
    result = []
    for s_id, s_data in grouped_stations.items():
        # Konwertujemy słownik odczytów na wygodną listę obiektów
        readings_list = [
            {"param": k, **v} for k, v in s_data["readings"].items()
        ]
        s_data["readings"] = readings_list
        result.append(s_data)
        
    return result

@app.get("/api/sensors/{sensor_id}/history")
def get_sensor_history(sensor_id: str):
    table = dynamodb.Table(TABLE_NAME)
    response = table.query(KeyConditionExpression=Key('sensor_id').eq(sensor_id))
    items = response.get('Items', [])
    for item in items:
        item['lat'] = float(item['lat'])
        item['lon'] = float(item['lon'])
        item['value'] = float(item['value'])
        if isinstance(item.get('extra'), str):
            item['extra'] = json.loads(item['extra'])
    return items

@app.websocket("/ws/live")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await asyncio.sleep(3600)
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# Konsumpcja danych z SQS i rozgłaszanie ich na żywo do Reacta
async def poll_sqs_and_broadcast():
    print("📥 Uruchamianie consumera SQS dla WebSocketów...")
    while True:
        try:
            # Pobieranie wiadomości (Long Polling przez 5 sekund)
            response = sqs.receive_message(
                QueueUrl=QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=5
            )
            
            messages = response.get('Messages', [])
            for msg in messages:
                body = msg['Body']
                
                # Wysyłamy odebrany pakiet (np. pozycję statku z Rusta) prosto do Reacta
                await manager.broadcast(body)
                
                # Usuwamy z kolejki, żeby się nie dublowało
                sqs.delete_message(
                    QueueUrl=QUEUE_URL,
                    ReceiptHandle=msg['ReceiptHandle']
                )
        except Exception as e:
            print(f"⚠️ Błąd SQS Polling: {e}")
        await asyncio.sleep(0.1)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(poll_sqs_and_broadcast())
    

import math
import time

import json
import os
import math
import time

@app.get("/api/vessels")
def get_live_realtime_vessels():
    # Pobieramy czas systemowy, aby wprowadzić dynamiczny mikro-ruch dla statków
    t = time.time() * 0.003
    
    # Określamy ścieżkę do pliku tekstowego zapisanego w tym samym katalogu
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, "statki.txt")
    
    try:
        # 1. Otwieramy i czytamy prawdziwe dane ze zrzutu Data Docked
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
            
            # Usuwamy ewentualne hackathonowe znaczniki , jeśli zostały w pliku
            import re
            clean_content = re.sub(r'\', '', content)
            
            data = json.loads(clean_content)
            raw_vessels = data.get("vessels", [])
            
            vessels_list = []
            for idx, v in enumerate(raw_vessels):
                if not v.get("latitude") or not v.get("longitude"):
                    continue
                
                mmsi = v.get("mmsi", "N/A")
                base_lat = float(v["latitude"])
                base_lon = float(v["longitude"])
                
                # Parsujemy surową prędkość i kurs, zabezpieczając się przed wartościami null
                try:
                    speed = float(v["speed"]) if v.get("speed") is not None else 0.0
                    # Przeliczamy prędkość w API (często podawaną w skali 0.1 węzła) na standardowe węzły
                    if speed > 30 and v.get("typeSpecific") not in ["Law enforcment", "Pleasure craft"]:
                        speed = speed / 10.0
                except (ValueError, TypeError):
                    speed = 0.0
                    
                try:
                    heading = int(v["heading"]) if v.get("heading") is not None else int(v.get("course") or 0)
                except (ValueError, TypeError):
                    heading = 0

                # Dodajemy mikro-ruch telemetryczny uzależniony od prędkości statku,
                # dzięki czemu na mapie symulujemy fizyczny ruch w czasie rzeczywistym
                movement_factor = (speed + 1.0) * 0.0002
                offset_lat = math.sin(t + idx) * movement_factor
                offset_lon = math.cos(t + idx) * movement_factor

                vessels_list.append({
                    "sensor_id": f"vessel-datadocked-{mmsi}",
                    "sensor_type": "vessel_position",
                    "lat": base_lat + offset_lat,
                    "lon": base_lon + offset_lon,
                    "value": round(speed, 1),
                    "unit": "knots",
                    "extra": {
                        "ship_name": str(v.get("name", f"Vessel {mmsi}")).strip(),
                        "mmsi": mmsi,
                        "true_heading": heading,
                        "flag": v.get("typeSpecific", "Commercial Vessel"), # Typ statku jako metadana
                        "dest": str(v.get("destination") or "Zatoka Gdańska").strip()
                    }
                })
                
            print(f"🛰️ [DATA DOCKED FILE] Sparsowano i uruchoimiono ruch dla {len(vessels_list)} autentycznych statków na Zatoce!")
            return vessels_list

    except Exception as e:
        print(f"❌ Błąd czytania pliku ze statkami ({e}). Uruchamiam flotę awaryjną...")
        
    # Awaryjny, bezpieczny fallback chroniący serwer przed przerwaniem działania
    return [
        {
            "sensor_id": "vessel-fallback-1", "sensor_type": "vessel_position", 
            "lat": 54.53, "lon": 18.66, "value": 16.2, "unit": "knots",
            "extra": {"ship_name": "STENA SPIRIT", "mmsi": 211281040, "true_heading": 280, "flag": "Passenger/Ro-Ro Cargo Ship", "dest": "Gdynia"}
        }
    ]
