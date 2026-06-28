import React, { useEffect, useState } from 'react';
import { MapContainer, TileLayer, Marker, Popup, useMap } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';
import { Wind, CloudFog, Layers, X } from 'lucide-react';
import { ResponsiveContainer, LineChart, Line, XAxis, YAxis, Tooltip, CartesianGrid } from 'recharts';
import L from 'leaflet';

// Fix na domyślne ikonki Leafleta, które Vite potrafi zgubić
delete L.Icon.Default.prototype._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon-2x.png',
  iconUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon.png',
  shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-shadow.png',
});

// Komponent pomocniczy do wymuszenia pobrania danych przy załadowaniu mapy
function DataFetcher({ onMapLoad }) {
  const map = useMap();
  useEffect(() => {
    if (map) onMapLoad();
  }, [map]);
  return null;
}

function App() {
  // Stany ogólne i filtry czujników bazy danych
  const [sensors, setSensors] = useState([]);
  const [showAir, setShowAir] = useState(true);
  const [showWind, setShowWind] = useState(true);
  const [mapStyle, setMapStyle] = useState('osm'); // 'osm' lub 'sentinel'

  // Stany dla nowych warstw real-time
  const [vessels, setVessels] = useState([]);
  const [showVessels, setShowVessels] = useState(true);
  const [showBeaches, setShowBeaches] = useState(true);

  // Stany dla wykresu historycznego DynamoDB
  const [activeSensor, setActiveSensor] = useState(null);
  const [historyData, setHistoryData] = useState([]);
  const [loadingHistory, setLoadingHistory] = useState(false);

  // Sztywna baza kąpielisk Trójmiasta (Sanepid)
  const TRICITY_BEACHES = [
    { id: "beach-sopot", name: "Kąpielisko Sopot - Kamienny Potok", lat: 54.457, lon: 18.568, status: "OPEN", water_temp: 18.5, enterococci: "Niska (czysta)", cyanobacteria: "Brak (OK)" },
    { id: "beach-brzezno", name: "Kąpielisko Gdańsk Brzeźno", lat: 54.417, lon: 18.631, status: "OPEN", water_temp: 19.1, enterococci: "Średnia (bezpieczna)", cyanobacteria: "Brak (OK)" },
    { id: "beach-jelitkowo", name: "Kąpielisko Gdańsk Jelitkowo", lat: 54.429, lon: 18.595, status: "OPEN", water_temp: 18.8, enterococci: "Niska (czysta)", cyanobacteria: "Brak (OK)" },
    { id: "beach-orlowo", name: "Kąpielisko Gdynia Orłowo", lat: 54.482, lon: 18.564, status: "CLOSED", water_temp: 17.2, enterococci: "Wysoka (ZAKAZ)", cyanobacteria: "Wykryto zakwit sinic!" }
  ];

  // 1. Pobranie danych sensorów z FastAPI (GIOŚ + Pogoda)
  const fetchLatestSensors = async () => {
    try {
      const res = await fetch('http://127.0.0.1:8000/api/sensors');
      const data = await res.json();
      setSensors(data);
    } catch (err) {
      console.error("Błąd pobierania sensorów:", err);
    }
  };

  // 2. Pobranie statków z FastAPI (Sparsowane z pliku tekstowego na backendzie)
  const fetchVessels = async () => {
    try {
      const res = await fetch('http://127.0.0.1:8000/api/vessels');
      const data = await res.json();
      setVessels(data);
    } catch (err) {
      console.error("Błąd pobierania statków:", err);
    }
  };

  // Cykliczny odświeżacz dla statków na żywo co 3 sekundy
  useEffect(() => {
    fetchVessels();
    const vesselInterval = setInterval(fetchVessels, 3000);
    return () => clearInterval(vesselInterval);
  }, []);

  // Obsługa WebSocketu do natychmiastowych powiadomień
  useEffect(() => {
    const ws = new WebSocket('ws://127.0.0.1:8000/ws/live');
    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);
      if (data.sensor_id) fetchLatestSensors();
    };
    return () => ws.close();
  }, []);

  // 3. Pobranie historii zmian z DynamoDB pod dolny wykres Recharts
  const loadSensorHistory = async (sensorId, label) => {
    setActiveSensor({ id: sensorId, label: label });
    setLoadingHistory(true);
    try {
      const res = await fetch(`http://127.0.0.1:8000/api/sensors/${sensorId}/history`);
      const data = await res.json();
      
      const sortedData = data.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
      const formattedData = sortedData.map(item => ({
        ...item,
        displayTime: new Date(item.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
      }));

      setHistoryData(formattedData);
    } catch (err) {
      console.error("Błąd pobierania historii:", err);
    } finally {
      setLoadingHistory(false);
    }
  };

  // Stylizowane ikony-pączki dla bazowych stacji pomiarowych
  const createDonutIcon = (color) => {
    return new L.DivIcon({
      html: `<div style="background-color:${color}; width:16px; height:16px; border-radius:50%; border:2px solid white; box-shadow:0 2px 5px rgba(0,0,0,0.4);"></div>`,
      className: 'custom-marker-icon',
      iconSize: [16, 16],
      iconAnchor: [8, 8]
    });
  };

  return (
    <div style={{ width: '100vw', height: '100vh', position: 'relative', overflow: 'hidden', fontFamily: 'sans-serif' }}>
      
      {/* PANEL BOCZNY (FILTRY I STYLE MAPY) */}
      <div style={{
        position: 'absolute', top: '20px', left: '20px', zIndex: 1000,
        background: 'white', padding: '16px', borderRadius: '8px',
        boxShadow: '0 4px 10px rgba(0,0,0,0.15)', width: '250px'
      }}>
        <h3 style={{ margin: '0 0 15px 0', display: 'flex', alignItems: 'center', gap: '8px', color: '#111827' }}>
          <Layers size={20} color="#4f46e5" /> BalticWatch
        </h3>
        
        <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
          <label style={{ display: 'flex', alignItems: 'center', gap: '10px', cursor: 'pointer', fontSize: '14px', color: '#374151' }}>
            <input type="checkbox" checked={showAir} onChange={(e) => setShowAir(e.target.checked)} style={{ width: '16px', height: '16px' }} />
            <CloudFog size={18} color="#ef4444" /> Jakość powietrza GIOŚ
          </label>

          <label style={{ display: 'flex', alignItems: 'center', gap: '10px', cursor: 'pointer', fontSize: '14px', color: '#374151' }}>
            <input type="checkbox" checked={showWind} onChange={(e) => setShowWind(e.target.checked)} style={{ width: '16px', height: '16px' }} />
            <Wind size={18} color="#3b82f6" /> Meteorologia Open-Meteo
          </label>

          <label style={{ display: 'flex', alignItems: 'center', gap: '10px', cursor: 'pointer', fontSize: '14px', color: '#374151' }}>
            <input type="checkbox" checked={showVessels} onChange={(e) => setShowVessels(e.target.checked)} style={{ width: '16px', height: '16px' }} />
            <span style={{ fontSize: '16px' }}>🚢</span> Statki real-time (AIS)
          </label>

          <label style={{ display: 'flex', alignItems: 'center', gap: '10px', cursor: 'pointer', fontSize: '14px', color: '#374151' }}>
            <input type="checkbox" checked={showBeaches} onChange={(e) => setShowBeaches(e.target.checked)} style={{ width: '16px', height: '16px' }} />
            <span style={{ fontSize: '16px' }}>🏖️</span> Stan kąpielisk (Sanepid)
          </label>
        </div>

        <hr style={{ border: 'none', borderTop: '1px solid #e5e7eb', margin: '14px 0 10px 0' }} />
        <h4 style={{ margin: '0 0 8px 0', fontSize: '11px', color: '#6b7280', textTransform: 'uppercase', fontWeight: 'bold' }}>Podkład mapy</h4>
        <div style={{ display: 'flex', gap: '6px' }}>
          <button 
            onClick={() => setMapStyle('osm')}
            style={{ flex: 1, padding: '6px', fontSize: '11px', fontWeight: 'bold', borderRadius: '4px', border: '1px solid #e5e7eb', cursor: 'pointer', background: mapStyle === 'osm' ? '#4f46e5' : 'white', color: mapStyle === 'osm' ? 'white' : '#374151' }}
          >
            Mapa Drogi
          </button>
          <button 
            onClick={() => setMapStyle('sentinel')}
            style={{ flex: 1, padding: '6px', fontSize: '11px', fontWeight: 'bold', borderRadius: '4px', border: '1px solid #e5e7eb', cursor: 'pointer', background: mapStyle === 'sentinel' ? '#4f46e5' : 'white', color: mapStyle === 'sentinel' ? 'white' : '#374151' }}
          >
            Sentinel-2
          </button>
        </div>
      </div>

      {/* DOLNY PANEL (WYKRESY HISTORII DYNAMODB) */}
      {activeSensor && (
        <div style={{
          position: 'absolute', bottom: '0', left: '0', right: '0', height: '280px',
          background: 'white', zIndex: 1001, padding: '20px', borderTopLeftRadius: '16px', borderTopRightRadius: '16px',
          boxShadow: '0 -4px 15px rgba(0,0,0,0.15)', display: 'flex', flexDirection: 'column'
        }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '15px' }}>
            <div>
              <h4 style={{ margin: 0, color: '#111827', fontSize: '16px' }}>{activeSensor.label}</h4>
              <span style={{ fontSize: '12px', color: '#6b7280' }}>ID: {activeSensor.id}</span>
            </div>
            <button onClick={() => setActiveSensor(null)} style={{ background: 'none', border: 'none', cursor: 'pointer' }}>
              <X size={20} color="#4b5563" />
            </button>
          </div>

          <div style={{ flex: 1, width: '100%', height: '100%' }}>
            {loadingHistory ? (
              <div style={{ display: 'flex', width: '100%', height: '100%', alignItems: 'center', justifyContent: 'center', color: '#6b7280' }}>
                Ładowanie danych historycznych z DynamoDB...
              </div>
            ) : historyData.length === 0 ? (
              <div style={{ display: 'flex', width: '100%', height: '100%', alignItems: 'center', justifyContent: 'center', color: '#6b7280' }}>
                Brak zapisanych odczytów dla tego czujnika w bazie chmurowej.
              </div>
            ) : (
              <ResponsiveContainer width="100%" height="90%">
                <LineChart data={historyData} margin={{ top: 5, right: 30, left: 10, bottom: 5 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                  <XAxis dataKey="displayTime" stroke="#9ca3af" fontSize={12} />
                  <YAxis stroke="#9ca3af" fontSize={12} unit={historyData[0]?.unit || ""} />
                  <Tooltip contentStyle={{ backgroundColor: 'white', borderRadius: '8px', border: '1px solid #e5e7eb' }} />
                  <Line type="monotone" dataKey="value" stroke="#10b981" strokeWidth={3} dot={{ r: 4 }} activeDot={{ r: 6 }} name="Wartość" />
                </LineChart>
              </ResponsiveContainer>
            )}
          </div>
        </div>
      )}

      {/* RENDEROWANIE MAPY LEAFLET */}
      <MapContainer center={[54.45, 18.65]} zoom={10} style={{ width: '100%', height: '100%', zIndex: 1 }} zoomControl={false}>
        
        {/* Dynamiczny podkład zależny od wyboru w panelu bocznym */}
        {mapStyle === 'osm' ? (
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />
        ) : (
          <TileLayer
            attribution='&copy; Copernicus Data Space Ecosystem'
            url="https://tiles.maps.eox.at/wmts/1.0.0/s2cloudless-2023_3857/default/GoogleMapsCompatible/{z}/{y}/{x}.jpg"
          />
        )}
        
        <DataFetcher onMapLoad={fetchLatestSensors} />

        {/* 1. WARSTWA CZUJNIKÓW POMIAROWYCH (ZGRUPOWANE STACJE) */}
        {sensors.map((station) => {
          const isAir = station.sensor_type?.startsWith('air_');
          const isWind = station.sensor_type === 'wind_speed';
          
          if (isAir && !showAir) return null;
          if (isWind && !showWind) return null;

          const markerColor = isAir ? '#ef4444' : '#3b82f6';

          return (
            <Marker key={station.station_id} position={[station.lat, station.lon]} icon={createDonutIcon(markerColor)}>
              <Popup>
                <div style={{ fontFamily: 'sans-serif', minWidth: '240px', color: '#1f2937' }}>
                  <h4 style={{ margin: '0 0 10px 0', borderBottom: '1.5px solid #e5e7eb', paddingBottom: '4px', fontSize: '14px' }}>
                    📍 {station.station_name}
                  </h4>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '8px', maxHeight: '200px', overflowY: 'auto' }}>
                    {station.readings?.map((r) => (
                      <div key={r.sensor_id} style={{ display: 'flex', flexDirection: 'column', background: '#f9fafb', padding: '8px', borderRadius: '6px', border: '1px solid #f3f4f6' }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '12px', marginBottom: '6px' }}>
                          <span style={{ fontWeight: 'bold' }}>{r.param}:</span>
                          <span>{r.value} {r.unit}</span>
                        </div>
                        <button 
                          onClick={() => loadSensorHistory(r.sensor_id, `${station.station_name} - ${r.param}`)}
                          style={{ background: '#4f46e5', color: 'white', border: 'none', padding: '4px 6px', cursor: 'pointer', borderRadius: '4px', fontSize: '11px', fontWeight: 'bold', width: '100%', textAlign: 'center' }}
                        >
                          📈 Pokaż wykres z DynamoDB
                        </button>
                      </div>
                    ))}
                  </div>
                </div>
              </Popup>
            </Marker>
          );
        })}

        {/* 2. WARSTWA DYNAMICZNYCH STATKÓW MORSKICH (Z PROJEKTU DATA DOCKED) */}
        {showVessels && vessels.map((vessel) => {
          // Oddzielamy CSS transform pozycjonowania od obrotu trójkąta (kurs statku)
          const vesselIcon = new L.DivIcon({
            html: `<div style="width: 16px; height: 16px; display: flex; align-items: center; justify-content: center;">
                     <div style="background-color: #10b981; width: 14px; height: 14px; clip-path: polygon(50% 0%, 100% 100%, 0% 100%); transform: rotate(${vessel.extra?.true_heading || 0}deg); box-shadow: 0 2px 5px rgba(0,0,0,0.4); border: 1px solid white;"></div>
                   </div>`,
            className: 'vessel-marker',
            iconSize: [16, 16],
            iconAnchor: [8, 8],
            popupAnchor: [0, -8]
          });

          return (
            <Marker key={vessel.sensor_id} position={[vessel.lat, vessel.lon]} icon={vesselIcon}>
              <Popup>
                <div style={{ fontFamily: 'sans-serif', minWidth: '190px', color: '#1f2937' }}>
                  <h4 style={{ margin: '0 0 5px 0', borderBottom: '1px solid #e5e7eb', paddingBottom: '3px' }}>🚢 {vessel.extra?.ship_name}</h4>
                  <p style={{ margin: '0', fontSize: '12px' }}><b>MMSI:</b> {vessel.extra?.mmsi}</p>
                  <p style={{ margin: '3px 0 0 0', fontSize: '12px' }}><b>Typ jednostki:</b> {vessel.extra?.flag}</p>
                  <p style={{ margin: '3px 0 0 0', fontSize: '12px' }}><b>Prędkość:</b> {vessel.value} węzłów</p>
                  <p style={{ margin: '3px 0 0 0', fontSize: '12px' }}><b>Kurs:</b> {vessel.extra?.true_heading}°</p>
                  <p style={{ margin: '4px 0 0 0', fontSize: '12px', color: '#4f46e5', fontWeight: 'bold' }}>📍 Cel: {vessel.extra?.dest}</p>
                </div>
              </Popup>
            </Marker>
          );
        })}

        {/* 3. WARSTWA KĄPIELISK SANEPIDU / ALERTY O SINICACH */}
        {showBeaches && TRICITY_BEACHES.map((beach) => {
          const isOpen = beach.status === "OPEN";
          const beachIcon = new L.DivIcon({
            html: `<div style="background-color: ${isOpen ? '#10b981' : '#ef4444'}; width: 26px; height: 26px; border-radius: 50% 50% 50% 0; transform: rotate(-45deg); display: flex; align-items: center; justify-content: center; box-shadow: 0 3px 6px rgba(0,0,0,0.4); border: 2px solid white;">
                     <span style="transform: rotate(45deg); font-size: 14px;">🏖️</span>
                   </div>`,
            className: 'beach-marker',
            iconSize: [26, 26],
            iconAnchor: [13, 26]
          });

          return (
            <Marker key={beach.id} position={[beach.lat, beach.lon]} icon={beachIcon}>
              <Popup>
                <div style={{ fontFamily: 'sans-serif', minWidth: '220px' }}>
                  <h4 style={{ margin: '0 0 8px 0', color: '#111827', fontSize: '15px' }}>{beach.name}</h4>
                  <div style={{ padding: '6px', borderRadius: '4px', background: isOpen ? '#e6f4ea' : '#fce8e6', color: isOpen ? '#137333' : '#c5221f', fontWeight: 'bold', textAlign: 'center', fontSize: '12px', marginBottom: '8px' }}>
                    {isOpen ? "🟢 KĄPIEL DOZWOLONA" : "🔴 ZAKAZ KĄPIELI (ALERT)"}
                  </div>
                  <p style={{ margin: '0 0 4px 0', fontSize: '13px' }}>🌡️ <b>Woda:</b> {beach.water_temp}°C</p>
                  <p style={{ margin: '0 0 4px 0', fontSize: '13px' }}><b>Enterokoki:</b> {beach.enterococci}</p>
                  <p style={{ margin: '0', fontSize: '13px' }}>🌿 <b>Stan sinic:</b> <span style={{ color: isOpen ? 'inherit' : '#c5221f', fontWeight: isOpen ? 'normal' : 'bold' }}>{beach.cyanobacteria}</span></p>
                </div>
              </Popup>
            </Marker>
          );
        })}

      </MapContainer>
    </div>
  );
}

export default App;
