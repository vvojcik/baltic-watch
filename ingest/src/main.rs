use reqwest;
use serde::{Deserialize, Serialize};
use chrono::Utc;
use std::sync::Arc;

// AWS SDK Importy
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_sqs::Client as SqsClient;

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

// Definiujemy strukturę wiadomości rejestracyjnej dla AISStream
#[derive(Serialize)]
struct AisSubscription {
    #[serde(rename = "APIKey")]
    api_key: String,
    #[serde(rename = "BoundingBoxes")]
    bounding_boxes: Vec<Vec<Vec<f64>>>,
}

async fn stream_ais(
    db_client: Arc<DynamoClient>,
    sqs_client: Arc<SqsClient>,
    table_name: String,
    queue_url: String,
    api_key: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let clean_key = api_key.trim();
    
    // Budujemy czysty string URL - proxy Envoy autoryzuje po query params
    let url_str = format!("wss://stream.aisstream.io/v1/stream?apiKey={}", clean_key);
    println!("🔌 Łączenie z AISStream WebSocket...");

    let url = reqwest::Url::parse(&url_str)?;

    // Łączymy się bezpośrednio przez zwalidowany URL
    let (ws_stream, _) = connect_async(url).await?;
    let (mut write, mut read) = ws_stream.split();

    // Współrzędne Bounding Box dla Zatoki Gdańskiej
    let sub_msg = AisSubscription {
        api_key: clean_key.to_string(),
        bounding_boxes: vec![vec![vec![54.2, 18.3], vec![54.8, 19.9]]],
    };

    let sub_text = serde_json::to_string(&sub_msg)?;
    write.send(Message::Text(sub_text)).await?;
    println!("🛰️ Subskrypcja AIS dla Zatoki Gdańskiej wysłana!");

    // Czytanie strumienia danych ze statków
    while let Some(message) = read.next().await {
        let message = match message {
            Ok(msg) => msg,
            Err(e) => {
                eprintln!("❌ Błąd WebSocket AIS: {:?}", e);
                break;
            }
        };

        if let Message::Text(text) = message {
            if let Ok(vessel_data) = serde_json::from_str::<serde_json::Value>(&text) {
                if let Some(msg_type) = vessel_data["MessageType"].as_str() {
                    if msg_type == "PositionReport" {
                        let metadata = &vessel_data["MetaData"];
                        let mmsi = metadata["MMSI"].as_i64().unwrap_or(0);
                        let ship_name = metadata["ShipName"].as_str().unwrap_or("Unknown Ship").trim();
                        
                        let lat = vessel_data["Message"]["PositionReport"]["Latitude"].as_f64().unwrap_or(0.0);
                        let lon = vessel_data["Message"]["PositionReport"]["Longitude"].as_f64().unwrap_or(0.0);
                        let speed = vessel_data["Message"]["PositionReport"]["Sog"].as_f64().unwrap_or(0.0);

                        if mmsi == 0 || lat == 0.0 { continue; }

                        let reading = SensorReading {
                            sensor_id: format!("vessel-ais-{}", mmsi),
                            timestamp: Utc::now().to_rfc3339(),
                            sensor_type: "vessel_position".to_string(),
                            lat,
                            lon,
                            value: speed,
                            unit: "knots".to_string(),
                            extra: serde_json::json!({
                                "ship_name": ship_name,
                                "mmsi": mmsi,
                                "true_heading": vessel_data["Message"]["PositionReport"]["TrueHeading"].as_i64().unwrap_or(0)
                            }),
                        };

                        let db = Arc::clone(&db_client);
                        let sqs = Arc::clone(&sqs_client);
                        let t_name = table_name.clone();
                        let q_url = queue_url.clone();

                        // Wrzucamy asynchronicznie do chmury za pomocą tokio tasków
                        tokio::spawn(async move {
                            let _ = save_to_dynamodb(&db, &t_name, &reading).await;
                            let _ = send_to_sqs(&sqs, &q_url, &reading).await;
                        });

                        println!("🚢 Statek: {} ({}) | Prędkość: {} węzłów", ship_name, mmsi, speed);
                    }
                }
            }
        }
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct WindResponse {
    current: WindCurrent,
}

#[derive(Debug, Deserialize)]
struct WindCurrent {
    wind_speed_10m: f64,
    wind_direction_10m: f64,
    temperature_2m: f64,
}

#[derive(Debug, Serialize, Clone)]
pub struct SensorReading {
    pub sensor_id: String,
    pub timestamp: String,
    pub sensor_type: String,
    pub lat: f64,
    pub lon: f64,
    pub value: f64,
    pub unit: String,
    pub extra: serde_json::Value,
}

async fn fetch_wind() -> Result<Vec<SensorReading>, Box<dyn std::error::Error>> {
    let url = "https://api.open-meteo.com/v1/forecast\
        ?latitude=54.5&longitude=18.8\
        &current=wind_speed_10m,wind_direction_10m,temperature_2m\
        &wind_speed_unit=ms";

    let resp: WindResponse = reqwest::get(url).await?.json().await?;
    let now = Utc::now().to_rfc3339();

    Ok(vec![SensorReading {
        sensor_id: "wind-gdansk-bay".to_string(),
        timestamp: now,
        sensor_type: "wind_speed".to_string(),
        lat: 54.5,
        lon: 18.8,
        value: resp.current.wind_speed_10m,
        unit: "m/s".to_string(),
        extra: serde_json::json!({
            "direction": resp.current.wind_direction_10m,
            "temperature": resp.current.temperature_2m
        }),
    }])
}

async fn fetch_air_quality() -> Result<Vec<SensorReading>, Box<dyn std::error::Error>> {
    let url = "https://api.gios.gov.pl/pjp-api/v1/rest/station/findAll?size=300";

    let client = reqwest::Client::new();
    let resp: serde_json::Value = client
        .get(url)
        .header("User-Agent", "baltic-watch/0.1")
        .send()
        .await?
        .json()
        .await?;

    let stations = resp["Lista stacji pomiarowych"]
        .as_array()
        .ok_or("brak listy stacji")?;

    let trojmiasto_ids: Vec<i64> = vec![16180, 16242, 20347];

    let trojmiasto: Vec<&serde_json::Value> = stations.iter().filter(|s| {
        let id = s["Identyfikator stacji"].as_i64().unwrap_or(0);
        trojmiasto_ids.contains(&id)
    }).collect();

    println!("  Znaleziono {} stacji w Trójmieście", trojmiasto.len());

    let mut readings = vec![];
    let now = Utc::now().to_rfc3339();

    for station in trojmiasto.iter() {
        let station_id = station["Identyfikator stacji"].as_i64().unwrap_or(0);
        let station_name = station["Nazwa stacji"].as_str().unwrap_or("unknown");
        let lat = station["WGS84 φ N"].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
        let lon = station["WGS84 λ E"].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0);

        let sensors_url = format!(
            "https://api.gios.gov.pl/pjp-api/v1/rest/station/sensors/{}",
            station_id
        );
        let sensors_resp: serde_json::Value = client
            .get(&sensors_url)
            .send().await?.json().await?;

        let sensors = match sensors_resp["Lista stanowisk pomiarowych dla podanej stacji"].as_array() {
            Some(s) => s.clone(),
            None => continue,
        };

        for sensor in sensors.iter() {
            let sensor_id = sensor["Identyfikator stanowiska"].as_i64().unwrap_or(0);
            let param = sensor["Wskaźnik"].as_str().unwrap_or("unknown");

            let data_url = format!(
                "https://api.gios.gov.pl/pjp-api/v1/rest/data/getData/{}",
                sensor_id
            );
            let data: serde_json::Value = client.get(&data_url)
                .send().await?.json().await?;

            if let Some(values) = data["Lista danych pomiarowych"].as_array() {
                if let Some(latest) = values.iter().find(|v| !v["Wartość"].is_null()) {
                    let value = latest["Wartość"].as_f64().unwrap_or(0.0);
                    let timestamp = latest["Data"].as_str().unwrap_or(&now).to_string();

                    readings.push(SensorReading {
                        sensor_id: format!("air-gios-{}-{}", station_id, param.replace(' ', "-")),
                        timestamp,
                        sensor_type: format!("air_{}", param.to_lowercase().replace(' ', "_")),
                        lat,
                        lon,
                        value,
                        unit: "µg/m³".to_string(),
                        extra: serde_json::json!({
                            "station_name": station_name,
                            "parameter": param,
                            "station_id": station_id
                        }),
                    });
                }
            }
        }
    }

    Ok(readings)
}

async fn save_to_dynamodb(db_client: &DynamoClient, table_name: &str, reading: &SensorReading) -> Result<(), aws_sdk_dynamodb::Error> {
    db_client
        .put_item()
        .table_name(table_name)
        .item("sensor_id", AttributeValue::S(reading.sensor_id.clone()))
        .item("timestamp", AttributeValue::S(reading.timestamp.clone()))
        .item("sensor_type", AttributeValue::S(reading.sensor_type.clone()))
        .item("lat", AttributeValue::N(reading.lat.to_string()))
        .item("lon", AttributeValue::N(reading.lon.to_string()))
        .item("value", AttributeValue::N(reading.value.to_string()))
        .item("unit", AttributeValue::S(reading.unit.clone()))
        .item("extra", AttributeValue::S(reading.extra.to_string()))
        .send()
        .await?;
    Ok(())
}

async fn send_to_sqs(sqs_client: &SqsClient, queue_url: &str, reading: &SensorReading) -> Result<(), aws_sdk_sqs::Error> {
    let message_body = serde_json::to_string(reading).unwrap_or_default();
    sqs_client
        .send_message()
        .queue_url(queue_url)
        .message_body(message_body)
        .send()
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() {
    println!("🌊 Baltic Ingest starting...");

    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let db_client = Arc::new(DynamoClient::new(&config));
    let sqs_client = Arc::new(SqsClient::new(&config));

    let table_name = "baltic_watch_sensors".to_string();
    let queue_url = "https://sqs.us-east-1.amazonaws.com/114538235433/baltic-watch-queue".to_string();
    
    let ais_api_key = "401d11c3cb1abcc716228dc7f016836101cd93b1".to_string();

    let db_ais = Arc::clone(&db_client);
    let sqs_ais = Arc::clone(&sqs_client);
    let t_ais = table_name.clone();
    let q_ais = queue_url.clone();

    // Odpalenie streamu AIS w tle
    tokio::spawn(async move {
        if let Err(e) = stream_ais(db_ais, sqs_ais, t_ais, q_ais, ais_api_key).await {
            eprintln!("❌ Krytyczny błąd streamu AIS: {:?}", e);
        }
    });

    loop {
        println!("\n🔄 Rozpoczynam cykliczny pobór danych (GIOŚ + Pogoda)...");
        let (wind_result, air_result) = tokio::join!(fetch_wind(), fetch_air_quality());

        let mut all_readings = vec![];
        if let Ok(w) = wind_result { all_readings.extend(w); }
        if let Ok(a) = air_result { all_readings.extend(a); }

        println!("📊 Pobrano {} odczytów ze stacji. Wysyłam do AWS...", all_readings.len());

        for reading in all_readings {
            let db = Arc::clone(&db_client);
            let sqs = Arc::clone(&sqs_client);
            let t_name = table_name.clone();
            let q_url = queue_url.clone();

            tokio::spawn(async move {
                let _ = save_to_dynamodb(&db, &t_name, &reading).await;
                let _ = send_to_sqs(&sqs, &q_url, &reading).await;
            });
        }

        println!("😴 Zasypiam na 10 minut...");
        tokio::time::sleep(std::time::Duration::from_secs(600)).await;
    }
}
