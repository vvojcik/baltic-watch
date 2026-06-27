use reqwest;
use serde::{Deserialize, Serialize};
use chrono::Utc;

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
struct SensorReading {
    sensor_id: String,
    timestamp: String,
    sensor_type: String,
    lat: f64,
    lon: f64,
    value: f64,
    unit: String,
    extra: serde_json::Value,
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

    // Hardcoded IDs stacji Trójmiasto - sprawdzone wcześniej
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

#[tokio::main]
async fn main() {
    println!("🌊 Baltic Ingest starting...");

    let (wind_result, air_result) = tokio::join!(
        fetch_wind(),
        fetch_air_quality()
    );

    let mut all_readings: Vec<SensorReading> = vec![];

    match wind_result {
        Ok(readings) => {
            println!("\n💨 WIATR:");
            for r in &readings {
                println!("  {:.1} {} | kierunek: {}° | temp: {}°C",
                    r.value, r.unit,
                    r.extra["direction"],
                    r.extra["temperature"]);
            }
            all_readings.extend(readings);
        }
        Err(e) => eprintln!("❌ Wind error: {}", e),
    }

    match air_result {
        Ok(readings) => {
            println!("\n🌫️ JAKOŚĆ POWIETRZA ({} odczytów):", readings.len());
            for r in &readings {
                println!("  {} | {} | {:.2} {}",
                    r.extra["station_name"],
                    r.extra["parameter"],
                    r.value,
                    r.unit);
            }
            all_readings.extend(readings);
        }
        Err(e) => eprintln!("❌ Air quality error: {}", e),
    }

    println!("\n✅ Łącznie {} odczytów", all_readings.len());
}
