# Multi-City Weather ETL Pipeline with Airflow and Tableau Analytics

A comprehensive ETL pipeline built with Apache Airflow that extracts weather data from Open-Meteo API for 6 global cities and loads it into PostgreSQL with advanced analytics dashboards.

## Features

- **Multi-City Weather ETL**: Hourly weather data for Boston, London, Mumbai, Tokyo, Sydney, Sao Paulo
- **Advanced Features**: Comfort index, feels-like temperature, weather categorizations
- **PostgreSQL Storage**: Optimized schema with 17 weather metrics per record
- **Tableau Dashboards**: Executive weather overview and city performance analytics
- **Docker Support**: Complete containerized setup with Astronomer Runtime

## Quick Start

### Prerequisites
- Docker & Docker Compose
- [Astronomer CLI](https://docs.astronomer.io/astro/cli/get-started)
- Tableau Desktop 2025.2.4+ (optional)

### Installation
```bash
# Clone and start
git clone <repo-url>
cd multi-city-weather-etl
astro dev start

# Access Airflow UI
open http://localhost:8080
# Login: admin/admin
```

## Data Schema

```sql
weather_data_multi_city (
    city_name VARCHAR(50),
    latitude FLOAT, longitude FLOAT, country VARCHAR(50),
    timestamp TIMESTAMP,
    temperature_2m FLOAT, relative_humidity_2m FLOAT,
    precipitation FLOAT, wind_speed_10m FLOAT,
    feels_like_temp FLOAT,           -- Calculated heat index/wind chill
    precipitation_category VARCHAR(20), -- none/light/moderate/heavy  
    wind_category VARCHAR(20),       -- calm/breeze/strong/gale
    comfort_index INT,               -- Business comfort score (0-100)
    PRIMARY KEY (city_name, timestamp)
);
```

## Project Structure

```
├── dags/                          # Airflow DAGs
│   ├── etl_multi_city_weather.py  # Main weather ETL pipeline
├── include/                       # Additional files for DAGs
├── plugins/                       # Custom Airflow plugins
├── tests/                         # Unit tests
│   └── dags/
│       └── test_dag_example.py   # Test files
├── airflow_settings.yaml          # Local development settings
├── docker-compose.yml            # Docker Compose configuration
├── Dockerfile                     # Custom Docker image
├── packages.txt                   # OS-level packages
├── requirements.txt              # Python dependencies
└── README.md                     # This file
```

## Configuration

**Airflow Connections:**
- `postgres_default`: PostgreSQL (localhost:5432, postgres/postgres)


## Tableau Analytics

The included dashboard provides:
- **Geographic Comfort Map**: City-level comfort visualization
- **Temperature Trends**: Multi-city comparisons with feels-like temps
- **Weather Patterns**: Precipitation vs wind categorization
- **City Rankings**: Comfort-based performance metrics
- **Executive KPIs**: Real-time weather intelligence

**Key Business Insights:**
- Which cities are most comfortable?
- Weather stability rankings
- Temperature extremes and risk assessment
- Hourly comfort patterns across time zones


## Tech Stack

- **Orchestration**: Apache Airflow (Astronomer Runtime)
- **Database**: PostgreSQL with optimized indexing
- **API**: Open-Meteo Weather API
- **Visualization**: Tableau Desktop/Server
- **Containerization**: Docker & Docker Compose
