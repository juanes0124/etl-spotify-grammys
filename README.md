# ETL Pipeline — Spotify & Grammy Awards 🎵🏆

## Descripción
Pipeline ETL completo construido con Apache Airflow que integra datos de Spotify y Grammy Awards para análisis musical.

## Tecnologías
- Python 3.11
- Apache Airflow 2.9.2
- PostgreSQL 15
- Docker & Docker Compose
- Pandas

## Setup e Instalación

### Requisitos
- Docker Desktop instalado y corriendo
- Git

### Pasos
```bash
# 1. Clonar el repositorio
git clone https://github.com/juanes0124/etl-spotify-grammys.git
cd etl-spotify-grammys

# 2. Construir y levantar los contenedores
docker compose build
docker compose up -d

# 3. Verificar que todo esté corriendo
docker compose ps
```

### Acceder a Airflow
- URL: http://localhost:8081
- Usuario: admin
- Contraseña: admin

### Ejecutar el Pipeline
1. Abrir http://localhost:8081
2. Activar el DAG 
3. Hacer clic  para ejecutar


## ETL Pipeline

### Extracción
- **Spotify:** Lectura directa del CSV con Pandas
- **Grammy:** Consulta SQL a PostgreSQL

### Transformación
- Eliminación de 450 duplicados en Spotify
- Conversión de `duration_ms` a `duration_min`
- Relleno de valores nulos en columna `artist` de Grammy
- Eliminación de columnas innecesarias (`img`, `workers`)
- Merge por nombre de artista (left join)

### Carga
- **Data Warehouse:** PostgreSQL tabla `music_fact`
- **Google Drive:** CSV exportado en `/data/merged_dataset.csv`

## DAG de Airflow