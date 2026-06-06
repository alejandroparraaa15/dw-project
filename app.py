from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from database import AssetRepository, TimeSeriesRepository, timeseries_col
from pymongo import MongoClient
import analytics
from typing import List

app = FastAPI(
    title="Acme Ltd - Data Warehouse Consumption API",
    version="1.0",
    description="API optimizada para lectura y descubrimiento de datos del laboratorio"
)

# Conexión nativa para leer los resultados derivados calculados por Spark
db_client = MongoClient("mongodb://localhost:27017/")
acme_db = db_client["acme_financial_dwh"]

# [Q1 / 1.1] GET /assets - Lista paginada de IDs de activos en formato JSON array
@app.get("/api/v1/assets", response_model=List[str])
def get_assets_paginated(
    offset: int = Query(default=0, ge=0, description="Posición inicial en la colección"),
    limit: int = Query(default=20, ge=1, le=100, description="Máximo número de elementos devueltos")
):
    """Devuelve un array plano con los identificadores únicos disponibles."""
    return AssetRepository.findAllPaginated(offset=offset, limit=limit)

# [Q2 / 1.2] GET /assets/{assetId} - Representación detallada del activo
@app.get("/api/v1/assets/{asset_id:path}")
def get_asset_detail(asset_id: str):
    """Retorna la metadata vigente (latest) para el identificador proveído."""
    asset = AssetRepository.findLatest(asset_id)
    if not asset:
        raise HTTPException(status_code=404, detail="El activo solicitado no existe.")
    return asset

# [Q3 y Q4 / 2] GET /data-sources - Listado de proveedores disponibles
@app.get("/api/v1/data-sources")
def get_data_sources():
    distinct_sources = timeseries_col.distinct("dataSourceId")
    return [{"dataSourceId": src} for src in distinct_sources]

@app.get("/api/v1/data-sources/{source_id}")
def get_data_source_detail(source_id: str):
    return {"dataSourceId": source_id, "verified": True}

# [Q5 / 3] GET /data - Extracción unificada y acotada de series temporales
@app.get("/api/v1/data")
def get_time_series_data(
    assetId: str = Query(..., description="ID del activo financiero"),
    dataSourceId: str = Query(..., description="ID del proveedor de datos"),
    startBusinessDate: str = Query(..., description="Fecha de inicio (YYYY-MM-DD)"),
    endBusinessDate: str = Query(..., description="Fecha de fin excluyente (YYYY-MM-DD)"),
    includeAttributes: bool = Query(default=False, description="Flag para incluir el catálogo de indicadores")
):
    """Retorna los puntos de la serie temporal en formato canónico de Acme Ltd."""
    records = TimeSeriesRepository.findConsumptionData(
        asset_id=assetId, source_id=dataSourceId, start_date=startBusinessDate, end_date=endBusinessDate
    )
    
    if not records:
        raise HTTPException(status_code=404, detail="No hay datos para los filtros especificados.")
    
    formatted_records = []
    collected_attributes = set()
    
    for r in records:
        if includeAttributes:
            collected_attributes.update(r["values"].keys())
            
        formatted_records.append({
            "businessDate": r["business_date"],
            "values": [r["values"]]
        })
        
    response = {
        "data": {
            "assetId": assetId,
            "datasourceId": dataSourceId,
            "records": formatted_records
        }
    }
    
    if includeAttributes:
        response["attributes"] = list(collected_attributes)
        
    return response

# =================================================================
# ENDPOINTS DE ANALÍTICA (APACHE SPARK INTEGRADO)
# =================================================================

@app.post("/api/v1/analytics/trigger-aggregation")
def trigger_spark_aggregation(background_tasks: BackgroundTasks):
    """[USE CASE A] Dispara el Job analítico masivo de Apache Spark para calcular totales históricos por año[cite: 890, 891]."""
    background_tasks.add_task(analytics.run_spark_aggregation_job)
    return {"status": "Job de agregación masiva enviado a la cola distribuida de Spark."}

@app.post("/api/v1/analytics/trigger-prediction/{asset_id:path}")
def trigger_spark_prediction(asset_id: str, background_tasks: BackgroundTasks):
    """[USE CASE B] Entrena un modelo predictivo de regresión lineal en Spark para estimar precios[cite: 910, 911]."""
    background_tasks.add_task(analytics.run_spark_machine_learning_job, asset_id)
    return {"status": f"Entrenamiento predictivo de ML agendado para el activo: {asset_id}."}

@app.get("/api/v1/analytics/results")
def get_analytical_summaries():
    """Recupera los totales descriptivos anuales procesados y guardados por Apache Spark[cite: 874, 888]."""
    cursor = acme_db["analytics_totals"].find({}, {"_id": 0})
    return list(cursor)