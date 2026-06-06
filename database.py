from pymongo import MongoClient
from datetime import datetime
from typing import Dict, Any, List, Optional

# Conexión NoSQL
client = MongoClient("mongodb://localhost:27017/")
db = client["acme_financial_dwh"]

assets_col = db["financial_assets"]
timeseries_col = db["time_series"]

class AssetRepository:
    @staticmethod
    def save(asset_id: str, symbol: str, asset_class: str, description: str, region: str, attributes: Dict[str, Any]) -> Dict[str, Any]:
        system_time = datetime.utcnow()
        assets_col.update_many({"assetId": asset_id, "valid_to": None}, {"$set": {"valid_to": system_time}})
        document = {
            "assetId": asset_id, "symbol": symbol, "class": asset_class,
            "description": description, "region": region, "system_date": system_time,
            "valid_to": None, "attributes": attributes
        }
        assets_col.insert_one(document)
        return document

    @staticmethod
    def findLatest(asset_id: str) -> Optional[Dict[str, Any]]:
        # Recupera la versión más reciente en base a la ordenación del catálogo (System Date activa)
        return assets_col.find_one({"assetId": asset_id, "valid_to": None}, {"_id": 0})

    @staticmethod
    def findAllPaginated(offset: int = 0, limit: int = 20) -> List[str]:
        """[REQ 1.1] Devuelve un array JSON con los IDs aplicando paginación real end-to-end[cite: 741, 802]."""
        cursor = assets_col.find(
            {"valid_to": None, "is_deleted_marker": {"$exists": False}}
        ).sort("assetId", 1).skip(offset).limit(limit) # Paginación determinista en Base de Datos [cite: 805, 806]
        
        return [doc["assetId"] for doc in cursor]


class TimeSeriesRepository:
    @staticmethod
    def save(asset_id: str, source_id: str, business_date: str, indicators: Dict[str, Any], system_date: Optional[datetime] = None) -> Dict[str, Any]:
        document = {
            "assetId": asset_id,
            "dataSourceId": source_id,
            "business_date": business_date,
            "system_date": system_date or datetime.utcnow(),
            "values": indicators
        }
        timeseries_col.insert_one(document)
        return document

    @staticmethod
    def findLatest(asset_id: str, source_id: Optional[str] = None, business_date: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Versión bi-temporal corregida. Soporta firmas simples y consultas
        acotadas de verificación de idempotencia para el pipeline de ingesta[cite: 292, 620].
        """
        query = {"assetId": asset_id}
        if source_id:
            query["dataSourceId"] = source_id
        if business_date:
            query["business_date"] = business_date
            
        cursor = timeseries_col.find(query, {"_id": 0}).sort("system_date", -1).limit(1)
        results = list(cursor)
        return results[0] if results else None

    @staticmethod
    def findConsumptionData(asset_id: str, source_id: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        query = {
            "assetId": asset_id,
            "dataSourceId": source_id,
            "business_date": {"$gte": start_date, "$lt": end_date}
        }
        cursor = timeseries_col.find(query, {"_id": 0}).sort("system_date", -1)
        
        seen_business_dates = set()
        deduplicated_records = []
        for record in cursor:
            b_date = record["business_date"]
            if b_date not in seen_business_dates:
                seen_business_dates.add(b_date)
                deduplicated_records.append(record)
                
        deduplicated_records.sort(key=lambda x: x["business_date"], reverse=True)
        return deduplicated_records

    @staticmethod
    def findAll(asset_id: Optional[str] = None, source_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Método explícito requerido por la suite de pruebas obligatoria test_dal.py[cite: 292, 301].
        """
        query = {}
        if asset_id:
            query["assetId"] = asset_id
        if source_id:
            query["dataSourceId"] = source_id
        cursor = timeseries_col.find(query, {"_id": 0}).sort("business_date", 1)
        return list(cursor)