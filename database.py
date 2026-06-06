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
    def findConsumptionData(asset_id: str, source_id: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        [REQ 3] Busca registros en la partición en el rango [start_date, end_date)[cite: 766].
        Maneja internamente la deduplicación bi-temporal (deja solo el system_date más reciente).
        Devuelve el resultado ordenado cronológicamente de forma descendente.
        """
        # 1. Consultar el rango indexado de la partición (Asset + Source)
        query = {
            "assetId": asset_id,
            "dataSourceId": source_id,
            "business_date": {"$gte": start_date, "$lt": end_date} # Rango de negocio excluyente [cite: 766]
        }
        cursor = timeseries_col.find(query, {"_id": 0}).sort("system_date", -1) # Trae primero lo último auditado [cite: 511]
        
        # 2. Algoritmo bi-temporal en memoria para conservar solo la versión más reciente por Business Date [cite: 767, 820]
        seen_business_dates = set()
        deduplicated_records = []
        
        for record in cursor:
            b_date = record["business_date"]
            if b_date not in seen_business_dates:
                seen_business_dates.add(b_date)
                deduplicated_records.append(record)
                
        # 3. Re-ordenar cronológicamente de forma descendente por business_date (exigencia estricta Q5) 
        deduplicated_records.sort(key=lambda x: x["business_date"], reverse=True)
        return deduplicated_records

def seed_database():
    """Inicialización con los datos exactos del PDF de Consumo[cite: 753, 776]."""
    assets_col.delete_many({})
    timeseries_col.delete_many({})
    
    # Activos de prueba con los identificadores jerárquicos del PDF 
    AssetRepository.save("QDL/BITFINEX/BTCUSD", "BTCUSD", "crypto", "Bitcoin / USD", "Global", {"status": "active"})
    AssetRepository.save("QDL/BITFINEX/SOLUSD", "SOLUSD", "crypto", "Solana / USD", "Global", {"status": "active"})
    
    # Insertar registros para probar la unificación bi-temporal (Múltiples versiones para una fecha de mercado) 
    TimeSeriesRepository.save(
        asset_id="QDL/BITFINEX/BTCUSD", source_id="BITFINEX", business_date="2018-01-05",
        indicators={"attr_id1": 11000.0, "attr_id2": 55000.0}, system_date=datetime(2023, 1, 1) # Antigua [cite: 753]
    )
    TimeSeriesRepository.save(
        asset_id="QDL/BITFINEX/BTCUSD", source_id="BITFINEX", business_date="2018-01-05",
        indicators={"attr_id1": 11200.0, "attr_id2": 55200.0}, system_date=datetime(2025, 4, 4) # Corrección más reciente [cite: 753, 767]
    )
    TimeSeriesRepository.save(
        asset_id="QDL/BITFINEX/BTCUSD", source_id="BITFINEX", business_date="2018-01-06",
        indicators={"attr_id1": 11500.0, "attr_id3": 99.0} # Otra fecha de negocio [cite: 778]
    )
    print("✅ Datos de consumo y control bi-temporal cargados.")

if __name__ == "__main__":
    seed_database()