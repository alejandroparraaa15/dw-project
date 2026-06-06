import time
from datetime import datetime
from typing import List, Dict, Any
from database import AssetRepository, TimeSeriesRepository

class DataIngestionPipeline:
    def __init__(self):
        # Almacenamiento global para la observabilidad del sistema
        self.metrics = {
            "last_run": None,
            "fetched_records": 0,
            "transformed_records": 0,
            "stored_records": 0,
            "skipped_records": 0,
            "failures": 0
        }

    # ==========================================
    # 1. ETAPA: EXTRACTION (Extracción de Datos)
    # ==========================================
    def _extract_mock_provider_data(self, symbol: str, provider: str, page: int) -> Dict[str, Any]:
        """
        Simula las llamadas a APIs externas manejando paginación interactiva.
        Emula respuestas nativas de Nasdaq Data Link o Bitfinex.
        """
        # Simulación de fin de páginas para el bucle de cursor
        if page > 2:
            return {"data": [], "next_cursor": None}

        if provider == "NasdaqDataLink":
            return {
                "data": [
                    {"ticker": symbol, "mkt_date": "2026-05-22", "op_p": 150.0, "cl_p": 152.3, "vol": 3200000},
                    {"ticker": symbol, "mkt_date": "2026-05-23", "op_p": 152.5, "cl_p": 151.1, "vol": 4100000}
                ],
                "next_cursor": f"cursor_page_{page + 1}"
            }
        elif provider == "Bitfinex":
            return {
                "data": [
                    {"pair": symbol, "timestamp": "2026-05-23", "mid_price": 94200.0, "volume_crypto": 850}
                ],
                "next_cursor": None # Bitfinex devuelve todo en una página en este ejemplo
            }
        return {"data": [], "next_cursor": None}

    # ==========================================
    # 2. ETAPA: TRANSFORMATION (Normalización)
    # ==========================================
    def _transform_to_canonical(self, raw_record: Dict[str, Any], provider: str) -> Dict[str, Any]:
        """
        Normaliza las variables heterogéneas de los feeds en el modelo unificado de Acme Ltd.
        Valida tipos numéricos y unifica fechas de mercado (Business Date).
        """
        canonical = {
            "asset_id": "",
            "symbol": "",
            "business_date": "",
            "indicators": {}
        }

        if provider == "NasdaqDataLink":
            canonical["symbol"] = raw_record["ticker"]
            canonical["asset_id"] = "1" if raw_record["ticker"] == "GM" else "2"
            canonical["business_date"] = raw_record["mkt_date"]
            # Estructura flexible de indicadores heterogéneos
            canonical["indicators"] = {
                "open": float(raw_record["op_p"]),
                "close": float(raw_record["cl_p"]),
                "volume": int(raw_record["vol"])
            }
        elif provider == "Bitfinex":
            canonical["symbol"] = raw_record["pair"]
            canonical["asset_id"] = "4" # ID asignado para nuevos activos cripto
            canonical["business_date"] = raw_record["timestamp"]
            canonical["indicators"] = {
                "mid": float(raw_record["mid_price"]),
                "volume": float(raw_record["volume_crypto"])
            }
        
        return canonical

    # ==========================================
    # 3. ETAPA: LOADING (Persistencia Idempotente)
    # ==========================================
    def run_ingestion(self, assets_to_fetch: List[str], provider: str):
        """
        Ejecuta el flujo completo de ingesta por lotes controlando la paginación,
        la creación de metadatos faltantes de manera segura e inmunidad a duplicados.
        """
        print(f"🚀 Iniciando proceso ETL desde la fuente: {provider}...")
        self.metrics["last_run"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        
        for symbol in assets_to_fetch:
            current_page = 1
            cursor_token = "start"

            while cursor_token is not None:
                try:
                    # FASE 1: Extraer
                    raw_response = self._extract_mock_provider_data(symbol, provider, current_page)
                    records = raw_response.get("data", [])
                    cursor_token = raw_response.get("next_cursor")
                    
                    self.metrics["fetched_records"] += len(records)

                    for raw in records:
                        # FASE 2: Transformar
                        canonical_data = self._transform_to_canonical(raw, provider)
                        self.metrics["transformed_records"] += 1

                        # Crear metadatos del activo de forma segura en caliente si no existe (Create-if-missing)
                        existing_asset = AssetRepository.findLatest(canonical_data["asset_id"])
                        if not existing_asset:
                            AssetRepository.save(
                                asset_id=canonical_data["asset_id"],
                                symbol=canonical_data["symbol"],
                                asset_class="crypto" if provider == "Bitfinex" else "stock",
                                description=f"Asset auto-ingestado de {provider}",
                                region="Global",
                                attributes={"derived_fields": list(canonical_data["indicators"].keys())}
                            )

                        # Verificación de Idempotencia: Si el punto exacto ya se encuentra en el DWH, se omite
                        is_duplicate = TimeSeriesRepository.findLatest(
                            canonical_data["asset_id"], provider, canonical_data["business_date"]
                        )
                        
                        if is_duplicate and is_duplicate["values"] == canonical_data["indicators"]:
                            self.metrics["skipped_records"] += 1
                            continue

                        # FASE 3: Cargar
                        TimeSeriesRepository.save(
                            asset_id=canonical_data["asset_id"],
                            source_id=provider,
                            business_date=canonical_data["business_date"],
                            indicators=canonical_data["indicators"]
                        )
                        self.metrics["stored_records"] += 1

                    current_page += 1
                    
                except Exception as e:
                    print(f"❌ Error crítico procesando lote: {str(e)}")
                    self.metrics["failures"] += 1
                    cursor_token = None # Forzar detención del hilo por fallo
                    
        print("✅ Ingestión finalizada. Estadísticas actualizadas del sistema.")
        return self.metrics

# Instancia única reutilizable para conservar el estado de la telemetría
ingestion_pipeline = DataIngestionPipeline()

if __name__ == "__main__":
    # Prueba de ejecución en consola del flujo por lotes
    pipeline = DataIngestionPipeline()
    summary = pipeline.run_ingestion(["GM", "AAPL"], "NasdaqDataLink")
    print("Resumen de Métricas de Ingesta:", summary)