import json
import requests
from datetime import datetime
from mcp.server.fastmcp import FastMCP

# Inicializar la plataforma del servidor MCP en cumplimiento de la arquitectura de Acme Ltd
mcp = FastMCP("Acme_Enterprise_Data_Warehouse_MCP")

# Endpoint base de nuestra capa de consumo (API REST v1)
API_URL = "http://localhost:8000/api/v1"

# =================================================================
# REQUISITO: EXPOSICIÓN DE CAPACIDADES COMO MCP TOOLS
# =================================================================

@mcp.tool()
def list_assets(offset: int = 0, limit: int = 20) -> str:
    """
    [SUGGESTED TOOL 1] Obtiene una página compacta de identificadores únicos (partition keys)
    de los activos financieros vigentes y disponibles en el catálogo del almacén de datos.
    Soporta paginación obligatoria end-to-end mediante offset y limit.
    """
    # Validación temprana de argumentos según requerimiento operativo
    if limit > 100:
        limit = 20  # Forzar cota máxima para proteger la estabilidad del servidor
        
    try:
        response = requests.get(f"{API_URL}/assets", params={"offset": offset, "limit": limit})
        if response.status_code != 200:
            return f"❌ Error del almacén de datos: {response.json().get('detail', 'Error desconocido')}"
            
        data = response.json()
        # Estructura devuelta predecible enriquecida con metadatos de paginación para el agente
        result_payload = {
            "metadata": {"offset": offset, "limit": limit, "records_returned": len(data)},
            "assets": data
        }
        return json.dumps(result_payload, indent=2)
    except Exception as e:
        return f"❌ Excepción en la capa de red al conectar con el DWH: {str(e)}"


@mcp.tool()
def get_asset_details(asset_id: str) -> str:
    """
    [SUGGESTED TOOL 2] Retorna la representación detallada de metadatos, propiedades heterogéneas
    y el estado de vigencia actual (latest temporal view) de un activo conociendo su identificador único.
    """
    if not asset_id or not asset_id.strip():
        return "❌ Error: El parámetro 'asset_id' no puede estar vacío."
        
    try:
        # Codificación segura del path de identificación financiera (ej. QDL/BITFINEX/BTCUSD)
        response = requests.get(f"{API_URL}/assets/{asset_id}")
        if response.status_code == 404:
            return f"❌ Identificador inválido: El activo '{asset_id}' no existe en el sistema."
            
        return json.dumps(response.json(), indent=2)
    except Exception as e:
        return f"❌ Excepción del servidor MCP: {str(e)}"


@mcp.tool()
def list_data_sources() -> str:
    """
    [SUGGESTED TOOL 3] Lista de forma compacta todas las fuentes de datos (Data Sources / Vendors)
    soportadas en el almacén que capturan proveniencia e histórico financiero reproducible.
    """
    try:
        response = requests.get(f"{API_URL}/data-sources")
        return json.dumps(response.json(), indent=2)
    except Exception as e:
        return f"❌ Excepción al recuperar fuentes: {str(e)}"


@mcp.tool()
def get_data_source_details(data_source_id: str) -> str:
    """
    [SUGGESTED TOOL 4] Devuelve información operativa sobre un proveedor de datos del sistema,
    incluyendo sus atributos soportados y su estado de verificación de trazabilidad.
    """
    try:
        response = requests.get(f"{API_URL}/data-sources/{data_source_id}")
        if response.status_code == 404:
            return f"❌ Proveedor '{data_source_id}' no encontrado."
        return json.dumps(response.json(), indent=2)
    except Exception as e:
        return f"❌ Excepción: {str(e)}"


@mcp.tool()
def get_time_series_data(
    asset_id: str, 
    data_source_id: str, 
    start_business_date: str, 
    end_business_date: str, 
    include_attributes: bool = False
) -> str:
    """
    [SUGGESTED TOOL 5] Extrae los puntos de la serie temporal en el intervalo bi-temporal [startBusinessDate, endBusinessDate).
    Garantiza de forma estricta:
    1. Ordenación cronológica descendente (Newest First).
    2. Deduplicación temporal automática (conservando solo la auditoría más reciente por fecha de mercado).
    3. Validación y rechazo explícito de intervalos excesivamente amplios para preservar la estabilidad.
    
    Formatos requeridos para fechas: 'YYYY-MM-DD'.
    """
    # Control de seguridad contra lecturas masivas no acotadas (Requisito de diseño del PDF)
    try:
        d1 = datetime.strptime(start_business_date, "%Y-%m-%d")
        d2 = datetime.strptime(end_business_date, "%Y-%m-%d")
        delta_days = (d2 - d1).days
        
        if delta_days <= 0:
            return "❌ Error de rango: 'end_business_date' debe ser estrictamente posterior a 'start_business_date'."
        if delta_days > 365:
            return "❌ Operación rechazada: El intervalo solicitado supera el límite máximo permitido de 1 año (365 días) para resguardar el canal."
    except ValueError:
        return "❌ Error de formato: Las fechas de negocio provistas deben respetar rigurosamente la máscara 'YYYY-MM-DD'."

    # Construcción limpia de parámetros tipados e independientes hacia el contrato de consumo
    params = {
        "assetId": asset_id,
        "dataSourceId": data_source_id,
        "startBusinessDate": start_business_date,
        "endBusinessDate": end_business_date,
        "includeAttributes": str(include_attributes).lower()
    }
    
    try:
        response = requests.get(f"{API_URL}/data", params=params)
        if response.status_code == 404:
            return "❌ No se localizaron registros para la partición y coordenadas temporales indicadas."
            
        return json.dumps(response.json(), indent=2)
    except Exception as e:
        return f"❌ Fallo crítico en el puente de comunicación MCP: {str(e)}"

# =================================================================
# CAPACIDADES ANALÍTICAS INTEGRADAS ADICIONALES (Fase de Spark previa)
# =================================================================

@mcp.tool()
def fetch_spark_analytical_summaries() -> str:
    """
    Recupera los promedios numéricos anuales agregados y calculados asíncronamente 
    por el motor avanzado distribuido Apache Spark desde el almacén NoSQL.
    """
    try:
        response = requests.get(f"{API_URL}/analytics/results")
        return json.dumps(response.json(), indent=2)
    except Exception as e:
        return f"❌ Error leyendo salidas de Spark: {str(e)}"


if __name__ == "__main__":
    # Iniciar la comunicación interactiva a través del transporte de entrada/salida estándar (stdio RPC)
    mcp.run(transport='stdio')