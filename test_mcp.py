import unittest
from mcp_server import get_time_series_data

class TestMCPToolsLayer(unittest.TestCase):
    
    def test_mcp_date_format_validation(self):
        """Prueba que el servidor MCP rechaza de manera determinista formatos de fecha erróneos."""
        error_msg = get_time_series_data(
            asset_id="QDL/BITFINEX/BTCUSD",
            data_source_id="BITFINEX",
            start_business_date="2026/05/23", # Separadores inválidos
            end_business_date="2026-05-30"
        )
        self.assertIn("❌ Error de formato", error_msg)

    def test_mcp_unbounded_range_rejection(self):
        """Prueba la inmunidad del sistema ante intervalos excesivamente amplios que afecten la escala."""
        error_msg = get_time_series_data(
            asset_id="QDL/BITFINEX/BTCUSD",
            data_source_id="BITFINEX",
            start_business_date="2010-01-01",
            end_business_date="2026-01-01" # Intervalo masivo de varios años
        )
        self.assertIn("❌ Operación rechazada: El intervalo solicitado supera", error_msg)

if __name__ == "__main__":
    unittest.main()