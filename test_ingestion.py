import unittest
from ingestion import DataIngestionPipeline

class TestIngestionPipeline(unittest.TestCase):
    
    def test_transformation_layer_mapping(self):
        pipeline = DataIngestionPipeline()
        
        # Entrada simulada nativa de un proveedor
        mock_raw = {"ticker": "GM", "mkt_date": "2026-05-23", "op_p": "34.15", "cl_p": "34.26", "vol": "107842000"}
        
        # Ejecutar la transformación aislada
        canonical = pipeline._transform_to_canonical(mock_raw, "NasdaqDataLink")
        
        # Verificaciones del modelo canónico interno
        self.assertEqual(canonical["symbol"], "GM")
        self.assertEqual(canonical["business_date"], "2026-05-23")
        self.assertEqual(canonical["indicators"]["open"], 34.15)
        self.assertEqual(type(canonical["indicators"]["volume"]), int)

if __name__ == "__main__":
    unittest.main()