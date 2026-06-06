import unittest
from database import AssetRepository, TimeSeriesRepository, assets_col, timeseries_col

class TestDataAccessLayer(unittest.TestCase):
    
    def setUp(self):
        """Prepara el entorno limpio antes de cada test."""
        assets_col.delete_many({"assetId": "TEST_A"})
        timeseries_col.delete_many({"assetId": "TEST_A"})

    def test_asset_save_and_find_latest(self):
        # 1. Probar el flujo SAVE
        AssetRepository.save("TEST_A", "TICKER", "stock", "Test Asset", "Global", {"test_prop": 100})
        
        # 2. Probar el flujo FIND LATEST
        latest = AssetRepository.findLatest("TEST_A")
        self.assertIsNotNone(latest)
        self.assertEqual(latest["symbol"], "TICKER")
        self.assertEqual(latest["attributes"]["test_prop"], 100)

    def test_timeseries_find_all_partition(self):
        # 1. Insertar múltiples puntos cronológicos
        TimeSeriesRepository.save("TEST_A", "TEST_SRC", "2026-01-01", {"close": 10.0})
        TimeSeriesRepository.save("TEST_A", "TEST_SRC", "2026-01-02", {"close": 11.5})
        
        # 2. Probar el flujo FIND ALL
        all_records = TimeSeriesRepository.findAll("TEST_A", "TEST_SRC")
        self.assertEqual(len(all_records), 2)
        self.assertEqual(all_records[0]["business_date"], "2026-01-01")

if __name__ == "__main__":
    unittest.main()