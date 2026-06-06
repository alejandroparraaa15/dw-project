import unittest
from analytics import get_spark_session

class TestSparkAnalyticsEngine(unittest.TestCase):
    
    def test_spark_session_initialization(self):
        """Valida que el motor distribuido de Spark se levante y configure de manera correcta en el entorno local[cite: 885]."""
        spark = get_spark_session()
        self.assertIsNotNone(spark)
        self.assertEqual(spark.conf.get("spark.app.name"), "Acme_Financial_Analytics_Engine")
        # No cerramos la sesión aquí para no interrumpir flujos continuos paralelos de testing

if __name__ == "__main__":
    unittest.main()