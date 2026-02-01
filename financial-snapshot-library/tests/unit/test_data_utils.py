"""
Unit tests for data utilities module with pandas.
"""

import unittest
import pandas as pd

from financial_snapshot.utils.data_utils import Deduplicator, DataValidator


class TestDeduplicator(unittest.TestCase):
    """Test Deduplicator class with pandas."""
    
    def test_deduplicate_no_duplicates(self):
        """Test deduplication with no duplicates."""
        dedup = Deduplicator(key_fields=["position_id", "account_id"])
        
        records = [
            {"value": {"position_id": "p1", "account_id": "a1"}},
            {"value": {"position_id": "p2", "account_id": "a2"}},
            {"value": {"position_id": "p3", "account_id": "a3"}},
        ]
        
        result = dedup.deduplicate(records)
        
        self.assertEqual(len(result), 3)
    
    def test_deduplicate_with_duplicates_keep_last(self):
        """Test deduplication keeping last occurrence."""
        dedup = Deduplicator(key_fields=["position_id", "account_id"])
        
        records = [
            {"value": {"position_id": "p1", "account_id": "a1", "quantity": 100}},
            {"value": {"position_id": "p2", "account_id": "a2", "quantity": 200}},
            {"value": {"position_id": "p1", "account_id": "a1", "quantity": 150}},
        ]
        
        result = dedup.deduplicate(records, keep="last")
        
        self.assertEqual(len(result), 2)
        # Verify the last occurrence is kept
        p1_records = [r for r in result if r.get("value", {}).get("position_id") == "p1"]
        self.assertEqual(len(p1_records), 1)
        self.assertEqual(p1_records[0]["value"]["quantity"], 150)
    
    def test_deduplicate_dataframe(self):
        """Test DataFrame deduplication."""
        dedup = Deduplicator(key_fields=["position_id", "account_id"])
        
        df = pd.DataFrame([
            {"position_id": "p1", "account_id": "a1", "quantity": 100},
            {"position_id": "p2", "account_id": "a2", "quantity": 200},
            {"position_id": "p1", "account_id": "a1", "quantity": 150},
        ])
        
        result = dedup.deduplicate_dataframe(df, keep="last")
        
        self.assertEqual(len(result), 2)
    
    def test_deduplicate_empty_list(self):
        """Test deduplication with empty list."""
        dedup = Deduplicator(key_fields=["position_id"])
        
        result = dedup.deduplicate([])
        
        self.assertEqual(len(result), 0)


class TestDataValidator(unittest.TestCase):
    """Test DataValidator class."""
    
    def test_validate_record_valid(self):
        """Test validation with valid record."""
        record = {
            "value": {
                "position_id": "p1",
                "account_id": "a1",
                "symbol": "AAPL",
                "quantity": 100,
            }
        }
        
        is_valid, error = DataValidator.validate_record(record)
        
        self.assertTrue(is_valid)
        self.assertEqual(error, "")
    
    def test_validate_record_missing_position_id(self):
        """Test validation with missing position_id."""
        record = {
            "value": {
                "account_id": "a1",
                "symbol": "AAPL",
                "quantity": 100,
            }
        }
        
        is_valid, error = DataValidator.validate_record(record)
        
        self.assertFalse(is_valid)
        self.assertIn("position_id", error)
    
    def test_validate_record_zero_quantity(self):
        """Test validation with zero quantity."""
        record = {
            "value": {
                "position_id": "p1",
                "account_id": "a1",
                "symbol": "AAPL",
                "quantity": 0,
            }
        }
        
        is_valid, error = DataValidator.validate_record(record)
        
        self.assertFalse(is_valid)
        self.assertIn("zero", error.lower())
    
    def test_validate_batch_all_valid(self):
        """Test batch validation with all valid records."""
        records = [
            {"value": {"position_id": "p1", "account_id": "a1", "symbol": "AAPL", "quantity": 100}},
            {"value": {"position_id": "p2", "account_id": "a2", "symbol": "GOOGL", "quantity": 50}},
        ]
        
        valid, invalid = DataValidator.validate_batch(records)
        
        self.assertEqual(len(valid), 2)
        self.assertEqual(len(invalid), 0)
    
    def test_validate_batch_mixed(self):
        """Test batch validation with mixed valid and invalid records."""
        records = [
            {"value": {"position_id": "p1", "account_id": "a1", "symbol": "AAPL", "quantity": 100}},
            {"value": {"position_id": "p2", "symbol": "GOOGL", "quantity": 50}},  # Missing account_id
            {"value": {"position_id": "p3", "account_id": "a3", "symbol": "MSFT", "quantity": 0}},  # Zero quantity
        ]
        
        valid, invalid = DataValidator.validate_batch(records)
        
        self.assertEqual(len(valid), 1)
        self.assertEqual(len(invalid), 2)
        self.assertEqual(valid[0]["value"]["position_id"], "p1")
    
    def test_validate_dataframe(self):
        """Test DataFrame validation."""
        df = pd.DataFrame([
            {"position_id": "p1", "account_id": "a1", "symbol": "AAPL", "quantity": 100},
            {"position_id": "p2", "account_id": "a2", "symbol": "GOOGL", "quantity": 50},
            {"position_id": "p3", "account_id": "", "symbol": "MSFT", "quantity": 75},  # Empty account_id
            {"position_id": "p4", "account_id": "a4", "symbol": "TSLA", "quantity": 0},  # Zero quantity
        ])
        
        valid_df, invalid_df = DataValidator.validate_dataframe(df)
        
        self.assertEqual(len(valid_df), 2)
        self.assertEqual(len(invalid_df), 2)


if __name__ == "__main__":
    unittest.main()
