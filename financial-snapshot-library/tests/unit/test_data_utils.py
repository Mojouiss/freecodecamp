"""
Unit tests for data utilities module.
"""

import unittest

from financial_snapshot.utils.data_utils import Deduplicator, DataValidator


class TestDeduplicator(unittest.TestCase):
    """Test Deduplicator class."""
    
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
        p1_record = [r for r in result if r["value"]["position_id"] == "p1"][0]
        self.assertEqual(p1_record["value"]["quantity"], 150)
    
    def test_deduplicate_with_duplicates_keep_first(self):
        """Test deduplication keeping first occurrence."""
        dedup = Deduplicator(key_fields=["position_id", "account_id"])
        
        records = [
            {"value": {"position_id": "p1", "account_id": "a1", "quantity": 100}},
            {"value": {"position_id": "p2", "account_id": "a2", "quantity": 200}},
            {"value": {"position_id": "p1", "account_id": "a1", "quantity": 150}},
        ]
        
        result = dedup.deduplicate(records, keep="first")
        
        self.assertEqual(len(result), 2)
        # Verify the first occurrence is kept
        p1_record = [r for r in result if r["value"]["position_id"] == "p1"][0]
        self.assertEqual(p1_record["value"]["quantity"], 100)
    
    def test_deduplicate_empty_list(self):
        """Test deduplication with empty list."""
        dedup = Deduplicator(key_fields=["position_id"])
        
        result = dedup.deduplicate([])
        
        self.assertEqual(len(result), 0)
    
    def test_get_duplicates_none_found(self):
        """Test finding duplicates when none exist."""
        dedup = Deduplicator(key_fields=["position_id"])
        
        records = [
            {"value": {"position_id": "p1"}},
            {"value": {"position_id": "p2"}},
        ]
        
        result = dedup.get_duplicates(records)
        
        self.assertEqual(len(result), 0)
    
    def test_get_duplicates_found(self):
        """Test finding duplicates when they exist."""
        dedup = Deduplicator(key_fields=["position_id"])
        
        records = [
            {"value": {"position_id": "p1"}},
            {"value": {"position_id": "p2"}},
            {"value": {"position_id": "p1"}},
            {"value": {"position_id": "p1"}},
        ]
        
        result = dedup.get_duplicates(records)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(len(result[("p1",)]), 3)


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
    
    def test_validate_record_missing_account_id(self):
        """Test validation with missing account_id."""
        record = {
            "value": {
                "position_id": "p1",
                "symbol": "AAPL",
                "quantity": 100,
            }
        }
        
        is_valid, error = DataValidator.validate_record(record)
        
        self.assertFalse(is_valid)
        self.assertIn("account_id", error)
    
    def test_validate_record_missing_symbol(self):
        """Test validation with missing symbol."""
        record = {
            "value": {
                "position_id": "p1",
                "account_id": "a1",
                "quantity": 100,
            }
        }
        
        is_valid, error = DataValidator.validate_record(record)
        
        self.assertFalse(is_valid)
        self.assertIn("symbol", error)
    
    def test_validate_record_missing_quantity(self):
        """Test validation with missing quantity."""
        record = {
            "value": {
                "position_id": "p1",
                "account_id": "a1",
                "symbol": "AAPL",
            }
        }
        
        is_valid, error = DataValidator.validate_record(record)
        
        self.assertFalse(is_valid)
        self.assertIn("quantity", error)
    
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
    
    def test_validate_record_invalid_quantity_type(self):
        """Test validation with invalid quantity type."""
        record = {
            "value": {
                "position_id": "p1",
                "account_id": "a1",
                "symbol": "AAPL",
                "quantity": "not_a_number",
            }
        }
        
        is_valid, error = DataValidator.validate_record(record)
        
        self.assertFalse(is_valid)
        self.assertIn("quantity", error.lower())
    
    def test_validate_record_empty_position_id(self):
        """Test validation with empty position_id."""
        record = {
            "value": {
                "position_id": "",
                "account_id": "a1",
                "symbol": "AAPL",
                "quantity": 100,
            }
        }
        
        is_valid, error = DataValidator.validate_record(record)
        
        self.assertFalse(is_valid)
        self.assertIn("position_id", error)
    
    def test_validate_record_empty_account_id(self):
        """Test validation with empty account_id."""
        record = {
            "value": {
                "position_id": "p1",
                "account_id": "   ",
                "symbol": "AAPL",
                "quantity": 100,
            }
        }
        
        is_valid, error = DataValidator.validate_record(record)
        
        self.assertFalse(is_valid)
        self.assertIn("account_id", error)
    
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
    
    def test_validate_batch_all_invalid(self):
        """Test batch validation with all invalid records."""
        records = [
            {"value": {"account_id": "a1"}},  # Missing fields
            {"value": {"position_id": "p2"}},  # Missing fields
        ]
        
        valid, invalid = DataValidator.validate_batch(records)
        
        self.assertEqual(len(valid), 0)
        self.assertEqual(len(invalid), 2)
    
    def test_validate_batch_empty(self):
        """Test batch validation with empty list."""
        valid, invalid = DataValidator.validate_batch([])
        
        self.assertEqual(len(valid), 0)
        self.assertEqual(len(invalid), 0)


if __name__ == "__main__":
    unittest.main()
