"""
Utilities for data deduplication and transformation.

Provides efficient deduplication and data quality checks.
"""

from typing import List, Dict, Any, Set, Tuple
from collections import defaultdict


class Deduplicator:
    """
    Efficient record deduplication based on composite keys.
    
    Handles deduplication of financial position records to prevent
    duplicate entries in the database.
    """
    
    def __init__(self, key_fields: List[str]):
        """
        Initialize deduplicator.
        
        Args:
            key_fields: List of field names to use as deduplication key
        """
        self.key_fields = key_fields
    
    def deduplicate(
        self,
        records: List[Dict[str, Any]],
        keep: str = "last",
    ) -> List[Dict[str, Any]]:
        """
        Remove duplicate records based on key fields.
        
        Args:
            records: List of records to deduplicate
            keep: Which duplicate to keep ('first' or 'last')
            
        Returns:
            Deduplicated list of records
        """
        if not records:
            return []
        
        seen: Dict[Tuple, Dict[str, Any]] = {}
        
        for record in records:
            key = self._extract_key(record)
            
            if key not in seen or keep == "last":
                seen[key] = record
        
        return list(seen.values())
    
    def _extract_key(self, record: Dict[str, Any]) -> Tuple:
        """
        Extract deduplication key from record.
        
        Args:
            record: Record to extract key from
            
        Returns:
            Tuple of key values
        """
        value = record.get("value", {})
        key_values = []
        
        for field in self.key_fields:
            key_values.append(value.get(field))
        
        return tuple(key_values)
    
    def get_duplicates(
        self,
        records: List[Dict[str, Any]],
    ) -> Dict[Tuple, List[Dict[str, Any]]]:
        """
        Find all duplicate records.
        
        Args:
            records: List of records to check
            
        Returns:
            Dictionary mapping keys to list of duplicate records
        """
        groups: Dict[Tuple, List[Dict[str, Any]]] = defaultdict(list)
        
        for record in records:
            key = self._extract_key(record)
            groups[key].append(record)
        
        # Filter to only duplicates
        duplicates = {k: v for k, v in groups.items() if len(v) > 1}
        
        return duplicates


class DataValidator:
    """
    Validates financial position records for data quality.
    """
    
    REQUIRED_FIELDS = [
        "position_id",
        "account_id",
        "symbol",
        "quantity",
    ]
    
    @staticmethod
    def validate_record(record: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Validate a single record.
        
        Args:
            record: Record to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        value = record.get("value", {})
        
        # Check required fields
        for field in DataValidator.REQUIRED_FIELDS:
            if field not in value or value[field] is None:
                return False, f"Missing required field: {field}"
        
        # Validate types and values
        try:
            quantity = float(value.get("quantity", 0))
            if quantity == 0:
                return False, "Quantity cannot be zero"
        except (ValueError, TypeError):
            return False, "Invalid quantity value"
        
        # Validate string fields
        if not value.get("position_id", "").strip():
            return False, "position_id cannot be empty"
        if not value.get("account_id", "").strip():
            return False, "account_id cannot be empty"
        if not value.get("symbol", "").strip():
            return False, "symbol cannot be empty"
        
        return True, ""
    
    @staticmethod
    def validate_batch(
        records: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[Tuple[Dict[str, Any], str]]]:
        """
        Validate a batch of records.
        
        Args:
            records: List of records to validate
            
        Returns:
            Tuple of (valid_records, invalid_records_with_errors)
        """
        valid = []
        invalid = []
        
        for record in records:
            is_valid, error = DataValidator.validate_record(record)
            if is_valid:
                valid.append(record)
            else:
                invalid.append((record, error))
        
        return valid, invalid
