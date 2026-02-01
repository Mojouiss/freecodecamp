"""
Utilities for data deduplication and transformation using pandas.

Provides efficient deduplication and data quality checks with pandas.
"""

from typing import List, Dict, Any, Tuple
import pandas as pd


class Deduplicator:
    """
    Efficient record deduplication based on composite keys using pandas.
    
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
        
        # Convert to DataFrame for efficient deduplication
        df = pd.DataFrame(records)
        
        # Extract value fields for deduplication keys
        value_df = pd.json_normalize([r.get('value', {}) for r in records])
        
        # Create composite key columns if they exist
        key_cols = [col for col in self.key_fields if col in value_df.columns]
        
        if not key_cols:
            # No key columns found, return original
            return records
        
        # Add key columns to main df temporarily
        for col in key_cols:
            df[f'_key_{col}'] = value_df[col]
        
        # Deduplicate based on key columns
        key_columns = [f'_key_{col}' for col in key_cols]
        df_dedup = df.drop_duplicates(subset=key_columns, keep=keep)
        
        # Remove temporary key columns
        df_dedup = df_dedup.drop(columns=key_columns)
        
        # Convert back to list of dicts
        return df_dedup.to_dict('records')
    
    def deduplicate_dataframe(
        self,
        df: pd.DataFrame,
        keep: str = "last",
    ) -> pd.DataFrame:
        """
        Remove duplicate records from DataFrame based on key fields.
        
        Args:
            df: DataFrame to deduplicate
            keep: Which duplicate to keep ('first' or 'last')
            
        Returns:
            Deduplicated DataFrame
        """
        if df.empty:
            return df
        
        # Check which key fields exist in the DataFrame
        key_cols = [col for col in self.key_fields if col in df.columns]
        
        if not key_cols:
            return df
        
        # Deduplicate
        return df.drop_duplicates(subset=key_cols, keep=keep)
    
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
        if not records:
            return {}
        
        # Convert to DataFrame
        df = pd.DataFrame(records)
        value_df = pd.json_normalize([r.get('value', {}) for r in records])
        
        # Create key columns
        key_cols = [col for col in self.key_fields if col in value_df.columns]
        
        if not key_cols:
            return {}
        
        for col in key_cols:
            df[f'_key_{col}'] = value_df[col]
        
        # Find duplicates
        key_columns = [f'_key_{col}' for col in key_cols]
        duplicated_mask = df.duplicated(subset=key_columns, keep=False)
        duplicated_df = df[duplicated_mask]
        
        if duplicated_df.empty:
            return {}
        
        # Group by key
        duplicates = {}
        for key_values, group in duplicated_df.groupby(key_columns):
            # Remove temporary columns before converting
            group_clean = group.drop(columns=key_columns)
            key_tuple = tuple(key_values) if isinstance(key_values, (list, tuple)) else (key_values,)
            duplicates[key_tuple] = group_clean.to_dict('records')
        
        return duplicates


class DataValidator:
    """
    Validates financial position records for data quality using pandas.
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
    
    @staticmethod
    def validate_dataframe(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Validate a DataFrame of records.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (valid_df, invalid_df)
        """
        if df.empty:
            return df, pd.DataFrame()
        
        # Check required fields
        missing_fields = set(DataValidator.REQUIRED_FIELDS) - set(df.columns)
        if missing_fields:
            # All records are invalid if required fields are missing
            return pd.DataFrame(), df
        
        # Create validation mask
        valid_mask = pd.Series([True] * len(df), index=df.index)
        
        # Validate each field
        for field in DataValidator.REQUIRED_FIELDS:
            # Check for null/empty values
            if field in ['position_id', 'account_id', 'symbol']:
                valid_mask &= df[field].notna() & (df[field].astype(str).str.strip() != '')
            else:
                valid_mask &= df[field].notna()
        
        # Validate quantity is not zero
        if 'quantity' in df.columns:
            valid_mask &= (pd.to_numeric(df['quantity'], errors='coerce') != 0)
        
        # Split into valid and invalid
        valid_df = df[valid_mask].copy()
        invalid_df = df[~valid_mask].copy()
        
        return valid_df, invalid_df
