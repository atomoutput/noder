#!/usr/bin/env python3
"""
CSV Auto-Repair Module
======================

Standalone CSV repair functionality extracted from csv_repair_tool.py
for seamless integration with the Node Cross-Reference application.

This module automatically detects and repairs common ServiceNow CSV export issues:
- Encoding detection and repair
- Malformed CSV structure fixing
- Character encoding standardization
- Data cleaning (empty rows, duplicates)

Author: Backtool Project
License: MIT
"""

import os
import tempfile
import shutil
from pathlib import Path
from typing import Optional, Tuple
import logging

# Optional dependencies with graceful fallback
try:
    import chardet
    CHARDET_AVAILABLE = True
except ImportError:
    CHARDET_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


class CSVRepairer:
    """
    Standalone CSV repair functionality for automatic integration.

    Detects and repairs common CSV corruption issues without user intervention.
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize the CSV repairer.

        Args:
            logger: Optional logger for repair operations. If None, creates a silent logger.
        """
        self.logger = logger or self._create_silent_logger()

    def _create_silent_logger(self) -> logging.Logger:
        """Create a silent logger that doesn't output anything"""
        logger = logging.getLogger('csv_auto_repair_silent')
        logger.setLevel(logging.CRITICAL + 1)  # Higher than CRITICAL to silence everything
        return logger

    def detect_encoding(self, filepath: str) -> str:
        """
        Detect the encoding of a file using chardet if available.

        Args:
            filepath: Path to the file to analyze

        Returns:
            Detected encoding string, defaults to 'utf-8' if detection fails
        """
        if not CHARDET_AVAILABLE:
            self.logger.debug("chardet not available, defaulting to utf-8")
            return 'utf-8'

        try:
            with open(filepath, 'rb') as f:
                raw_data = f.read(10000)  # Read first 10KB for detection

            result = chardet.detect(raw_data)
            encoding = result['encoding']
            confidence = result['confidence']

            self.logger.debug(f"Detected encoding: {encoding} (confidence: {confidence:.2f})")

            # Only use detected encoding if confidence is high
            return encoding if confidence > 0.7 else 'utf-8'

        except Exception as e:
            self.logger.debug(f"Encoding detection failed: {e}")
            return 'utf-8'

    def repair_csv_data(self, filepath: str, target_encoding: str = 'utf-8') -> Tuple[bool, Optional[str]]:
        """
        Repair a CSV file and return path to repaired version.

        Args:
            filepath: Path to the original CSV file
            target_encoding: Target encoding for repaired file

        Returns:
            Tuple of (success: bool, repaired_file_path: Optional[str])
            If success is True, repaired_file_path contains path to temporary repaired file
            If success is False, repaired_file_path is None and original file should be used
        """
        if not PANDAS_AVAILABLE:
            self.logger.debug("pandas not available, skipping repair")
            return False, None

        try:
            self.logger.debug(f"Attempting to repair CSV: {os.path.basename(filepath)}")

            # Detect current encoding
            current_encoding = self.detect_encoding(filepath)

            # Try to read the CSV with multiple encoding strategies
            encodings_to_try = [current_encoding, 'utf-8', 'windows-1252', 'iso-8859-1', 'latin-1']
            df = None
            successful_encoding = None

            for encoding in encodings_to_try:
                try:
                    # Use pandas with error handling for malformed lines
                    df = pd.read_csv(filepath, encoding=encoding, on_bad_lines='skip')
                    successful_encoding = encoding
                    self.logger.debug(f"Successfully read with {encoding} encoding")
                    break
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    self.logger.debug(f"Failed to read with {encoding}: {str(e)[:100]}")
                    continue

            if df is None:
                self.logger.debug("Failed to read file with any encoding")
                return False, None

            # Clean and validate data
            original_rows = len(df)

            # Remove completely empty rows
            df = df.dropna(how='all')

            # Remove duplicate rows
            df = df.drop_duplicates()

            cleaned_rows = len(df)
            repair_needed = False

            # Check if repair was actually needed
            if cleaned_rows != original_rows:
                repair_needed = True
                self.logger.info(f"Cleaned CSV data: {original_rows} → {cleaned_rows} rows")

            if successful_encoding != target_encoding:
                repair_needed = True
                self.logger.info(f"Encoding conversion: {successful_encoding} → {target_encoding}")

            # If no repair needed and encoding matches, return original file
            if not repair_needed and successful_encoding == target_encoding:
                self.logger.debug("No repair needed")
                return False, None

            # Create temporary repaired file
            temp_fd, temp_path = tempfile.mkstemp(suffix='.csv', prefix='repaired_')
            os.close(temp_fd)  # Close the file descriptor, we'll write with pandas

            # Write the repaired file
            df.to_csv(temp_path, index=False, encoding=target_encoding)

            self.logger.info(f"CSV repaired successfully")
            self.logger.debug(f"Repaired file: {temp_path}")
            self.logger.debug(f"Rows: {cleaned_rows}, Columns: {len(df.columns)}")

            return True, temp_path

        except Exception as e:
            self.logger.error(f"Error repairing CSV file: {str(e)}")
            return False, None

    def auto_repair_csv(self, filepath: str, target_encoding: str = 'utf-8') -> str:
        """
        Automatically repair a CSV file if needed, returning path to use.

        This is the main method for transparent CSV repair integration.

        Args:
            filepath: Path to the original CSV file
            target_encoding: Target encoding for repaired file

        Returns:
            Path to CSV file to use (either original or temporary repaired file)
            Caller is responsible for cleaning up temporary files if returned path differs from input
        """
        if not os.path.exists(filepath):
            self.logger.error(f"CSV file not found: {filepath}")
            return filepath

        self.logger.debug(f"Auto-repairing CSV: {os.path.basename(filepath)}")

        success, repaired_path = self.repair_csv_data(filepath, target_encoding)

        if success and repaired_path:
            self.logger.info(f"Using repaired CSV file")
            return repaired_path
        else:
            self.logger.debug(f"Using original CSV file")
            return filepath


def cleanup_temp_file(filepath: str) -> None:
    """
    Clean up a temporary file if it exists and is in the system temp directory.

    Args:
        filepath: Path to file to clean up
    """
    if filepath and os.path.exists(filepath):
        temp_dir = tempfile.gettempdir()
        if filepath.startswith(temp_dir):
            try:
                os.unlink(filepath)
            except OSError:
                pass  # Ignore cleanup errors


# Convenience function for simple usage
def auto_repair_csv_file(filepath: str, logger: Optional[logging.Logger] = None) -> str:
    """
    Convenience function to auto-repair a CSV file.

    Args:
        filepath: Path to CSV file to repair
        logger: Optional logger for repair operations

    Returns:
        Path to CSV file to use (original or repaired)
    """
    repairer = CSVRepairer(logger)
    return repairer.auto_repair_csv(filepath)


if __name__ == "__main__":
    # Simple test/demo when run directly
    import sys

    if len(sys.argv) != 2:
        print("Usage: python csv_auto_repair.py <csv_file>")
        sys.exit(1)

    # Setup basic logging for demo
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    logger = logging.getLogger('csv_auto_repair_demo')

    input_file = sys.argv[1]
    result_file = auto_repair_csv_file(input_file, logger)

    if result_file != input_file:
        print(f"Repaired CSV created: {result_file}")
        print("Remember to clean up temporary file when done")
    else:
        print(f"No repair needed: {input_file}")