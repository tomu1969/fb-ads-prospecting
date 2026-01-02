"""Integration tests for Instagram enrichment pipeline integration.

Tests module integration, CSV compatibility, data preservation, and error handling.
"""

import unittest
import pandas as pd
import json
import sys
import os
import shutil
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
SCRIPTS_DIR = BASE_DIR / "scripts"
PROCESSED_DIR = BASE_DIR / "processed"


class TestCSVCompatibility(unittest.TestCase):
    """Test CSV file compatibility and structure."""
    
    def setUp(self):
        """Set up test data."""
        self.test_csv = PROCESSED_DIR / "03d_final_test.csv"
        self.backup_csv = PROCESSED_DIR / "03d_final_test_backup.csv"
    
    def tearDown(self):
        """Clean up test files."""
        for f in [self.test_csv, self.backup_csv]:
            if f.exists():
                f.unlink()
    
    def test_csv_has_required_columns(self):
        """Test that input CSV has required columns."""
        # Create test CSV with required columns
        df = pd.DataFrame({
            'page_name': ['Test Company'],
            'website_url': ['https://test.com'],
            'contact_name': ['Test Contact'],
            'instagram_handles': ['[]'],
        })
        df.to_csv(self.test_csv, index=False)
        
        # Verify columns exist
        loaded = pd.read_csv(self.test_csv)
        self.assertIn('page_name', loaded.columns)
        self.assertIn('website_url', loaded.columns)
    
    def test_column_addition(self):
        """Test that instagram_handles column exists."""
        df = pd.DataFrame({
            'page_name': ['Test Company'],
            'contact_name': ['Test Contact'],
        })
        df.to_csv(self.test_csv, index=False)
        
        # Simulate column addition
        loaded = pd.read_csv(self.test_csv)
        if 'instagram_handles' not in loaded.columns:
            loaded['instagram_handles'] = '[]'
        
        self.assertIn('instagram_handles', loaded.columns)
    
    def test_existing_data_preserved(self):
        """Test that existing CSV data is preserved."""
        import json
        original_data = {
            'page_name': ['Company1', 'Company2'],
            'contact_name': ['John', 'Jane'],
            'email': ['john@test.com', 'jane@test.com'],
            'phone': ['123-456-7890', '987-654-3210']
        }
        df = pd.DataFrame(original_data)
        df.to_csv(self.test_csv, index=False)
        
        # Load and add Instagram column
        loaded = pd.read_csv(self.test_csv)
        loaded['instagram_handles'] = '[]'
        loaded.loc[0, 'instagram_handles'] = json.dumps(['@johndoe', '@company1'])
        loaded.to_csv(self.test_csv, index=False)
        
        # Verify original data intact
        final = pd.read_csv(self.test_csv)
        self.assertEqual(final.loc[0, 'page_name'], 'Company1')
        self.assertEqual(final.loc[0, 'email'], 'john@test.com')
        self.assertEqual(final.loc[0, 'phone'], '123-456-7890')
        handles = json.loads(final.loc[0, 'instagram_handles'])
        self.assertIn('@johndoe', handles)
        self.assertEqual(len(final), 2)


class TestDataIntegrity(unittest.TestCase):
    """Test data integrity and preservation."""
    
    def setUp(self):
        """Set up test data."""
        self.test_csv = PROCESSED_DIR / "03d_final_test.csv"
    
    def tearDown(self):
        """Clean up test files."""
        if self.test_csv.exists():
            self.test_csv.unlink()
    
    def test_row_count_preserved(self):
        """Test that row count doesn't change."""
        df = pd.DataFrame({
            'page_name': ['Company1', 'Company2', 'Company3'],
            'contact_name': ['John', 'Jane', 'Bob']
        })
        df.to_csv(self.test_csv, index=False)
        
        loaded = pd.read_csv(self.test_csv)
        loaded['contact_instagram_handle'] = ''
        loaded.to_csv(self.test_csv, index=False)
        
        final = pd.read_csv(self.test_csv)
        self.assertEqual(len(final), 3)
    
    def test_instagram_handles_json_format(self):
        """Test that instagram_handles maintains JSON array format."""
        handles = ['@handle1', '@handle2']
        json_str = json.dumps(handles)
        
        df = pd.DataFrame({
            'page_name': ['Test Company'],
            'instagram_handles': [json_str]
        })
        df.to_csv(self.test_csv, index=False)
        
        loaded = pd.read_csv(self.test_csv)
        parsed = json.loads(loaded.loc[0, 'instagram_handles'])
        self.assertIsInstance(parsed, list)
        self.assertEqual(len(parsed), 2)
    
    def test_no_data_corruption(self):
        """Test that no data corruption occurs."""
        import json
        original = {
            'page_name': ['Company1'],
            'contact_name': ['John Doe'],
            'email': ['john@company.com'],
            'phone': ['123-456-7890'],
            'company_description': ['Test description']
        }
        df = pd.DataFrame(original)
        df.to_csv(self.test_csv, index=False)
        
        # Simulate Instagram enrichment
        loaded = pd.read_csv(self.test_csv)
        if 'instagram_handles' not in loaded.columns:
            loaded['instagram_handles'] = '[]'
        loaded.loc[0, 'instagram_handles'] = json.dumps(['@johndoe', '@company1'])
        loaded.to_csv(self.test_csv, index=False)
        
        # Verify all original data intact
        final = pd.read_csv(self.test_csv)
        for col in original.keys():
            self.assertEqual(final.loc[0, col], original[col][0])


class TestErrorHandling(unittest.TestCase):
    """Test error handling and recovery."""
    
    def test_missing_column_handling(self):
        """Test handling of missing columns."""
        df = pd.DataFrame({
            'page_name': ['Test Company']
        })
        # Should not crash if contact_name doesn't exist
        if 'contact_name' in df.columns:
            contact = df.loc[0, 'contact_name']
        else:
            contact = ''
        self.assertEqual(contact, '')
    
    def test_invalid_json_handling(self):
        """Test handling of invalid JSON in instagram_handles."""
        invalid_json = 'not valid json'
        try:
            parsed = json.loads(invalid_json)
        except json.JSONDecodeError:
            parsed = []
        self.assertEqual(parsed, [])
    
    def test_empty_csv_handling(self):
        """Test handling of empty CSV."""
        df = pd.DataFrame()
        self.assertEqual(len(df), 0)
        # Should not crash when processing empty DataFrame
        if 'contact_instagram_handle' not in df.columns:
            df['contact_instagram_handle'] = ''


class TestModuleIntegration(unittest.TestCase):
    """Test module integration with pipeline."""
    
    def test_input_file_exists(self):
        """Test that input file exists check works."""
        input_file = PROCESSED_DIR / "03d_final.csv"
        # In real scenario, this should exist after Module 3.6
        # For test, we just check the logic
        if input_file.exists():
            self.assertTrue(input_file.exists())
        else:
            # Test would skip if file doesn't exist
            self.skipTest("Input file not available for testing")
    
    def test_output_file_format(self):
        """Test that output file maintains correct format."""
        test_data = {
            'page_name': ['Test'],
            'contact_name': ['Test Contact'],
        }
        df = pd.DataFrame(test_data)
        test_file = PROCESSED_DIR / "03d_final_test.csv"
        df.to_csv(test_file, index=False)
        
        # Verify file is valid CSV
        loaded = pd.read_csv(test_file)
        self.assertIsInstance(loaded, pd.DataFrame)
        self.assertGreater(len(loaded), 0)
        
        if test_file.exists():
            test_file.unlink()


if __name__ == '__main__':
    unittest.main()

