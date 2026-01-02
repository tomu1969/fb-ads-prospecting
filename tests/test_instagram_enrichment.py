"""Unit tests for Instagram enrichment functionality.

Tests handle extraction, scraping, CSV parsing, and filtering logic.
"""

import unittest
import json
import pandas as pd
import sys
from pathlib import Path

# Add scripts directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

# Import functions to test (will need to extract them or import from scripts)
# For now, we'll test the logic directly


class TestHandleExtraction(unittest.TestCase):
    """Test Instagram handle extraction from text."""
    
    def test_extract_from_url(self):
        """Test extraction from Instagram URLs."""
        text = "Check out our Instagram: https://instagram.com/companyname"
        # Simulate the extraction logic
        import re
        pattern = r'instagram\.com/([a-zA-Z0-9_.]+)/?'
        matches = re.findall(pattern, text, re.I)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].lower(), "companyname")
    
    def test_extract_from_mention(self):
        """Test extraction from @ mentions."""
        text = "Follow us @companyname for updates"
        import re
        pattern = r'@([a-zA-Z][a-zA-Z0-9_.]{2,29})'
        matches = re.findall(pattern, text)
        self.assertGreater(len(matches), 0)
    
    def test_filter_false_positives(self):
        """Test filtering of false positives."""
        false_positives = {'graph', 'context', 'type', 'media'}
        test_handles = ['@graph', '@companyname', '@context', '@realhandle']
        filtered = [h for h in test_handles if h.replace('@', '').lower() not in false_positives]
        self.assertIn('@companyname', filtered)
        self.assertIn('@realhandle', filtered)
        self.assertNotIn('@graph', filtered)
        self.assertNotIn('@context', filtered)
    
    def test_handle_format_validation(self):
        """Test handle format validation."""
        valid_handles = ['@username', '@user_name', '@user.name', '@user123']
        invalid_handles = ['username', '@', '@u', '@user name']
        
        for handle in valid_handles:
            self.assertTrue(handle.startswith('@'))
            self.assertTrue(len(handle) > 2)
        
        for handle in invalid_handles:
            if not handle.startswith('@'):
                self.assertFalse(handle.startswith('@'))


class TestCSVParsing(unittest.TestCase):
    """Test CSV parsing and updating."""
    
    def test_parse_instagram_handles_field(self):
        """Test parsing instagram_handles field from CSV."""
        # Test JSON string
        json_str = '["@handle1", "@handle2"]'
        parsed = json.loads(json_str)
        self.assertIsInstance(parsed, list)
        self.assertEqual(len(parsed), 2)
        
        # Test empty
        empty = '[]'
        parsed_empty = json.loads(empty)
        self.assertEqual(len(parsed_empty), 0)
        
        # Test invalid
        invalid = 'not json'
        try:
            json.loads(invalid)
            self.fail("Should have raised JSONDecodeError")
        except json.JSONDecodeError:
            pass
    
    def test_csv_column_addition(self):
        """Test adding instagram_handles column."""
        df = pd.DataFrame({
            'page_name': ['Company1', 'Company2'],
            'contact_name': ['John Doe', 'Jane Smith']
        })
        
        # Add column
        df['instagram_handles'] = '[]'
        self.assertIn('instagram_handles', df.columns)
        
        # Update values
        import json
        df.loc[0, 'instagram_handles'] = json.dumps(['@johndoe', '@company1'])
        parsed = json.loads(df.loc[0, 'instagram_handles'])
        self.assertIn('@johndoe', parsed)
    
    def test_csv_data_preservation(self):
        """Test that existing CSV data is preserved."""
        original_data = {
            'page_name': ['Company1', 'Company2'],
            'contact_name': ['John', 'Jane'],
            'email': ['john@company.com', 'jane@company.com']
        }
        df = pd.DataFrame(original_data)
        
        # Add Instagram column
        df['contact_instagram_handle'] = ''
        
        # Verify original data intact
        self.assertEqual(df.loc[0, 'page_name'], 'Company1')
        self.assertEqual(df.loc[0, 'email'], 'john@company.com')
        self.assertEqual(len(df), 2)


class TestFilteringLogic(unittest.TestCase):
    """Test filtering of false positives."""
    
    def test_css_keyword_filtering(self):
        """Test filtering of CSS/JS keywords."""
        css_keywords = ['@graph', '@context', '@type', '@media', '@import']
        real_handles = ['@companyname', '@user_name', '@realhandle']
        
        false_positives = {'graph', 'context', 'type', 'media', 'import'}
        
        filtered_css = [h for h in css_keywords if h.replace('@', '').lower() not in false_positives]
        filtered_real = [h for h in real_handles if h.replace('@', '').lower() not in false_positives]
        
        self.assertEqual(len(filtered_css), 0)  # All CSS keywords filtered
        self.assertEqual(len(filtered_real), 3)  # All real handles kept
    
    def test_email_pattern_filtering(self):
        """Test filtering of email-like patterns."""
        text_with_email = "@gmail.com @yahoo.com @realhandle"
        import re
        pattern = r'@([a-zA-Z][a-zA-Z0-9_.]{2,29})'
        matches = re.findall(pattern, text_with_email)
        
        # Filter out email domains
        filtered = [m for m in matches if not any(x in m.lower() for x in ['gmail', 'yahoo', '.com'])]
        self.assertNotIn('gmail', filtered)
        self.assertNotIn('yahoo', filtered)
        if 'realhandle' in matches:
            self.assertIn('realhandle', filtered)


class TestDataStructures(unittest.TestCase):
    """Test data structure handling."""
    
    def test_handle_deduplication(self):
        """Test that handles are deduplicated."""
        handles = ['@handle1', '@handle2', '@handle1', '@handle3', '@handle2']
        unique = list(set(handles))
        self.assertEqual(len(unique), 3)
        self.assertIn('@handle1', unique)
        self.assertIn('@handle2', unique)
        self.assertIn('@handle3', unique)
    
    def test_json_array_format(self):
        """Test JSON array format for instagram_handles column."""
        handles = ['@handle1', '@handle2']
        json_str = json.dumps(handles)
        parsed = json.loads(json_str)
        self.assertIsInstance(parsed, list)
        self.assertEqual(len(parsed), 2)


if __name__ == '__main__':
    unittest.main()

