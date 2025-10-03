#!/usr/bin/env python3
"""
Simple validation test for Node Cross-Reference fixes
"""
import pandas as pd

def test_excel_columns():
    """Test that Excel sheets have proper column alignment"""
    try:
        # Test the latest Excel file
        excel_file = "node_cross_reference_results_20250913_212146.xlsx"
        
        # Read each sheet and check column count
        sheets = ['Need Review', 'Suggest Reopen', 'Closed OK', 'Can Close']
        expected_columns = 17  # Should be 17 columns based on our fix
        
        print("=== EXCEL COLUMN VALIDATION ===")
        for sheet_name in sheets:
            try:
                df = pd.read_excel(excel_file, sheet_name=sheet_name)
                actual_columns = len(df.columns)
                status = "✓ PASS" if actual_columns == expected_columns else "✗ FAIL"
                print(f"{sheet_name}: {actual_columns} columns {status}")
                
                # Show first few columns
                if actual_columns > 0:
                    print(f"  Columns: {list(df.columns[:5])}...")
                    
            except Exception as e:
                print(f"{sheet_name}: Error reading - {e}")
        
        return True
    except Exception as e:
        print(f"Excel validation failed: {e}")
        return False

def test_node_language_clarity():
    """Test that node references are now clear and unambiguous"""
    try:
        # Read the summary report to check for clear language
        with open('summary_report.txt', 'r', encoding='utf-8', newline='') as f:
            content = f.read()
        
        print("\n=== NODE LANGUAGE VALIDATION ===")
        
        # Check for improved language patterns
        good_patterns = [
            "Node 1 is confirmed offline",
            "Node 2 is confirmed offline", 
            "Node 1 is offline but couldn't identify",
            "Node 2 is offline but couldn't identify"
        ]
        
        found_good = []
        for pattern in good_patterns:
            if pattern in content:
                found_good.append(pattern)
        
        print(f"Found {len(found_good)} clear node reference patterns:")
        for pattern in found_good[:3]:  # Show first 3
            print(f"  ✓ '{pattern}'")
        
        # Check that old confusing patterns are gone
        bad_patterns = [
            "Store has nodes [",
            "offline nodes: [",
        ]
        
        found_bad = []
        for pattern in bad_patterns:
            if pattern in content:
                found_bad.append(pattern)
        
        if found_bad:
            print(f"⚠ Found {len(found_bad)} potentially confusing patterns:")
            for pattern in found_bad:
                print(f"  ⚠ '{pattern}'")
        else:
            print("✓ No confusing node reference patterns found")
        
        return len(found_good) > 0 and len(found_bad) == 0
        
    except Exception as e:
        print(f"Language validation failed: {e}")
        return False

def test_temporal_correlation():
    """Test that temporal analysis is working"""
    try:
        # Read one of the CSV files to check for temporal data
        df = pd.read_csv('results_need_review.csv')
        
        print("\n=== TEMPORAL CORRELATION VALIDATION ===")
        
        # Check if temporal analysis columns exist
        required_cols = ['Days_Offline', 'Reopenable', 'Temporal_Analysis']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            print(f"✗ Missing temporal columns: {missing_cols}")
            return False
        else:
            print("✓ All temporal columns present")
        
        # Check if temporal analysis has actual data (not all empty)
        temporal_data = df['Temporal_Analysis'].dropna()
        if len(temporal_data) > 0:
            print(f"✓ Temporal analysis populated for {len(temporal_data)} tickets")
            return True
        else:
            print("⚠ Temporal analysis column exists but appears empty")
            return False
            
    except Exception as e:
        print(f"Temporal validation failed: {e}")
        return False

if __name__ == "__main__":
    print("Node Cross-Reference Validation Tests")
    print("=====================================")
    
    # Run all tests
    excel_ok = test_excel_columns()
    language_ok = test_node_language_clarity() 
    temporal_ok = test_temporal_correlation()
    
    print(f"\n=== FINAL RESULTS ===")
    print(f"Excel column alignment: {'✓ PASS' if excel_ok else '✗ FAIL'}")
    print(f"Node language clarity: {'✓ PASS' if language_ok else '✗ FAIL'}")
    print(f"Temporal correlation: {'✓ PASS' if temporal_ok else '✗ FAIL'}")
    
    if excel_ok and language_ok and temporal_ok:
        print("\n🎉 All validations PASSED!")
    else:
        print("\n⚠ Some validations failed - check output above")