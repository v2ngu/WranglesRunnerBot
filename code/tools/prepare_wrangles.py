import json
import os
import glob
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# --- Configuration ---
OUTPUT_FILENAME = 'wrangles_to_load.jsonl'

# URLs to process
URLS_TO_PROCESS = [
    "https://dev.wrangles.io/en/excel/my_wrangles/extract",
    "https://dev.wrangles.io/en/python/recipes/wrangles/extract",
    "https://dev.wrangles.io/en/excel/extract"
]

# Track processed items to avoid duplicates
processed_items = set()

# --- Helper function to download and extract JSON-LD from a URL ---
def download_and_extract_json_ld(url):
    print(f"--> Attempting to download and extract JSON-LD from: {url}")
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        json_ld_scripts = soup.find_all('script', type='application/ld+json')

        extracted_data = []
        for script in json_ld_scripts:
            try:
                if not script.string or not script.string.strip():
                    continue
                    
                json_data = json.loads(script.string)
                
                # Handle different JSON-LD structures
                if isinstance(json_data, dict):
                    if '@graph' in json_data:
                        extracted_data.extend(json_data['@graph'])
                    else:
                        extracted_data.append(json_data)
                elif isinstance(json_data, list):
                    extracted_data.extend(json_data)

            except json.JSONDecodeError as e:
                print(f"Warning: Could not decode JSON-LD from script tag on {url}: {e}")
        
        if not extracted_data:
            print(f"Warning: No valid JSON-LD data found on {url}")
        else:
            print(f"Found {len(extracted_data)} JSON-LD objects on {url}")
        
        return extracted_data

    except requests.exceptions.RequestException as e:
        print(f"!!! ERROR: Failed to download {url}: {e}")
        return []
    except Exception as e:
        print(f"!!! Unexpected error processing {url}: {e}")
        return []

def normalize_url(url, base_url):
    """Normalize URL - make relative URLs absolute"""
    if not url:
        return None
    if isinstance(url, dict) and '@id' in url:
        url = url['@id']
    if isinstance(url, str):
        if url.startswith('http'):
            return url
        if url.startswith('#'):
            return base_url + url
        return urljoin(base_url, url)
    return None

def extract_text_or_id(content):
    """Extract text content or @id from various content formats"""
    if not content:
        return None
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, dict):
        if '@id' in content:
            return content['@id']
        if 'text' in content:
            return content['text'].strip()
        if 'name' in content:
            return content['name'].strip()
    return str(content).strip() if content else None

def create_comprehensive_record(item, source_url, context=None):
    """Create a comprehensive record that preserves all JSON-LD data"""
    
    # Start with the original item to preserve all data
    record = dict(item)
    
    # Add metadata
    record['_source_url'] = source_url
    record['_extracted_at'] = None  # Could add timestamp if needed
    
    # Ensure we have both @type and type fields for compatibility
    if '@type' in record and 'type' not in record:
        record['type'] = record['@type']
    elif 'type' in record and '@type' not in record:
        record['@type'] = record['type']
    
    # Normalize URLs throughout the record
    def normalize_urls_recursive(obj, base_url):
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key in ['@id', 'url', 'contentUrl', 'item', 'sameAs']:
                    obj[key] = normalize_url(value, base_url)
                elif isinstance(value, (dict, list)):
                    normalize_urls_recursive(value, base_url)
        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, (dict, list)):
                    normalize_urls_recursive(item, base_url)
    
    normalize_urls_recursive(record, source_url)
    
    return record

def create_unique_key_from_record(record):
    """Create a unique key for deduplication from the full record"""
    type_val = record.get('@type', record.get('type', 'Unknown'))
    if isinstance(type_val, list):
        type_val = '_'.join(type_val)
    
    # Use @id if available, otherwise try other identifying fields
    id_val = record.get('@id')
    if not id_val:
        id_val = record.get('url') or record.get('name') or record.get('headline')
    
    return f"{type_val}:{id_val}"

def should_include_record(record):
    """Determine if a record should be included in output"""
    type_val = record.get('@type', record.get('type'))
    if not type_val:
        return False
    
    # Convert to list for consistent handling
    if not isinstance(type_val, list):
        type_val = [type_val]
    
    # Include content-rich types
    content_types = [
        'TechArticle', 'Article', 'WebPage', 'HowTo', 'ImageObject',
        'SoftwareSourceCode', 'SoftwareApplication', 'Organization',
        'CreativeWork', 'Thing'
    ]
    
    # Skip purely structural types unless they have rich content
    skip_types = ['BreadcrumbList', 'ListItem']
    
    # Include if it has any content type
    has_content_type = any(t in content_types for t in type_val)
    has_skip_type = any(t in skip_types for t in type_val)
    
    if has_skip_type and not has_content_type:
        return False
    
    # Include if it has meaningful content
    has_content = any(field in record for field in [
        'name', 'headline', 'description', 'text', 'step', 'contentUrl', 
        'programmingLanguage', 'codeSampleType'
    ])
    
    return has_content

def process_json_data_objects(data_objects, source_url="URL"):
    """
    Processes a list of JSON objects from JSON-LD.
    Preserves the complete structure while ensuring compatibility.
    """
    if not data_objects:
        return

    for item in data_objects:
        if not isinstance(item, dict):
            continue
        
        # Create comprehensive record preserving all data
        record = create_comprehensive_record(item, source_url)
        
        # Check if we should include this record
        if not should_include_record(record):
            continue
        
        # Create unique key for deduplication
        unique_key = create_unique_key_from_record(record)
        
        if unique_key not in processed_items:
            processed_items.add(unique_key)
            
            # Get display info for logging
            type_display = record.get('@type', record.get('type', 'Unknown'))
            if isinstance(type_display, list):
                type_display = ', '.join(type_display)
            
            name_display = (record.get('name') or 
                          record.get('headline') or 
                          record.get('@id', 'Unnamed'))
            
            print(f"  -> Extracted '{type_display}': {name_display}")
            
            # Add enriched metadata for better search/retrieval
            if 'step' in record and isinstance(record['step'], list):
                # Extract step text for HowTo items
                step_texts = []
                for step in record['step']:
                    if isinstance(step, dict):
                        step_text = step.get('text', '')
                        if step_text:
                            step_texts.append(step_text)
                if step_texts:
                    record['_step_content'] = ' '.join(step_texts)
            
            # Extract code content for SoftwareSourceCode
            if record.get('@type') == 'SoftwareSourceCode' or 'SoftwareSourceCode' in str(record.get('@type', [])):
                code_text = record.get('text', '')
                if code_text:
                    # Clean up escaped newlines
                    record['_code_content'] = code_text.replace('\\n', '\n')
            
            yield record
        else:
            type_display = record.get('@type', record.get('type', 'Unknown'))
            name_display = record.get('name', record.get('@id', 'Unnamed'))
            print(f"  -> Skipping duplicate: {type_display} - {name_display}")

if __name__ == "__main__":
    print(f"--> Starting comprehensive data extraction process...")
    
    # Clear output file
    if os.path.exists(OUTPUT_FILENAME):
        os.remove(OUTPUT_FILENAME)
        print(f"Cleared existing output file: {OUTPUT_FILENAME}")

    total_extracted = 0
    total_found = 0

    # Process URLs
    print(f"\n--- Processing URLs ---")
    if not URLS_TO_PROCESS:
        print("No URLs specified for processing.")
    else:
        for url in URLS_TO_PROCESS:
            print(f"\nProcessing: {url}")
            json_ld_objects = download_and_extract_json_ld(url)
            total_found += len(json_ld_objects)
            
            url_count = 0
            for extracted_doc in process_json_data_objects(json_ld_objects, url):
                try:
                    with open(OUTPUT_FILENAME, 'a', encoding='utf-8') as outfile:
                        outfile.write(json.dumps(extracted_doc, ensure_ascii=False, indent=None) + '\n')
                    url_count += 1
                    total_extracted += 1
                except Exception as e:
                    print(f"!!! ERROR: Could not write extracted data to '{OUTPUT_FILENAME}': {e}")
            
            print(f"Extracted {url_count} unique items from {url}")

    print(f"\n--> Comprehensive data extraction complete!")
    print(f"Total JSON-LD objects found: {total_found}")
    print(f"Total unique items extracted: {total_extracted}")
    print(f"Results written to '{OUTPUT_FILENAME}'")
    
    # Verify output file and show detailed stats
    if os.path.exists(OUTPUT_FILENAME):
        with open(OUTPUT_FILENAME, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        print(f"Output file contains {len(lines)} lines")
        
        # Analyze content types
        type_counts = {}
        for line in lines:
            try:
                item = json.loads(line)
                item_type = item.get('@type', item.get('type', 'Unknown'))
                if isinstance(item_type, list):
                    for t in item_type:
                        type_counts[t] = type_counts.get(t, 0) + 1
                else:
                    type_counts[item_type] = type_counts.get(item_type, 0) + 1
            except:
                continue
        
        print(f"\nContent type breakdown:")
        for content_type, count in sorted(type_counts.items()):
            print(f"  {content_type}: {count}")
        
        # Show sample of first item
        if lines:
            try:
                first_item = json.loads(lines[0])
                type_display = first_item.get('@type', first_item.get('type', 'Unknown'))
                name_display = first_item.get('name', first_item.get('headline', 'Unnamed'))
                print(f"\nSample item: {type_display} - {name_display}")
                print(f"Fields preserved: {len(first_item)} fields")
                print(f"Key fields: {list(first_item.keys())[:10]}...")
            except:
                print("Could not parse first item for preview")
    
    print("\n--- Next Step Hint ---")
    print("Run the script from 'NLWeb/code' using: python -m tools.prepare_wrangles")
    print("Then use your 'db_load' tool, for example:")
    print(f"    python -m tools.db_load {OUTPUT_FILENAME} <YOUR_SITE_NAME_HERE>")