import json
import copy
from typing import List, Dict, Any
import requests
from bs4 import BeautifulSoup

def detect_content_selector(article_template: Dict[str, Any], soup: BeautifulSoup) -> str:
    """Detect the appropriate content selector from the template or by analyzing the page."""
    
    # First, check if the template has a contentPointer
    if 'contentPointer' in article_template:
        selector = article_template['contentPointer']
        print(f"--> Using contentPointer from schema: '{selector}'")
        return selector
    
    # If no contentPointer, try common patterns
    common_selectors = [
        'div[id^="extract-"]',  # Your current pattern
        'div[id^="wrangle-"]',
        'div[class*="content-section"]',
        'div[class*="wrangle"]',
        'section[id]',
        'div[id][class*="section"]',
        'article[id]'
    ]
    
    for selector in common_selectors:
        elements = soup.select(selector)
        if elements:
            print(f"--> Auto-detected content selector: '{selector}' (found {len(elements)} elements)")
            return selector
    
    # Fallback to any div with an id
    divs_with_id = soup.select('div[id]')
    if divs_with_id:
        print(f"--> Fallback: Using 'div[id]' (found {len(divs_with_id)} elements)")
        return 'div[id]'
    
    raise ValueError("Could not detect appropriate content selector for this page.")
# --- Configuration ---
URL = "https://dev.wrangles.io/en/excel/extract"
OUTPUT_FILENAME = "wrangles_to_load.jsonl"

def extract_schema_from_page(soup: BeautifulSoup) -> Dict[str, Any]:
    """Extract the JSON-LD schema from the webpage."""
    json_ld_script = soup.find('script', {'type': 'application/ld+json'})
    if not json_ld_script:
        raise ValueError("Could not find JSON-LD script tag on the page.")
    
    try:
        schema = json.loads(json_ld_script.string)
        return schema
    except json.JSONDecodeError as e:
        raise ValueError(f"Could not parse JSON-LD content: {e}")

def get_page_content(url: str) -> BeautifulSoup:
    """Fetch and parse the webpage content."""
    print(f"--> Fetching page content from: {url}")
    response = requests.get(url)
    response.raise_for_status()
    return BeautifulSoup(response.content, 'html.parser')

def find_template_article(schema: Dict[str, Any]) -> Dict[str, Any]:
    """Find the TechArticle template from the schema graph."""
    # Handle both graph-based and direct schema structures
    items_to_check = []
    
    if '@graph' in schema:
        items_to_check = schema.get('@graph', [])
    elif '@type' in schema:
        # Single schema object
        items_to_check = [schema]
    else:
        # Try to find any objects with @type
        for key, value in schema.items():
            if isinstance(value, dict) and '@type' in value:
                items_to_check.append(value)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict) and '@type' in item:
                        items_to_check.append(item)
    
    # Look for TechArticle or WebPage types
    for item in items_to_check:
        item_types = item.get('@type', [])
        if isinstance(item_types, str):
            item_types = [item_types]
        
        if any(t in ['TechArticle', 'WebPage', 'Article'] for t in item_types):
            return item
    
    # If no specific article type found, return the first item with @type
    for item in items_to_check:
        if '@type' in item:
            print(f"Warning: Using {item.get('@type')} as template (no TechArticle found)")
            return item
    
    raise ValueError("Could not find any suitable schema object to use as a template.")

def extract_section_content(div_element, content_type: str) -> Dict[str, str]:
    """Extract detailed content from a div section with enhanced debugging."""
    print(f"    --> Extracting content from div with id: {div_element.get('id', 'no-id')}")
    print(f"    --> Div tag name: {div_element.name}")
    print(f"    --> Div classes: {div_element.get('class', [])}")
    
    # Get the raw text content
    full_text = div_element.get_text(separator=' ', strip=True)
    print(f"    --> Raw text length: {len(full_text)} characters")
    if len(full_text) > 0:
        print(f"    --> First 100 chars: {full_text[:100]}...")
    else:
        print(f"    --> WARNING: No text content found!")
        print(f"    --> Div HTML: {str(div_element)[:200]}...")
    
    content_data = {
        'full_text': full_text,
        'html_content': str(div_element),
        'content_type': content_type
    }
    
    # Extract specific elements with debugging
    headings = div_element.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
    print(f"    --> Found {len(headings)} headings")
    if headings:
        content_data['headings'] = [h.get_text(strip=True) for h in headings if h.get_text(strip=True)]
        print(f"    --> Heading texts: {content_data['headings']}")
    else:
        content_data['headings'] = []
    
    paragraphs = div_element.find_all('p')
    print(f"    --> Found {len(paragraphs)} paragraphs")
    if paragraphs:
        content_data['paragraphs'] = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]
        print(f"    --> Non-empty paragraphs: {len(content_data['paragraphs'])}")
    else:
        content_data['paragraphs'] = []
    
    # Also try to extract from common content containers
    content_containers = div_element.find_all(['div', 'section', 'article'])
    if content_containers and not full_text:
        print(f"    --> Trying {len(content_containers)} content containers as fallback")
        for container in content_containers:
            container_text = container.get_text(separator=' ', strip=True)
            if len(container_text) > len(full_text):
                full_text = container_text
                content_data['full_text'] = full_text
                print(f"    --> Updated text from container: {len(full_text)} chars")
    
    code_blocks = div_element.find_all(['code', 'pre'])
    print(f"    --> Found {len(code_blocks)} code blocks")
    if code_blocks:
        content_data['code_examples'] = [code.get_text(strip=True) for code in code_blocks if code.get_text(strip=True)]
    else:
        content_data['code_examples'] = []
    
    lists = div_element.find_all(['ul', 'ol'])
    print(f"    --> Found {len(lists)} lists")
    if lists:
        content_data['lists'] = []
        for ul in lists:
            items = [li.get_text(strip=True) for li in ul.find_all('li') if li.get_text(strip=True)]
            if items:
                content_data['lists'].append(items)
    else:
        content_data['lists'] = []
    
    # Final check - if we still have no content, try a more aggressive approach
    if not content_data['full_text'] and not content_data['paragraphs'] and not content_data['headings']:
        print(f"    --> WARNING: Still no content found, trying aggressive extraction...")
        
        # Try getting all text from any child elements
        all_text_elements = div_element.find_all(text=True)
        if all_text_elements:
            aggressive_text = ' '.join([text.strip() for text in all_text_elements if text.strip()])
            content_data['full_text'] = aggressive_text
            print(f"    --> Aggressive extraction found: {len(aggressive_text)} chars")
        
        # Try getting text from specific child elements
        child_divs = div_element.find_all('div')
        if child_divs:
            for child in child_divs:
                child_text = child.get_text(separator=' ', strip=True)
                if len(child_text) > len(content_data['full_text']):
                    content_data['full_text'] = child_text
                    print(f"    --> Found better content in child div: {len(child_text)} chars")
    
    return content_data

def create_database_document(article_data: Dict[str, Any], section_content: Dict[str, str]) -> Dict[str, Any]:
    """Create a document formatted for database loading."""
    
    # Create the main document structure
    document = {
        'url': article_data.get('@id', ''),
        'title': article_data.get('headline', ''),
        'content': section_content.get('full_text', ''),
        'metadata': {
            'schema_type': article_data.get('@type', []),
            'content_type': section_content.get('content_type', ''),
            'author': article_data.get('author', {}),
            'publisher': article_data.get('publisher', {}),
            'breadcrumb': article_data.get('breadcrumb', {}),
            'main_entity': article_data.get('mainEntity', {}),
            'image': article_data.get('image', {}),
            'description': article_data.get('description', ''),
            'headings': section_content.get('headings', []),
            'paragraphs': section_content.get('paragraphs', []),
            'code_examples': section_content.get('code_examples', []),
            'lists': section_content.get('lists', [])
        },
        'full_schema': copy.deepcopy(article_data)  # Include the complete schema
    }
    
    return document

def run_robust_scraper():
    """
    Main function that scrapes content and creates detailed database documents.
    Works with any schema structure it encounters.
    """
    try:
        # Get the webpage content
        soup = get_page_content(URL)
        
        # Extract the schema from the page (no hardcoded master schema)
        page_schema = extract_schema_from_page(soup)
        print(f"--> Extracted schema with {len(page_schema.get('@graph', [page_schema]))} elements")
        
        # Find the template article from the extracted schema
        article_template = find_template_article(page_schema)
        print(f"--> Using template of type: {article_template.get('@type', 'Unknown')}")
        
        # Detect the appropriate content selector
        content_selector = detect_content_selector(article_template, soup)
        wrangle_divs = soup.select(content_selector)
        
        if not wrangle_divs:
            raise ValueError(f"Could not find any elements matching the selector: '{content_selector}'.")
        
        print(f"--> Found {len(wrangle_divs)} sections to process")
        
        # Process each section
        final_documents = []
        
        for i, div in enumerate(wrangle_divs):
            print(f"--> Processing section {i+1}/{len(wrangle_divs)}: {div.get('id', 'unknown')}")
            
            # Create a deep copy of the template
            new_article = copy.deepcopy(article_template)
            
            # Get section-specific information
            div_id = div.get('id', f'section-{i}')
            content_type = div_id.replace('extract-', '').replace('-', ' ').title()
            
            # Find the section name using namePointer or fallback
            name_pointer = new_article.get('namePointer', 'h2, h3, h4, .title, .heading')
            name_element = div.select_one(name_pointer)
            specific_name = name_element.get_text(strip=True) if name_element else content_type
            
            # Extract detailed content from the section
            section_content = extract_section_content(div, content_type)
            
            # Update the article with specific information
            new_article['@id'] = f"{URL}#{div_id}"
            new_article['headline'] = f"{specific_name}" + (f" - {article_template.get('headline', '')}" if article_template.get('headline') else "")
            new_article['description'] = section_content['full_text'][:200] + "..." if len(section_content['full_text']) > 200 else section_content['full_text']
            new_article['content_section_id'] = div_id
            new_article['content_type'] = content_type
            
            # Clean up template-specific properties
            new_article.pop('contentPointer', None)
            new_article.pop('namePointer', None)
            
            # Create the database document
            db_document = create_database_document(new_article, section_content)
            final_documents.append(db_document)
        
        # Add the complete schema as context for all documents
        schema_context = {
            'complete_schema': page_schema,  # Use the extracted schema
            'processing_info': {
                'source_url': URL,
                'total_sections': len(final_documents),
                'content_selector': content_selector,
                'template_type': article_template.get('@type', 'Unknown'),
                'schema_structure': 'graph' if '@graph' in page_schema else 'single'
            }
        }
        
        # Write the documents to JSONL format
        with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as f:
            # First write the schema context
            f.write(json.dumps({
                'url': f"{URL}#schema-context",
                'title': f"Schema Context - {article_template.get('headline', 'Documentation')}",
                'content': f"Complete schema and processing context for {URL}",
                'metadata': schema_context,
                'is_context': True
            }) + '\n')
            
            # Then write all the section documents
            for doc in final_documents:
                f.write(json.dumps(doc) + '\n')
        
        print(f"\nSUCCESS! Created '{OUTPUT_FILENAME}' with {len(final_documents) + 1} detailed documents.")
        print(f"Documents include:")
        print(f"  - 1 schema context document")
        print(f"  - {len(final_documents)} section-specific documents")
        print(f"Schema type: {article_template.get('@type', 'Unknown')}")
        print(f"Content selector used: {content_selector}")
        print(f"Each document contains:")
        print(f"  - Full content text")
        print(f"  - Detailed metadata")
        print(f"  - Complete schema information")
        print(f"  - Structured content (headings, paragraphs, code, lists)")
        
        return final_documents

    except Exception as e:
        print(f"!!! An unexpected error occurred: {e}")
        raise

if __name__ == "__main__":
    run_robust_scraper()