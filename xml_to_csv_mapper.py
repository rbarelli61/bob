import xml.etree.ElementTree as ET
import csv
import re
from typing import Dict, Any, Optional, List

def clean_text(text: Optional[str]) -> str:
    """Remove non-printable characters from text."""
    if text is None:
        return ""
    # Remove control characters and other non-printable characters
    cleaned = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f-\x9f]', '', text)
    # Clean up multiple whitespaces
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned

def extract_text_by_lang(elements: list, lang: str = "en") -> str:
    """Extract text from XML elements by language attribute."""
    for elem in elements:
        if elem.get('{http://www.w3.org/XML/1998/namespace}lang') == lang or \
           elem.get('lang') == lang:
            text = elem.text or ""
            return clean_text(text)
    
    # Fallback to any element if language not found
    if elements and len(elements) > 0:
        text = elements[0].text or ""
        return clean_text(text)
    
    return ""

def format_categories(categories: List[str]) -> str:
    """Format categories as <'category_1'> <'category_2'> ... <'category_n'>"""
    if not categories:
        return ""
    # Remove duplicates while preserving order
    unique_categories = []
    seen = set()
    for cat in categories:
        if cat and cat not in seen:
            unique_categories.append(cat)
            seen.add(cat)
    
    # Format with <'...'>
    formatted = " ".join([f"<'{cat}'>" for cat in unique_categories])
    return formatted

def parse_venue_xml(venue_elem) -> Dict[str, Any]:
    """Parse a single Venue XML element and extract data."""
    
    # Extract fields
    data = {
        'category': '',
        'name': clean_text(venue_elem.findtext('Name', '')),
        'nameEN': extract_text_by_lang(venue_elem.findall('Name'), 'en'),
        'description': extract_text_by_lang(venue_elem.findall('Description'), 'it'),
        'descriptionEN': extract_text_by_lang(venue_elem.findall('Description'), 'en'),
        'hours': '',
        'manager': '',
        'website': '',
        'email': '',
        'phone': '',
        'fax': '',
        'weeklyClosing': '',
        'services': '',
        'ticketCost': '',
        'ticketEmail': '',
        'ticketPhone': '',
        'ticketHours': '',
        'ticketReductions': '',
        'addressStreet': '',
        'addressLocation': '',
        'addressCap': '',
        'addressMunicipalities': '',
        'addressProvince': '',
        'addressRegion': '',
        'addressText': '',
        'latitude': '',
        'longitude': '',
        'photo': '',
        'accessibility': '',
        'visibleCopy': '',
        'visibleCopyImage': '',
        'sponsorEndDate': '',
        'sponsorStartDate': '',
        'hidden': '',
        'unreachable': '',
        'source': clean_text(venue_elem.findtext('DataSource', '')),
        'sourceId': clean_text(venue_elem.findtext('Id', '')),
        'lockUpdate': '',
        'accessibilityInfo': '',
        'blogUrl': ''
    }
    
    # Extract categories (collect all unique ones)
    categories = venue_elem.findall('Category')
    if categories:
        category_list = [clean_text(cat.text) for cat in categories if cat.text]
        data['category'] = format_categories(category_list)
    
    # Extract media resource (photo)
    media_resource = venue_elem.find('MediaResource')
    if media_resource is not None:
        uri_elem = media_resource.find('Uri')
        if uri_elem is not None and uri_elem.text:
            data['photo'] = clean_text(uri_elem.text)
    
    # Extract geolocation
    geolocation = venue_elem.find('Geolocation')
    if geolocation is not None:
        geocoords = geolocation.find('Geocoordinates')
        if geocoords is not None:
            x_coord = geocoords.findtext('XCoord', '')
            y_coord = geocoords.findtext('YCoord', '')
            data['longitude'] = clean_text(x_coord)
            data['latitude'] = clean_text(y_coord)
    
    return data

def parse_venues_from_file(input_file: str) -> List[Dict[str, Any]]:
    """Parse XML file with multiple Venue elements."""
    venues = []
    
    try:
        tree = ET.parse(input_file)
        root = tree.getroot()
        
        # Find all Venue elements (at any level in the tree)
        for venue_elem in root.findall('.//Venue'):
            venue_data = parse_venue_xml(venue_elem)
            venues.append(venue_data)
        
        print(f"✓ Parsed {len(venues)} venues from '{input_file}'")
        return venues
    
    except FileNotFoundError:
        print(f"✗ Error: File '{input_file}' not found!")
        return []
    except ET.ParseError as e:
        print(f"✗ Error: Invalid XML file - {e}")
        return []

def write_to_csv(venues: List[Dict[str, Any]], output_file: str = 'venues.csv') -> None:
    """Write parsed venues to CSV file."""
    if not venues:
        print("No data to write!")
        return
    
    fieldnames = [
        'category', 'name', 'nameEN', 'description', 'descriptionEN',
        'hours', 'manager', 'website', 'email', 'phone', 'fax',
        'weeklyClosing', 'services', 'ticketCost', 'ticketEmail',
        'ticketPhone', 'ticketHours', 'ticketReductions', 'addressStreet',
        'addressLocation', 'addressCap', 'addressMunicipalities',
        'addressProvince', 'addressRegion', 'addressText', 'latitude',
        'longitude', 'photo', 'accessibility', 'visibleCopy',
        'visibleCopyImage', 'sponsorEndDate', 'sponsorStartDate', 'hidden',
        'unreachable', 'source', 'sourceId', 'lockUpdate',
        'accessibilityInfo', 'blogUrl'
    ]
    
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(venues)
        
        print(f"✓ CSV file '{output_file}' created successfully with {len(venues)} rows!")
    except IOError as e:
        print(f"✗ Error writing CSV file: {e}")

def main():
    # Input XML file
    input_file = 'input.xml'
    output_file = 'venues.csv'
    
    print(f"Reading venues from '{input_file}'...")
    
    # Parse all venues from XML file
    venues = parse_venues_from_file(input_file)
    
    if venues:
        # Write to CSV
        write_to_csv(venues, output_file)
        
        # Print summary
        print(f"\nProcessed {len(venues)} venues:")
        for i, venue in enumerate(venues, 1):
            print(f"  {i}. {venue['name']} (ID: {venue['sourceId']}, Categories: {venue['category']})")
    else:
        print("No venues found in XML file.")

if __name__ == "__main__":
    main()
