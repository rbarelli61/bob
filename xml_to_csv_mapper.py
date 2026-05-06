import xml.etree.ElementTree as ET
import csv
import re
from typing import Dict, Any, Optional

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

def parse_venue_xml(xml_content: str) -> Dict[str, Any]:
    """Parse Venue XML and extract data."""
    root = ET.fromstring(xml_content)
    
    # Define namespace mapping
    ns = {
        'xml': 'http://www.w3.org/XML/1998/namespace'
    }
    
    # Extract fields
    data = {
        'category': '',
        'name': clean_text(root.findtext('Name', '')),
        'nameEN': extract_text_by_lang(root.findall('Name'), 'en'),
        'description': extract_text_by_lang(root.findall('Description'), 'it'),
        'descriptionEN': extract_text_by_lang(root.findall('Description'), 'en'),
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
        'source': clean_text(root.findtext('DataSource', '')),
        'sourceId': clean_text(root.findtext('Id', '')),
        'lockUpdate': '',
        'accessibilityInfo': '',
        'blogUrl': ''
    }
    
    # Extract categories (concatenate all)
    categories = root.findall('Category')
    if categories:
        category_list = [clean_text(cat.text) for cat in categories]
        data['category'] = '; '.join(dict.fromkeys(category_list))  # Remove duplicates
    
    # Extract media resource (photo)
    media_resource = root.find('MediaResource')
    if media_resource is not None:
        uri_elem = media_resource.find('Uri')
        if uri_elem is not None and uri_elem.text:
            data['photo'] = clean_text(uri_elem.text)
    
    # Extract geolocation
    geolocation = root.find('Geolocation')
    if geolocation is not None:
        geocoords = geolocation.find('Geocoordinates')
        if geocoords is not None:
            x_coord = geocoords.findtext('XCoord', '')
            y_coord = geocoords.findtext('YCoord', '')
            data['longitude'] = clean_text(x_coord)
            data['latitude'] = clean_text(y_coord)
    
    return data

def write_to_csv(data: Dict[str, Any], output_file: str = 'venues.csv') -> None:
    """Write parsed data to CSV file."""
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
    
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(data)
    
    print(f"CSV file '{output_file}' created successfully!")

def main():
    # Read XML from file or paste content here
    xml_content = """<Venue>
		<IssueDateTime>2026-02-03T00:38:00+00:00</IssueDateTime>
		<DataSource>manual</DataSource>
		<License>Chiesa</License>
		<Id>135</Id>
		<Name xml:lang="en">Chiesa di San Colombano</Name>
		<Name xml:lang="it">Chiesa di San Colombano</Name>
		<Description xml:lang="en">La chiesa di San Colombano è un vero e proprio gioiello di arte romanico-lombarda.</Description>
		<Description xml:lang="en" formatted="true">
			<![CDATA[La chiesa di San Colombano è un vero e proprio gioiello di arte romanico-lombarda.]]>
		</Description>
		<Description xml:lang="it">La chiesa di San Colombano è un vero e proprio gioiello di arte romanico-lombarda.</Description>
		<Description xml:lang="it" formatted="true">
			<![CDATA[La chiesa di San Colombano è un vero e proprio gioiello di arte romanico-lombarda.]]>
		</Description>
		<Abstract xml:lang="en">La sua leggendaria connessione col Santo monaco irlandese e le sue origini, X oppure XII sec., sono ancora motivo di ricerca da parte degli storici dell'arte e ciò aumenta il fascino nella scoperta di questo edificio.

L'interno, a navata unica, presenta un sorprendente arco trionfale ed enigmatiche sculture, tra cui dei misteriosi telamoni, e affreschi di diverse epoche, rappresentanti, tra gli altri, San Colombano, San Bernardino e Sant'Orsola: oltre a ciò una inusuale e imponente Croce Patente nel catino absidale, il cui significato è tutto da scoprire, offre una possibile soluzione al problema della committenza dell'edificio.

«Tutti i rilievi scultorei costituiscono un'importante testimonianza delle così dette Bibbie di pietra che nel Medioevo coprirono le chiese di tutta Europa.»</Abstract>
		<Abstract xml:lang="it">La sua leggendaria connessione col Santo monaco irlandese e le sue origini, X oppure XII sec., sono ancora motivo di ricerca da parte degli storici dell'arte e ciò aumenta il fascino nella scoperta di questo edificio.

L'interno, a navata unica, presenta un sorprendente arco trionfale ed enigmatiche sculture, tra cui dei misteriosi telamoni, e affreschi di diverse epoche, rappresentanti, tra gli altri, San Colombano, San Bernardino e Sant'Orsola: oltre a ciò una inusuale e imponente Croce Patente nel catino absidale, il cui significato è tutto da scoprire, offre una possibile soluzione al problema della committenza dell'edificio.

«Tutti i rilievi scultorei costituiscono un'importante testimonianza delle così dette Bibbie di pietra che nel Medioevo coprirono le chiese di tutta Europa.»</Abstract>
		<Tag xml:lang="en">#CHIESA</Tag>
		<Tag xml:lang="it">#CHIESA</Tag>
		<Category xml:lang="en">Affreschi</Category>
		<Category xml:lang="en">Edifici storici</Category>
		<Category xml:lang="it">Affreschi</Category>
		<Category xml:lang="it">Edifici storici</Category>
		<Status>published</Status>
		<Contact/>
		<MediaResource isLogo="false" isInMediaGallery="false">
			<Id>341</Id>
			<Name xml:lang="en">Chiesa Romanica San Colombano</Name>
			<Name xml:lang="it">Chiesa Romanica San Colombano</Name>
			<Description xml:lang="en">La chiesa di San Colombano è situata sul mitico percorso utilizzato da generazioni di monaci, pellegrini e cavalieri, che giungevano da tutta Europa per recarsi a pregare sulle spoglie mortali del Santo, conservate a Bobbio, ed è un vero e proprio gioiello di arte romanico-lombarda.&#13;
&#13;
La sua leggendaria connessione col Santo monaco irlandese e le sue origini, X oppure XII sec., sono ancora motivo di ricerca da parte degli storici dell'arte e ciò aumenta il fascino nella scoperta di questo edificio.</Description>
			<Description xml:lang="en" formatted="true">
				<![CDATA[La chiesa di San Colombano è situata sul mitico percorso utilizzato da generazioni di monaci, pellegrini e cavalieri, che giungevano da tutta Europa per recarsi a pregare sulle spoglie mortali del Santo, conservate a Bobbio, ed è un vero e proprio gioiello di arte romanico-lombarda.

La sua leggendaria connessione col Santo monaco irlandese e le sue origini, X oppure XII sec., sono ancora motivo di ricerca da parte degli storici dell'arte e ciò aumenta il fascino nella scoperta di questo edificio.]]>
			</Description>
			<Description xml:lang="it">La chiesa di San Colombano è situata sul mitico percorso utilizzato da generazioni di monaci, pellegrini e cavalieri, che giungevano da tutta Europa per recarsi a pregare sulle spoglie mortali del Santo, conservate a Bobbio, ed è un vero e proprio gioiello di arte romanico-lombarda.&#13;
&#13;
La sua leggendaria connessione col Santo monaco irlandese e le sue origini, X oppure XII sec., sono ancora motivo di ricerca da parte degli storici dell'arte e ciò aumenta il fascino nella scoperta di questo edificio.</Description>
			<Description xml:lang="it" formatted="true">
				<![CDATA[La chiesa di San Colombano è situata sul mitico percorso utilizzato da generazioni di monaci, pellegrini e cavalieri, che giungevano da tutta Europa per recarsi a pregare sulle spoglie mortali del Santo, conservate a Bobbio, ed è un vero e proprio gioiello di arte romanico-lombarda.

La sua leggendaria connessione col Santo monaco irlandese e le sue origini, X oppure XII sec., sono ancora motivo di ricerca da parte degli storici dell'arte e ciò aumenta il fascino nella scoperta di questo edificio.]]>
			</Description>
			<Type>image</Type>
			<MimeType>image/jpeg</MimeType>
			<Uri>
				<![CDATA[https://teknet-e015.s3.eu-south-1.wasabisys.com/media-resources/moTbCKgIdNkXWCdcWrltJiviawBfXVotXiRjh863.jpg?X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=3PEEKB9MMWXU4JAQQM3R%2F20260506%2Feu-south-1%2Fs3%2Faws4_request&X-Amz-Date=20260506T141847Z&X-Amz-SignedHeaders=host&X-Amz-Expires=172800&X-Amz-Signature=88a54bd407bafb92ee1c85b9be4a0630428ba3b513415226de4e35021ce1ada8]]>
			</Uri>
		</MediaResource>
		<Geolocation>
			<Id>146</Id>
			<Geocoordinates>
				<XCoord>9.52787</XCoord>
				<YCoord>45.57508</YCoord>
				<SRSCode></SRSCode>
			</Geocoordinates>
		</Geolocation>
	</Venue>"""
    
    # Parse XML and extract data
    venue_data = parse_venue_xml(xml_content)
    
    # Write to CSV
    write_to_csv(venue_data, 'venues.csv')
    
    # Print extracted data for verification
    print("\nExtracted data:")
    for key, value in venue_data.items():
        if value:
            print(f"  {key}: {value}")

if __name__ == "__main__":
    main()
