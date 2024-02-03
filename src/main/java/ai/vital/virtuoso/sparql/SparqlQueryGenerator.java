package ai.vital.virtuoso.sparql;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import ai.vital.vitalsigns.datatype.Truth;
import ai.vital.vitalsigns.model.property.URIProperty;
import ai.vital.vitalsigns.ontology.VitalCoreOntology;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.XSD;


public class SparqlQueryGenerator {

	static DateFormat xsdDateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	
	static { 
		xsdDateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
	}
	
	public static String convertValue(Object value) throws Exception {
		return convertValue(value, null);
	}
	
	public static String convertValue(Object value, String xsdPrefix) throws Exception {
		
		if(value instanceof URIProperty) {
			
			return "<" + value.toString() + ">";
			
		} else {

			String literal = null;
			
			Resource xsdR = null;	
			
			if(value instanceof Boolean) {
				literal = value.toString();
				xsdR = XSD.xboolean;
				return literal;
			} else if(value instanceof Truth) {
				
				literal = value.toString();
				xsdR = VitalCoreOntology.truth;
				
			} else if(value instanceof String) {
			
				//encode to n-triples
			    literal = encodeString((String) value);
			
				xsdR = XSD.xstring;
					
				//			\\ 	US-ASCII backslash character (decimal 92, #x5c)
				//\" 	US-ASCII double quote (decimal 34, #x22)
				//\n 	US-ASCII linefeed (decimal 10, #xA) - lf character
				//\r 	US-ASCII carriage return (decimal 13, #xD) - cr character
				//\t 	US-ASCII horizontal tab (decimal 9, #x9) - tab character	
				//			
			} else if(value instanceof Integer) {
			
				literal = "" + value;
				xsdR = XSD.xint;	
				return literal;
			
			} else if( value instanceof Long) {
			
				literal = "" + value;
			
				xsdR = XSD.xlong;
				
				return literal;
			} else if( value instanceof Double) {
			
				literal = "" + value;

				xsdR = XSD.xdouble;
				return literal;
								
			} else if( value instanceof Float ) {
			
				literal = "" + value;
				
				xsdR = XSD.xfloat;
				return literal;
			
			} else if( value instanceof Date) {
				
				// GregorianCalendar gregorianCalendar = new GregorianCalendar();
				// gregorianCalendar.setTime((Date) value);
				// XSDDateTime dt = new XSDDateTime(gregorianCalendar);
				
				literal = xsdDateTimeFormat.format((Date)value);
				xsdR = XSD.dateTime;
				// return literal;
				
			} else {
			
				throw new Exception("Unsupported data type: " + value.getClass().getCanonicalName());
				
			}
			
			
//			
//		} else if(XSD.xint.equals(range)) {
//			
//			pClass = Integer.class;
//			
//		} else if(XSD.xlong.equals(range)) {
//		
//			pClass = Long.class;
//		
//		} else if(XSD.xdouble.equals(range)) {
//			
//			pClass = Double.class;
//			
//		} else if(XSD.xfloat.equals(range)) {
//			
//			pClass = Float.class;
//			
//		} else {
//			throw new Exception("Unsupported data type: " + range.getURI());
//		}
		
			return "\"" + literal + "\"^^" +
				( xsdPrefix != null ? 
						(xsdPrefix + ":" + xsdR.getLocalName()) : 
						("<" + xsdR.getURI() + ">" )
				)
			;
		}
		
	}

	public static String encodeString(String literal) {
		//encode to n-triples
//		literal = literal
//			.replace("\\", "\\\\")
//			.replace("\r", "\\r")
//			.replace("\t", "\\t")
//			.replace("\n", "\\n")
//			.replace("\"", "\\\"");
		StringBuilder sb = new StringBuilder();
		stringEsc(sb, literal, true);
		return sb.toString();
	}
	
	static boolean applyUnicodeEscapes = false ;
	
	public static void stringEsc(StringBuilder sbuff, String s, boolean singleLineString)
	{
		int len = s.length() ;
		for (int i = 0; i < len; i++) {
			char c = s.charAt(i);
		
			// Escape escapes and quotes
			if (c == '\\' || c == '"' )
			{
				sbuff.append('\\') ;
				sbuff.append(c) ;
				continue ;
			}
					
			// Characters to literally output.
			// This would generate 7-bit safe files
//            if (c >= 32 && c < 127)
//            {
//                sbuff.append(c) ;
//                continue;
//            }
		
			// Whitespace
			if ( singleLineString && ( c == '\n' || c == '\r' || c == '\f' || c == '\t' ) )
			{
				if (c == '\n') sbuff.append("\\n");
				if (c == '\t') sbuff.append("\\t");
				if (c == '\r') sbuff.append("\\r");
				if (c == '\f') sbuff.append("\\f");
				continue ;
			}
					
			// Output as is (subject to UTF-8 encoding on output that is)
				
			if ( ! applyUnicodeEscapes )
				sbuff.append(c) ;
			else
			{
				// Unicode escapes
				// c < 32, c >= 127, not whitespace or other specials
				if ( c >= 32 && c < 127 )
				{
					sbuff.append(c) ;
				}
				else
				{
					String hexstr = Integer.toHexString(c).toUpperCase();
					int pad = 4 - hexstr.length();
					sbuff.append("\\u");
					for (; pad > 0; pad--)
						sbuff.append("0");
					sbuff.append(hexstr);
				}
			}
		}
	}
			
}
