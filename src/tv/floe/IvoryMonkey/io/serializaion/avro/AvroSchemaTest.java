package tv.floe.IvoryMonkey.io.serializaion.avro;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;

import org.apache.avro.Schema;
//
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonDecoder;
//import org.apache.avro

/**
 * 
 * This Avro test is not meant to do much more than demonstrate the basics of avro serde.
 * 
 * Eventually I want to do some of my own comparisons of avro and other serde techniques to do my own perf tests.
 * 
 */
public class AvroSchemaTest {

		
		
		public static void testAvro() {

	/*		
	{
		"type": "array", 
		"items": 
			{ "name": "Rec1", 
			"type": "record", 
			"fields" : [
	       		{
	       			"name": "Array1", 
	       			"type": 
	       				{ 
	       					"type": "array", 
	       					"items": 
	       						{
	       							"name": "Rec2", 
	       							"type":"record", 
	       							"fields" : 
	       								[
	                   						{ 
	                   							"name": "Scalar", 
	                   							"type": "string"
	                   						}
	                   					]
	                   				}
	                   			}
	                   		}
	                   	]
	               }
	}	
			*/

			
	/*		
	          							"name": "Rec2", 
	           							"type":"record", 
	           							"fields" : 
	           								[
	                       						{ 
	                       							"name": "Scalar", 
	                       							"type": "string"
	                       						}
	                       					]		
	*/		
		    String schemaDescription =
	            " {    \n" +
	                    " \"name\": \"FacebookUser\", \n" +
	                    " \"type\": \"record\",\n" +
	                    " \"fields\": [\n" +
	                    "   {\"name\": \"name\", \"type\": \"string\"},\n" +
	                    "   {\"name\": \"num_likes\", \"type\": \"int\"},\n" +
	                    "   {\"name\": \"num_photos\", \"type\": \"int\"},\n" +
	                    "   {\"name\": \"num_groups\", \"type\": \"int\"}, \n" +

	                    " { \"name\": \"Descendants\", " +
	           			" \"type\": 	{ 	\"type\": \"array\", \"items\": { \"name\": \"str\", \"type\": \"string\" } } " +
	                    "} " +
	                    
	                    " ]\n" +
	                    "}";
	// { \"type\": \"array\", \"items\": \"int\"} 
	    Schema s = null; 
	    try {
	    	s = Schema.parse(schemaDescription);
	    } catch (Exception e) {
	    	System.out.println( "err: " + e );
	    }
			
	    ArrayList<String> ar = new ArrayList<String>();
	    ar.add( "alpha");
	    ar.add( "alpha");
	    ar.add( "alpha");
	    ar.add( "beta");
	    ar.add( "betaalphagamma");
			
			ByteArrayOutputStream bao = new ByteArrayOutputStream();
		    GenericDatumWriter w = new GenericDatumWriter(s);
		    Encoder e = new BinaryEncoder(bao);
		    //e.init(new FileOutputStream(new File("test_data.avro")));
		 
		    GenericRecord r = new GenericData.Record(s);
		    r.put("name", new org.apache.avro.util.Utf8("Doctor Who"));
		    r.put("num_likes", 1);
		    r.put("num_photos", 0);
		    r.put("num_groups", 423);
		    r.put("Descendants", ar);
		    
		    //ArrayList<String> foo;
		    
		 
		    try {
				w.write(r, e);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		    //e.flush();	
		    
		    
		    
		    System.out.println( "bytes:" + bao.size() );
			
		    
		    //Decoder d = BinaryDecoder( bao );
		    
		    GenericDatumReader<GenericRecord> r_out = new GenericDatumReader<GenericRecord>(s);
		    //Decoder decoder = new JsonDecoder(s, new FileInputStream(new File("test_data_json.avro")));
		    
		    Decoder decoder = null;
			decoder = new BinaryDecoder( new ByteArrayInputStream( bao.toByteArray() ) );
		    
		    GenericRecord rec = null;
			try {
				rec = (GenericRecord)r_out.read(null, decoder);
				
				System.out.println( "field: " + rec.get("num_groups") );
				
				GenericData.Array<org.apache.avro.util.Utf8> arOut = (GenericData.Array<org.apache.avro.util.Utf8>)rec.get("Descendants");
				
				System.out.println( "\ncount: " + arOut.size() );
				
				//Charset csets = Charset.forName("UTF-8");
				
				
				
				for ( int z = 0; z< arOut.size(); z++ ) {
					
					System.out.println( "el: " + arOut.get(z).toString() );
					
				}
				
				
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		    if (s.equals(rec.getSchema())) {
		        // handle regular fields
		    	
		    	System.out.println( "schema is same" );
		    	
		    	int num_groups = (Integer)rec.get("num_groups");
		    	String n = (String) rec.get("name").toString();
		    	
		    	System.out.println( "int: " + num_groups + ", name: " + n );
		    	
		    	
		    	
		    	
		    } else {
		        // handle differences
		    	
		    	System.out.println( "schema is NOT same" );
		    	
		    }	    
		    
		    
		}
		
		/**
		 * @param args
		 */
		public static void main(String[] args) {
			// TODO Auto-generated method stub
			
			
			testAvro();

		}

	}
