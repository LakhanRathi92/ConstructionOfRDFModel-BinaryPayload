import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.ext.com.google.common.base.Predicate;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Selector;
import org.apache.jena.rdf.model.SimpleSelector;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.vocabulary.RDF;

public class test {

	
	public static int helperPayloadOffset1 = 0;
	public static int helperPayloadBytes1 = 1;
	
	public static int helperPayloadOffset2 = 1;
	public static int helperPayloadBytes2 = 3;
	
	//FastRDF Node structure
	public static String fastRDFUidUrl = "http://www.dfki.de/fastRDF/uid";
	public static String constantUri = "http://www.dfki.de/fastRDF/constantLiteral";
	
	
	public static String javaClassBoolean = "java.lang.Boolean";
	public static String javaClassInteger = "java.math.BigInteger";

	
	public static class FastRDFNode{
		private Boolean constantLiteral;
		private int uid;
		private String dataType;
		FastRDFNode(){			
		};		
	}
	
	//BlankNode -> FastRDFNode
	public static HashMap<Resource, FastRDFNode> linker = new HashMap<>();
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Model newModel = ModelFactory.createDefaultModel();	
		newModel.read("http://localhost:8000/observations.ttl",null,"TTL"); 
		
		ArrayList<Integer> publishedUIDs = new ArrayList<Integer>();
	
		publishedUIDs.add(123); // 0
		publishedUIDs.add(456); // 500
		
		byte[] b = {48,53,48,48}; //0,5,0,0
				
		Model m = generateRDFModel(newModel, publishedUIDs , b);
	
	}
	

	private static Model generateRDFModel(Model newModel, ArrayList<Integer> publishedUIDs, byte[] b) {
		
		
		//Find Blank Nodes which are FastRDF Nodes
		Selector selector = new SimpleSelector(null, newModel.getProperty(fastRDFUidUrl), (RDFNode) null);  		
		StmtIterator it =  newModel.listStatements(selector);				
		
		ArrayList<Resource> BlankFastRDFNodes = new ArrayList<>();
		
		//Identify fastRDF Blank Nodes
		while(it.hasNext()){
			Statement statementWithFastRDFuid = it.nextStatement();
			Resource Subject = statementWithFastRDFuid.getSubject();
			BlankFastRDFNodes.add(Subject);		
		}
		it.close();
		
		
		
		//Resolve Blank Nodes
		ArrayList<FastRDFNode> Nodes = new ArrayList<>();
		
		for(Resource sub: BlankFastRDFNodes){
			Selector selector2 = new SimpleSelector(sub, null, (RDFNode) null);  		
			StmtIterator it2 =  newModel.listStatements(selector2);				
			FastRDFNode fastRDFNode = new FastRDFNode();
			
			
			while(it2.hasNext()){
				Statement blankSubjectStatement = it2.nextStatement();
				
				//Check which statement it is and then update your fastRDFNode
				RDFNode node = blankSubjectStatement.getObject();
				
				if(node.isLiteral()){
					Literal l = node.asLiteral();
					RDFDatatype dType = l.getDatatype();
					String inJava = dType.getJavaClass().getCanonicalName();
					
					//Either Boolean or Integer
					if(inJava.equals(javaClassBoolean)){
						fastRDFNode.constantLiteral = l.getBoolean(); 
					}
					else if (inJava.equals(javaClassInteger)) {
						fastRDFNode.uid = l.getInt(); 
					}					
				}
				else{
					//node isn't a literal then check out fastRDF data type
					Resource p = blankSubjectStatement.getPredicate();
					if(p.equals(RDF.type)){
						fastRDFNode.dataType = p.getURI();
					}
				}				
				Nodes.add(fastRDFNode);
				linker.put(sub, fastRDFNode);				
			}			
			it2.close();	
		}
		
		
		
		//Replace blank nodes with data value from payload. 
		ArrayList<Statement> goIntoModel = new ArrayList<>();
		ArrayList<Statement> toBeDeleted = new ArrayList<>();
		
		for(Resource blankNodeID: BlankFastRDFNodes){
			Selector selector3 = new SimpleSelector(null, null, blankNodeID);  		
			StmtIterator it3 =  newModel.listStatements(selector3);				
					
			while(it3.hasNext()){
				Statement statementToBeChanged = it3.nextStatement(); // (blah blah blankNode) 
				FastRDFNode replacingNode = linker.get(blankNodeID);
				//temp model
				Model m = ModelFactory.createDefaultModel();	
				
				/**
				 * Further processing on fastRDF terms could be done here Discuss it with Christian
				 * */
				
				for(Integer x1: publishedUIDs){
					if(x1.equals(replacingNode.uid)){										
						//create an object to be put instead of blank node, have to identify the type too
						// and appropriately cast it					
						byte[] fromPayload = Arrays.copyOfRange(b, helperPayloadOffset1, helperPayloadBytes1);	
						String contentFromPayload = new String(fromPayload);					
						Statement state = m.createLiteralStatement(statementToBeChanged.getSubject(), statementToBeChanged.getPredicate(), contentFromPayload);	
						goIntoModel.add(state);
						toBeDeleted.add(statementToBeChanged);
					}
				}
				
			}
			it3.close();			
		}
		
		newModel.remove(toBeDeleted);
		newModel.add(goIntoModel);
		
		newModel.write(System.out, "ttl");
		
		return null;
		
	}
	
}
