package test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;

public class Neo4jTest {

	public static void main(String[] args) {
		GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase("c:/testgraph");
		ExecutionEngine ee = new ExecutionEngine(graphDb);
		Transaction tx = null;
		try {
			try {
				tx = graphDb.beginTx();
				Iterable<IndexDefinition> existing = graphDb.schema().getIndexes(DynamicLabel.label("User"));
				if (existing == null || !existing.iterator().hasNext())
					graphDb.schema().indexFor(DynamicLabel.label("User")).on("name").create();
				tx.success();
				tx.close();
				tx = graphDb.beginTx();
				graphDb.schema().awaitIndexesOnline(10, TimeUnit.SECONDS);
				tx.success();
				tx.close();
			} catch(Exception e) {
				if (tx != null) tx.failure();
				tx.close();
				throw new RuntimeException(e);
			}
			
			Thread.sleep(1000);
			
			tx = graphDb.beginTx();
			
			Label type = DynamicLabel.label("User");

			for (int i = 0; i < 1000; i++) {
				Node a = graphDb.createNode(type);
				a.setProperty("name", "Pepe"+i);
			}
			for (int i = 0; i < 1000; i++) {
				Node b = graphDb.createNode(type);
				b.setProperty("name", "María"+i);
			}
			for (int i = 0; i < 200; i++) {
				long rand1 = Math.round(Math.random() * i);
				long rand2 = Math.round(Math.random() * i);
				Node a = graphDb.findNodesByLabelAndProperty(type, "name", "Pepe"+rand1).iterator().next();
				Node b = graphDb.findNodesByLabelAndProperty(type, "name", "María"+rand2).iterator().next();
				a.createRelationshipTo(b, Relationships.LIKES);
			}
			for (int i = 0; i < 200; i++) {
				long rand1 = Math.round(Math.random() * i);
				long rand2 = Math.round(Math.random() * i);
				Node a = graphDb.findNodesByLabelAndProperty(type, "name", "Pepe"+rand1).iterator().next();
				Node b = graphDb.findNodesByLabelAndProperty(type, "name", "María"+rand2).iterator().next();
				b.createRelationshipTo(a, Relationships.LIKES);
			}
			
			tx.success();
			tx.close();
			
			Thread.sleep(1000);
			
			tx = graphDb.beginTx();

			// warmup
			String q = "match (a:User)-[r]->(b:User)-[r2]->(a:User) where a.name={from} and b.name={to} return a.name";
			long rand1 = Math.round(Math.random() * 100) % 100;
			long rand2 = Math.round(Math.random() * 100) % 100;
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("from", "Pepe"+rand1);
			params.put("to", "María"+rand2);
			for (int i = 0; i < 100; i++) {
				ee.execute(q, params);
			}

			long start = System.currentTimeMillis();
			Iterator<Map<String,Object>> results = ee.execute(q, params).iterator();
			if (results != null && results.hasNext()) System.out.println("Encontrado un resultado!");
			else System.out.println("No se ha encontrado relación :(");
			long end = System.currentTimeMillis();
			System.out.println("Tiempo de ejecución de la consulta: " + String.valueOf(end - start));

			tx.success();
		} catch(Exception e) {
			if (tx != null) tx.failure();
			tx.close();
			throw new RuntimeException(e);
		} finally {
			if (graphDb != null) graphDb.shutdown();
		}
	}

}
