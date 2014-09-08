package test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.tooling.GlobalGraphOperations;

public class Neo4jTest {

	private static enum RelTypes implements RelationshipType {
	    LIKES
	}

	private static final int USER_COUNT = 100000;
	private static final int MAX_CARDINALITY = 1000;
	private static final float REL_PROBABILITY = 0.05f;
	
	private static final Label userLabel = DynamicLabel.label("user");
	
	private GraphDatabaseService graphDb;
	private ExecutionEngine engine;
	
	public Neo4jTest(GraphDatabaseService graphDb, ExecutionEngine engine) {
		this.graphDb = graphDb;
		this.engine = engine;
	}

	public static void main(String[] args) {
		System.out.println("Creando base de datos");
		GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder("graph-data").newGraphDatabase();
		if (graphDb == null) {
			System.out.println("No se ha podido crear la base de datos");
			System.exit(0);
		}
		ExecutionEngine engine = new ExecutionEngine(graphDb);
		
		Neo4jTest t = new Neo4jTest(graphDb, engine);
		long start;

		System.out.println("Inicializando base de datos");
		t.initialize();
		
		for(int x = 0; x < 5; x++) {
		System.out.println();
		
		Random rand = new Random(System.currentTimeMillis());
		int idA = rand.nextInt(Neo4jTest.USER_COUNT);
		int idB = rand.nextInt(Neo4jTest.USER_COUNT);
		System.out.println("EN CYPHER: Buscando si existe una relacion reflexiva entre los usuarios " + idA + " y " + idB);
		start = System.currentTimeMillis();
		t.findWithCypherReflexiveRelationForUsers(idA, idB);
		System.out.println("Milisegundos utilizados: " + String.valueOf(System.currentTimeMillis() - start));
		
		System.out.println("EN CODIGO: Buscando si existe una relacion reflexiva entre los usuarios " + idA + " y " + idB);
		start = System.currentTimeMillis();
		t.findReflexiveRelationForUsers(idA, idB);
		System.out.println("Milisegundos utilizados: " + String.valueOf(System.currentTimeMillis() - start));

		
		int idUser = rand.nextInt(Neo4jTest.USER_COUNT);
		System.out.println("EN CYPHER: Buscando si existe una relacion reflexiva para el usuario " + idUser);
		start = System.currentTimeMillis();
		t.findWithCypherReflexiveRelationForUser(idUser);
		System.out.println("Milisegundos utilizados: " + String.valueOf(System.currentTimeMillis() - start));
		
		System.out.println("EN CODIGO: Buscando si existe una relacion reflexiva para el usuario " + idUser);
		start = System.currentTimeMillis();
		t.findReflexiveRelationForUser(idUser);
		System.out.println("Milisegundos utilizados: " + String.valueOf(System.currentTimeMillis() - start));

		
		System.out.println("EN CYPHER: Buscando cualquier relacion reflexiva");
		start = System.currentTimeMillis();
		t.findWithCypherAnyReflexiveRelation();
		System.out.println("Milisegundos utilizados: " + String.valueOf(System.currentTimeMillis() - start));
		
		System.out.println("EN CODIGO: Buscando cualquier relacion reflexiva");
		start = System.currentTimeMillis();
		t.findAnyReflexiveRelation();
		System.out.println("Milisegundos utilizados: " + String.valueOf(System.currentTimeMillis() - start));

		
		System.out.println("Usuario con mas relaciones entrantes");
		start = System.currentTimeMillis();
		t.findMostLiked();
		System.out.println("Milisegundos utilizados: " + String.valueOf(System.currentTimeMillis() - start));
		
		System.out.println("Usuario con mas relaciones salientes");
		start = System.currentTimeMillis();
		t.findMostLiker();
		System.out.println("Milisegundos utilizados: " + String.valueOf(System.currentTimeMillis() - start));

		System.out.println();
		}
		System.out.println("Cerrando base de datos");
		graphDb.shutdown();
	}
	

	private void findWithCypherReflexiveRelationForUsers(int idA, int idB) {
		Transaction tx = graphDb.beginTx();
		Map<String,Object> params = new HashMap<String,Object>(2);
		params.put("userId", Integer.valueOf(idA));
		params.put("anotherId", Integer.valueOf(idB));
		ExecutionResult result = engine.execute( "match (n:user{id:{userId}})-[r:LIKES]->(m:user{id:{anotherId}})-[r2:LIKES]->(n) return n.id limit 1", params );
		ResourceIterator<Integer> ids = result.columnAs("n.id");
		if (ids.hasNext()) {
			System.out.println("Se ha encontrado una relacion reflexiva entre los usuarios " + idA + " y " + idB);
		} else {
			System.out.println("No se ha encontrado ninguna relacion reflexiva para los usuarios " + idA + " y " + idB);
		}
		tx.success();
		tx.close();
	}
	private void findReflexiveRelationForUsers(int idA, int idB) {
		Transaction tx = graphDb.beginTx();
		boolean found = false;
		Node a = findUserById(idA);
		Node b = findUserById(idB);
		Iterator<Relationship> aRels = a.getRelationships(RelTypes.LIKES, Direction.OUTGOING).iterator();
		while (aRels.hasNext()) {
			Relationship r = aRels.next();
			Node n = r.getEndNode();
			if (n.getId() == b.getId()) {
				Iterator<Relationship> nRels = n.getRelationships(RelTypes.LIKES, Direction.OUTGOING).iterator();
				while (nRels.hasNext()) {
					Relationship r2 = nRels.next();
					Node m = r2.getEndNode();
					if (m.getId() == a.getId()) {
						found = true;
						break;
					}
				}
				break;
			}
		}
		if (found) {
			System.out.println("Se ha encontrado una relacion entre los usuarios " + idA + " y " + idB);
		} else {
			System.out.println("No se ha encontrado ninguna relacion reflexiva para los usuarios " + idA + " y " + idB);
		}
		tx.success();
		tx.close();
	}

	private void findWithCypherReflexiveRelationForUser(int idUser) {
		Transaction tx = graphDb.beginTx();
		Map<String,Object> params = new HashMap<String,Object>(1);
		params.put("userId", Integer.valueOf(idUser));
		ExecutionResult result = engine.execute( "match (n:user{id:{userId}})-[r:LIKES]->(m:user)-[r2:LIKES]->(n) return m.id limit 1", params );
		ResourceIterator<Integer> ids = result.columnAs("m.id");
		if (ids.hasNext()) {
			Integer id = ids.next();
			System.out.println("Se ha encontrado una relacion entre los usuarios " + idUser + " y " + id);
		} else {
			System.out.println("No se ha encontrado ninguna relacion reflexiva para el usuario " + idUser);
		}
		tx.success();
		tx.close();
	}
	private void findReflexiveRelationForUser(int idUser) {
		Transaction tx = graphDb.beginTx();
		boolean found = false;
		int foundId = 0;
		Node a = findUserById(idUser);
		Iterator<Relationship> aRels = a.getRelationships(RelTypes.LIKES, Direction.OUTGOING).iterator();
		while (aRels.hasNext()) {
			Relationship r = aRels.next();
			Node n = r.getEndNode();
			Iterator<Relationship> nRels = n.getRelationships(RelTypes.LIKES, Direction.OUTGOING).iterator();
			while (nRels.hasNext()) {
				Relationship r2 = nRels.next();
				Node m = r2.getEndNode();
				if (m.getId() == a.getId()) {
					found = true;
					foundId = ((Integer)n.getProperty("id")).intValue();
					break;
				}
			}
			if (found) break;
		}
		if (found) {
			System.out.println("Se ha encontrado una relacion entre los usuarios " + idUser + " y " + foundId);
		} else {
			System.out.println("No se ha encontrado ninguna relacion reflexiva para el usuario " + idUser);
		}
		tx.success();
		tx.close();
	}

	private void findWithCypherAnyReflexiveRelation() {
		Transaction tx = graphDb.beginTx();
		ExecutionResult result = engine.execute( "match (n:user)-[r:LIKES]->(m:user)-[r2:LIKES]->(n) return n.id, m.id limit 1" );
		ResourceIterator<Map<String,Object>> ids = result.iterator();
		if (ids.hasNext()) {
			Map<String,Object> entry = ids.next();
			int foundIdA = ((Integer)entry.get("n.id")).intValue();
			int foundIdB = ((Integer)entry.get("m.id")).intValue();
			System.out.println("Se ha encontrado una relacion reflexiva entre los usuarios " + foundIdA + " y " + foundIdB);
		} else {
			System.out.println("No se ha encontrado una relacion reflexiva entre ningun par de usuarios");
		}
		tx.success();
		tx.close();
	}
	private void findAnyReflexiveRelation() {
		Transaction tx = graphDb.beginTx();
		boolean found = false;
		int foundIdA = 0;
		int foundIdB = 0;
		Set<Long> trimmedNodes = new HashSet<Long>(100);
		GlobalGraphOperations ops = GlobalGraphOperations.at(graphDb);
		ResourceIterator<Node> nodes = ops.getAllNodesWithLabel(userLabel).iterator();
		while (nodes.hasNext()) {
			Node n = nodes.next();
			if (trimmedNodes.contains(Long.valueOf(n.getId()))) continue;
			Iterator<Relationship> nRels = n.getRelationships(RelTypes.LIKES, Direction.OUTGOING).iterator();
			while (nRels.hasNext()) {
				Relationship r = nRels.next();
				Node m = r.getEndNode();
				Iterator<Relationship> mRels = m.getRelationships(RelTypes.LIKES, Direction.OUTGOING).iterator();
				while (mRels.hasNext()) {
					Relationship r2 = mRels.next();
					Node u = r2.getEndNode();
					if (u.getId() == n.getId()) {
						found = true;
						foundIdA = ((Integer)n.getProperty("id")).intValue();
						foundIdB = ((Integer)m.getProperty("id")).intValue();
						break;
					}
				}
				if (found) break;
				else trimmedNodes.add(m.getId());
			}
			if (found) break;
		}
		if (found) {
			System.out.println("Se ha encontrado una relacion entre los usuarios " + foundIdA + " y " + foundIdB);
		} else {
			System.out.println("No se ha encontrado una relacion reflexiva entre ningun par de usuarios");
		}
		tx.success();
		tx.close();
	}
	
	private void findMostLiked() {
		Transaction tx = graphDb.beginTx();
		int[] likes = new int[USER_COUNT];
		GlobalGraphOperations ops = GlobalGraphOperations.at(graphDb);
		ResourceIterator<Node> nodes = ops.getAllNodesWithLabel(userLabel).iterator();
		while (nodes.hasNext()) {
			Node n = nodes.next();
			likes[(int)n.getId()] = n.getDegree(RelTypes.LIKES, Direction.INCOMING);
		}
		int highestValue = 0;
		int highestLiked = 0;
		for (int i = 0; i < likes.length; i++) {
			if (highestValue < likes[i]) {
				highestValue = likes[i];
				highestLiked = i;
			}
		}
		System.out.println("El usuario con mayor numero de likes recibidos es " + highestLiked + ", con " + highestValue + " relaciones");
		tx.success();
		tx.close();
	}
	private void findMostLiker() {
		Transaction tx = graphDb.beginTx();
		int[] likes = new int[USER_COUNT];
		GlobalGraphOperations ops = GlobalGraphOperations.at(graphDb);
		ResourceIterator<Node> nodes = ops.getAllNodesWithLabel(userLabel).iterator();
		while (nodes.hasNext()) {
			Node n = nodes.next();
			likes[(int)n.getId()] = n.getDegree(RelTypes.LIKES, Direction.OUTGOING);
		}
		int highestValue = 0;
		int highestLiker = 0;
		for (int i = 0; i < likes.length; i++) {
			if (highestValue < likes[i]) {
				highestValue = likes[i];
				highestLiker = i;
			}
		}
		System.out.println("El usuario con mayor numero de likes realizados es " + highestLiker + ", con " + highestValue + " relaciones");
		tx.success();
		tx.close();
	}
	


	private Node findUserById(int id) {
		ResourceIterable<Node> results = graphDb.findNodesByLabelAndProperty(userLabel, "id", Integer.valueOf(id));
		if (!results.iterator().hasNext()) return null;
		return results.iterator().next();
	}

	
	
	private void initialize() {
		Transaction tx = null;
		try {
			tx = graphDb.beginTx();
			Schema schema = graphDb.schema();
			if (schema.getIndexes(userLabel) == null || !schema.getIndexes(userLabel).iterator().hasNext()) {
				System.out.println("Creando indices");
				schema.constraintFor(userLabel).assertPropertyIsUnique("id").create();
			}
			tx.success();
			tx.close();

			System.out.println("Comprobando usuarios");
			checkAndCreateUsers();
			
			System.out.println("Esperando a que los indices esten online");
			tx = graphDb.beginTx();
			graphDb.schema().awaitIndexesOnline(10, TimeUnit.SECONDS);
			tx.success();
			tx.close();
		} catch(Exception e) {
			if (tx != null) {
				tx.failure();
				tx.close();
			}
		}
	}

	private void checkAndCreateUsers() {
		Transaction tx = graphDb.beginTx();
		Node temp = null;
		Random rand = new Random(System.currentTimeMillis());
		try {
			temp = graphDb.getNodeById(0);
		} catch(NotFoundException e) {
		} finally {
			tx.success();
			tx.close();
		}
		
		if (temp != null) {
			System.out.println("Ya existen usuarios en la base de datos");
			return;
		}

		System.out.println("Creando usuarios");
		tx = graphDb.beginTx();
		for (int i = 0; i < USER_COUNT; i++) {
			Node user = graphDb.createNode(userLabel);
			user.setProperty("id", Integer.valueOf(i));
			if (i % 2000 == 0) {
				tx.success();
				tx.close();
				tx = graphDb.beginTx();
				System.out.println("Usuarios creados: " + i);
			}
		}
		System.out.println("Usuarios creados: " + USER_COUNT);
		tx.success();
		tx.close();
		
		System.out.println("Creando relaciones");
		int numRels = 0;
		tx = graphDb.beginTx();
		for (int i = 0; i < USER_COUNT; i++) {
			Node user1 = graphDb.getNodeById(i);
			for (int j = 0; j < MAX_CARDINALITY; j++) {
				if (rand.nextFloat() < REL_PROBABILITY) {
					Node user2 = graphDb.getNodeById(rand.nextInt(USER_COUNT));
					user1.createRelationshipTo(user2, RelTypes.LIKES);
					if (numRels % 2000 == 0) {
						tx.success();
						tx.close();
						tx = graphDb.beginTx();
						System.out.println("Relaciones creadas: " + numRels);
					}
					numRels += 1;
				}
			}
		}
		System.out.println("Relaciones creadas: " + numRels);
		tx.success();
		tx.close();
	}

}
