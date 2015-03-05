package test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.Schema;

public class Neo4jTest {

	private static enum RelTypes implements RelationshipType {
	    LIKES
	}

	private static final int USER_COUNT = 100000;
	private static final int MAX_CARDINALITY = 500;
	private static final float REL_PROBABILITY = 0.1f;
	
	private static final Label userLabel = DynamicLabel.label("user");
	
	private GraphDatabaseService graphDb;
	
	public Neo4jTest(GraphDatabaseService graphDb) {
		this.graphDb = graphDb;
	}

	public static void main(String[] args) {
		System.out.println("Creando base de datos");
		GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder("graph-data").newGraphDatabase();
		if (graphDb == null) {
			System.out.println("No se ha podido crear la base de datos");
			System.exit(0);
		}
		
		Neo4jTest t = new Neo4jTest(graphDb);
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

		
		idUser = rand.nextInt(Neo4jTest.USER_COUNT);
		System.out.println("EN CYPHER: Buscando usuarios similares para el usuario " + idUser);
		start = System.currentTimeMillis();
		t.findWithCypherSimilarLikesForUser(idUser);
		System.out.println("Milisegundos utilizados: " + String.valueOf(System.currentTimeMillis() - start));

		System.out.println("EN CODIGO: Buscando usuarios similares para el usuario " + idUser);
		start = System.currentTimeMillis();
		t.findSimilarLikesForUser(idUser);
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
		Result result = graphDb.execute( "match (n:user{id:{userId}})-[r:LIKES]->(m:user{id:{anotherId}})-[r2:LIKES]->(n) return n.id limit 1", params );
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
		Result result = graphDb.execute( "match (n:user{id:{userId}})-[r:LIKES]->(m:user)-[r2:LIKES]->(n) return m.id limit 1", params );
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
		Result result = graphDb.execute( "match (n:user)-[r:LIKES]->(m:user)-[r2:LIKES]->(n) return n.id, m.id limit 1" );
		if (result.hasNext()) {
			Map<String,Object> entry = result.next();
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
		ResourceIterator<Node> nodes = graphDb.findNodes(userLabel);
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
		ResourceIterator<Node> nodes = graphDb.findNodes(userLabel);
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
		ResourceIterator<Node> nodes = graphDb.findNodes(userLabel);
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

	
	private void findWithCypherSimilarLikesForUser(int idUser) {
		Transaction tx = graphDb.beginTx();
		Map<String,Object> params = new HashMap<String,Object>(1);
		params.put("userId", Integer.valueOf(idUser));
		Result result = graphDb.execute("match (n:user{id:{userId}})<-[r:LIKES]-(m:user)-[r2:LIKES]->(t:user) where t <> n return distinct t.id, count(t) as num order by num desc, t.id asc limit 5", params );
		ResourceIterator<Integer> ids = result.columnAs("t.id");
		if (ids.hasNext()) {
			System.out.print("Se han encontrado los siguientes usuarios similares para el usuario " + idUser + ": ");
			while (ids.hasNext()) {
				Integer id = ids.next();
				System.out.print(id + " ");
			}
			System.out.println();
		} else {
			System.out.println("No se ha encontrado ningún usuario similar para el usuario " + idUser);
		}
		tx.success();
		tx.close();
	}
	private void findSimilarLikesForUser(int idUser) {
		Transaction tx = graphDb.beginTx();
		Node a = findUserById(idUser);
		List<Node> likers = new LinkedList<Node>();
		Iterator<Relationship> aRels = a.getRelationships(RelTypes.LIKES, Direction.INCOMING).iterator();
		while (aRels.hasNext()) {
			Relationship r = aRels.next();
			Node m = r.getStartNode();
			likers.add(m);
		}
		System.out.println("Numero de usuarios a los que les ha gustado el usuario: " + likers.size());
		Map<Integer,Integer> similar = new HashMap<>(likers.size() * 100);
		for (Node m : likers) {
			Iterator<Relationship> mRels = m.getRelationships(RelTypes.LIKES, Direction.OUTGOING).iterator();
			while (mRels.hasNext()) {
				Relationship r2 = mRels.next();
				Node t = r2.getEndNode();
				if (t.equals(a)) continue;
				Integer tId = (Integer)t.getProperty("id");
				if (similar.containsKey(tId)) {
					similar.put(tId, Integer.valueOf(similar.get(tId) + 1));
				} else {
					similar.put(tId, Integer.valueOf(1));
				}
			}
		}
		System.out.println("Numero de usuarios similares encontrados: " + similar.size());
		if (similar.size() > 0) {
			@SuppressWarnings("unchecked")
			Entry<Integer,Integer>[] entries = new Entry[similar.entrySet().size()];
			entries = similar.entrySet().toArray(entries);
			Arrays.sort(entries, new Comparator<Entry>() {
				@Override
				public int compare(Entry arg0, Entry arg1) {
					Entry<Integer,Integer> a0 = (Entry<Integer,Integer>)arg0;
					Entry<Integer,Integer> a1 = (Entry<Integer,Integer>)arg1;
					int c1 = a1.getValue().compareTo(a0.getValue());
					if (c1 != 0) return c1;
					else return a0.getKey().compareTo(a1.getKey());
				}
			});
			System.out.print("Se han encontrado los siguientes usuarios similares para el usuario " + idUser + ": ");
			int limit = entries.length > 5 ? 5 : entries.length;
			for (int i = 0; i < limit; i++) {
				System.out.print(entries[i].getKey() + "(" + entries[i].getValue() + ") ");
			}
			System.out.println();
		} else {
			System.out.println("No se ha encontrado ningún usuario similar para el usuario " + idUser);
		}
		tx.success();
		tx.close();
	}


	private Node findUserById(int id) {
		return graphDb.findNode(userLabel, "id", Integer.valueOf(id));
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
					if (numRels % 10000 == 0) {
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
