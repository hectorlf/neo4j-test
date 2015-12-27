package test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Node;
import org.neo4j.driver.v1.ResultCursor;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Values;

public class Neo4jTest {

	private static final int USER_COUNT = 100;
	private static final int MAX_CARDINALITY = 5;
	private static final float REL_PROBABILITY = 0.1f;
	
	private final Driver driver;
	
	public Neo4jTest(Driver driver) {
		this.driver = driver;
	}

	public static void main(String[] args) {
		System.out.println("Creando base de datos");
		/*
		File dir = new File("build/graph-data");
		dir.mkdirs();
		InProcessServerBuilder serverBuilder = new InProcessServerBuilder(dir);
		ServerControls server = serverBuilder.newServer();
		*/
		Driver driver = GraphDatabase.driver("bolt://localhost");
		Neo4jTest t = new Neo4jTest(driver);
		
		System.out.println("Inicializando base de datos");
		t.initialize();

		for(int x = 0; x < 5; x++) {
		System.out.println();
		long start;
		
		Random rand = new Random(System.currentTimeMillis());
		int idA = rand.nextInt(Neo4jTest.USER_COUNT);
		int idB = rand.nextInt(Neo4jTest.USER_COUNT);
		System.out.println("Buscando si existe una relacion reflexiva entre los usuarios " + idA + " y " + idB);
		start = System.currentTimeMillis();
		t.findWithCypherReflexiveRelationForUsers(idA, idB);
		System.out.println("Milisegundos utilizados: " + String.valueOf(System.currentTimeMillis() - start));
		
		
		int idUser = rand.nextInt(Neo4jTest.USER_COUNT);
		System.out.println("Buscando si existe una relacion reflexiva para el usuario " + idUser);
		start = System.currentTimeMillis();
		t.findWithCypherReflexiveRelationForUser(idUser);
		System.out.println("Milisegundos utilizados: " + String.valueOf(System.currentTimeMillis() - start));
		
		
		System.out.println("Buscando cualquier relacion reflexiva");
		start = System.currentTimeMillis();
		t.findWithCypherAnyReflexiveRelation();
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
		System.out.println("Buscando usuarios similares para el usuario " + idUser);
		start = System.currentTimeMillis();
		t.findWithCypherSimilarLikesForUser(idUser);
		System.out.println("Milisegundos utilizados: " + String.valueOf(System.currentTimeMillis() - start));

		
		System.out.println();
		}
		System.out.println("Cerrando base de datos");
		/*
		server.close();
		*/
	}
	

	private void findWithCypherReflexiveRelationForUsers(int idA, int idB) {
		Map<String,Object> params = new HashMap<String,Object>(2);
		params.put("userId", Integer.valueOf(idA));
		params.put("anotherId", Integer.valueOf(idB));
		/*
		Result result = graphDb.execute( "match (n:user{id:{userId}})-[r:LIKES]->(m:user{id:{anotherId}})-[r2:LIKES]->(n) return n.id limit 1", params );
		ResourceIterator<Integer> ids = result.columnAs("n.id");
		if (ids.hasNext()) {
			System.out.println("Se ha encontrado una relacion reflexiva entre los usuarios " + idA + " y " + idB);
		} else {
			System.out.println("No se ha encontrado ninguna relacion reflexiva para los usuarios " + idA + " y " + idB);
		}
		*/
	}

	private void findWithCypherReflexiveRelationForUser(int idUser) {
		Map<String,Object> params = new HashMap<String,Object>(1);
		params.put("userId", Integer.valueOf(idUser));
		/*
		Result result = graphDb.execute( "match (n:user{id:{userId}})-[r:LIKES]->(m:user)-[r2:LIKES]->(n) return m.id limit 1", params );
		ResourceIterator<Integer> ids = result.columnAs("m.id");
		if (ids.hasNext()) {
			Integer id = ids.next();
			System.out.println("Se ha encontrado una relacion entre los usuarios " + idUser + " y " + id);
		} else {
			System.out.println("No se ha encontrado ninguna relacion reflexiva para el usuario " + idUser);
		}
		*/
	}

	private void findWithCypherAnyReflexiveRelation() {
		/*
		Result result = graphDb.execute( "match (n:user)-[r:LIKES]->(m:user)-[r2:LIKES]->(n) return n.id, m.id limit 1" );
		if (result.hasNext()) {
			Map<String,Object> entry = result.next();
			int foundIdA = ((Integer)entry.get("n.id")).intValue();
			int foundIdB = ((Integer)entry.get("m.id")).intValue();
			System.out.println("Se ha encontrado una relacion reflexiva entre los usuarios " + foundIdA + " y " + foundIdB);
		} else {
			System.out.println("No se ha encontrado una relacion reflexiva entre ningun par de usuarios");
		}
		*/
	}
	
	private void findMostLiked() {
		int[] likes = new int[USER_COUNT];
		/*
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
		*/
	}
	private void findMostLiker() {
		int[] likes = new int[USER_COUNT];
		/*
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
		*/
	}

	
	private void findWithCypherSimilarLikesForUser(int idUser) {
		Map<String,Object> params = new HashMap<String,Object>(1);
		params.put("userId", Integer.valueOf(idUser));
		/*
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
			System.out.println("No se ha encontrado ning�n usuario similar para el usuario " + idUser);
		}
		*/
	}

	
	
	private void initialize() {
		Session ses = driver.session();
		Random rand = new Random(System.currentTimeMillis());
		
		ResultCursor result = ses.run("match (n:User) return count(n)");
		if (result.single() && result.value(0).asInt() > 0) {
			System.out.println("Ya existen usuarios en la base de datos");
			return;
		}

		System.out.println("Creando usuarios");
		for (int i = 0; i < USER_COUNT; i++) {
			ses.run("create (n:User { id: {id} })", Values.parameters("id", Integer.valueOf(i)));
			System.out.println("Usuarios creados: " + i);
			/*
			if (i % 2000 == 0) {
				tx.success();
				tx.close();
				tx = graphDb.beginTx();
				System.out.println("Usuarios creados: " + i);
			}
			*/
		}
		
		System.out.println("Creando relaciones");
		int numRels = 0;
		for (int i = 0; i < USER_COUNT; i++) {
			for (int j = 0; j < MAX_CARDINALITY; j++) {
				if (rand.nextFloat() < REL_PROBABILITY) {
					Node user2 = getNode(rand.nextInt(USER_COUNT));
					if (user2 != null) {
						ses.run("match (n:User {id:{nid}}), (m:User {id:{mid}}) with n,m create (n)-[:LIKES]->(m)", Values.parameters("nid", Integer.valueOf(i), "mid", user2.value("id").asInt()));
						numRels++;
					}
					System.out.println("Relaciones creadas: " + numRels);
				}
			}
		}
	}

	private Node getNode(Integer id) {
		Session ses = driver.session();
		ResultCursor r1 = ses.run("match (n:User) where n.id = {id} return n", Values.parameters("id", id));
		if (!r1.single()) return null;
		return r1.value(0).asNode();
	}

}