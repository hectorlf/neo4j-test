package test;

import java.io.File;
import java.util.Map;
import java.util.Random;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Node;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.ResultCursor;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.exceptions.NoSuchRecordException;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilder;
import org.neo4j.harness.TestServerBuilders;

public class Neo4jTest {

	private static final int USER_COUNT = 1000;
	private static final int MAX_CARDINALITY = 50;
	private static final float REL_PROBABILITY = 0.1f;
	
	private final Driver driver;
	
	public Neo4jTest(Driver driver) {
		this.driver = driver;
	}

	public static void main(String[] args) {
		System.out.println("Creando base de datos");
		File dir = new File("build/graph-data");
		dir.mkdirs();
		TestServerBuilder tsb = TestServerBuilders.newInProcessBuilder(dir);
		ServerControls server = tsb.newServer();
		Driver driver = GraphDatabase.driver(server.boltURI());
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
		
		server.close();
	}
	

	private void findWithCypherReflexiveRelationForUsers(int idA, int idB) {
		Session ses = driver.session();
		Map<String,Value> params = Values.parameters("userId", Integer.valueOf(idA), "anotherId", Integer.valueOf(idB));
		ResultCursor result = ses.run("match (n:User{id:{userId}})-[:LIKES]->(m:User{id:{anotherId}})-[:LIKES]->(n) return n.id limit 1", params);
		if (result.next()) {
			System.out.println("Se ha encontrado una relacion reflexiva entre los usuarios " + idA + " y " + idB);
		} else {
			System.out.println("No se ha encontrado ninguna relacion reflexiva para los usuarios " + idA + " y " + idB);
		}
		ses.close();
	}


	private void findWithCypherReflexiveRelationForUser(int idUser) {
		Session ses = driver.session();
		Map<String,Value> params = Values.parameters("userId", Integer.valueOf(idUser));
		ResultCursor result = ses.run("match (n:User{id:{userId}})-[r:LIKES]->(m:User)-[r2:LIKES]->(n) return m.id limit 1", params);
		try {
			Integer id = result.single().get(0).asInt();
			System.out.println("Se ha encontrado una relacion entre los usuarios " + idUser + " y " + id);
		} catch(NoSuchRecordException nsre) {
			System.out.println("No se ha encontrado ninguna relacion reflexiva para el usuario " + idUser);
		}
		ses.close();
	}


	private void findWithCypherAnyReflexiveRelation() {
		Session ses = driver.session();
		ResultCursor result = ses.run("match (n:User)-[r:LIKES]->(m:User)-[r2:LIKES]->(n) return n.id, m.id limit 1");
		try {
			Record r = result.single();
			int foundIdA = r.get(0).asInt();
			int foundIdB = r.get(1).asInt();
			System.out.println("Se ha encontrado una relacion reflexiva entre los usuarios " + foundIdA + " y " + foundIdB);
		} catch(NoSuchRecordException nsre) {
			System.out.println("No se ha encontrado una relacion reflexiva entre ningun par de usuarios");
		}
		ses.close();
	}


	private void findMostLiked() {
		Session ses = driver.session();
		ResultCursor result = ses.run("match (n:User)<--() return n.id, count(*) as degree order by degree desc limit 1");
		result.first();
		System.out.println("El usuario con mayor numero de likes recibidos es " + result.get(0).asInt() + ", con " + result.get(1).asInt() + " relaciones");
		ses.close();
	}
	private void findMostLiker() {
		Session ses = driver.session();
		ResultCursor result = ses.run("match (n:User)-->() return n.id, count(*) as degree order by degree desc limit 1");
		result.first();
		System.out.println("El usuario con mayor numero de likes enviados es " + result.get(0).asInt() + ", con " + result.get(1).asInt() + " relaciones");
		ses.close();
	}


	private void findWithCypherSimilarLikesForUser(int idUser) {
	/*
		Session ses = driver.session();
		Map<String,Value> params = Values.parameters("userId", Integer.valueOf(idUser));
		ResultCursor result = ses.run("match (n:User{id:{userId}})<-[:LIKES]-(m:User)-[:LIKES]->(t:User) where t <> n return distinct t.id, count(t) as num order by num desc, t.id asc limit 5", params);
		if (result.peek() != null) {
			System.out.print("Se han encontrado los siguientes usuarios similares para el usuario " + idUser + ": ");
			while (result.next()) {
				System.out.print(result.get(0).asInt() + " ");
			}
			System.out.println();
		} else {
			System.out.println("No se ha encontrado ning�n usuario similar para el usuario " + idUser);
		}
		ses.close();
	*/
		Session ses = driver.session();
		Map<String,Value> params = Values.parameters("userId", Integer.valueOf(idUser));
		ResultCursor result = ses.run("match (n:User{id:{userId}})", params);
		if (result.size() > 0) {
			System.out.print("Not empty!");
		} else {
			System.out.print("Empty!");
		}
		ses.close();

	}



	private void initialize() {
		Session ses = driver.session();
		Transaction tx = null;
		Random rand = new Random(System.currentTimeMillis());
		
		ResultCursor result = ses.run("match (n:User) return count(n)");
		if (result.next() && result.get(0).asInt() > 0) {
			System.out.println("Ya existen usuarios en la base de datos");
			return;
		}

		System.out.println("Creando indices");
		ses.run("create index on :User(id)");
		
		System.out.println("Creando usuarios");
		tx = ses.beginTransaction();
		for (int i = 0; i < USER_COUNT; i++) {
			tx.run("create (n:User { id: {id} })", Values.parameters("id", Integer.valueOf(i)));
			if (i % 2000 == 0) {
				tx.success();
				tx.close();
				tx = ses.beginTransaction();
				System.out.println("Usuarios creados: " + i);
			}
		}
		tx.success();
		tx.close();
		System.out.println("Usuarios creados: " + USER_COUNT);
		
		System.out.println("Creando relaciones");
		int numRels = 0;
		tx = ses.beginTransaction();
		for (int i = 0; i < USER_COUNT; i++) {
			for (int j = 0; j < MAX_CARDINALITY; j++) {
				if (rand.nextFloat() < REL_PROBABILITY) {
					Node user2 = getNode(rand.nextInt(USER_COUNT));
					if (user2 != null) {
						tx.run("match (n:User {id:{nid}}), (m:User {id:{mid}}) with n,m create (n)-[:LIKES]->(m)", Values.parameters("nid", Integer.valueOf(i), "mid", user2.get("id").asInt()));
						numRels++;
						if (numRels % 5000 == 0) {
							tx.success();
							tx.close();
							tx = ses.beginTransaction();
							System.out.println("Relaciones creadas: " + numRels);
						}
					}
				}
			}
		}
		tx.success();
		tx.close();
		ses.close();
		System.out.println("Relaciones creadas: " + numRels);
	}

	private Node getNode(Integer id) {
		Session ses = null;
		try {
			ses = driver.session();
			ResultCursor r1 = ses.run("match (n:User) where n.id = {id} return n", Values.parameters("id", id));
			return r1.single().get(0).asNode();
		} catch(NoSuchRecordException nsre) {
			return null;
		} finally {
			if (ses != null && ses.isOpen()) ses.close();
		}
	}

}