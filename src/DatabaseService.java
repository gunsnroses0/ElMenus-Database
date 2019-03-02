import com.rabbitmq.client.*;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.bson.*;
import com.mongodb.client.MongoDatabase;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

import Commands.Command;
import Commands.DbCommand;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
//import org.postgresql.ds.PGPoolingDataSource;
//import org.postgresql.jdbc3.Jdbc3PoolingDataSource;
import org.postgresql.ds.PGSimpleDataSource;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;

public class DatabaseService {

	private static final String RPC_QUEUE_NAME = "database-request";
	private static HikariDataSource source;

	public static void main(String[] argv) {

		MongoClientURI uri = new MongoClientURI(
				"mongodb://admin:admin@cluster0-shard-00-00-nvkqp.gcp.mongodb.net:27017,cluster0-shard-00-01-nvkqp.gcp.mongodb.net:27017,cluster0-shard-00-02-nvkqp.gcp.mongodb.net:27017/El-Menus?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&retryWrites=true");

		MongoClient mongoClient = new MongoClient(uri);
		MongoDatabase database = mongoClient.getDatabase("El-Menus");
		
		System.out.println("Connected");
		database.createCollection("mycollection");
		MongoCollection<Document> orders = database.getCollection("Orders");
		
		Document document = new Document("title", "MongoDB").append("id", 1).append("description", "database")
				.append("likes", 100).append("url", "http://www.tutorialspoint.com/mongodb/")
				.append("by", "tutorials point");
		orders.insertOne(document);		
		System.out.println(orders);
		System.out.println(document);



		// initialize thread pool of fixed size
		final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);
		initDBPool();

		ConnectionFactory factory = new ConnectionFactory();
		String host = System.getenv("RABBIT_MQ_SERVICE_HOST");
		factory.setHost(host);
		Connection connection = null;
		try {
			connection = factory.newConnection();
			final Channel channel = connection.createChannel();

			channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);

			System.out.println(" [x] Awaiting RPC requests");

			Consumer consumer = new DefaultConsumer(channel) {
				@Override
				public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
						byte[] body) throws IOException {
					AMQP.BasicProperties replyProps = new AMQP.BasicProperties.Builder()
							.correlationId(properties.getCorrelationId()).build();
					System.out.println("Responding to corrID: " + properties.getCorrelationId());

					String response = "";
					java.sql.Connection conn = null;
					try {
						String message = new String(body, "UTF-8");
//                        Command cmd = (Command) Class.forName("commands."+"DbCommand").newInstance();
						Command cmd = new DbCommand();
						conn = source.getConnection();

						HashMap<String, Object> props = new HashMap<String, Object>();
						props.put("channel", channel);
						props.put("properties", properties);
						props.put("replyProps", replyProps);
						props.put("envelope", envelope);
						props.put("body", message);
						props.put("dbConnection", conn);

						cmd.init(props);
						executor.submit(cmd);
					} catch (RuntimeException e) {
						System.out.println(" [.] " + e.toString());
					}
//                    catch (IllegalAccessException e) {
//                        e.printStackTrace();
//                    } catch (InstantiationException e) {
//                        e.printStackTrace();
//                    } catch (ClassNotFoundException e) {
//                        e.printStackTrace();
//                    } 
					catch (SQLException e) {
						e.printStackTrace();
					} finally {
						synchronized (this) {
							this.notify();
						}
					}
				}
			};

			channel.basicConsume(RPC_QUEUE_NAME, true, consumer);
		} catch (IOException | TimeoutException e) {
			e.printStackTrace();
		}
//        finally {
//            if (connection != null)
//                try {
//                    connection.close();
//                } catch (IOException _ignore) {
//                }
//        }

	}

	public static void initDBPool() {
//        String host = System.getenv("POSTGRES_SERVICE_HOST");
//        System.out.println(host);
		HikariConfig jdbcConfig = new HikariConfig();
		jdbcConfig.setPoolName("test pool");
		jdbcConfig.setMaximumPoolSize(10);
		jdbcConfig.setMinimumIdle(2);
		jdbcConfig.setJdbcUrl("jdbc:postgresql://localhost:5432/El-Menus");
		jdbcConfig.setUsername("eletreby");
		jdbcConfig.setPassword("1234");
		source = new HikariDataSource(jdbcConfig);
	}

}