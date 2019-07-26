package rabbit.receiver.maven;

import java.sql.SQLException;
import java.sql.Statement;
import java.sql.*;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public class MySQLReceiver {

	private static final String QUEUE_NAME = "rabbit-queue";

	public static void main(String[] args) {

		String message;

		System.out.println("Starting the SQL receiver program!");

		try {
			// Get the connection with SQL.
			Statement conexao = ObterConexao();
			

			// Create the connection.
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost("localhost");
			Connection connection = factory.newConnection();

			// Create the channel and the queue.
			Channel channel = connection.createChannel();
			channel.queueDeclare(QUEUE_NAME, true, false, false, null);

			// Create the consumer.
			QueueingConsumer consumer = new QueueingConsumer(channel);
			channel.basicConsume(QUEUE_NAME, true, consumer);

			while(true) {
				// Take the next "message".
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();

				message = new String(delivery.getBody());

				// Print the message that came.
				System.out.println(message);

				// Send the message to MySQL.
				conexao.execute(String.format(QUERY, message));
			}
		} catch (Exception e) {
			System.out.println(String.format("An error occurs [%s]. [%s]", e.getMessage(), e));
		}
	}

	// Database query.
	private static String QUERY = "INSERT INTO TB_MESSAGES (message) VALUES ('%s')";

	// Driver used to connect with the database.
	//private static String DRIVER = "com.mysql.cj.jdbc.Driver";

	// Database URL.
	//private static String URL = "jdbc:mysql://localhost:1521/rabbit";

	// Database username.
	//private static String USERNAME = "system";

	// Database password.
	//private static String PASSWORD = "123";

	//public static Statement connectToSQL() {

	private static Statement ObterConexao() {
		
			try {
				Class.forName("oracle.jdbc.driver.OracleDriver");
				java.sql.Connection con =  DriverManager.getConnection(
						"jdbc:oracle:thin:@localhost:1521:xe", "system", "123");
				Statement stmt = con.createStatement();
				return stmt;
			} catch (Exception e) {
				System.out.println(String.format("Error while connecting to the MySQL [%s]. [%s]", e.getMessage(), e));
			}
			return null;
	}
}

