package rabbit.receiver.maven;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public class Receiver {

	private static final String QUEUE_NAME = "rabbit-queue";

	public static void main(String[] args) {

		System.out.println("Starting the receiver program!");

		try {
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

				// Print the message that came
				System.out.println(new String(delivery.getBody()));
			}
		} catch (Exception e) {
			System.out.println(String.format("An error occurs [%s]. [%s]", e.getMessage(), e));
		}
	}

}
