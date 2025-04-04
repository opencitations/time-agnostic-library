package org.hobbit.core.rabbit;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;

/**
 * This class extends the {@link DefaultConsumer} class 
 * with blocking semantics. The class provides a linked blocking queue
 * of {@link Delivery} class to fetch the next delivery message.
 * 
 * @author Altaf &amp; Sourabh
 *
 */
public class QueueingConsumer extends DefaultConsumer {
    
    private LinkedBlockingQueue<Delivery> deliveryQueue;
    
    
    public QueueingConsumer(Channel channel) {
    	this(channel, new LinkedBlockingQueue<Delivery>());
    }
    
    public QueueingConsumer(Channel channel, LinkedBlockingQueue<Delivery> deliveryQueue) {
        super(channel);
        this.deliveryQueue = deliveryQueue;
    }
    
   /**
    * Adds the delivery object to linked blocking queue for every receive
    */
   @Override
   public void handleDelivery(String consumerTag,
                              Envelope envelope,
                              AMQP.BasicProperties properties,
                              byte[] body)
       throws IOException
   {
        deliveryQueue.add(new Delivery(envelope,properties,body));
   }

	public LinkedBlockingQueue<Delivery> getDeliveryQueue() {
		return deliveryQueue;
	}
    
}
