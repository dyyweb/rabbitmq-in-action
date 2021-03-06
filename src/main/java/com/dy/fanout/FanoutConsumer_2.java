package com.dy.fanout;

import com.dy.Constants;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

/**
 * Created by dy on 16-4-28.
 */
public class FanoutConsumer_2 {
    public String host ="127.0.0.1";

    public void init() throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(Constants.queue_fanout_2, false, false, false, null);

        //绑定队列,交换器,路由键（该模式路由不用了）
        channel.queueBind(Constants.queue_fanout_2, Constants.fanout_exchange,"");

        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(Constants.queue_fanout_2, false,consumer);

        while (true) {
            try {
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                String msg = new String(delivery.getBody(), "UTF-8");

                System.out.println("我接收到的消息是:"+msg);

                // 返回接收到消息的确认信息
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } catch (Exception e) {
                System.out.println(e.toString());
            }
        }
    }

    public static void main(String[] args) {
        try {
            new FanoutConsumer_2().init();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
