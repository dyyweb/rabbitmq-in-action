package com.dy.topic;

import com.dy.Constants;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

/**
 * Created by dy on 16-4-28.
 */
public class TopicConsumer_3 {

    public void init() throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");

        //获取链接
        Connection connection = factory.newConnection();
        //创建信道
        Channel channel = connection.createChannel();

        channel.queueDeclare(Constants.queue_topic_3, false, false, false, null);

        //绑定队列,交换器,路由键（交换器根据路由规则把消息放入匹配的对队列）
        channel.queueBind(Constants.queue_topic_3,Constants.topic_exchange, "#");

        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(Constants.queue_topic_3, false, consumer);

        while (true) {
            try {
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();

                String msg = new String(delivery.getBody(), "UTF-8");

                System.out.println("我接收到的消息是:"+msg);

                // 显示确认
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) {
        try {
            new TopicConsumer_3().init();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
