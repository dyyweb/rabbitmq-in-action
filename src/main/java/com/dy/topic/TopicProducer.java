package com.dy.topic;

import com.dy.Constants;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 *
 */
public class TopicProducer {
    public Connection connection;
    public Channel channel;

    public void init() throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");

        //获取链接
        connection = factory.newConnection();
        //创建信道
        channel = connection.createChannel();

        channel.exchangeDeclare(Constants.topic_exchange, "topic");

    }
    public void publisch(String msg) throws Exception{
        //basicPublish(String exchange, String routingKey, BasicProperties props, byte[] body)
        // routingKey=test,
        this.channel.basicPublish(Constants.topic_exchange, "topic", null, msg.getBytes("UTF-8"));

    }
    public void close() throws Exception{
        channel.close();
        connection.close();
    }

    public static void main(String[] args) {
        try {
            TopicProducer producer = new TopicProducer();
            producer.init();
            String msg ="this is a msg from producer!我的序列是:";
            for (int i = 1;i<8;i++){
                producer.publisch(msg+i);
            }
            producer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
