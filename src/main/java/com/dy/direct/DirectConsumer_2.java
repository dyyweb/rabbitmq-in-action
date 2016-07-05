package com.dy.direct;

import com.dy.Constants;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

/**
 * Created by dy on 16-4-28.
 */
public class DirectConsumer_2 {
    public String host ="127.0.0.1";


    public void init() throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
//        factory.setUsername("rpc_user");
//        factory.setPassword("rpcme");
        factory.setHost(host);

        //获取链接
        Connection connection = factory.newConnection();
        //创建信道
        Channel channel = connection.createChannel();

        /**
         * 声明队列
         * durable是否持久化,exclusive是否私有,autoDelete当最后一个消费者取消订阅的时候，对列是否自动移出
         * queueDeclare(String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments)
         */
        channel.queueDeclare(Constants.queue_direct, false, false, false, null);

        //绑定队列,交换器,路由键（交换器根据路由规则把消息放入匹配的对队列）
        //不同消费者，绑定相同队列到相同交换器，这时候会均衡消费队列中的消息,不管路由规则
        //不同消费者，绑定不同队列到相同交换器，这时候（交换器根据路由规则把消息放入匹配的对队列）
        channel.queueBind(Constants.queue_direct, Constants.direct_exchange, "haha");

        /**
         * 订阅消息
         * autoAck是否自动确认,是否消息订阅到队列就确认
         * basicConsume(String queue, boolean autoAck, String consumerTag, TopicConsumer_1 callback)
         * basicConsume(String queue, boolean autoAck, TopicConsumer_1 callback)
         */
        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(Constants.queue_direct, false, consumer);

        while (true) {
            try {
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();

                String msg = new String(delivery.getBody(), "UTF-8");

                System.out.println("我接收到的消息是:"+msg);       // 返回接收到消息的确认信息

                // 显示确认
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } catch (Exception e) {
                System.out.println(e.toString());
            }
        }
    }


    public static void main(String[] args) {
        try {
            new DirectConsumer_2().init();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
