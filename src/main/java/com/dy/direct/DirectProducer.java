package com.dy.direct;

import com.dy.Constants;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 *消费完，就没有了
 */
public class DirectProducer {
    public String host ="127.0.0.1";
    public Connection connection;
    public Channel channel;

    public void init() throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
//        factory.setUsername("rpc_user");
//        factory.setPassword("rpcme");
        factory.setHost(host);

        //获取链接
        connection = factory.newConnection();
        //创建信道
        channel = connection.createChannel();


        /**
         * 声明交换器
         * 交换器类型主要有三种
         * direct：精准匹配路由键,fanout：广播匹配,topic模糊(多)匹配路由键(可以优先级)
         * exchangeDeclare(String exchange, String type)
         */
        channel.exchangeDeclare(Constants.direct_exchange, "direct");


//        /**
//         * 声明队列
//         * durable是否持久化,exclusive是否私有,autoDelete当最后一个消费者取消订阅的时候，对列是否自动移出
//         * queueDeclare(String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments)
//         */
//        channel.queueDeclare(queueName, false, false, false, null);
//
//        //绑定队列,交换器,路由键（交换器根据路由规则把消息放入匹配的对队列）
//        channel.queueBind(queueName, exchangeName, "routeKey");

    }
    public void publisch(String msg) throws Exception{
        //basicPublish(String exchange, String routingKey, BasicProperties props, byte[] body)
        this.channel.basicPublish(Constants.direct_exchange, "test", null, msg.getBytes("UTF-8"));

    }
    public void close() throws Exception{
        channel.close();
        connection.close();
    }

    public static void main(String[] args) {
        try {
            DirectProducer producer = new DirectProducer();
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
