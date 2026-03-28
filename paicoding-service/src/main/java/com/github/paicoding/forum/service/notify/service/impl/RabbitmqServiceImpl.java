package com.github.paicoding.forum.service.notify.service.impl;

import com.github.paicoding.forum.api.model.enums.NotifyTypeEnum;
import com.github.paicoding.forum.core.common.CommonConstants;
import com.github.paicoding.forum.core.rabbitmq.RabbitmqConnection;
import com.github.paicoding.forum.core.rabbitmq.RabbitmqConnectionPool;
import com.github.paicoding.forum.core.util.JsonUtil;
import com.github.paicoding.forum.core.util.SpringUtil;
import com.github.paicoding.forum.service.notify.service.NotifyService;
import com.github.paicoding.forum.service.notify.service.RabbitmqService;
import com.github.paicoding.forum.service.user.repository.entity.UserFootDO;
import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

@Slf4j
@Service
public class RabbitmqServiceImpl implements RabbitmqService {

    @Autowired
    private NotifyService notifyService;

    @Override
    public boolean enabled() {
        return "true".equalsIgnoreCase(SpringUtil.getConfig("rabbitmq.switchFlag"));
    }


    /*
    1. 获取连接
    2. 创建Channel并开启发布确认
    3. 创建队列并绑定交换机
    4. 设置消息持久化属性
    5. 发送消息
    6. 等待确认
    7. 确保资源释放，连接归还
     */
@Override
public void publishMsg(String exchange,
                       BuiltinExchangeType exchangeType,
                       String routingKey,
                       String message) {
    RabbitmqConnection rabbitmqConnection = null;
    Channel channel = null;
    try {
        // 1. 获取连接（try-with-resources 不适用，需手动归还）
        rabbitmqConnection = RabbitmqConnectionPool.getConnection();
        Connection connection = rabbitmqConnection.getConnection();
        // 2. 创建Channel并开启发布确认
        channel = connection.createChannel();
        channel.confirmSelect(); // 开启发布确认
        // 3. 声明交换机（幂等）
        channel.exchangeDeclare(exchange, exchangeType, true, false, null);
        // 4. 设置消息持久化属性
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .deliveryMode(2) // 2=持久化，1=非持久化
                .contentType("application/json")
                .build();
        // 5. 发送消息
        channel.basicPublish(exchange, routingKey, props, message.getBytes());
        log.info("Publish msg: {}", message);
        // 6. 等待确认（同步确认，也可使用异步确认）
        if (!channel.waitForConfirms(5000)) {
            log.error("Msg publish confirm failed: {}", message);
            // 可选：重试逻辑（需防止死循环）
            retryPublish(exchange, exchangeType, routingKey, message);
        }
    } catch (InterruptedException | IOException | TimeoutException e) {
        log.error("rabbitMq消息发送异常: exchange: {}, msg: {}", exchange, message, e);
        // 可选：持久化到本地（如数据库/本地文件），后续补偿
    } finally {
        // 7. 确保资源释放，连接归还
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
        } catch (IOException | TimeoutException e) {
            log.error("关闭Channel失败", e);
        }
        if (rabbitmqConnection != null) {
            RabbitmqConnectionPool.returnConnection(rabbitmqConnection);
        }
    }
}


    /*
    1. 创建消费者内部类
    2. 创建消费者
    3. 启动消费者
    4. 确保资源释放，连接归还
     */
    @Override
    public void consumerMsg(String exchange,
                            String queueName,
                            String routingKey) {
        // 1. 将被内部类/Lambda访问的变量声明为final（核心修复）
        final RabbitmqConnection rabbitmqConnection;
        final Channel channel;

        try {
            // 2. 变量仅赋值一次（满足“有效final”）
            rabbitmqConnection = RabbitmqConnectionPool.getConnection();
            Connection connection = rabbitmqConnection.getConnection();
            channel = connection.createChannel();

            // 3. 声明死信交换机/队列（原逻辑保留）
            String dlxExchange = exchange + "_dlx";
            String dlxQueue = queueName + "_dlx";
            String dlxRoutingKey = routingKey + "_dlx";
            channel.exchangeDeclare(dlxExchange, BuiltinExchangeType.DIRECT, true);
            channel.queueDeclare(dlxQueue, true, false, false, null);
            channel.queueBind(dlxQueue, dlxExchange, dlxRoutingKey);

            // 4. 声明业务队列（原逻辑保留）
            Map<String, Object> queueArgs = new HashMap<>();
            queueArgs.put("x-dead-letter-exchange", dlxExchange);
            queueArgs.put("x-dead-letter-routing-key", dlxRoutingKey);
            queueArgs.put("x-message-ttl", 60000);

            try {
                channel.queueDeclare(queueName, true, false, false, queueArgs);
            } catch (IOException e) {
                if (e.getCause() instanceof ShutdownSignalException &&
                        ((ShutdownSignalException) e.getCause()).getReason().toString().contains("PRECONDITION_FAILED")) {
                    log.warn("队列参数不匹配，删除旧队列后重新声明：{}", queueName);
                    channel.queueDelete(queueName);
                    channel.queueDeclare(queueName, true, false, false, queueArgs);
                } else {
                    throw e;
                }
            }
            channel.queueBind(queueName, exchange, routingKey);
            channel.basicQos(1);

            // 5. 消费者内部类（此时channel是final的，可正常访问）
            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                           byte[] body) throws IOException {
                    String message = new String(body, "UTF-8");
                    log.info("Consumer msg: {}", message);

                    Integer retryCount = properties.getHeaders() != null ?
                            (Integer) properties.getHeaders().get("retry_count") : 0;

                    try {
                        notifyService.saveArticleNotify(JsonUtil.toObj(message, UserFootDO.class), NotifyTypeEnum.PRAISE);
                        channel.basicAck(envelope.getDeliveryTag(), false); // 访问final的channel
                        log.info("Consumer msg success, ack done: {}", message);
                    } catch (Exception e) {
                        log.error("Process msg failed, retryCount: {}", retryCount, e);
                        if (retryCount < 3) {
                            AMQP.BasicProperties newProps = new AMQP.BasicProperties.Builder()
                                    .deliveryMode(2)
                                    .headers(Collections.singletonMap("retry_count", retryCount + 1))
                                    .build();
                            channel.basicPublish(exchange, routingKey, newProps, body); // 访问final的channel
                            channel.basicAck(envelope.getDeliveryTag(), false);
                        } else {
                            channel.basicReject(envelope.getDeliveryTag(), false); // 访问final的channel
                            log.warn("Msg retry over limit, send to DLX: {}", message);
                        }
                    }
                }
            };
            channel.basicConsume(queueName, false, consumer);

            // 6. 关闭钩子（访问final的channel和rabbitmqConnection）
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    if (channel.isOpen()) {
                        channel.close(); // 访问final的channel
                    }
                    RabbitmqConnectionPool.returnConnection(rabbitmqConnection); // 访问final的rabbitmqConnection
                } catch (Exception e) {
                    log.error("Shutdown hook close channel failed", e);
                }
            }));

        } catch (InterruptedException | IOException e) {
            log.error("Init consumer failed", e);
        }
    }

    @Override
    public void processConsumerMsg() {
        log.info("Begin to processConsumerMsg (init consumer once)");
        // 只初始化一次消费者（保持长连接消费）
        consumerMsg(CommonConstants.EXCHANGE_NAME_DIRECT,
                CommonConstants.QUEUE_NAME_PRAISE, // 修正为正确的常量名
                CommonConstants.QUEUE_KEY_PRAISE);
    }


    // 简单重试逻辑（可优化为带退避策略的重试）
    private void retryPublish(String exchange, BuiltinExchangeType exchangeType, String routingKey, String message) {
        int retryTimes = 3;
        while (retryTimes-- > 0) {
            try {
                Thread.sleep(100 * (3 - retryTimes)); // 指数退避
                publishMsg(exchange, exchangeType, routingKey, message);
                return;
            } catch (Exception e) {
                log.warn("重试发送消息失败，剩余次数: {}", retryTimes, e);
            }
        }
        log.error("消息重试发送失败，需人工补偿: {}", message);
    }


    //    @Override
//    public void publishMsg(String exchange,
//                           BuiltinExchangeType exchangeType,
//                           String routingKey,
//                           String message) {
//        try {
//            //创建连接
//            RabbitmqConnection rabbitmqConnection = RabbitmqConnectionPool.getConnection();
//            Connection connection = rabbitmqConnection.getConnection();
//            //创建消息通道
//            Channel channel = connection.createChannel();
//            // 声明exchange中的消息为可持久化，不自动删除
//            channel.exchangeDeclare(exchange, exchangeType, true, false, null);
//            // 发布消息
//            channel.basicPublish(exchange, routingKey, null, message.getBytes());
//            log.info("Publish msg: {}", message);
//            channel.close();
//            RabbitmqConnectionPool.returnConnection(rabbitmqConnection);
//        } catch (InterruptedException | IOException | TimeoutException e) {
//            log.error("rabbitMq消息发送异常: exchange: {}, msg: {}", exchange, message, e);
//        }
//    }
//    @Override
//    public void consumerMsg(String exchange,
//                            String queueName,
//                            String routingKey) {
//
//        try {
//            //创建连接
//            RabbitmqConnection rabbitmqConnection = RabbitmqConnectionPool.getConnection();
//            Connection connection = rabbitmqConnection.getConnection();
//            //创建消息信道
//            final Channel channel = connection.createChannel();
//            //消息队列
//            channel.queueDeclare(queueName, true, false, false, null);
//            //绑定队列到交换机
//            channel.queueBind(queueName, exchange, routingKey);
//
//            Consumer consumer = new DefaultConsumer(channel) {
//                @Override
//                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
//                                           byte[] body) throws IOException {
//                    String message = new String(body, "UTF-8");
//                    log.info("Consumer msg: {}", message);
//
//                    // 获取Rabbitmq消息，并保存到DB
//                    // 说明：这里仅作为示例，如果有多种类型的消息，可以根据消息判定，简单的用 if...else 处理，复杂的用工厂 + 策略模式
//                    notifyService.saveArticleNotify(JsonUtil.toObj(message, UserFootDO.class), NotifyTypeEnum.PRAISE);
//
//                    channel.basicAck(envelope.getDeliveryTag(), false);
//                    log.info("Consumer msg: {}", message);
//                }
//            };
//            // 取消自动ack
//            channel.basicConsume(queueName, false, consumer);
//            channel.close();
//            RabbitmqConnectionPool.returnConnection(rabbitmqConnection);
//        } catch (InterruptedException | IOException | TimeoutException e) {
//            e.printStackTrace();
//        }
//    }
//@Override
//public void consumerMsg(String exchange,
//                        String queueName,
//                        String routingKey) {
//    try {
//        // 从连接池获取连接（保持连接复用）
//        RabbitmqConnection rabbitmqConnection = RabbitmqConnectionPool.getConnection();
//        Connection connection = rabbitmqConnection.getConnection();
//        // 创建Channel（消费用的Channel需保持打开）
//        final Channel channel = connection.createChannel();
//
//        // 声明队列（幂等操作，重复声明不会有问题）
//        channel.queueDeclare(queueName, true, false, false, null);
//        // 绑定队列到交换机
//        channel.queueBind(queueName, exchange, routingKey);
//
//        Consumer consumer = new DefaultConsumer(channel) {
//            @Override
//            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
//                                       byte[] body) throws IOException {
//                String message = new String(body, "UTF-8");
//                log.info("Consumer msg: {}", message);
//
//                try {
//                    // 处理消息
//                    notifyService.saveArticleNotify(JsonUtil.toObj(message, UserFootDO.class), NotifyTypeEnum.PRAISE);
//                    // 消息处理成功后，手动Ack
//                    channel.basicAck(envelope.getDeliveryTag(), false);
//                    log.info("Consumer msg success, ack done: {}", message);
//                } catch (Exception e) {
//                    log.error("Process msg failed, reject msg: {}", message, e);
//                    // 处理失败，拒绝消息（避免重复投递）
//                    channel.basicReject(envelope.getDeliveryTag(), false);
//                }
//            }
//        };
//        // 注册消费者（保持Channel打开，后台线程会持续消费）
//        channel.basicConsume(queueName, false, consumer);
//
//        // 注意：此处不能关闭Channel/Connection！消费需要保持Channel长连接
//    } catch (InterruptedException | IOException e) {
//        log.error("Init consumer failed", e);
//    }
//}


//    @Override
//    public void processConsumerMsg() {
//        log.info("Begin to processConsumerMsg.");
//
//        Integer stepTotal = 1;
//        Integer step = 0;
//
//        // TODO: 这种方式非常 Low，后续会改造成阻塞 I/O 模式
//        while (true) {
//            step++;
//            try {
//                log.info("processConsumerMsg cycle.");
//                consumerMsg(CommonConstants.EXCHANGE_NAME_DIRECT
//                        , CommonConstants.QUERE_NAME_PRAISE,
//                        CommonConstants.QUERE_KEY_PRAISE);
//                if (step.equals(stepTotal)) {
//                    Thread.sleep(10000);
//                    step = 0;
//                }
//            } catch (Exception e) {
//
//            }
//        }
//    }

}
