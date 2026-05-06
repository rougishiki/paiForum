package com.github.paicoding.forum.service.notify.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.annotation.Resource;

/**
 * RabbitMQ 自动配置类
 *
 * <p>三队列设计（按优先级）：</p>
 * <ol>
 *   <li>queue.notify.pay — 支付通知（高优先级，独立隔离）</li>
 *   <li>queue.interact — 点赞/收藏互动通知</li>
 *   <li>queue.notify.social — 评论/回复/关注社交通知</li>
 * </ol>
 *
 * <p>启用条件：rabbitmq.switchFlag=true</p>
 */
@Slf4j
@Configuration
@EnableRabbit
@ConditionalOnProperty(value = "rabbitmq.switchFlag", havingValue = "true")
public class RabbitMqConfig {

    // ==================== 常量定义 ====================

    /** 交换机名称（所有队列共用） */
    public static final String EXCHANGE_NAME = "direct.exchange";

    /** 死信交换机名称（所有队列共用） */
    public static final String DLX_EXCHANGE = "direct.exchange_dlx";

    // ========== 互动通知（点赞/收藏）==========
    public static final String QUEUE_NAME_INTERACT = "queue.interact";
    public static final String ROUTING_KEY_INTERACT = "interact";
    public static final String DLX_QUEUE_INTERACT = "queue.interact_dlx";
    public static final String DLX_ROUTING_KEY_INTERACT = "interact_dlx";

    // ========== 支付通知 ==========
    public static final String QUEUE_NAME_PAY = "queue.notify.pay";
    public static final String ROUTING_KEY_PAY = "notify.pay";
    public static final String DLX_QUEUE_PAY = "queue.notify.pay_dlx";
    public static final String DLX_ROUTING_KEY_PAY = "notify.pay_dlx";

    // ========== 社交通知（评论/回复/关注）==========
    public static final String QUEUE_NAME_SOCIAL = "queue.notify.social";
    public static final String ROUTING_KEY_SOCIAL = "notify.social";
    public static final String DLX_QUEUE_SOCIAL = "queue.notify.social_dlx";
    public static final String DLX_ROUTING_KEY_SOCIAL = "notify.social_dlx";

    // 注入自定义的连接工厂 Bean
    @Resource(name = "connectionFactory")
    private ConnectionFactory rabbitConnectionFactory;

    // ==================== 连接工厂 ====================

    @Bean("connectionFactory")
    @Primary
    public ConnectionFactory connectionFactory(
            @Value("${spring.rabbitmq.host}") String host,
            @Value("${spring.rabbitmq.port}") int port,
            @Value("${spring.rabbitmq.username}") String username,
            @Value("${spring.rabbitmq.password}") String password,
            @Value("${spring.rabbitmq.virtual-host}") String virtualHost) {
        CachingConnectionFactory factory = new CachingConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(username);
        factory.setPassword(password);
        factory.setVirtualHost(virtualHost);
        factory.setPublisherConfirmType(CachingConnectionFactory.ConfirmType.CORRELATED);
        return factory;
    }

    // ==================== JSON 序列化 ====================

    @Bean
    public Jackson2JsonMessageConverter messageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    // ==================== 交换机 ====================

    @Bean
    public DirectExchange notifyExchange() {
        return new DirectExchange(EXCHANGE_NAME, true, false);
    }

    @Bean
    public DirectExchange notifyDlxExchange() {
        return new DirectExchange(DLX_EXCHANGE, true, false);
    }

    // ==================== 互动通知（点赞/收藏）队列 + 死信 ====================

    @Bean
    public Queue interactDlxQueue() {
        return QueueBuilder.durable(DLX_QUEUE_INTERACT).build();
    }

    @Bean
    public Binding interactDlxBinding() {
        return BindingBuilder.bind(interactDlxQueue()).to(notifyDlxExchange()).with(DLX_ROUTING_KEY_INTERACT);
    }

    @Bean
    public Queue interactQueue() {
        return QueueBuilder.durable(QUEUE_NAME_INTERACT)
                .deadLetterExchange(DLX_EXCHANGE)
                .deadLetterRoutingKey(DLX_ROUTING_KEY_INTERACT)
                .ttl(60000)
                .build();
    }

    @Bean
    public Binding interactBinding() {
        return BindingBuilder.bind(interactQueue()).to(notifyExchange()).with(ROUTING_KEY_INTERACT);
    }

    // ==================== 支付通知队列 + 死信 ====================

    @Bean
    public Queue payDlxQueue() {
        return QueueBuilder.durable(DLX_QUEUE_PAY).build();
    }

    @Bean
    public Binding payDlxBinding() {
        return BindingBuilder.bind(payDlxQueue()).to(notifyDlxExchange()).with(DLX_ROUTING_KEY_PAY);
    }

    @Bean
    public Queue payQueue() {
        return QueueBuilder.durable(QUEUE_NAME_PAY)
                .deadLetterExchange(DLX_EXCHANGE)
                .deadLetterRoutingKey(DLX_ROUTING_KEY_PAY)
                .ttl(60000)
                .build();
    }

    @Bean
    public Binding payBinding() {
        return BindingBuilder.bind(payQueue()).to(notifyExchange()).with(ROUTING_KEY_PAY);
    }

    // ==================== 社交通知队列 + 死信 ====================

    @Bean
    public Queue socialDlxQueue() {
        return QueueBuilder.durable(DLX_QUEUE_SOCIAL).build();
    }

    @Bean
    public Binding socialDlxBinding() {
        return BindingBuilder.bind(socialDlxQueue()).to(notifyDlxExchange()).with(DLX_ROUTING_KEY_SOCIAL);
    }

    @Bean
    public Queue socialQueue() {
        return QueueBuilder.durable(QUEUE_NAME_SOCIAL)
                .deadLetterExchange(DLX_EXCHANGE)
                .deadLetterRoutingKey(DLX_ROUTING_KEY_SOCIAL)
                .ttl(60000)
                .build();
    }

    @Bean
    public Binding socialBinding() {
        return BindingBuilder.bind(socialQueue()).to(notifyExchange()).with(ROUTING_KEY_SOCIAL);
    }

    // ==================== RabbitTemplate ====================

    @Bean
    public RabbitTemplate rabbitTemplate() {
        RabbitTemplate template = new RabbitTemplate(rabbitConnectionFactory);
        template.setMessageConverter(messageConverter());
        template.setConfirmCallback((correlationData, ack, cause) -> {
            if (!ack) {
                log.error("消息发布确认失败 - correlationData: {}, cause: {}", correlationData, cause);
            } else {
                log.debug("消息发布确认成功 - correlationData: {}",
                         correlationData != null ? correlationData.getId() : "null");
            }
        });
        template.setReturnsCallback(returned -> {
            log.error("消息路由失败 - exchange: {}, routingKey: {}, replyCode: {}, replyText: {}, message: {}",
                    returned.getExchange(),
                    returned.getRoutingKey(),
                    returned.getReplyCode(),
                    returned.getReplyText(),
                    new String(returned.getMessage().getBody()));
        });
        template.setMandatory(true);
        return template;
    }

    // ==================== 监听容器（手动 ACK + 限流）====================

    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(rabbitConnectionFactory);
        factory.setMessageConverter(messageConverter());
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        factory.setPrefetchCount(1);
        return factory;
    }
}
