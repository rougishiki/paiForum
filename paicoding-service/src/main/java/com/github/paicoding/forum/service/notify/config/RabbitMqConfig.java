package com.github.paicoding.forum.service.notify.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.annotation.Resource;

/**
 * RabbitMQ 自动配置类
 * 
 * <p>功能说明：</p>
 * <ol>
 *   <li>配置 RabbitMQ 连接工厂（Connection Factory）</li>
 *   <li>声明交换机（Exchange）、队列（Queue）和绑定关系（Binding）</li>
 *   <li>配置消息转换器（Message Converter）支持 JSON 序列化/反序列化</li>
 *   <li>配置 RabbitTemplate 用于发送消息</li>
 *   <li>配置消费者容器工厂用于接收消息</li>
 *   <li>配置死信队列（DLX）处理失败消息</li>
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
    
    /** 点赞消息交换机名称 */
    public static final String EXCHANGE_NAME = "direct.exchange";

    /** 死信交换机名称 */
    public static final String DLX_EXCHANGE = "direct.exchange_dlx";
    
    /** 点赞消息队列名称 */
    public static final String QUEUE_NAME = "queue.praise";
    
    /** 点赞消息路由键 */
    public static final String ROUTING_KEY = "praise";
    
    /** 死信队列名称 */
    public static final String DLX_QUEUE = "queue.praise_dlx";
    
    /** 死信队列路由键 */
    public static final String DLX_ROUTING_KEY = "praise_dlx";

    // 注入自定义的连接工厂 Bean（避免直接调用 @Bean 方法导致 @Value 失效）
    @Resource(name = "connectionFactory")
    private ConnectionFactory rabbitConnectionFactory;

    // ==================== 连接工厂 ====================

    /**
     * 创建 RabbitMQ 连接工厂
     * 
     * <p>功能说明：</p>
     * <ul>
     *   <li>从配置文件读取 RabbitMQ 连接信息（host、port、username、password、virtual-host）</li>
     *   <li>创建 CachingConnectionFactory 实现连接池管理</li>
     *   <li>启用发布确认机制（Publisher Confirm）确保消息可靠投递</li>
     * </ul>
     * 
     * <p>PublisherConfirmType 有三种模式：</p>
     * <ul>
     *   <li>NONE（默认）：不启用发布确认，性能最高但可靠性最低</li>
     *   <li>SIMPLE：同步确认，send() 方法会阻塞等待确认，适合低并发场景</li>
     *   <li>CORRELATED：异步确认，通过 ConfirmCallback 回调通知，性能高且可靠（推荐）</li>
     * </ul>
     * 
     * @param host RabbitMQ 服务器地址
     * @param port RabbitMQ 服务器端口
     * @param username 登录用户名
     * @param password 登录密码
     * @param virtualHost 虚拟主机路径
     * @return 配置好连接工厂
     */
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
        // 启用发布确认机制，确保消息成功到达交换机
        // 使用 CORRELATED 模式：异步确认，高性能 + 高可靠
        factory.setPublisherConfirmType(CachingConnectionFactory.ConfirmType.CORRELATED);
        return factory;
    }

    // ==================== JSON 序列化 ====================

    /**
     * 创建 JSON 消息转换器
     * 
     * <p>功能说明：</p>
     * <ul>
     *   <li>使用 Jackson2JsonMessageConverter 实现 Java 对象与 JSON 的自动转换</li>
     *   <li>发送消息时：Java 对象 → JSON 字符串</li>
     *   <li>接收消息时：JSON 字符串 → Java 对象（如 UserFootDO）</li>
     * </ul>
     * 
     * @return JSON 消息转换器
     */
    @Bean
    public Jackson2JsonMessageConverter messageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    // ==================== 队列 / 交换机 / 死信队列 ====================

    /**
     * 创建点赞消息交换机（Direct Exchange）
     * 
     * <p>功能说明：</p>
     * <ul>
     *   <li>Direct Exchange 根据 routingKey 精确匹配路由到队列</li>
     *   <li>durable=true：交换机持久化，RabbitMQ 重启后依然存在</li>
     *   <li>autoDelete=false：不自动删除</li>
     * </ul>
     * 
     * @return Direct 交换机
     */
    @Bean
    public DirectExchange praiseExchange() {
        return new DirectExchange(EXCHANGE_NAME, true, false);
    }

    /**
     * 创建死信交换机（DLX Exchange）
     * 
     * <p>功能说明：</p>
     * <ul>
     *   <li>用于接收无法消费的消息（重试超过限制、消息过期等）</li>
     *   <li>死信交换机也是 Direct 类型，便于按路由键分类处理</li>
     * </ul>
     * 
     * @return 死信交换机
     */
    @Bean
    public DirectExchange praiseDlxExchange() {
        return new DirectExchange(DLX_EXCHANGE, true, false);
    }

    /**
     * 创建死信队列
     * 
     * <p>功能说明：</p>
     * <ul>
     *   <li>存储无法被正常消费的消息</li>
     *   <li>durable=true：队列持久化，RabbitMQ 重启后消息不丢失</li>
     *   <li>需要配置对应的消费者来处理死信消息（记录日志、告警等）</li>
     * </ul>
     * 
     * @return 死信队列
     */
    @Bean
    public Queue praiseDlxQueue() {
        return QueueBuilder.durable(DLX_QUEUE).build();
    }

    /**
     * 绑定死信队列到死信交换机
     * 
     * <p>功能说明：</p>
     * <ul>
     *   <li>将死信队列绑定到死信交换机，指定路由键</li>
     *   <li>当消息被拒绝（basicReject）或过期时，会自动路由到死信队列</li>
     * </ul>
     * 
     * @return 死信队列绑定关系
     */
    @Bean
    public Binding praiseDlxBinding() {
        return BindingBuilder.bind(praiseDlxQueue()).to(praiseDlxExchange()).with(DLX_ROUTING_KEY);
    }

    /**
     * 创建点赞消息队列（带死信配置）
     * 
     * <p>功能说明：</p>
     * <ul>
     *   <li>durable=true：队列持久化</li>
     *   <li>deadLetterExchange：指定死信交换机，消息被拒绝后转发到此交换机</li>
     *   <li>deadLetterRoutingKey：指定死信路由键，决定消息进入哪个死信队列</li>
     *   <li>ttl=60000：消息存活时间 60 秒，超时未消费则进入死信队列</li>
     * </ul>
     * 
     * <p>注意：TTL 配置意味着消息在队列中最多等待 60 秒，
     * 如果消费者一直不消费，消息会自动进入死信队列。</p>
     * 
     * @return 点赞消息队列
     */
    @Bean
    public Queue praiseQueue() {
        return QueueBuilder.durable(QUEUE_NAME)
                .deadLetterExchange(DLX_EXCHANGE)
                .deadLetterRoutingKey(DLX_ROUTING_KEY)
                .ttl(60000)
                .build();
    }

    /**
     * 绑定点赞队列到点赞交换机
     * 
     * <p>功能说明：</p>
     * <ul>
     *   <li>将点赞队列绑定到点赞交换机，指定路由键 "praise"</li>
     *   <li>发送消息时使用 routingKey="praise"，消息会路由到此队列</li>
     * </ul>
     * 
     * @return 点赞队列绑定关系
     */
    @Bean
    public Binding praiseBinding() {
        return BindingBuilder.bind(praiseQueue()).to(praiseExchange()).with(ROUTING_KEY);
    }

    // ==================== RabbitTemplate ====================

    /**
     * 创建 RabbitTemplate 用于发送消息
     * 
     * <p>功能说明：</p>
     * <ul>
     *   <li>设置消息转换器，支持自动序列化 Java 对象为 JSON</li>
     *   <li>设置发布确认回调，监控消息是否成功到达交换机</li>
     *   <li>设置返回回调，监控消息是否从交换机路由到队列</li>
     *   <li>启用 mandatory 模式，确保无法路由的消息会触发返回回调</li>
     *   <li>如果消息发送失败，记录错误日志便于排查问题</li>
     * </ul>
     * 
     * <p>注意：消息持久化应在发送时通过 MessageProperties 设置，
     * 或在消费者重试时设置（见 RabbitmqServiceImpl.doRetry 方法）。</p>
     * 
     * @return 配置好的 RabbitTemplate
     */
    @Bean
    public RabbitTemplate rabbitTemplate() {
        RabbitTemplate template = new RabbitTemplate(rabbitConnectionFactory);
        template.setMessageConverter(messageConverter());
        // 设置发布确认回调，确保消息成功到达交换机
        template.setConfirmCallback((correlationData, ack, cause) -> {
            if (!ack) {
                log.error("消息发布确认失败 - correlationData: {}, cause: {}", correlationData, cause);
            } else {
                log.debug("消息发布确认成功 - correlationData: {}", 
                         correlationData != null ? correlationData.getId() : "null");
            }
        });
        // 设置返回回调，检测消息是否从交换机路由到队列（mandatory 模式下生效）
        template.setReturnsCallback(returned -> {
            log.error("消息路由失败 - exchange: {}, routingKey: {}, replyCode: {}, replyText: {}, message: {}",
                    returned.getExchange(),
                    returned.getRoutingKey(),
                    returned.getReplyCode(),
                    returned.getReplyText(),
                    new String(returned.getMessage().getBody()));
        });
        // 启用 mandatory 模式，确保无法路由的消息会触发返回回调
        template.setMandatory(true);
        return template;
    }

    // ==================== 监听容器（手动 ACK + 限流）====================

    /**
     * 创建消费者监听容器工厂
     * 
     * <p>功能说明：</p>
     * <ul>
     *   <li>配置消费者容器的通用属性，所有 @RabbitListener 都会使用此工厂</li>
     *   <li>setAcknowledgeMode(MANUAL)：手动确认模式，消费者处理成功后才确认消息</li>
     *   <li>setPrefetchCount(1)：每次只预取 1 条消息，避免消息堆积在消费者端</li>
     *   <li>setMessageConverter：设置消息转换器，自动反序列化为 Java 对象</li>
     * </ul>
     * 
     * <p>手动确认的优势：</p>
     * <ul>
     *   <li>可以控制消息何时确认（处理成功后）</li>
     *   <li>支持重试逻辑（处理失败时可以重新发送或拒绝）</li>
     *   <li>支持死信队列（拒绝的消息可以转发到死信队列）</li>
     * </ul>
     * 
     * @return 配置好的消费者容器工厂
     */
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
