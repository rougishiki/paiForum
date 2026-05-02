package com.github.paicoding.forum.service.notify.service.impl;

import com.github.paicoding.forum.api.model.enums.NotifyTypeEnum;
import com.github.paicoding.forum.core.util.SpringUtil;
import com.github.paicoding.forum.service.notify.config.RabbitMqConfig;
import com.github.paicoding.forum.service.notify.service.NotifyService;
import com.github.paicoding.forum.service.notify.service.RabbitmqService;
import com.github.paicoding.forum.service.user.repository.entity.UserFootDO;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Slf4j
@Service
public class RabbitmqServiceImpl implements RabbitmqService {

    @Autowired(required = false)
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private NotifyService notifyService;

    @Override
    public boolean enabled() {
        return "true".equalsIgnoreCase(SpringUtil.getConfig("rabbitmq.switchFlag"));
    }

    @Override
    public void publishMsg(String exchange, String routingKey, Object message) {
        if (rabbitTemplate != null) {
            rabbitTemplate.convertAndSend(exchange, routingKey, message);
            log.info("Publish msg to {}:{} — {}", exchange, routingKey, message);
        } else {
            log.warn("RabbitTemplate not available, msg not sent");
        }
    }

    @RabbitListener(queues = RabbitMqConfig.QUEUE_NAME, ackMode = "MANUAL")
    public void handlePraiseMessage(UserFootDO userFoot, Message message, Channel channel,
                                     @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) {
        // Spring 自动：
        // 1. 应用启动时自动创建消费者
        // 2. 自动监听队列
        // 3. 自动反序列化消息（UserFootDO）
        // 4. 自动注入 Channel 和 deliveryTag
        // 5. 线程池管理（并发消费）

        log.info("Consumer praise msg: userId={}, documentId={}", userFoot.getUserId(), userFoot.getDocumentId());

        Integer retryCount = message.getMessageProperties().getHeader("retry_count");
        if (retryCount == null) {
            retryCount = 0;
        }

        try {
            notifyService.saveArticleNotify(userFoot, NotifyTypeEnum.PRAISE);
            channel.basicAck(deliveryTag, false);
            log.info("Consumer praise msg success, ack done");
        } catch (Exception e) {
            log.error("Process praise msg failed, retryCount: {}", retryCount, e);
            try {
                if (retryCount < 3) {
                    doRetry(message, retryCount + 1);
                    channel.basicAck(deliveryTag, false);
                } else {
                    channel.basicReject(deliveryTag, false);
                    log.warn("Praise msg retry over limit, send to DLX");
                }
            } catch (IOException ex) {
                log.error("Republish/Reject praise msg failed", ex);
            }
        }
    }

    private void doRetry(Message message, int newRetryCount) {
        MessageProperties retryProps = new MessageProperties();
        retryProps.setDeliveryMode(MessageProperties.DEFAULT_DELIVERY_MODE);
        retryProps.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        message.getMessageProperties().getHeaders().forEach((k, v) -> {
            if (!"retry_count".equals(k)) {
                retryProps.setHeader(k, v);
            }
        });
        retryProps.setHeader("retry_count", newRetryCount);
        rabbitTemplate.send(RabbitMqConfig.EXCHANGE_NAME, RabbitMqConfig.ROUTING_KEY,
                new Message(message.getBody(), retryProps));
    }
}
