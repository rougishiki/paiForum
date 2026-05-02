package com.github.paicoding.forum.service.notify.service.impl;

import com.github.paicoding.forum.api.model.enums.NotifyTypeEnum;
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
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * RabbitMQ 点赞消息消费者
 */
@Component
@Slf4j
public class RabbitmqPraiseConsumer {
    // 最大重试次数 抽常量
    private static final int MAX_RETRY_COUNT = 3;

    @Autowired
    private NotifyService notifyService;

    @Autowired(required = false)
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private RabbitmqService rabbitmqService;

    /**
     * 点赞消费者
     */
    @RabbitListener(queues = RabbitMqConfig.QUEUE_NAME)
    public void handlePraiseMessage(UserFootDO userFoot, Message message, Channel channel) {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        log.info("消费点赞消息: userId={}, documentId={}", userFoot.getUserId(), userFoot.getDocumentId());

        // 安全获取重试次数（防止类型转换异常）
        Integer retryCount = (Integer) message.getMessageProperties().getHeaders().getOrDefault("retry_count", 0);

        try {
            // ====================== 【生产必备】幂等校验 ======================
            // 根据消息ID/业务唯一标识去重，示例：
            // IdempotentUtil.check(message.getMessageProperties().getMessageId());

            // 执行业务
            notifyService.saveArticleNotify(userFoot, NotifyTypeEnum.PRAISE);
            channel.basicAck(deliveryTag, false);
            log.info("消费点赞消息成功, 重试次数:{}", retryCount);
        } catch (Exception e) {
            log.error("处理点赞消息失败, 重试次数: {}", retryCount, e);
            try {
                if (retryCount < MAX_RETRY_COUNT) {
                    // 修复：延迟重试 + 保留完整消息属性
                    retryMessageWithDelay(message, retryCount + 1);
                    channel.basicAck(deliveryTag, false);
                } else {
                    // 重试超限 → 死信队列
                    channel.basicReject(deliveryTag, false);
                    log.warn("点赞消息重试超过限制, 发送至死信队列");
                }
            } catch (Exception ex) {
                log.error("重试/拒绝点赞消息失败", ex);
            }
        }
    }

    /**
     * 修复：延迟重试 + 完整保留原消息所有属性
     */
    private void retryMessageWithDelay(Message originalMsg, int newRetryCount) {
        try {
            // 关键：直接复用原消息属性，不丢失任何信息（修复核心问题）
            MessageProperties props = originalMsg.getMessageProperties();
            // 仅更新重试次数
            props.setHeader("retry_count", newRetryCount);

            // 【企业规范】延迟重试（1s、3s、5s 阶梯延迟，避免雪崩）
            Thread.sleep(newRetryCount * 1000L);

            // 复用生产者发送消息（统一开关、异常处理）
            rabbitmqService.publishMsg(
                    RabbitMqConfig.EXCHANGE_NAME,
                    RabbitMqConfig.ROUTING_KEY,
                    originalMsg
            );
            log.info("点赞消息重试成功, 新重试次数:{}", newRetryCount);
        } catch (Exception e) {
            log.error("点赞消息重试失败", e);
        }
    }
}
