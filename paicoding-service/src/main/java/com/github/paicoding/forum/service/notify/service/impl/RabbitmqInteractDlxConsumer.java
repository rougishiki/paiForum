package com.github.paicoding.forum.service.notify.service.impl;

import com.github.paicoding.forum.service.notify.config.RabbitMqConfig;
import com.github.paicoding.forum.service.user.repository.entity.UserFootDO;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * 死信队列消费者 - 处理失败的互动消息（点赞/收藏）
 */
@Slf4j
@Component
public class RabbitmqInteractDlxConsumer {

    @RabbitListener(queues = RabbitMqConfig.DLX_QUEUE_INTERACT)
    public void handleDeadLetterMessage(UserFootDO userFoot, Message message, Channel channel) {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();

        Integer retryCount = (Integer) message.getMessageProperties()
            .getHeaders()
            .getOrDefault("retry_count", 0);

        log.error("========== 收到互动死信消息 ==========");
        log.error("消息ID: {}", message.getMessageProperties().getMessageId());
        log.error("重试次数: {}", retryCount);
        log.error("用户ID: {}", userFoot.getUserId());
        log.error("文章/评论ID: {}", userFoot.getDocumentId());
        log.error("作者ID: {}", userFoot.getDocumentUserId());

        try {
            channel.basicAck(deliveryTag, false);
            log.info("互动死信消息处理完成，已ACK确认");
        } catch (Exception e) {
            log.error("处理互动死信消息时发生异常", e);
            try {
                channel.basicReject(deliveryTag, false);
            } catch (Exception ex) {
                log.error("拒绝互动死信消息时发生异常", ex);
            }
        }
    }
}
