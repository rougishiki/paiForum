package com.github.paicoding.forum.service.notify.service.impl;

import com.github.paicoding.forum.service.notify.config.RabbitMqConfig;
import com.github.paicoding.forum.service.user.repository.entity.UserFootDO;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * ========================================
 * 死信队列消费者 - 处理失败的点赞消息
 * ========================================
 *
 * 功能说明：
 * 1. 接收超过重试次数的点赞消息
 * 2. 记录失败日志，便于排查问题
 * 3. 可选：发送告警通知（邮件/钉钉/企业微信）
 * 4. 可选：存储到数据库，供后续人工处理
 *
 * 触发条件：
 * - 消息重试次数超过 MAX_RETRY_COUNT（3次）
 * - 消息在队列中超过 TTL（60秒）未被消费
 * - 消费者主动拒绝消息（basicReject）
 */
@Slf4j
@Component
public class RabbitmqPraiseDlxConsumer {

    /**
     * ========================================
     * 处理死信队列中的点赞消息
     * ========================================
     *
     * @param userFoot 点赞消息体
     * @param message 原始消息对象（包含 headers、deliveryTag 等）
     * @param channel RabbitMQ 通道
     */
    @RabbitListener(queues = RabbitMqConfig.DLX_QUEUE)
    public void handleDeadLetterMessage(UserFootDO userFoot, Message message, Channel channel) {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();

        // 获取重试次数
        Integer retryCount = (Integer) message.getMessageProperties()
            .getHeaders()
            .getOrDefault("retry_count", 0);

        log.error("========== 收到死信消息 ==========");
        log.error("消息ID: {}", message.getMessageProperties().getMessageId());
        log.error("重试次数: {}", retryCount);
        log.error("点赞用户ID: {}", userFoot.getUserId());
        log.error("文章ID: {}", userFoot.getDocumentId());
        log.error("作者ID: {}", userFoot.getDocumentUserId());
        log.error("消息内容: {}", userFoot);

        try {
            // ===== 1. 记录失败日志（已在上文完成）=====

            // ===== 2. 可选：发送告警通知 =====
            // sendAlertNotification(userFoot, retryCount);

            // ===== 3. 可选：存储到失败消息表 =====
            // saveToFailedMessageTable(userFoot, retryCount);

            // ===== 4. ACK 确认（死信消息不需要重试）=====
            channel.basicAck(deliveryTag, false);

            log.info("死信消息处理完成，已ACK确认");

        } catch (Exception e) {
            log.error("处理死信消息时发生异常", e);
            try {
                // 如果处理死信消息本身也失败，直接拒绝（不再重试）
                channel.basicReject(deliveryTag, false);
                log.warn("死信消息处理失败，已拒绝");
            } catch (Exception ex) {
                log.error("拒绝死信消息时发生异常", ex);
            }
        }
    }

    /**
     * ========================================
     * 发送告警通知（可选功能）
     * ========================================
     *
     * @param userFoot 失败的点赞消息
     * @param retryCount 重试次数
     */
    private void sendAlertNotification(UserFootDO userFoot, int retryCount) {
        // TODO: 实现告警通知逻辑
        // 示例：
        // 1. 发送邮件给管理员
        // 2. 发送钉钉/企业 webhook
        // 3. 调用监控系统的 API

        String alertMessage = String.format(
            "【点赞消息处理失败】\n" +
            "用户ID: %d\n" +
            "文章ID: %d\n" +
            "重试次数: %d\n" +
            "时间: %s",
            userFoot.getUserId(),
            userFoot.getDocumentId(),
            retryCount,
            java.time.LocalDateTime.now()
        );

        log.warn("告警通知: {}", alertMessage);

        // 实际项目中可以集成：
        // - 钉钉机器人: DingTalkUtils.sendWebhook(alertMessage)
        // - 企业微信: WeComUtils.sendText(alertMessage)
        // - 邮件: EmailService.sendAlert(adminEmail, alertMessage)
    }

    /**
     * ========================================
     * 存储到失败消息表（可选功能）
     * ========================================
     *
     * @param userFoot 失败的点赞消息
     * @param retryCount 重试次数
     */
    private void saveToFailedMessageTable(UserFootDO userFoot, int retryCount) {
        // TODO: 实现失败消息持久化逻辑
        // 示例：
        // FailedMessageDO failedMsg = new FailedMessageDO();
        // failedMsg.setMessageType("PRAISE");
        // failedMsg.setMessageContent(JsonUtil.toStr(userFoot));
        // failedMsg.setRetryCount(retryCount);
        // failedMsg.setFailReason("超过最大重试次数");
        // failedMsg.setCreateTime(new Date());
        // failedMessageDao.save(failedMsg);

        log.info("失败消息已记录到数据库（待实现）");
    }
}
