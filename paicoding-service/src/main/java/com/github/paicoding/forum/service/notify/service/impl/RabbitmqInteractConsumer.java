package com.github.paicoding.forum.service.notify.service.impl;

import com.github.paicoding.forum.api.model.enums.DocumentTypeEnum;
import com.github.paicoding.forum.api.model.enums.NotifyStatEnum;
import com.github.paicoding.forum.api.model.enums.NotifyTypeEnum;
import com.github.paicoding.forum.api.model.vo.notify.NotifyMessage;
import com.github.paicoding.forum.service.article.repository.entity.ArticleDO;
import com.github.paicoding.forum.service.article.service.ArticleReadService;
import com.github.paicoding.forum.service.comment.repository.entity.CommentDO;
import com.github.paicoding.forum.service.comment.service.CommentReadService;
import com.github.paicoding.forum.service.notify.config.RabbitMqConfig;
import com.github.paicoding.forum.service.notify.repository.dao.NotifyMsgDao;
import com.github.paicoding.forum.service.notify.repository.entity.NotifyMsgDO;
import com.github.paicoding.forum.service.notify.service.NotifyService;
import com.github.paicoding.forum.service.user.repository.entity.UserFootDO;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Objects;

/**
 * 互动通知消费者（点赞/收藏）
 */
@Component
@Slf4j
public class RabbitmqInteractConsumer {
    private static final int MAX_RETRY_COUNT = 3;

    @Autowired
    private NotifyMsgDao notifyMsgDao;

    @Autowired
    private NotifyService notifyService;

    @Autowired
    private CommentReadService commentReadService;

    @Autowired
    private ArticleReadService articleReadService;

    @Autowired(required = false)
    private RabbitTemplate rabbitTemplate;

    @RabbitListener(queues = RabbitMqConfig.QUEUE_NAME_INTERACT)
    public void handleInteractMessage(NotifyMessage<UserFootDO> notifyMessage, Message message, Channel channel) {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        UserFootDO foot = notifyMessage.getContent();
        NotifyTypeEnum notifyType = notifyMessage.getNotifyType();
        log.info("消费互动消息: type={}, userId={}, documentId={}", notifyType, foot.getUserId(), foot.getDocumentId());

        Integer retryCount = (Integer) message.getMessageProperties().getHeaders().getOrDefault("retry_count", 0);

        try {
            saveArticleNotify(foot, notifyType);
            channel.basicAck(deliveryTag, false);
            log.info("消费互动消息成功, type={}, retryCount={}", notifyType, retryCount);
        } catch (Exception e) {
            log.error("处理互动消息失败, type={}, retryCount={}", notifyType, retryCount, e);
            try {
                if (retryCount < MAX_RETRY_COUNT) {
                    retryMessageWithDelay(message, retryCount + 1);
                    channel.basicAck(deliveryTag, false);
                } else {
                    channel.basicReject(deliveryTag, false);
                    log.warn("互动消息重试超过限制, 发送至死信队列");
                }
            } catch (Exception ex) {
                log.error("重试/拒绝互动消息失败", ex);
            }
        }
    }

    private void saveArticleNotify(UserFootDO foot, NotifyTypeEnum notifyType) {
        NotifyMsgDO msg = new NotifyMsgDO()
                .setRelatedId(foot.getDocumentId())
                .setNotifyUserId(foot.getDocumentUserId())
                .setOperateUserId(foot.getUserId())
                .setType(notifyType.getType())
                .setState(NotifyStatEnum.UNREAD.getStat())
                .setMsg("");
        if (Objects.equals(foot.getDocumentType(), DocumentTypeEnum.COMMENT.getCode())) {
            CommentDO comment = commentReadService.queryComment(foot.getDocumentId());
            ArticleDO article = articleReadService.queryBasicArticle(comment.getArticleId());
            msg.setMsg(String.format("赞了您在文章 <a href=\"/article/detail/%d\">%s</a> 下的评论 <span style=\"color:darkslategray;font-style: italic;font-size: 0.9em\">%s</span>",
                    article.getId(), article.getTitle(), comment.getContent()));
        }

        NotifyMsgDO record = notifyMsgDao.getByUserIdRelatedIdAndType(msg);
        if (record == null) {
            notifyMsgDao.save(msg);
            notifyService.notifyToUser(msg.getNotifyUserId(), notifyType,
                    String.format("太棒了，您的%s %s数+1!!!",
                            Objects.equals(foot.getDocumentType(), DocumentTypeEnum.ARTICLE.getCode()) ? "文章" : "评论",
                            notifyType.getMsg()));
        }
    }

    private void retryMessageWithDelay(Message originalMsg, int newRetryCount) {
        try {
            MessageProperties props = originalMsg.getMessageProperties();
            props.setHeader("retry_count", newRetryCount);
            Thread.sleep(newRetryCount * 1000L);
            rabbitTemplate.send(
                    RabbitMqConfig.EXCHANGE_NAME,
                    RabbitMqConfig.ROUTING_KEY_INTERACT,
                    originalMsg);
            log.info("互动消息重试成功, 新重试次数:{}", newRetryCount);
        } catch (Exception e) {
            log.error("互动消息重试失败", e);
        }
    }
}
