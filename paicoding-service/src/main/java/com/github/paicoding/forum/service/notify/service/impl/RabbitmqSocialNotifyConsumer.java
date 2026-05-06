package com.github.paicoding.forum.service.notify.service.impl;

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
import com.github.paicoding.forum.service.user.repository.entity.UserRelationDO;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * 社交通知消费者（评论/回复/关注）
 */
@Component
@Slf4j
public class RabbitmqSocialNotifyConsumer {
    private static final int MAX_RETRY_COUNT = 3;

    @Autowired
    private NotifyMsgDao notifyMsgDao;

    @Autowired
    private NotifyService notifyService;

    @Autowired
    private ArticleReadService articleReadService;

    @Autowired
    private CommentReadService commentReadService;

    @Autowired(required = false)
    private RabbitTemplate rabbitTemplate;

    @RabbitListener(queues = RabbitMqConfig.QUEUE_NAME_SOCIAL)
    public void handleSocialMessage(NotifyMessage<?> notifyMessage, Message message, Channel channel) {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        NotifyTypeEnum notifyType = notifyMessage.getNotifyType();
        log.info("消费社交消息: type={}", notifyType);

        Integer retryCount = (Integer) message.getMessageProperties().getHeaders().getOrDefault("retry_count", 0);

        try {
            switch (notifyType) {
                case COMMENT:
                    saveCommentNotify((CommentDO) notifyMessage.getContent());
                    break;
                case REPLY:
                    saveReplyNotify((CommentDO) notifyMessage.getContent());
                    break;
                case FOLLOW:
                    saveFollowNotify((UserRelationDO) notifyMessage.getContent());
                    break;
                default:
                    log.warn("未知社交通知类型: {}", notifyType);
            }
            channel.basicAck(deliveryTag, false);
            log.info("消费社交消息成功, type={}, retryCount={}", notifyType, retryCount);
        } catch (Exception e) {
            log.error("处理社交消息失败, type={}, retryCount={}", notifyType, retryCount, e);
            try {
                if (retryCount < MAX_RETRY_COUNT) {
                    retryMessageWithDelay(message, retryCount + 1);
                    channel.basicAck(deliveryTag, false);
                } else {
                    channel.basicReject(deliveryTag, false);
                    log.warn("社交消息重试超过限制, 发送至死信队列, type={}", notifyType);
                }
            } catch (Exception ex) {
                log.error("重试/拒绝社交消息失败", ex);
            }
        }
    }

    private void saveCommentNotify(CommentDO comment) {
        NotifyMsgDO msg = new NotifyMsgDO();
        ArticleDO article = articleReadService.queryBasicArticle(comment.getArticleId());
        msg.setNotifyUserId(article.getUserId())
                .setOperateUserId(comment.getUserId())
                .setRelatedId(article.getId())
                .setType(NotifyTypeEnum.COMMENT.getType())
                .setState(NotifyStatEnum.UNREAD.getStat())
                .setMsg(comment.getContent());
        notifyMsgDao.save(msg);

        notifyService.notifyToUser(msg.getNotifyUserId(), NotifyTypeEnum.COMMENT,
                String.format("您的文章《%s》收到一个新的评论，快去看看吧", article.getTitle()));
    }

    private void saveReplyNotify(CommentDO comment) {
        NotifyMsgDO msg = new NotifyMsgDO();
        CommentDO parent = commentReadService.queryComment(comment.getParentCommentId());
        msg.setNotifyUserId(parent.getUserId())
                .setOperateUserId(comment.getUserId())
                .setRelatedId(comment.getArticleId())
                .setCommentId(comment.getId())
                .setType(NotifyTypeEnum.REPLY.getType())
                .setState(NotifyStatEnum.UNREAD.getStat())
                .setMsg(comment.getContent());
        notifyMsgDao.save(msg);

        notifyService.notifyToUser(msg.getNotifyUserId(), NotifyTypeEnum.REPLY,
                String.format("您的评价《%s》收到一个新的回复，快去看看吧", parent.getContent()));
    }

    private void saveFollowNotify(UserRelationDO relation) {
        NotifyMsgDO msg = new NotifyMsgDO().setRelatedId(0L)
                .setNotifyUserId(relation.getUserId())
                .setOperateUserId(relation.getFollowUserId())
                .setType(NotifyTypeEnum.FOLLOW.getType())
                .setState(NotifyStatEnum.UNREAD.getStat())
                .setMsg("");
        NotifyMsgDO record = notifyMsgDao.getByUserIdRelatedIdAndType(msg);
        if (record == null) {
            notifyMsgDao.save(msg);
            notifyService.notifyToUser(msg.getNotifyUserId(), NotifyTypeEnum.FOLLOW, "恭喜您获得一枚新粉丝~");
        }
    }

    private void retryMessageWithDelay(Message originalMsg, int newRetryCount) {
        try {
            MessageProperties props = originalMsg.getMessageProperties();
            props.setHeader("retry_count", newRetryCount);
            Thread.sleep(newRetryCount * 1000L);
            rabbitTemplate.send(
                    RabbitMqConfig.EXCHANGE_NAME,
                    RabbitMqConfig.ROUTING_KEY_SOCIAL,
                    originalMsg);
            log.info("社交消息重试成功, 新重试次数:{}", newRetryCount);
        } catch (Exception e) {
            log.error("社交消息重试失败", e);
        }
    }
}
