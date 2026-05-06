package com.github.paicoding.forum.service.notify.service.impl;

import com.github.paicoding.forum.api.model.context.ReqInfoContext;
import com.github.paicoding.forum.api.model.enums.NotifyTypeEnum;
import com.github.paicoding.forum.core.cache.RedisClient;
import com.github.paicoding.forum.service.comment.repository.entity.CommentDO;
import com.github.paicoding.forum.service.rank.service.UserActivityRankService;
import com.github.paicoding.forum.service.rank.service.model.ActivityScoreBo;
import com.github.paicoding.forum.service.statistics.constants.CountConstants;
import com.github.paicoding.forum.service.user.repository.entity.UserFootDO;
import com.github.paicoding.forum.service.user.repository.entity.UserRelationDO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

/**
 * 通知统计服务——MQ 成功时直接调用，不再绕 Spring Event
 */
@Slf4j
@Service
public class NotifyStatisticsService {

    @Autowired
    private UserActivityRankService userActivityRankService;

    @Async
    public void updateStatistics(NotifyTypeEnum type, Object content) {
        try {
            switch (type) {
                case COMMENT:
                case REPLY:
                    updateCommentStatistics((CommentDO) content);
                    break;
                case PRAISE:
                    updatePraiseStatistics((UserFootDO) content);
                    break;
                case COLLECT:
                    updateCollectStatistics((UserFootDO) content);
                    break;
                case FOLLOW:
                    updateFollowStatistics((UserRelationDO) content);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            log.error("更新统计失败, type={}", type, e);
        }
    }

    private void updateCommentStatistics(CommentDO comment) {
        // UserStatisticEventListener: 评论数+1
        RedisClient.hIncr(CountConstants.ARTICLE_STATISTIC_INFO + comment.getArticleId(),
                CountConstants.COMMENT_COUNT, 1);
        // UserActivityListener: 活跃积分
        userActivityRankService.addActivityScore(ReqInfoContext.getReqInfo().getUserId(),
                new ActivityScoreBo().setRate(true).setArticleId(comment.getArticleId()));
    }

    private void updatePraiseStatistics(UserFootDO foot) {
        // UserStatisticEventListener
        RedisClient.hIncr(CountConstants.USER_STATISTIC_INFO + foot.getDocumentUserId(),
                CountConstants.PRAISE_COUNT, 1);
        RedisClient.hIncr(CountConstants.ARTICLE_STATISTIC_INFO + foot.getDocumentId(),
                CountConstants.PRAISE_COUNT, 1);
        // UserActivityListener
        userActivityRankService.addActivityScore(ReqInfoContext.getReqInfo().getUserId(),
                new ActivityScoreBo().setPraise(true).setArticleId(foot.getDocumentId()));
    }

    private void updateCollectStatistics(UserFootDO foot) {
        // UserStatisticEventListener
        RedisClient.hIncr(CountConstants.USER_STATISTIC_INFO + foot.getDocumentUserId(),
                CountConstants.COLLECTION_COUNT, 1);
        RedisClient.hIncr(CountConstants.ARTICLE_STATISTIC_INFO + foot.getDocumentId(),
                CountConstants.COLLECTION_COUNT, 1);
        // UserActivityListener
        userActivityRankService.addActivityScore(ReqInfoContext.getReqInfo().getUserId(),
                new ActivityScoreBo().setCollect(true).setArticleId(foot.getDocumentId()));
    }

    private void updateFollowStatistics(UserRelationDO relation) {
        // UserStatisticEventListener
        RedisClient.hIncr(CountConstants.USER_STATISTIC_INFO + relation.getUserId(),
                CountConstants.FANS_COUNT, 1);
        RedisClient.hIncr(CountConstants.USER_STATISTIC_INFO + relation.getFollowUserId(),
                CountConstants.FOLLOW_COUNT, 1);
        // UserActivityListener
        userActivityRankService.addActivityScore(ReqInfoContext.getReqInfo().getUserId(),
                new ActivityScoreBo().setFollow(true).setFollowedUserId(relation.getUserId()));
    }
}
