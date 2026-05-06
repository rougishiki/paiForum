package com.github.paicoding.forum.api.model.vo.notify;

import com.github.paicoding.forum.api.model.enums.NotifyTypeEnum;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class NotifyMessage<T> {

    private NotifyTypeEnum notifyType;

    private T content;
}
