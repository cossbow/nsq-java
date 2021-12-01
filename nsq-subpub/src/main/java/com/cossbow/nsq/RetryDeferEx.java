package com.cossbow.nsq;

/**
 * 用于标记正常重试（非错误异常）
 */
public class RetryDeferEx extends RuntimeException {
    private static final long serialVersionUID = -7295539688802200321L;


    private final int defer;     // 按定义值操作

    public RetryDeferEx() {
        this(PubSubUtil.ATTEMPT_STOP_VALUE);
    }

    /**
     * @param defer 毫秒
     */
    public RetryDeferEx(int defer) {
        super(null, null, false, false);
        this.defer = defer;
    }


    public int getDefer() {
        return defer;
    }

    public int getDefer(int defVal) {
        return defer != PubSubUtil.ATTEMPT_STOP_VALUE ? defer : defVal;
    }

    public int getDefer(long defVal) {
        return getDefer((int) defVal);
    }

}
