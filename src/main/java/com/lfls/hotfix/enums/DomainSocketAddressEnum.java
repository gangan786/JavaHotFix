package com.lfls.hotfix.enums;



/**
 * 记录DomainSocket地址
 */
public enum DomainSocketAddressEnum {

    /**
     * 用于传输服务器监听端口fd
     */
    TRANSFER_LISTENER("/tmp/transfer-listener.sock"),
    TRANSFER_FD("/tmp/transfer-fd.sock"),
    TRANSFER_DATA("/tmp/transfer-data.sock"),

    /**
     * 新者与老者沟通是否hot-fix的桥梁
     */
    TAG_HOTFIX("/tmp/hotfix.sock");


    private String domainSocketAddress;

    DomainSocketAddressEnum(String domainSocketAddress) {
        this.domainSocketAddress = domainSocketAddress;
    }

    public String path() {
        return domainSocketAddress;
    }
}
