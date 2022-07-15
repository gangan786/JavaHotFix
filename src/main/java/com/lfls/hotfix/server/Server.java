package com.lfls.hotfix.server;

import com.lfls.hotfix.enums.DomainSocketAddressEnum;
import com.lfls.hotfix.enums.ServerStatus;
import com.lfls.hotfix.transfer.TransferClientDataHandler;
import com.lfls.hotfix.transfer.TransferServer;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.epoll.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author lingfenglangshao
 * @since 27/01/2020
 */
public class Server {

    //key:old channelId  value:data transfer channel
    public final Map<String, Channel> oldChannelIdToDataTransferChannelMap = new ConcurrentHashMap<>();

    //key:old channelId  value:new channelId
    private final Map<String, String> oldChannelIdToNewChannelIdMap = new ConcurrentHashMap<>();

    /**
     * 记录业务服务器每次接受到的请求
     */
    private final ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    //1. 接收调用端请求
    //2. 接收热更新事件
    private EventLoopGroup bossGroup = new EpollEventLoopGroup(2);
    private EventLoopGroup workerGroup = new EpollEventLoopGroup();

    private volatile ServerStatus status = ServerStatus.NORMAL;

    /**
     * 服务器channel
     */
    private ChannelFuture serverChannelFuture;

    private ChannelFuture hotFixServerChannelFuture;

    private ChannelFuture hotFixChannelFuture;

    private static final Server sever = new Server();

    private Server(){}

    public static Server getInstance() {
        return sever;
    }

    public void start() throws Exception {
        //使用Listener迁移代替使用SO_REUSEPORT，因此要先判断是否进行平滑升级
        if (needHotFix()){
            //通过客户端试探DomainSocket（/tmp/hotfix.sock）是否就绪
            status = ServerStatus.HOT_FIX;
        }else {
            //如果需要热更新，则更新完成以后再启动
            // 启动DomainSocket服务端
            startHotFixServer();

            //启动监听
            ServerBootstrap b = getServerBootstrapWithoutChannel();
            b.channel(EpollServerSocketChannel.class);
            System.out.println("8989服务启动");
            serverChannelFuture = b.bind(8989).sync();
        }
    }

    public ServerBootstrap getServerBootstrapWithoutChannel(){
        return new ServerBootstrap().group(bossGroup, workerGroup)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast("decode", new ServerReadHandler("server"));
                        ch.pipeline().addLast(new ChannelOutboundHandlerAdapter(){
                            @Override
                            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
                                ByteBuf buf = (ByteBuf) msg;
                                buf.retain();
                                ctx.writeAndFlush(buf).addListener(future -> {
                                    if (future.isSuccess()){
                                        buf.release();
                                    }else {
                                        if (status == ServerStatus.HOT_FIX){
                                            //热更新中，尝试发送写失败数据给new server
                                            String oldChannelId = ctx.channel().id().asLongText();
                                            if (oldChannelIdToNewChannelIdMap.containsKey(oldChannelId)){

                                                //1: read/write
                                                //4: newChannelId length
                                                //length: newChannelId
                                                //4: remain data length
                                                //length : remain data

                                                String newChannelId = oldChannelIdToNewChannelIdMap.get(oldChannelId);
                                                ByteBuf newChannelIdBuf = Unpooled.copiedBuffer(newChannelId, StandardCharsets.UTF_8);

                                                Channel transferChannel = oldChannelIdToDataTransferChannelMap.get(oldChannelId);

                                                ByteBuf buffer = transferChannel.alloc().buffer(1 + 4 + newChannelIdBuf.readableBytes() + 4 + buf.readableBytes());

                                                buffer.writeByte(1);
                                                buffer.writeInt(newChannelIdBuf.readableBytes());
                                                buffer.writeBytes(newChannelIdBuf);
                                                newChannelIdBuf.release();

                                                buffer.writeInt(buf.readableBytes());
                                                buffer.writeBytes(buf);
                                                buf.release();

                                                transferChannel.writeAndFlush(buffer).addListener(future1 -> {
                                                    if (!future1.isSuccess()){
                                                        future1.cause().printStackTrace();
                                                    }
                                                });

                                            }else {
                                                //处于热更新状态，但找不到用于迁移数据的channel，可能是超时了？
                                                buf.release();
                                            }
                                        }else {
                                            //单纯写失败
                                            System.out.println("业务服务写失败，context：" + buf);
                                            buf.release();
                                        }
                                    }
                                });
                            }
                        });
                        ch.pipeline().addLast(new ServerWriteHandler());
                        System.out.println("连接初始化完成：" + ch.id());
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
//                .option(EpollChannelOption.SO_REUSEADDR, true)
//                .option(EpollChannelOption.SO_REUSEPORT, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.TCP_NODELAY, true);
    }

    public boolean needHotFix() {
        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup)
                    .channel(EpollDomainSocketChannel.class)
                    .handler(new ChannelInboundHandlerAdapter() {
                        @Override
                        public void channelActive(ChannelHandlerContext ctx) throws Exception {
                            //连接成功，进入hotfix状态
                            /*
                            新者开启三个通道准备接受连接信息
                            /tmp/transfer-listener.sock
                            /tmp/transfer-fd.sock
                            /tmp/transfer-data.sock
                             */
                            System.out.println("新者发现老者，开启线程准备接收");
                            TransferServer.getInstance().start();

                            ByteBuf buf = ctx.alloc().buffer(4);
                            buf.writeInt(1);
                            /*
                            新者发现老者正在运行，于是写了1作为标志
                            以通知老者开始迁移listenerFd、socketFd、剩余数据
                             */
                            System.out.println("新者准备完成，通知老者开始迁移");
                            ctx.writeAndFlush(buf).addListener(future -> {
                                if (future.isSuccess()){
                                    hotFixChannelFuture.channel().close().addListener(future1 -> {
                                        if (!future1.isSuccess()){
                                            future1.cause().printStackTrace();
                                        }else {
                                            System.out.println("新者通知成功");
                                        }
                                    });
                                }else {
                                    future.cause().printStackTrace();
                                }
                            });
                        }
                    });
            SocketAddress s = new DomainSocketAddress(DomainSocketAddressEnum.TAG_HOTFIX.path());
            hotFixChannelFuture = b.connect(s).sync();
        }catch (Exception e){
            return false;
        }
        return true;
    }

    public void startHotFixServer() {
        new Thread(() -> {
            try {
                ServerBootstrap b = new ServerBootstrap();
                b.group(bossGroup, workerGroup)
                        .channel(EpollServerDomainSocketChannel.class)
                        .childHandler(new ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                                ByteBuf buf = (ByteBuf) msg;
                                if (buf.readInt() == 1){
                                    //关闭监听
                                    hotFixServerChannelFuture.channel().close().addListener(future -> {
                                        if (!future.isSuccess()){
                                            future.cause().printStackTrace();
                                        }
                                    });
                                    /*
                                    老者接收到迁移信号：1，开始迁移连接信息
                                     */
                                    System.out.println("老者接收到迁移信号：1，开始迁移连接信息");
                                    status = ServerStatus.HOT_FIX;
                                    startListenerTransferTask();
                                }
                            }
                        });
                SocketAddress s = new DomainSocketAddress(DomainSocketAddressEnum.TAG_HOTFIX.path());
                hotFixServerChannelFuture = b.bind(s).sync();
            }catch (Exception e){
                e.printStackTrace();
            }
        }).start();
        System.out.println("启动监听hot-fix");
    }

    private final ExecutorService transferExecutors = Executors.newFixedThreadPool(10);

    public void startListenerTransferTask() {
        transferExecutors.execute(() -> {
            try {
                Bootstrap bootstrap = new Bootstrap();
                bootstrap.group(workerGroup)
                        .channel(EpollDomainSocketChannel.class)
                        .handler(new ChannelInboundHandlerAdapter(){
                            @Override
                            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                /*
                                在该连接建立完成的时候，
                                会将服务器channel对应的文件描述符发给：/tmp/transfer-listener.sock的接收方
                                 */
                                System.out.println("老者开始迁移listener");
                                ctx.writeAndFlush(((EpollServerSocketChannel)serverChannelFuture.channel()).fd()).addListener(future -> {
                                    if (!future.isSuccess()){
                                        future.cause().printStackTrace();
                                    }
                                });
                            }

                            @Override
                            public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                                ByteBuf buf = (ByteBuf) msg;
                                if (buf.readInt() == 1){
                                    //Listener迁移完成，关闭当前Listener
                                    serverChannelFuture.channel().close().addListener(future -> {
                                        if (future.isSuccess()) {
                                            //开始存量连接的迁移
                                            System.out.println("老者完成迁移listener，开始存量连接的迁移");
                                            startFDTransferTask();
                                        }else {
                                            future.cause().printStackTrace();
                                        }
                                    });
                                }else {
                                    //TODO 非法响应？
                                }
                            }
                        });

                SocketAddress fdAddr = new DomainSocketAddress(DomainSocketAddressEnum.TRANSFER_LISTENER.path());
                bootstrap.connect(fdAddr).sync();
            }catch (Exception e){
                e.printStackTrace();
            }
        });
    }

    public void startFDTransferTask() {
        if (channelGroup.isEmpty()){
            //没有存量连接需要迁移，直接退出当前进程
            System.exit(1);
        }
        /*
        遍历所有客户端连接
        通过/tmp/transfer-fd.sock
        并从老者传送给新者
         */
        for (Channel channel : channelGroup) {
            transferExecutors.execute(() -> {
                try {
                    Bootstrap bootstrap = new Bootstrap();
                    bootstrap.group(workerGroup)
                            .channel(EpollDomainSocketChannel.class)
                            .handler(new ChannelInboundHandlerAdapter(){
                                @Override
                                public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                    ctx.writeAndFlush(((EpollSocketChannel)channel).fd()).addListener(future -> {
                                        if (!future.isSuccess()) {
                                            future.cause().printStackTrace();
                                        } else {
                                            System.out.println("老连接发送成功：" + "oldChannelId:" + ((EpollSocketChannel)channel).fd());
                                        }
                                    });
                                }

                                @Override
                                public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                                    ByteBuf buf = (ByteBuf) msg;
                                    int newChannelIdLength = buf.readInt();
                                    String newChannelId = buf.toString(buf.readerIndex(), newChannelIdLength, StandardCharsets.UTF_8);

                                    oldChannelIdToNewChannelIdMap.put(channel.id().asLongText(), newChannelId);
                                    System.out.println("老连接接收成功：" + "oldChannelId:" + ((EpollSocketChannel) channel).fd() + ", newChannelId:" + newChannelId);
                                    //迁移存量数据
                                    System.out.println("开始迁移存量数据");
                                    startTransferReadData(channel);

                                    //FD已经迁移完，关闭用来迁移FD的连接
                                    ctx.channel().close().addListener(future -> {
                                        if (!future.isSuccess()){
                                            future.cause().printStackTrace();
                                        }
                                    });
                                }
                            });

                    SocketAddress fdAddr = new DomainSocketAddress(DomainSocketAddressEnum.TRANSFER_FD.path());
                    bootstrap.connect(fdAddr).sync();
                }catch (Exception e){
                    e.printStackTrace();
                }
            });
        }
    }

    public void startTransferReadData(Channel oldChannel) {
        transferExecutors.execute(() -> {
            try {
                Bootstrap bootstrap = new Bootstrap();
                bootstrap.group(workerGroup)
                        .channel(EpollDomainSocketChannel.class)
                        .handler(new ChannelInitializer<EpollDomainSocketChannel>() {
                            @Override
                            protected void initChannel(EpollDomainSocketChannel ch) throws Exception {
                                ch.pipeline().addLast(new IdleStateHandler(5, 5, 5));
                                ch.pipeline().addLast(new TransferClientDataHandler(oldChannel));
                                ch.pipeline().addLast(new ChannelInboundHandlerAdapter(){
                                    @Override
                                    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
                                        if (TransferServer.getInstance().closeEvent(evt, ctx.channel().closeFuture())){
                                            //清理
                                            oldChannelIdToDataTransferChannelMap.remove(oldChannel.id().asLongText());
                                            oldChannelIdToNewChannelIdMap.remove(oldChannel.id().asLongText());
                                            System.out.println("老者的oldChannel:" + oldChannel.id().asLongText() + " 剩余数据迁移完成");
                                            if (oldChannelIdToDataTransferChannelMap.size() == 0){
                                                //完成迁移，退出老进程
                                                System.out.println("老者完成所有channel的迁移，关机下线");
                                                System.exit(1);
                                            }
                                        }else {
                                            super.userEventTriggered(ctx,evt);
                                        }
                                    }
                                });
                            }
                        });

                SocketAddress dataAddr = new DomainSocketAddress("/tmp/transfer-data.sock");
                bootstrap.connect(dataAddr).sync();
            }catch (Exception e){
                e.printStackTrace();
            }
        });
    }

    public ChannelFuture registerBossChannel(Channel channel){
        assert serverChannelFuture == null;
        serverChannelFuture = bossGroup.register(channel);
        return serverChannelFuture;
    }

    public ChannelFuture registerChannel(Channel channel){
        return workerGroup.register(channel);
    }

    public boolean addChannel(Channel channel){
        return channelGroup.add(channel);
    }

    public String getNewChannelIdByOldChannelId(String oldChannelId){
        return oldChannelIdToNewChannelIdMap.get(oldChannelId);
    }

    public void addTransferDataChannel(String channelId, Channel channel) {
        oldChannelIdToDataTransferChannelMap.put(channelId, channel);
    }

    public void changeStatus(ServerStatus status){
        this.status = status;
    }

    public ServerStatus getServerStatus(){
        return status;
    }

    public void shutDown(){
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
    }

}
