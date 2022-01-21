package com.lfls.hotfix.transfer;

import com.lfls.hotfix.enums.DomainSocketAddressEnum;
import com.lfls.hotfix.enums.ServerStatus;
import com.lfls.hotfix.server.Server;
import com.lfls.hotfix.server.ServerReadHandler;
import com.lfls.hotfix.server.ServerWriteHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.epoll.*;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.channel.unix.DomainSocketReadMode;
import io.netty.channel.unix.FileDescriptor;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;

import java.lang.reflect.Method;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author lingfenglangshao
 * @since 28/01/2020
 */
public class TransferServer {

    //1. 接收listener的迁移
    //2. 接收被迁移的连接
    //3. 接收连接的存量数据
    private EventLoopGroup bossGroup = new EpollEventLoopGroup(3);
    private EventLoopGroup workerGroup = new EpollEventLoopGroup();

    /**
     * 从老者迁移过来的新channel_id --> 新EpollSocketChannel
     */
    private final Map<String, Channel> transferChannels = new ConcurrentHashMap<>();

    private ChannelFuture listenerChannelFuture;
    private ChannelFuture fdChannelFuture;
    private ChannelFuture dataChannelFuture;

    private static final TransferServer server = new TransferServer();

    private TransferServer(){}

    public static TransferServer getInstance(){
        return server;
    }

    /**
     * 新者开启三个通道准备接受连接信息
     * /tmp/transfer-listener.sock
     * /tmp/transfer-fd.sock
     * /tmp/transfer-data.sock
     */
    public void start(){

        Thread listenerServer = new Thread(() -> {
            try {
                ServerBootstrap b = new ServerBootstrap();
                b.group(bossGroup, workerGroup)
                        .channel(EpollServerDomainSocketChannel.class)
                        .handler(new ChannelInitializer<EpollServerDomainSocketChannel>() {
                            @Override
                            protected void initChannel(EpollServerDomainSocketChannel ch) throws Exception {
                                ch.pipeline().addLast(new IdleStateHandler(10, 10, 10));
                                ch.pipeline().addLast(new ChannelInboundHandlerAdapter(){
                                    @Override
                                    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
                                        if (!closeEvent(evt, listenerChannelFuture)){
                                            super.userEventTriggered(ctx,evt);
                                        }
                                    }
                                });
                            }
                        })
                        .childHandler(new ChannelInitializer<EpollDomainSocketChannel>() {

                            @Override
                            protected void initChannel(EpollDomainSocketChannel ch) throws Exception {

                                ch.pipeline().addLast(new IdleStateHandler(10, 10, 10));
                                ch.pipeline().addLast(new ChannelInboundHandlerAdapter(){
                                    @Override
                                    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
                                        if (!closeEvent(evt, ctx.channel().closeFuture())){
                                            super.userEventTriggered(ctx,evt);
                                        }
                                    }
                                });
                                ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {

                                    @Override
                                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                                        //给收到的Listener FD构建新的Server Channel
                                        FileDescriptor fd = (FileDescriptor) msg;
                                        EpollServerSocketChannel serverSocketChannel = new EpollServerSocketChannel(fd.intValue());

                                        //构造完整的ServerSocket处理链路
                                        ServerBootstrap serverBootstrap = Server.getInstance().getServerBootstrapWithoutChannel();
                                        Method initMethod = serverBootstrap.getClass().getDeclaredMethod("init", Channel.class);
                                        initMethod.setAccessible(true);
                                        initMethod.invoke(serverBootstrap, serverSocketChannel);
                                        /*
                                        完成监听端口的迁移
                                         */
                                        Server.getInstance().registerListener(serverSocketChannel).addListener(future -> {
                                            if (future.isSuccess()){
                                                //注册成功以后进行响应
                                                /*
                                                通知老者已经完成监听端口迁移
                                                 */
                                                ctx.writeAndFlush(Unpooled.copyInt(1)).addListener(future1 -> {
                                                    if (!future1.isSuccess()){
                                                        future1.cause().printStackTrace();
                                                    }
                                                });
                                            }else {
                                                future.cause().printStackTrace();
                                            }
                                        });
                                    }

                                });
                            }
                        })
                        .childOption(EpollChannelOption.DOMAIN_SOCKET_READ_MODE, DomainSocketReadMode.FILE_DESCRIPTORS);
                SocketAddress s = new DomainSocketAddress(DomainSocketAddressEnum.TRANSFER_LISTENER.path());
                listenerChannelFuture = b.bind(s).sync();
            }catch (Exception e){
                e.printStackTrace();
            }
        });

        Thread fdServer = new Thread(() -> {
            try {
                ServerBootstrap b = new ServerBootstrap();
                b.group(bossGroup, workerGroup)
                        .channel(EpollServerDomainSocketChannel.class)
                        .handler(new ChannelInitializer<EpollServerDomainSocketChannel>() {
                            @Override
                            protected void initChannel(EpollServerDomainSocketChannel ch) throws Exception {
                                ch.pipeline().addLast(new IdleStateHandler(10, 10, 10));
                                ch.pipeline().addLast(new ChannelInboundHandlerAdapter(){
                                    @Override
                                    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
                                        if (!closeEvent(evt, fdChannelFuture)){
                                            super.userEventTriggered(ctx,evt);
                                        }
                                    }
                                });
                            }
                        })
                        .childHandler(new ChannelInitializer<EpollDomainSocketChannel>() {

                            @Override
                            protected void initChannel(EpollDomainSocketChannel ch) throws Exception {

                                ch.pipeline().addLast(new IdleStateHandler(10, 10, 10));
                                ch.pipeline().addLast(new ChannelInboundHandlerAdapter(){
                                    @Override
                                    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
                                        if (!closeEvent(evt, ctx.channel().closeFuture())){
                                            super.userEventTriggered(ctx,evt);
                                        }
                                    }
                                });
                                ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {

                                    @Override
                                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                                        //给收到的FD构建新的Channel
                                        FileDescriptor fd = (FileDescriptor) msg;
                                        /*
                                        这个新构建的channel怎么绑定Service？
                                        新构建的channel在收到并处理老连接的存量数据后
                                        就会注册到 workerGroup --> EpollEventLoopGroup
                                         */
                                        EpollSocketChannel socketChannel = new EpollSocketChannel(fd.intValue());
                                        /*
                                           ServerReadHandler 作用是啥？
                                           应该是用于处理长连接的业务处理
                                         */
                                        socketChannel.pipeline().addLast("decode", new ServerReadHandler("transfer server"));
                                        socketChannel.pipeline().addLast(new ChannelOutboundHandlerAdapter(){
                                            @Override
                                            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
                                                ctx.writeAndFlush(msg);
                                            }
                                        });
                                        socketChannel.pipeline().addLast(new ServerWriteHandler());

                                        //通知old server正在迁移的连接对应的new channel ID
                                        String newChannelId = socketChannel.id().asLongText();
                                        ByteBuf newChannelIdBuf = Unpooled.copiedBuffer(newChannelId, StandardCharsets.UTF_8);
                                        ByteBuf newIdBuf = ctx.alloc().buffer(4 + newChannelIdBuf.readableBytes());
                                        newIdBuf.writeInt(newChannelIdBuf.readableBytes());
                                        newIdBuf.writeBytes(newChannelIdBuf);
                                        newChannelIdBuf.release();

                                        ctx.writeAndFlush(newIdBuf).addListener(future -> {
                                            if (future.isSuccess()){
                                                transferChannels.put(newChannelId, socketChannel);
                                            }
                                        });
                                    }

                                });
                            }
                        })
                        .childOption(EpollChannelOption.DOMAIN_SOCKET_READ_MODE, DomainSocketReadMode.FILE_DESCRIPTORS);
                SocketAddress s = new DomainSocketAddress(DomainSocketAddressEnum.TRANSFER_FD.path());
                fdChannelFuture = b.bind(s).sync();
            }catch (Exception e){
                e.printStackTrace();
            }
        });

        Thread dataServer = new Thread(() -> {
            try {
                AtomicInteger transferChannelCount = new AtomicInteger(0);
                ServerBootstrap b = new ServerBootstrap();
                b.group(bossGroup, workerGroup)
                        .channel(EpollServerDomainSocketChannel.class)
                        .handler(new ChannelInitializer<EpollServerDomainSocketChannel>() {
                            @Override
                            protected void initChannel(EpollServerDomainSocketChannel ch) throws Exception {
                                ch.pipeline().addLast(new IdleStateHandler(10, 10, 10));
                                ch.pipeline().addLast(new ChannelInboundHandlerAdapter(){
                                    @Override
                                    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
                                        if (!closeEvent(evt, dataChannelFuture)){
                                            super.userEventTriggered(ctx,evt);
                                        }
                                    }
                                });
                            }
                        })
                        .childHandler(new ChannelInitializer<Channel>() {
                            @Override
                            protected void initChannel(Channel ch) throws Exception {
                                ch.pipeline().addLast(new IdleStateHandler(10, 10, 10));
                                ch.pipeline().addLast(new TransferServerDataHandler());
                                ch.pipeline().addLast(new ChannelInboundHandlerAdapter(){

                                    @Override
                                    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                        /*
                                        当连接表的非活跃时
                                        说明该连接的剩余数据已经完成迁移
                                         */
                                        if (transferChannelCount.incrementAndGet() == transferChannels.size()){
                                            transferChannels.clear();
                                            Server.getInstance().changeStatus(ServerStatus.NORMAL);
                                        }
                                    }

                                    @Override
                                    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
                                        if (!closeEvent(evt, ctx.channel().closeFuture())){
                                            super.userEventTriggered(ctx,evt);
                                        }
                                    }
                                });
                            }
                        });
                SocketAddress s = new DomainSocketAddress(DomainSocketAddressEnum.TRANSFER_DATA.path());
                dataChannelFuture = b.bind(s).sync();
            }catch (Exception e){
                e.printStackTrace();
            }
        });

        listenerServer.start();
        fdServer.start();
        dataServer.start();

        try {
            listenerServer.join();
            fdServer.join();
            dataServer.join();
        }catch (InterruptedException ignore){}

    }

    public Channel getChannelById(String channelId){
        return transferChannels.get(channelId);
    }

    /**
     * 作为监听下面三个domainSocket的三个线程的同步器
     * TRANSFER_LISTENER("/tmp/transfer-listener.sock"),
     * TRANSFER_FD("/tmp/transfer-fd.sock"),
     * TRANSFER_DATA("/tmp/transfer-data.sock")
     */
    private AtomicInteger shutDownCount = new AtomicInteger(3);

    public boolean closeEvent(Object evt, ChannelFuture future){
        if (evt instanceof IdleStateEvent){
            IdleStateEvent event = (IdleStateEvent) evt;
            if (event.state() == IdleState.READER_IDLE
                    || event.state() == IdleState.WRITER_IDLE
                    || event.state() == IdleState.ALL_IDLE){
                //5s没有事件发生，认为连接迁移完毕，关闭server
                future.channel().close().addListener(f -> {
                    if (f.isSuccess()){
                        if (future == fdChannelFuture || future == dataChannelFuture || future == listenerChannelFuture){
                            if (shutDownCount.decrementAndGet() == 0){
                                /*
                                三种类型数据迁移完成
                                启动监听/tmp/hotfix.sock
                                为下一次更新做好准备
                                 */
                                Server.getInstance().startHotFixServer();
                            }
                        }
                    }else {
                        f.cause().printStackTrace();
                    }
                });
            }
            return true;
        }
        return false;
    }

    public void shutDown(){
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

}
