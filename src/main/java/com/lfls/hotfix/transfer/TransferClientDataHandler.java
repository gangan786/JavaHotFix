package com.lfls.hotfix.transfer;

import com.lfls.hotfix.server.Server;
import com.lfls.hotfix.server.ServerReadHandler;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.nio.charset.StandardCharsets;

/**
 * @author lingfenglangshao
 * @since 28/01/2020
 * 用于传输老连接剩余数据
 */
public class TransferClientDataHandler extends ChannelInboundHandlerAdapter {

    private Channel oldChannel;

    public TransferClientDataHandler(Channel oldChannel){
        this.oldChannel = oldChannel;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        /*
        将oldChannelId与用于传输剩余数据的Channel做映射
         */
        Server.getInstance().addTransferDataChannel(oldChannel.id().asLongText(), ctx.channel());
        String newChannelId = Server.getInstance().getNewChannelIdByOldChannelId(oldChannel.id().asLongText());
        ByteBuf newChannelIdBuf = Unpooled.copiedBuffer(newChannelId, StandardCharsets.UTF_8);
        //先注销，注销以后该channel上面不再读取任何数据，但写数据不影响
        oldChannel.deregister().addListener(future -> {
            if (future.isSuccess()){
                //发送读余量数据
                ByteBuf remain = ((ServerReadHandler) oldChannel.pipeline().get("decode")).getRemainData();
                ByteBuf readRemain = remain.retainedSlice();
                //1: read/write
                //4: newChannelId length
                //length: newChannelId
                //4: remain data length
                //length : remain data
                ByteBuf buffer = ctx.alloc().buffer(1 + 4 + newChannelIdBuf.readableBytes() + 4 + readRemain.readableBytes());
                buffer.writeByte(0);
                buffer.writeInt(newChannelIdBuf.readableBytes());
                buffer.writeBytes(newChannelIdBuf);
                buffer.writeInt(readRemain.readableBytes());
                buffer.writeBytes(readRemain);

                newChannelIdBuf.release();

                ctx.writeAndFlush(buffer).addListener(future1 -> {
                    if (future1.isSuccess()){
                        /*
                        剩余数据发送完成后，close oldChannel
                         */
                        oldChannel.close().addListener(future2 -> {
                            if (!future2.isSuccess()){
                                future2.cause().printStackTrace();
                            }
                        });
                    }else {
                        future1.cause().printStackTrace();
                    }
                });
            }else {
                /*
                老者oldChannel注销失败
                 */
                future.cause().printStackTrace();
            }
        });

    }

}
