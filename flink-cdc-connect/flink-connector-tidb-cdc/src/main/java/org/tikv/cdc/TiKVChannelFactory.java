package org.tikv.cdc;

import lombok.extern.slf4j.Slf4j;
import org.tikv.common.HostMapping;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.pd.PDUtils;
import org.tikv.shade.io.grpc.ManagedChannel;
import org.tikv.shade.io.grpc.netty.NettyChannelBuilder;
import org.tikv.shade.io.netty.handler.ssl.SslContext;
import org.tikv.shade.io.netty.handler.ssl.SslContextBuilder;

import javax.net.ssl.SSLException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
public class TiKVChannelFactory implements AutoCloseable {
    private final long connRecycleTime;
    private final int maxFrameSize;
    private final int keepaliveTime;
    private final int keepaliveTimeout;
    private final int idleTimeout;
    public final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    TiKVChannelFactory(int maxFrameSize, int keepaliveTime, int keepaliveTimeout, int idleTimeout) {
        this.maxFrameSize = maxFrameSize;
        this.keepaliveTime = keepaliveTime;
        this.keepaliveTimeout = keepaliveTimeout;
        this.idleTimeout = idleTimeout;
        this.connRecycleTime = 0;
    }

    public static Builder builder() {
        return new Builder();
    }

    public ManagedChannel getChannel(String address, HostMapping mapping) {
        return channels.computeIfAbsent(address, key -> createChannel(null, address, mapping));
    }

    private ManagedChannel createChannel(
            SslContextBuilder sslContextBuilder, String address, HostMapping mapping) {
        URI uri, mapped;
        try {
            uri = PDUtils.addrToUri(address);
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to form address " + address, e);
        }
        try {
            mapped = mapping.getMappedURI(uri);
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to get mapped address " + uri, e);
        }
        // Channel should be lazy without actual connection until the first call,
        // So a coarse grain lock is ok here
        NettyChannelBuilder builder =
                NettyChannelBuilder.forAddress(mapped.getHost(), mapped.getPort())
                        .maxInboundMessageSize(maxFrameSize)
                        .keepAliveTime(keepaliveTime, TimeUnit.SECONDS)
                        .keepAliveTimeout(keepaliveTimeout, TimeUnit.SECONDS)
                        .keepAliveWithoutCalls(true)
                        .idleTimeout(idleTimeout, TimeUnit.SECONDS);
        if (sslContextBuilder == null) {
            return builder.usePlaintext().build();
        } else {
            SslContext sslContext;
            try {
                sslContext = sslContextBuilder.build();
            } catch (SSLException e) {
                log.error("create ssl context failed!", e);
                throw new IllegalArgumentException(e);
            }
            return builder.sslContext(sslContext).build();
        }
    }

    @Override
    public void close() throws Exception {
        for (ManagedChannel ch : channels.values()) {
            ch.shutdown();
        }
        channels.clear();
    }

    public static class Builder {
        public TiKVChannelFactory build(TiSession session) {
            TiConfiguration conf = session.getConf();
            return new TiKVChannelFactory(conf.getMaxFrameSize(),
                    conf.getKeepaliveTime(),
                    conf.getKeepaliveTimeout(),
                    conf.getIdleTimeout());
        }
    }
}
