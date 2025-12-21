---
layout: post
title: "Java dynamic consumer structure"
date: 2025-12-13 07:36:26 +0900
categories: [java, concurrency, design-pattern]
tags: [WatchService, StrategyPattern, Consumer, Redis, RabbitMQ]
---

<div class="mermaid">
graph TD
    A[설정 로드] --> B{파일 감시}
    B -- 변경됨 --> C[기존 종료]
    C --> D[새 컨슈머 시작]
</div>

## 🚀 프로젝트 개요: 동적 Consumer 관리 시스템

이 포스트는 Java NIO의 `WatchService`를 활용하여 외부 설정 파일(`.properties`)의 변경을 실시간으로 감지하고, 설정에 따라 실행 중인 메시지 Consumer(Redis 또는 RabbitMQ)를 안전하게 동적으로 교체하는 시스템의 핵심 코드를 소개합니다. 이는 **Strategy Pattern**과 **Observer Pattern**을 결합하여 유연하고 확장 가능한 아키텍처를 구현한 예시입니다.

---

### 1. Consumer 인터페이스 정의 (`Consumer.java`)

모든 Consumer 구현체가 따라야 할 계약을 정의합니다. `connect()`, `start()`, `close()` 세 가지 핵심 메서드를 통해 생명주기를 관리합니다.

{% highlight java %}
package ProCon;

public interface Consumer {
    void connect() throws Exception;
    void start() throws Exception;
    void close() throws Exception;
    String getName();
}
{% endhighlight %}

---

### 2. 추상 Consumer 기본 클래스 (`AbstractConsumer.java`)

`Consumer` 인터페이스를 구현하며, 모든 구체적인 Consumer가 공유할 공통 필드(`name`, `connected`)와 로깅 유틸리티 메서드를 제공합니다.

{% highlight java %}
package ProCon;

public abstract class AbstractConsumer implements Consumer {

    protected String name;
    protected boolean connected = false;

    public AbstractConsumer(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    protected void log(String message) {
        System.out.println("[" + name + "] " + message);
    }
}
{% endhighlight %}

---

### 3. Redis 기반 Consumer 구현 (`RedisConsumer.java`)

Redis의 Pub/Sub 기능을 사용하여 특정 채널을 구독합니다. 구독(Subscribe)은 메인 스레드를 블로킹(Blocking)하므로, `start()` 메서드 내에서 별도의 스레드를 생성하여 실행하는 것이 특징입니다. `close()` 시에는 구독 해제 후 스레드를 안전하게 종료합니다.

{% highlight java %}
package ProCon;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;


public class RedisConsumer extends AbstractConsumer {

    private String host;
    private int port;
    private Jedis jedis;
    private Thread subscriberThread;
    private JedisPubSub jedisPubSub;

    public RedisConsumer(String name, String host, int port) {
        super(name);
        this.host = host;
        this.port = port;
    }

    @Override
    public void connect() throws Exception {
        jedis = new Jedis(host, port);
        connected = true;
        log("Connected to Redis " + host + ":" + port);
    }

    @Override
    public void start() {
        subscriberThread = new Thread(() -> {
            jedisPubSub = new JedisPubSub() {
                @Override
                public void onMessage(String channel, String message) {
                    log("Received: " + message);
                }
            };
            try {
                jedis.subscribe(jedisPubSub, "redis_channel");
            } catch (Exception e) {
                log("Subscribe stopped: " + e.getMessage());
            }
        });
        subscriberThread.start();
    }

    @Override
    public void close() {
        if (jedisPubSub != null) jedisPubSub.unsubscribe();
        if (subscriberThread != null && subscriberThread.isAlive()) {
            subscriberThread.interrupt();
            try { subscriberThread.join(); } catch (InterruptedException e) { e.printStackTrace(); }
        }
        if (jedis != null) jedis.close();
        log("RedisConsumer stopped safely");
    }

    @Override
    public String getName() {
        return name;
    }
}
{% endhighlight %}

---

### 4. RabbitMQ 기반 Consumer 구현 (`RabbitConsumer.java`)

RabbitMQ 클라이언트를 사용하여 메시지 큐에 연결하고 비동기적으로 메시지를 소비합니다. `basicConsume()` 메서드가 비동기 처리를 담당하며, `close()` 시에는 채널과 커넥션을 닫아 자원을 정리합니다.

{% highlight java %}
package ProCon;

import com.rabbitmq.client.*;

public class RabbitConsumer extends AbstractConsumer {

    private String host;
    private int port;
    private String queue;
    private Connection connection;
    private Channel channel;

    public RabbitConsumer(String name, String host, int port, String queue) {
        super(name);
        this.host = host;
        this.port = port;
        this.queue = queue;
    }

    @Override
    public void connect() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        connection = factory.newConnection();
        channel = connection.createChannel();
        connected = true;
        log("Connected to RabbitMQ " + host + ":" + port);
    }

    @Override
    public void start() throws Exception {
        if (!connected) return;

        channel.basicConsume(queue, true, (tag, msg) -> {
            String body = new String(msg.getBody());
            log("Received message: " + body);
        }, tag -> {});
    }

    @Override
    public void close() throws Exception {
        if (channel != null) channel.close();
        if (connection != null) connection.close();
        log("RabbitConsumer stopped");
    }
}
{% endhighlight %}

---

### 5. 설정 파일 로더 (`PropertyLoader.java`)

클래스 패스에서 `.properties` 파일을 안전하게 읽어와 `java.util.Properties` 객체로 반환하는 유틸리티 메서드를 제공합니다.

{% highlight java %}
package ProCon;

import java.io.InputStream;
import java.util.Properties;

public class PropertyLoader {

    public static Properties load(String fileName) {
        Properties props = new Properties();

        try (InputStream input = PropertyLoader.class
                .getClassLoader()
                .getResourceAsStream(fileName)) {

            if (input == null) {
                throw new RuntimeException("Cannot find " + fileName);
            }

            props.load(input);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load " + fileName, e);
        }

        return props;
    }
}
{% endhighlight %}

---

### 6. 메인 애플리케이션 및 동적 교체 로직 (`DynamicConsumerMain.java`)

이 클래스는 프로젝트의 핵심 제어부입니다.

1.  최초 Consumer를 실행합니다.
2.  `WatchService` 를 설정하여 `a.properties` 파일의 수정 이벤트를 감시합니다.
3.  파일 변경이 감지되면, 기존 Consumer를 `close()`로 안전하게 종료하고, 새 설정에 따라 `createConsumerFromProps()` 팩토리 메서드를 통해 새로운 Consumer를 생성하여 실행합니다.

{% highlight java %}
package ProCon;

import ProducerPack.PropertyLoader; // 패키지 구조에 따라 수정 필요
import java.nio.file.*;
import java.util.Properties;

public class DynamicConsumerMain {

    private static Consumer currentConsumer = null;

    public static void main(String[] args) throws Exception {

        String configDir = "src/main/resources";
        String configFile = "a.properties";

        // 최초 properties 로드
        Properties props = PropertyLoader.load(configFile);
        currentConsumer = createConsumerFromProps(props);
        currentConsumer.connect();
        currentConsumer.start();

        System.out.println("Watching " + configFile + " for changes...");

        // WatchService 설정
        WatchService watcher = FileSystems.getDefault().newWatchService();
        Paths.get(configDir).register(watcher, StandardWatchEventKinds.ENTRY_MODIFY);

        while (true) {
            WatchKey key = watcher.take();
            for (WatchEvent<?> event : key.pollEvents()) {
                Path changed = (Path) event.context();
                if (changed.toString().equals(configFile)) {
                    System.out.println("Config file changed! Reloading...");

                    // 새 properties 로드
                    Properties newProps = PropertyLoader.load(configFile);

                    // 기존 Consumer 종료
                    if (currentConsumer != null) {
                        currentConsumer.close();
                    }

                    // 새 Consumer 생성 및 실행
                    currentConsumer = createConsumerFromProps(newProps);
                    currentConsumer.connect();
                    currentConsumer.start();
                }
            }
            key.reset();
        }
    }

    // properties 기반으로 Consumer 생성 (Strategy Factory)
    private static Consumer createConsumerFromProps(Properties props) {
        if ("true".equalsIgnoreCase(props.getProperty("use.redis"))) {
            String host = props.getProperty("redis.host");
            int port = Integer.parseInt(props.getProperty("redis.port"));
            return new RedisConsumer("RedisConsumer", host, port);
        } else if ("true".equalsIgnoreCase(props.getProperty("use.rabbitmq"))) {
            String host = props.getProperty("rabbitmq.host");
            int port = Integer.parseInt(props.getProperty("rabbitmq.port"));
            String queue = "sample.queue";  
            return new RabbitConsumer("RabbitConsumer", host, port, queue);
        } else {
            throw new RuntimeException("No consumer type enabled in properties");
        }
    }
}
{% endhighlight %}

이제 이 마크다운 파일을 `.md` 확장자로 저장하시면 Jekyll 기반 블로그에 쉽게 게시하실 수 있습니다.
