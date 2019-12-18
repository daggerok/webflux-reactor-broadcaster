package com.example.broadcaster;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import javax.annotation.PostConstruct;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
class MyMessage {
    private Long id;
    private String body;
}

@Configuration
class MyConfig {

    @Bean
    FluxProcessor<MyMessage, MyMessage> myProcessor() {
        // return reactor.core.publisher.WorkQueueProcessor.create(); // deprecated
        // return reactor.core.publisher.TopicProcessor.create(); // deprecated, see EmitterProcessor
        // return reactor.core.publisher.UnicastProcessor.create(); // direct with backpressure (internal buffer)
        // return reactor.core.publisher.DirectProcessor.create(); // direct without backpressure
        // return reactor.core.publisher.ReplayProcessor.create(); // synchronous: caching
        return reactor.core.publisher.EmitterProcessor.create(); // synchronous: backpressure clears internal buffer
    }

    @Bean
    Consumer<MyMessage> publisher(FluxProcessor<MyMessage, MyMessage> myProcessor) {
        return myProcessor::onNext;
    }

    @Bean
    Flux<MyMessage> subscription(Scheduler myScheduler,
                                 FluxProcessor<MyMessage, MyMessage> myProcessor) {

        return myProcessor.publishOn(myScheduler)
                          .subscribeOn(myScheduler)
                          .onBackpressureBuffer()
                          .share();
    }

    @Bean
    Scheduler myScheduler() {
        return Schedulers.single();
    }

    @Bean
    Collection<MyMessage> db() {
        return new CopyOnWriteArrayList<>();
    }
}

@RestController
@RequiredArgsConstructor
class MyResource {

    private final Collection<MyMessage> db;
    private final Flux<MyMessage> subscription;
    private final Consumer<MyMessage> publisher;

    @GetMapping({ "/last", "/last/{amount}" })
    Flux<MyMessage> last(@PathVariable(required = false) Long amount) {
        long n = Optional.ofNullable(amount)
                         .filter(l -> l >= 0)
                         .orElse(Long.MAX_VALUE);
        return Flux.fromStream(db.stream()
                                 .sorted((o1, o2) -> -o1.getId().compareTo(o2.getId())))
                   .take(n);
    }

    @GetMapping(produces = {
            MediaType.APPLICATION_STREAM_JSON_VALUE,
            MediaType.TEXT_EVENT_STREAM_VALUE,
    })
    Flux<MyMessage> stream() {
        return subscription;
    }

    @PostMapping
    Mono<Void> send(@RequestBody MyMessage myMessage) {
        return Mono.fromRunnable(() -> publisher.accept(myMessage));
    }
}

@Log4j2
@Component
@RequiredArgsConstructor
class MyListener {

    private final Collection<MyMessage> db;
    private final Flux<MyMessage> subscription;

    @PostConstruct
    public void collect() {
        AtomicLong id = new AtomicLong(0);
        subscription.map(m -> m.setId(id.getAndIncrement()))
                    .doOnNext(log::info)
                    .subscribe(db::add);
    }
}

@SpringBootApplication
public class MyBroadcaster {

    public static void main(String[] args) {
        SpringApplication.run(MyBroadcaster.class, args);
    }
}
