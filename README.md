# kafka producer & consumer skeleton

1. This project includes Skeleton Kafka producer/consumer class & LoopExecutorServiceWrap class.
2. Skeleton kafka producer & consumer class is designed for multi-thread.
3. LoopExecutorServiceWrap is designed for executing multiple producers or consumers.

# Usage

1. Import this project as module in your project.
2. Extends Skeleton class and write business logic (SkeletonProducerLoop for Producer, SekeltonConsumerLoop for Consumer).
3. override methods & write bussiness logic.
4. Producer supply methods such as init/shutdown/release, read/send/sent
5. Consumer supply methods such as init/shutdown/release, process
6. Execute your producer or consumer using LoopExecutorServiceWrap.

# Defendencis

    <dependencies>
        <dependency>
            <groupId>org.apache.commons</groupId>
            <artifactId>commons-lang3</artifactId>
            <version>3.4</version>
        </dependency>

        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>0.10.2.1</version>
        </dependency>
    </dependencies>
