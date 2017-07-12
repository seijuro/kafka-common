# kafka producer & consumer skeleton

1. This project includes Skeleton Kafka producer & consumer class & LoopExecutorServiceWrap.
2. Skeleton kafka producer & consumer is designed for multi-thread.

# Usage

1. Import this project as module in your project.
2. Extends Skeleton class and write business logic (SkeletonProducerLoop for Producer, SekeltonConsumerLoop for Consumer).
3. override methods & write bussiness logic.
4. Producer supply methods such as init/shutdown/release, read/send/sent
5. Consumer supply methods such as init/shutdown/release, process
6. Execute your producer or consumer using LoopExecutorServiceWrap.
