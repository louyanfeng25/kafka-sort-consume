**Hello，这里是爱 Coding，爱 Hiphop，爱喝点小酒的 AKA 柏炎。**

**Kafka**是一个分布式的，支持多分区、多副本，基于 Zookeeper 的分布式消息流平台，它同时也是一款开源的**基于发布订阅模式的消息引擎系统**。

**kafka如何保证消息不丢失、顺序消费、重复消费？**

这三个问题熟不熟悉？是不是在面试的时候经常被问到，在日常工作中也经常碰到？

`保证消息不丢失与重复消费其实操作上还是比较简单的。是一些常规的八股文，本文不展开讨论，感兴趣的同学可以给我留言，我单独出一期讲解。`

本文将着重与大家讨论Kafka在consumer是单线程与多线程情况下如何保证顺序消费。

![image-20220120110050420](https://p3-juejin.byteimg.com/tos-cn-i-k3u1fbpfcp/7072aa229bb5475d8a14e59601b3b75d~tplv-k3u1fbpfcp-zoom-1.image)

> 我的第一本掘金小册[《深入浅出DDD》](https://juejin.cn/book/7049273428938850307)已经在掘金上线，欢迎大家试读~

# 一、单线程顺序消费

为了避免有的小伙伴第一次接触顺序消费的概念，我还是先介绍一下顺序消费是个什么东西。

![](https://p3-juejin.byteimg.com/tos-cn-i-k3u1fbpfcp/c1fa306490d343f1a4442e82966d7bc2~tplv-k3u1fbpfcp-zoom-1.image)

双十一，大量的用户抢在0点下订单。为了用户的友好体验，我们把订单生成逻辑与支付逻辑包装成一个个的MQ消息发送到Kafka中，让kafka积压部分消息，防止瞬间的流量压垮服务。

那么这里的问题就出现了，订单生成与支付都被包装成了消息。这两个消息是有严格的先后顺序的，订单生成逻辑肯定在支付之前。

**那么kafka怎么保证它们的顺序呢？**

`不同topic：`

如果支付与订单生成对应不同的topic，你只能在consumer层面去处理了。而因为consumer是分布式的，所以你为了保证顺序消费，只能找一个中间方（比如redis的队列）来维护MQ的顺序，成本太大，逻辑太恶心。

`同一个topic：`

如果我们把消息发送到同一个topic呢？我们知道一个topic可以对应多个分区，分别对应了多个consumer。其实与不同topic没什么本质上的差别。

`同一个topic，同一个分区：`

Kafka的消息在分区内是严格有序的。也就是说我们可以把同一笔订单的所有消息，按照生成的顺序一个个发送到同一个topic的同一个分区。那么consumer就能顺序的消费到同一笔订单的消息。

![image-20220120113215784](https://p3-juejin.byteimg.com/tos-cn-i-k3u1fbpfcp/e0288764d89a476c8134c9aab5dc9938~tplv-k3u1fbpfcp-zoom-1.image)

生产者在发送消息时，将消息对应的id进行取模处理，相同的id发送到相同的分区。消息在分区内有序，一个分区对应了一个消费者，保证了消息消费的顺序性。

![](https://p3-juejin.byteimg.com/tos-cn-i-k3u1fbpfcp/fece83f0c1d24613a090c3b6c53a8c03~tplv-k3u1fbpfcp-zoom-1.image)

# 二、多线程顺序消费

单线程顺序消费已经解决了顺序消费的问题，但是它的扩展能力很差。为了提升消费者的处理速度，但又要保证顺序性，我们只能横向扩展分区数，增加消费者。

这就意味着需要加机器来增加你的系统处理能力。

emmm，是的，那你离开除不远了。

不行就加机器，老板给你死。

![芭比Q了(猫咪表情包)_芭比_猫咪表情](https://p3-juejin.byteimg.com/tos-cn-i-k3u1fbpfcp/baa891d483b945b2835e02c9dda54359~tplv-k3u1fbpfcp-zoom-1.image)

所以说我们必然要在消费者端接收到kafka消息后做并发处理。

我们来捋一下，如果我们拿到消息，直接把消息扔到线程池呢？

不合理，线程的处理速度有快慢，还是会导致支付消息快于订单消息处理。

![](https://p3-juejin.byteimg.com/tos-cn-i-k3u1fbpfcp/2181d438d588423382bd60eff4a0511d~tplv-k3u1fbpfcp-zoom-1.image)

我是不是可以模仿一下kafka的分区思想操作。将接收到的kafka数据进行hash取模`（注意注意，你kafka分区接受消息已经是取模的了，这里一定要对id做一次hash再取模）`发送到不同的队列，然后我们开启多个线程去消费对应队列里面的数据。

芜湖，nice~

![image-20220120115253042](https://p3-juejin.byteimg.com/tos-cn-i-k3u1fbpfcp/1ac0a8394335489a91b85841a06d9639~tplv-k3u1fbpfcp-zoom-1.image)

# 三、多线程消费代码实现

在吗？看看代码：[kafka-sort-consume](https://github.com/louyanfeng25/kafka-sort-consume)

可以拉代码下来阅读下文，更加容易理解。

**整体思路：**

1.  在应用启动时初始化对应业务的顺序消费线程池（demo中为订单消费线程池）
1.  订单监听类拉取消息提交任务至线程池中对应的队列
1.  线程池的线程处理绑定队列中的任务数据
1.  每个线程处理完任务后增加待提交的offsets标识数
1.  监听类中校验待提交的offsets数与拉取到的记录数是否相等，如果相等则**手动提交offset**

## 3.1.顺序消费线程池定义

我们可以通过指定消费的线程数来提升消息的处理能力。

```
 /**
  * kafka顺序消费工具类线程池1.0
  *
  * 平滑扩容缩容待设计，stopped的钩子可以支持
  *
  * @author baiyan
  * @date 2022/01/19
  */
 @Slf4j
 @Data
 public class KafkaConsumerPool<E> {
 ​
     /**
      * 线程并发级别
      */
     private Integer concurrentSize;
 ​
     /**
      * 工作线程线程
      */
     private List<Thread> workThreads;
 ​
     /**
      * 任务处理队列
      */
     private List<ConcurrentLinkedQueue<E>> queues;
 ​
     /**
      * 是否全量停止任务,留个钩子，以便后续动态扩容
      */
     private volatile boolean stopped;
 ​
     /**
      * 待提交的记录数
      */
     private AtomicLong pendingOffsets;
 ​
     /**
      * kafka线程名前缀
      */
     private final static String KAFKA_CONSUMER_WORK_THREAD_PREFIX = "kafka-sort-consumer-thread-";
 ​
     /**
      * 顺序消费任务池初始化
      *
      * @param config 业务配置
      */
     public KafkaConsumerPool(KafkaSortConsumerConfig<E> config){
         this.concurrentSize = config.getConcurrentSize();
         //初始化任务队列
         this.initQueue();
         this.workThreads = new ArrayList<>();
         this.stopped = false;
         this.pendingOffsets = new AtomicLong(0L);
         //初始化线程
         this.initWorkThread(config.getBizName(),config.getBizService());
     }
 ​
     /**
      * 初始化队列
      */
     private void initQueue(){
         this.queues = new ArrayList<>();
         for (int i = 0; i < this.concurrentSize; i++) {
             this.queues.add(new ConcurrentLinkedQueue<>());
         }
     }
 ​
     /**
      * 初始化工作线程
      */
     private void initWorkThread(String bizName, Consumer<E> bizService){
         //创建规定的线程
         for (int i = 0; i < this.concurrentSize; i++) {
 ​
             String threadName = KAFKA_CONSUMER_WORK_THREAD_PREFIX + bizName + i;
             int num = i;
             Thread workThread = new Thread(()->{
 ​
                 //如果队列不为空 或者 线程标识为false则进入循环
                 while (!queues.get(num).isEmpty() || !stopped){
                     try{
                         E task = pollTask(threadName,bizName);
                         if(Objects.nonNull(task)){
 ​
                             //模拟业务处理耗时
                             bizService.accept(task);
 ​
                            log.info("线程：{},执行任务：{},成功",threadName, GsonUtil.beanToJson(task));
 ​
                            //执行完成的任务加1
                             pendingOffsets.incrementAndGet();
                         }
                     }catch (Exception e){
                        log.error("线程：{},执行任务：{},失败",threadName,e);
                     }
                 }
                 log.info("线程：{}退出",threadName);
             },threadName);
 ​
             //加入线程管理
             workThreads.add(workThread);
 ​
             //开启线程
             workThread.start();
         }
     }
 ​
     /**
      * 根据id取模，将需要保证顺序的任务添加至同一队列
      *
      * @param id 能够取模的键
      * @param task 需要提交处理的任务
      */
     public void submitTask(Long id, E task){
         ConcurrentLinkedQueue<E> taskQueue = queues.get((int) (id % this.concurrentSize));
         taskQueue.offer(task);
     }
 ​
     /**
      * 根据线程名获取对应的待执行的任务
      *
      * @param threadName 线程名称
      * @return 队列内的任务
      */
     private E pollTask(String threadName,String bizName){
         int threadNum = Integer.valueOf(threadName.replace(KAFKA_CONSUMER_WORK_THREAD_PREFIX+bizName, ""));
         ConcurrentLinkedQueue<E> taskQueue = queues.get(threadNum);
         return taskQueue.poll();
     }
 }
```

流程图

![image-20220120122116859](https://p3-juejin.byteimg.com/tos-cn-i-k3u1fbpfcp/844eba0f0af449c6b1d41a4c0d8bd419~tplv-k3u1fbpfcp-zoom-1.image)

## 3.2.消费者端

一个消费者可以消费多个topic，所以说，每个需要多线程顺序处理的监听类都需要单独绑定一个顺序消费线程池。

在监听类接受到消息之后通过线程池提交待执行的任务执行。

这里我们需要关闭kafka的自动提交，待本次拉取到的任务处理完成之后再提交位移。

```
 /**
  * 订单消费者
  *
  * @author baiyan
  * @date 2022/01/19
  */
 @Component
 @Slf4j
 @ConfigurationProperties(prefix = "kafka.order")
 @Data
 @EqualsAndHashCode(callSuper = false)
 public class OrderKafkaListener extends AbstractConsumerSeekAware {
 ​
     @Autowired
     private OrderService orderService;
 ​
     /**
      * 顺序消费并发级别
      */
     private Integer concurrent;
 ​
     /**
      * order业务顺序消费池
      */
     private KafkaConsumerPool<OrderDTO> kafkaConsumerPool;
 ​
     /**
      * 初始化顺序消费池
      */
     @PostConstruct
     public void init(){
         KafkaSortConsumerConfig<OrderDTO> config = new KafkaSortConsumerConfig<>();
         config.setBizName("order");
         config.setBizService(orderService::solveRetry);
         config.setConcurrentSize(concurrent);
         kafkaConsumerPool = new KafkaConsumerPool<>(config);
     }
 ​
     @KafkaListener(topics = {"${kafka.order.topic}"}, containerFactory = "baiyanCommonFactory")
     public void consumerMsg(List<ConsumerRecord<?, ?>> records, Acknowledgment ack){
         if(records.isEmpty()){
             return;
         }
 ​
         records.forEach(consumerRecord->{
             OrderDTO order = GsonUtil.gsonToBean(consumerRecord.value().toString(), OrderDTO.class);
             kafkaConsumerPool.submitTask(order.getId(),order);
         });
 ​
         // 当线程池中任务处理完成的计数达到拉取到的记录数时提交
         // 注意这里如果存在部分业务阻塞时间很长，会导致位移提交不上去，务必做好一些熔断措施
         while (true){
            if(records.size() == kafkaConsumerPool.getPendingOffsets().get()){
                ack.acknowledge();
                log.info("offset提交：{}",records.get(records.size()-1).offset());
                kafkaConsumerPool.getPendingOffsets().set(0L);
                break;
            }
         }
     }
 ​
 }
```

对应数据处理流程图

![image-20220120122152862](https://p3-juejin.byteimg.com/tos-cn-i-k3u1fbpfcp/deb1915e1af34465a43df5adffc46172~tplv-k3u1fbpfcp-zoom-1.image)

## 3.3.扩展点

demo中我们提供的思路是定死的并发级别数去处理消息。

但是比如打车软件，早高峰跟晚高峰的时候是流量的高峰期，对应的打车消息负载会很高。而平峰的时候流量就会小很多。

所以我们应该在高峰期设置一个相对较高的并发级别数用来快速处理消息，平峰期设置一个较小的并发级别数来让出系统资源。

难道我们要不断的重启应用去修改并发级别数？太麻瓜了。

我在[如何使用nacos在分布式环境下同步全局配置](https://juejin.cn/post/6993917304606031903#heading-13)提到过，美团提供了一种配置中心修改配置动态设置线程池参数的思路。

我们同样可以模仿这个思路去实现动态的扩容或者缩容顺序消费线程池。

我的demo中为了让大家更好的理解并没有实现这部分的逻辑，但是我留了一个钩子。

在**KafkaConsumerPool**中有一个属性是**stopped**，将它设置为true是可以中断启动中的线程池，但是会将待执行的任务执行完毕再退出。

因此如果我们要实现动态扩容，可以通过配置中心刷新**OrderKafkaListener**监听类中的配置**concurrent**的值，在通过set方法修改concurrent的值时，先修改stopped的值去停止当前正在执行的线程池。执行完毕后通过新的并发级别数新建一个新的线程池，实现了动态扩容与缩容。

不过这里需要注意哦，扩容阶段的时候，记得阻塞kafka的数据的消费提交，会报错哦~

最后，贴上流程图

![image-20220120124442428](https://p3-juejin.byteimg.com/tos-cn-i-k3u1fbpfcp/88ca540f6d524b0eb7fee833595144ce~tplv-k3u1fbpfcp-zoom-1.image)

# 四、总结

本文为大家介绍了kafka单线程与多线程顺序消费的思路。两者都是通过将消息绑定到定向的分区或者队列来保证顺序性，通过增加分区或者线程来提升消费能力。

![image-20220120125140790](https://p3-juejin.byteimg.com/tos-cn-i-k3u1fbpfcp/eca03a91976a451db6e6c02793a0d166~tplv-k3u1fbpfcp-zoom-1.image)

最后还为大家提供了通过配置中心动态扩容与缩容线程池的思路，如果大家感兴趣的可以编写对应的逻辑merge给我哦~

![](https://p3-juejin.byteimg.com/tos-cn-i-k3u1fbpfcp/84b99f7166a34a9884c3189bda66baf9~tplv-k3u1fbpfcp-zoom-1.image)

# 五、最后

文中如有不正确之处，欢迎指正，写文不易，点个赞吧，么么哒~

微信：baiyan_lou

公众号：柏炎大叔

小册：[深入浅出DDD](https://juejin.cn/book/7049273428938850307)

**转载请在文章开头显眼位置贴上原文链接与作者。**
