## **定位** 

 通过Spark的UI定位如下，可以发现整个任务中有一个stage的**执行时间过长**（Spark UI 每一列点击可以排序，这里倒序排列），由此可以定位到执行时间较长的stage。

![img](http://img-hxy021.didistatic.com/static/km/do1_xyGGBLmylSusPD6WMlWj)

深入这个stage的具体运行情况可以看到，其中一个task的运行时间占到了整个stage的运行周期，进入Stage UI（点击上图Description下面的链接），通过所有taskde统计信息可以很明显的发现，这个stage（task set）存在异常，存在**数据倾斜**（**75%的task运行在17秒内**）。

![img](http://img-hxy021.didistatic.com/static/km/do1_UWAGUHAEjqQkHvZ8XMdt)

接下来就可以定位到问题的本尊了，通过下Stage UI下面的task详情，点击任务运行时间（Duration）排序，终于见到了问题task的本尊（运行时间较长还被推测执行了）。

![img](http://img-hxy021.didistatic.com/static/km/do1_ipJvawQpXJXJXsd2cdAG)

2）分析

  定位到了具体的异常的task，接下来就是task怎么关联到我们的SQL业务逻辑了。这里主要分为两步，第一步，**获取**这个stage的**coordinate id（**必须在AQE开启的情况下）（在stage的同一个页面下）,在这里获取到两个ID，Exchange(coordinator id: 240643451), Exchange(coordinator id: 593822874)，这里需要记下。

![img](http://img-hxy021.didistatic.com/static/km/do1_iXXatAgGp4Kdd51DvrtD) 

  第二步后我们需要**跳转**到主界面的，SQL的分页的页面下去，进入左下角的链接。搜索上一步获取到的coordinator id: 240643451，就可以关联到具体的业务逻辑了。![img](http://img-hxy021.didistatic.com/static/km/do1_Qf4hQWelLiXLuNA3KYlp) 

 通过**coordinator id**的位置**定位**，和黑框里发生Sort MergeJoin的key（这里是on的条件），对比原来的SQL逻辑可以定位到，这是业务SQL逻辑里t5和t6做关联的那一段逻辑发生的shuffle read的task,回到SQL看看到底做了什么。

![img](http://img-hxy021.didistatic.com/static/km/do1_VFwupBSjvjg9yH1TDMQh)

![img](http://img-hxy021.didistatic.com/static/km/do1_jshlEjPwdLQ5cBxhEFGa)

 进一步分析，关键的SQL业务逻辑，定位到发生数据倾斜的地方。通过UI的信息，可以很快定位到这里。

 ![img](http://img-hxy021.didistatic.com/static/km/do1_MzxtfxfwSHMw4C3naKq2)     可以清楚地看到，对应的（t0 join t4 join t5 的结果集）left join的逻辑右表是t6 dwd_order_base_capital_di 所在的表。到这里我们从Spark UI获取到的信息就**映射到倾斜的逻辑上**了。

![img](http://img-hxy021.didistatic.com/static/km/do1_YCAzg15Bs3kDZVlX4apL)

  应对数据倾斜问题的常规步骤是**采样分析**，如果是用spark core可以使用采样的算子，对于SparkSQL的采样，可以根据shuffle的原理，观察 t5.driver_id,t5.city_id,t5.order_id作为shuffle key的**分布情况**。

![img](http://img-hxy021.didistatic.com/static/km/do1_C9dMNCZrLqGLIX5cyzaI)

 这里当时通过presto进行采样分析结果如下（原作业是用Spark跑的，想快速拿到分布情况改了一些逻辑，感觉不影响join的，因为有些语法不兼容），发现没有明显的**key组合**数量过多，都很平均，这里就很奇怪这和我的UI定位的情况不一致，鉴于当时时间比较紧就放弃了定位shuffle join解决的思路想着回过头看看。

![img](http://img-hxy021.didistatic.com/static/km/do1_LfdvQWhicIAbHrkWkHSd)

## **三 、思路**

**思路一：解决不了shuffle就绕过shuffle**

 考虑右表日增量不多，所以自然想到**shuffle join的转换到map join**。策略如下，第一，参数层面，需要调整mapjoin的阈值由原来的10M, 放大到1个G。第二，mapjoin会首先把数据传输到driver所以需要预先适当调大driver的内存大小，而后driver会分发数据到每一个excutor中，所以需要适当调大excutor的内存。第三，使用spark hint手动广播需要广播的表，保证广播一定会生效（**调高mapjoin阈值不一定自动广播，10M以下Spark自动广播问题不大**)。

![img](http://img-hxy021.didistatic.com/static/km/do1_jgGLmb8yVN34ikrMwtxR)

 再次查看作业的Spark SQL页面，可以看到，shuffle join（sort merge join） 变成了mapjoin (broadcast join), 这样业务SQL执行逻辑调整后直接绕过了定位倾斜所在的stage了。这就是map join的闪光点 不需要左右表做shuffle传输数据到下下游，直接在左表（t0 join t4 join t5 的结果集）所在的stage做关联计算，当然右表已经存在这个stage所在的excutor内存中了。

![img](http://img-hxy021.didistatic.com/static/km/do1_CSVwHZurcNUdJHIm369e)

 第一种思路，对比一下两次作业的执行时间，实现效果如下。

| 之前的一批次执行时间 （Spark UI） | 之后的一批次执行时间 (Spark UI） |
| --------------------------------- | -------------------------------- |
| **Total Uptime:** 37 min          | **Total Uptime:** 11 min         |

  

**思路二：硬钢 shuffle join - (如果下次遇到稍大的表不能绕过 shuffle join 怎么办)**

 回到上次定位倾斜的shuffle join的思路，为什么找不到倾斜的key呢，考虑到上次用presto验证（原因就是图快,当时感觉对join结果影响不大）对逻辑做了改动（原因是有些语法presto不兼容），所以想用Spark再跑一下，结果如图 ，果然它一直在这里等我 。第一个行的数量超大key终于出现了几个数量级的差距，还是都是null, 脑补的诸多方法都不需要了（文末推荐好的文章，里面说的很细了，我就不赘述了），确定了全为空的key组合对业务没有意义,直接过滤到这些没有意义的数据。

 问题的关键就落到了，在进行倾斜的shuffle join之前，**如何过滤掉这些数据**（key组合全部为null的数据）。进一步说如何在（t0 join t4 join t5）的结果集和t6 进行shuffle join之前，（t0 join t4 join t5）的结果集进行shuffle传输之前过滤掉这部分数据，使得这部分数据不会进入下游的task里面。

![img](http://img-hxy021.didistatic.com/static/km/do1_s6PSw29IREvkY1LeHfAW) 最后的问题是这里该如何处理,对业务SQL的**逻辑调整代价最小**呢，毕竟我还有这种逻辑在最后查询里面,让我们回到业务逻辑里。

```java
count(distinct case when t6.is_td_finish = 1 and t4.broad_time between config_start_time and config_end_time and ((scheme_type = 0 and array_contains(product_id_arr, cast(t6.product_id as string))) or (scheme_type = 4 and t6.product_id in (21060, 21066, 21067)) or (scheme_type = 5)) then t6.order_id end) as rides_broad_cnt
,count(distinct case when t6.is_td_reassign_grab_after_dri_cancel = 1 and t4.broad_time between config_start_time and config_end_time and ((scheme_type = 0 and array_contains(product_id_arr, cast(t6.product_id as string))) or (scheme_type = 4 and t6.product_id in (21060, 21066, 21067)) or (scheme_type = 5)) then t6.order_id end) as cancels
```

  回到完整的业务逻辑看，最外层的select里面这段复杂逻辑，如果大改会很吃力，基本用到了这一段逻辑涉及到所以的子查询的(t0 到 t5)。

![img](http://img-hxy021.didistatic.com/static/km/do1_Gmv4jzFyEgnXsZMeBBnK)  考虑可以预先过滤数据的地方，如何最小代价的过滤这些数据，我开始了思考。**第一个想法**，尝试在t5 的on条件加上空值过滤，想到对于left join 语义的完整性保证，没有预先过滤key为null的效果(如果可以左表就不完整了)。**第二个想法**，尝试在t6 改left join 为 inner join，基于inner join 语义的理解，预先过滤null的是符合语义的，但是有一点，我无法保证确认类似 t5.city t5.order_id 不为空 t5.driver_id 为空 的数据没有意义，不然inner join的替换肯定是可以的。**第三个想法**，把（to join t4 join t5）包起来做一层where条件过滤再和t6关联，但是对于整体的逻辑改动很大包括, 外层的count(distinct)。**最后一个执念**，不单独包起来，我可以直接过滤（t0 join t4 join t5）的结果集，懒人的懒想法可能只有各种SQL解析器和有**优化器**能实现了，试一试SparkSQL的优化器给不给力了（如果在这种场景下可以类似谓词下推的话），做出如下调整。

![img](http://img-hxy021.didistatic.com/static/km/do1_yJP5D9ZFm8QNPS3a2BfK)

 然后到新的任务UI 对比一下前后stage的执行时间，同样的执行流程，对应的stage 37，执行时间由27分钟到29秒。可以正式，新加的where条件是作用在（t0 join t4 join t5）的结果集和t6 进行shuffle join之前，所以切斜的key没有被传输到stage 37对应的task，**类似谓词下推**的操作是成功的，到达预期。顺便一提，从UI上可以看到也会下推到各个数据源进行过滤且原语义不变（这里没有配图，脑补哈，可以下去试试）。

![img](http://img-hxy021.didistatic.com/static/km/do1_XY7V9ga1d2B5hlCib3V3)

 第二种思路，处理完定位到的倾斜的key以后，之后实现效果如下。考虑到提交作业和集群的负载对于**资源伸缩**的影响，不一定说明这里比mapjoin块，毕竟是不同时间提交的。但是效果是符合预期的。

| 之前的一批次执行时间 （Spark UI） | 之后的一批次执行时间 (Spark UI） |
| --------------------------------- | -------------------------------- |
| **Total Uptime:** 37 min          | **Total Uptime:** 8.1 min        |

 