#_Transformation_
##Batch

**map()** MapFunction  
**flatMap()** FlatMapFunction  
**partitionMap()** PartitionMapFunction,note that if there are 8 processors, it could produce 8 partitions by default  

**filter()** FilterFunction //preform the function on every note return true/false representing whether discarding  
**project()** //**_extract_** several fields from dataset  
**distinct()** //when multiple fields, only when all fields diverse, it is a distinct element  

**group()** //by position or field name or function telling which field is the key, it put the elements with same key in a single group  
**reduce()**  //reduce the intermediate result from group transformation, compacting each group. The reduce function subsequently combines pairs of elements into one element until only a single element remains.
**reduceGroup()**  //similar to partitionMap  
**combineGroup()** //similar to Hadoop's **_Combiner_**, however, using a greedy algorithm to compute local result, so there should be a group after that to perform groupReduce transformation  

**aggregate(method,field)** Sum,Max,Min,and //first grouping the tuples, then aggregating element according to specified fields  
**minBy()**//similar to aggregate  
**maxBy()** //similar to aggregate  

`note:`reduce could perform on the whole dataset or the intermediate key/value-list, however, aggregate(method,field) is used more on key/value-list

**join()**//dataset1.join(dataset2).where(dataset1.field).equalTo(dataset2.field).projectFirst().projectSecond().with(JoinFunction/FlatJoinFunction), Join and FlatJoin are similar to Map and FlatMap  
//In order to guide the optimizer to pick the right execution strategy,we can hint the size of the dataset to join with .joinWithTiny()/.joinWithHuge()  
//We can also user hint, such like .join(dataset,Hint.strategy), to tell flink to use which strategy to join  


