```	
Q：kettle “插入更新”数据时会在目标库自建临时表，但其根据原始库的字段类型和长度 或自定义类型或长度，这个在目标数据库中会报错（一般是字符长度的问题）
  	
A：在目标库中自建临时表，使用“表输出” 到该表，再从该表抽取-“插入更新”到目标表

Q: kettle “插入更新”到目标表时 “1,0” 会变成 “是、否” 
  
A: 解决方法： 在抽取“表输入”sql 中该字段“cast 字段 as signed”  即可
  
Q:问题：多分组的求和/差分
  
A： (A.CENTER_ACTIVITY -LEAD(A.CENTER_ACTIVITY,1,0) over(partition  by 
	AGENT_ID order by A.TIMESS desc)),使用“公式 over （partition by 分组项 order by Timess  时间降序排列）”
```
