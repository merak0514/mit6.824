# mit 6.824
实验代码

## lab1
[lab 1官方网站](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)

### 我是怎么做的？（踩坑）
1. 最开始先按照网站上的提示，从写一个mapreduce的处理开始；先完成处理一个文件。
2. 接着发现这样做是没有前途的，Coordinator（上位机）和worker（下位机）之间没有建立强关联；即上位机其实不知道现在都有哪些下位机available，也不知道分配过去的任务执行的怎么样了。
3. 开始自顶向下设计，先做了一个Register函数，让下位机去主动注册，并接受分配的id；同时上位机储存一张数据表，记录都有哪些下位机来注册的，以及下位机的状态。
4. 完成Sanity check（打乒乓）：下位机过一段时间就ping一下上位机，告知自己存活；超过一段时间没收到下位机的信号，上位机就将下位机标注为died。
5. 完成ArrangeTask函数，试图自顶向下，先设计任务的分配模板，而缺省具体的reduce分配方式。
6. 设计判断Map阶段是否结束的方法；满足 `所有Map文件都被分配出去`以及 `所有分配出去的Map文件都被告知执行完成`即为Map阶段完成；前者使用计数器实现，后者使用WatiGroup实现
    * 注意区分worker id和task id，一个worker可能依次执行多个map task。


### 问题
* 为什么官方的设计构造中worker没有采用面向对象的方法？而是拿了一个函数？
