import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}

object SparkMovie {
  //基于用户的推荐，例如根据一些用户看完电影，评分高推荐给用户
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Alsmovie").setMaster("local")
    val sc=new SparkContext(conf)
    val data=sc.textFile("hdfs://hadoop01:9000/movie/u.data")
    //第一步:数据格式转换
    val ratings=data.map { line =>
      val info=line.split("\t")
      val userId=info(0).toInt
      val movieId=info(1).toInt
      val score=info(2).toDouble
      Rating(userId,movieId,score)
    }
    //获取电影id，名称map
   /* val data1=sc.textFile("hdfs://hadoop01:9000/movie/u.item")
    val movieMap=data1.map { line =>
      val info=line.split("\\|")
      val movieId=info(0).toInt
      val movieName=info(1)
      (movieId,movieName)
    }.collectAsMap*/
    //第二步:建立推荐系统模型
    //隐藏因子k的数量不宜过大,避免产生过大的计算代价
    //ratings:训练集 rank：根据数据的分散情况测试出来的值，特征向量纬度，如果这个值太小拟合的就会不够，误差就很大；如果这个值很大，就会导致模型大泛化能力较差；所以就需要自己把握一个度了，一般情况下10～1000都是可以的；
    //iterations，这个设置的越大肯定是越精确，但是设置的越大也就意味着越耗时；
    //lambda也是和rank一样的，如果设置很大就可以防止过拟合问题，如果设置很小，
    // 其实可以理解为直接设置为0，那么就不会有防止过拟合的功能了；怎么设置呢？
    // 可以从0.0001 ，0.0003，0.001，0.003，0.01，0.03，0.1，0.3，1，3，10
    // 这样每次大概3倍的设置，先大概看下哪个值效果比较好，然后在那个比较好的值（比如说0.01）
    // 前后再设置一个范围，比如（0.003，0.3）之间，间隔设置小点，即0.003，0.005，0.007，0.009，0.011，，，，。
    // 当然，如果机器性能够好，而且你够时间，可以直接设置从0到100，间隔很小，然后一组参数一组的试试也是可以的。
    val model=ALS.train(ratings,50,10,0.01)



    //第三步:为7号用户推荐10部电影
    val rs=model.recommendProducts(7,10)
   //根据id获取电影名
    val movies=sc.textFile("hdfs://hadoop01:9000/movie/u.item")

    val titles=movies.map(line=>line.split("\\|").take(2))

      .map(array=>(array(0).toInt,array(1))).collectAsMap()

    println(titles(7))//查看编号7的电影名称

    //7号用户看过的电影
    val movieForUser7=ratings.keyBy(_.user).lookup(7)

    println(movieForUser7.size)//查看789用户评价了多少部电影
    //取出前十部并打印
    movieForUser7.sortBy(-_.rating).take(10)

      .map(rating=>(titles(rating.product),rating.rating))

      .foreach(println)

    rs.map(rating=>(titles(rating.product),rating.rating)).foreach(println)


    //预测5对功夫的评分
    val llgfPredict=model.predict(7,1)
    println(llgfPredict)

  }
}
