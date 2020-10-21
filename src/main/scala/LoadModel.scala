import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.{SparkConf, SparkContext}

object LoadModel {
  //基于物品的推荐，例如用户看完一部电影，推荐类似电影给他

  def cosArray(a1:Array[Double],a2:Array[Double])={
    val a1a2=a1 zip a2
    val fenzi=a1a2.map{x=>x._1*x._2}.sum
    val a1Fenmu=Math.sqrt(a1.map{x=>x*x}.sum)
    val a2Fenmu=Math.sqrt(a2.map{x=>x*x}.sum)

    val a1a2Cos=fenzi/(a1Fenmu*a2Fenmu)

    //返回计算得到的余弦距离
    a1a2Cos
  }
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("load")

    val sc=new SparkContext(conf)

    //加载模型
    val model=MatrixFactorizationModel.load(sc,"hdfs://hadoop01:9000/rec-result-1909")
    //第一步:获取物品因子矩阵
    //RDD[(movieId,电影的因子数组)]
    val movieFactors=model.productFeatures
    //第二步:获取123号电影的因子值数组
    //下面表示以电影id为key来查找
    val movie123Factor=movieFactors.keyBy{x=>x._1}.lookup(123).head._2
    //第三步:计算其他所有电影和123号电影的相似度(使用余弦距离来衡量)
    val movie123Coss=movieFactors.map{case(movieId,factor)=>
      //用当前电影的因子数组和123号电影的因子数组计算相似度
      val cos=cosArray(movie123Factor, factor)
      (movieId,cos)
    }
    //第四步:根据相似度降序排序,返回最相关的前10部电影,从而完成基于物品的推荐
    val top10=movie123Coss.sortBy{x=> -x._2}.take(10)

    //根据id获取名字
    val data=sc.textFile("hdfs://hadoop01:9000/movie/u.item")
    val movieMap=data.map { line =>
      val info=line.split("\\|")
      val movieId=info(0).toInt
      val movieName=info(1)
      (movieId,movieName)
    }.collectAsMap
    println(movieMap(123))
   top10.map(s =>(movieMap(s._1),s._2)).foreach{println}
  }
}
