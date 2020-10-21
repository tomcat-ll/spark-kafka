import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, Rating}

object model {
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
    val model=ALS.train(ratings,100,20,0.01)
     //模型的存储,为了避免每次推荐时都重新训练模型,使用时仅需加载模型使用即可
    model.save(sc, "hdfs://hadoop01:9000/rec-result-1909")
  }
}
