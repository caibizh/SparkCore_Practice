package spark_code_git

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
// 需求
// 分析不同类型的电影总数 输出格式:(类型,数量),按照数量降序排列

// 结果示例
// (Drama,1603)
// (Comedy,1200)
// (Action,503)
// (Thriller,492)
// (Romance,471)
// (Horror,343)
// (Adventure,283)
// (Sci-Fi,276)
// (Children's,251)
// (Crime,211)
// (War,143)
// (Documentary,127)
// (Musical,114)
// (Mystery,106)
// (Animation,105)
// (Fantasy,68)
// (Western,68)
// (Film-Noir,44)

object MovieDemo6 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    val filepath = "moive-data"
    // "movies.dat"：MovieID::Title::Genres
    val movie_rdd = sc.textFile(filepath + "/movies.dat")
    // 难点剖析
    // Genres中不一定只有一个类别，以 | 分隔
    val movie_basic = movie_rdd
      .map(_.split("::"))
      .map(value => {
        value(2)
      })
      .flatMap(_.split("\\|"))
//        .take(10).foreach(println)
      .map(value => (value,1))
      .reduceByKey(_+_)
      .sortBy(_._2,false)

    movie_basic.collect().foreach(println)



    sc.stop()
  }

}
