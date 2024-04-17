package spark_code_git

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 需求
//
// 1、所有电影中平均得分最高的前 10 部电影:( 分数,电影 ID)
// 2、观看人数最多的前 10 部电影:(观影人数,电影ID)

// 结果示例
// 平均得分最高的前 10 名的电影名称简单版
//  (5.0,787)
//  (5.0,3382)
//  (5.0,3607)
//  (5.0,989)
//  (5.0,3656)
//  (5.0,3881)
//  (5.0,1830)
//  (5.0,3280)
//  (5.0,3233)
//  (5.0,3172)
//  按平均分取前 10 部电影输出详情:(平均分,(movieId,Title,Genres,总分,总次数))
//  (5.0,(787,Gate of Heavenly Peace, The (1995),Documentary,15.0,3))
//  (5.0,(3382,Song of Freedom (1936),Drama,5.0,1))
//  (5.0,(3607,One Little Indian (1973),Comedy|Drama|Western,5.0,1))
//  (5.0,(989,Schlafes Bruder (Brother of Sleep) (1995),Drama,5.0,1))
//  (5.0,(3656,Lured (1947),Crime,5.0,1))
//  (5.0,(3881,Bittersweet Motel (2000),Documentary,5.0,1))
//  (5.0,(1830,Follow the Bitch (1998),Comedy,5.0,1))
//  (5.0,(3280,Baby, The (1973),Horror,5.0,1))
//  (5.0,(3233,Smashing Time (1967),Comedy,10.0,2))
//  (5.0,(3172,Ulysses (Ulisse) (1954),Adventure,5.0,1))
//  观影人数最多的前 10 部电影
//  (3428,2858)
//  (2991,260)
//  (2990,1196)
//  (2883,1210)
//  (2672,480)
//  (2653,2028)
//  (2649,589)
//  (2590,2571)
//  (2583,1270)
//  (2578,593)

object MovieDemo3 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    // "ratings.dat"：UserID::MovieID::Rating::Timestamp
    // "movies.dat"：MovieID::Title::Genres
    val filepath = "moive-data"
    val ratings_rdd = sc.textFile(filepath + "/ratings.dat").cache()
    val movies_rdd = sc.textFile(filepath + "/movies.dat")
    val rate_basic = ratings_rdd
      .map(_.split("::"))
      .map(rate => {
        // id,(分数,1)
        (rate(1), (rate(2).toDouble,1))
      })
        .cache()
    val movie_basic = movies_rdd
      .map(_.split("::"))
      .map(movie => {
        (movie(0), (movie(1), movie(2)))
      })
      .cache()
    val reduce1 = rate_basic.reduceByKey(
      (x, y) => {
        // 某个movie的评分和，打分次数
        (x._1 + y._1, x._2 + y._2)
      }
    )
    reduce1.cache()

    // TODO 平均得分最高的前 10 名的电影名称简单版
    println("平均得分最高的前 10 名的电影名称简单版")
    reduce1.map(value => {
      (value._2._1/value._2._2,value._1)
    })
      .sortByKey(false)
        .take(10)
        .foreach(println)

    // TODO 按平均分取前 10 部电影输出详情:(平均分,(movieId,Title,Genres,总分,总次数))
    println("按平均分取前 10 部电影输出详情:(平均分,(movieId,Title,Genres,总分,总次数))")
    val joinRDD: RDD[(String, ((Double, Int), (String, String)))] = reduce1.join(movie_basic)
    joinRDD
      .map(value => {
      (value._2._1._1/value._2._1._2,(value._1,value._2._2._1,value._2._2._2,value._2._1._1,value._2._1._2))
    })
        .sortByKey(false)
        .take(10)
        .foreach(println)

    // TODO 观影人数最多的前 10 部电影
    println("观影人数最多的前 10 部电影")
    rate_basic
        .map(value => {
          // id,1
          (value._1,1)
        })
        .reduceByKey( (x,y) => {
          x+y
        })
      .map(value => {
        // 人次，id
        (value._2,value._1)
      })
        .sortByKey(false)
        .take(10)
        .foreach(println)


    sc.stop()
  }

}
