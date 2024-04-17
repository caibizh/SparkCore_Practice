package spark_code_git

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
// 需求
// 1.分析男性用户最喜欢看的前 10 部电影  (id,总评分)
// 2.女性用户最喜欢看的前 10 部电影 输出格式: ( movieId, (电影名,总分，打分次数，平均分))

// 结果展示
// 分析男性用户最喜欢看的前 10 部电影  (id,总评分)
//  (10790.0,2858)
//  (10537.0,260)
//  (10175.0,1196)
//  (9141.0,2028)
//  (9074.0,1210)
//  (9056.0,2571)
//  (9025.0,589)
//  (8779.0,1198)
//  (8203.0,593)
//  (8153.0,110)
//  女性用户最喜欢看的前 10 部电影 输出格式: ( movieId, (电影名,总分，打分次数，平均分))
//  (2858,(American Beauty (1999),4010.0,946,4.238900634249472))
//  (2396,(Shakespeare in Love (1998),3337.0,798,4.181704260651629))
//  (593,(Silence of the Lambs, The (1991),3016.0,706,4.2719546742209635))
//  (2762,(Sixth Sense, The (1999),2973.0,664,4.477409638554217))
//  (318,(Shawshank Redemption, The (1994),2846.0,627,4.539074960127592))
//  (527,(Schindler's List (1993),2806.0,615,4.56260162601626))
//  (260,(Star Wars: Episode IV - A New Hope (1977),2784.0,647,4.302936630602782))
//  (608,(Fargo (1996),2771.0,657,4.21765601217656))
//  (1197,(Princess Bride, The (1987),2762.0,636,4.3427672955974845))
//  (1196,(Star Wars: Episode V - The Empire Strikes Back (1980),2661.0,648,4.106481481481482))
object MovieDemo4 {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    val filepath = "moive-data"
    val user_rdd = sc.textFile(filepath + "/users.dat")
    val ratings_rdd = sc.textFile(filepath + "/ratings.dat")
    // "users.dat"：UserID::Gender::Age::OccupationID::Zip-code
    // "ratings.dat"：UserID::MovieID::Rating::Timestamp
    val user_basic = user_rdd
      .map(_.split("::"))
      .map(user => {
        // userid,(gender,age)
        (user(0), (user(1), user(2)))
      })
      .cache()
    val rate_basic = ratings_rdd
      .map(_.split("::"))
      .map(rate => {
        // userid,(movieid,rate)
        (rate(0), (rate(1), rate(2).toDouble))
      })
      .cache()
    // TODO 1.分析男性用户最喜欢看的前 10 部电影  (id,总评分)
    val user_rate: RDD[(String, ((String, String), (String, Double)))] = user_basic.join(rate_basic).cache()
    println("分析男性用户最喜欢看的前 10 部电影  (id,总评分)")
    user_rate
        .map(value => {
          // 拿到gender，movieid，rate
          (value._2._1._1,value._2._2._1,value._2._2._2)
        })
        .filter(value => {
          // 仅算男性前十
          value._1 == "M"
        })
        .map(value => {
          (value._2,value._3)
        })
        .reduceByKey(_+_)
        .map(value => {
        (value._2,value._1)
      })
        .sortByKey(false)
        .take(10)
        .foreach(println)

    // TODO 2.女性用户最喜欢看的前 10 部电影 输出格式: ( movieId, 电影名,总分，打分次数，平均分)
    // "movies.dat"：MovieID::Title::Genres
    val movies_rdd = sc.textFile(filepath + "/movies.dat")
    val movie = movies_rdd
      .map(_.split("::"))
      .map(movie => {
        // movieid,name
        (movie(0), movie(1))
      })
    // userid,movieid,rate,gener
    val user_rate_m = user_rate
      .map(value => {
        // movieid,(gender,rate)
        (value._2._2._1, (value._2._1._1, value._2._2._2))
      })
      .join(movie)
      .filter(value => {
        value._2._1._1 == "F"
      })
      // id,(name,rate,1)
      .map(value => {
        ((value._1,value._2._2), ( value._2._1._2, 1))
      })
    println("女性用户最喜欢看的前 10 部电影 输出格式: ( movieId, (电影名,总分，打分次数，平均分))")
    user_rate_m
        .reduceByKey(
          (x,y) => {
            (x._1+y._1, x._2+y._2)
          }
        )
        .map(value => {
          // movieId, 电影名,总分，打分次数，平均分
          (value._1._1,(value._1._2,value._2._1,value._2._2,value._2._1/value._2._2))
        })
        .sortBy(_._2._2,false)
        .take(10)
        .foreach(println)


    sc.stop()
  }

}
