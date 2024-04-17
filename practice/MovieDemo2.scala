package spark_code_git

import org.apache.spark.{SparkConf, SparkContext}

/*   需求
* 1、某个用户看过的电影数量  userid：20
  2、这些电影的信息，结果格式为: MovieID,UserID,Title,Genres
  */

//    结果示例
//  20 观看过的电影数: 24
//  (1375,20,Star Trek III: The Search for Spock (1984),Action|Adventure|Sci-Fi)
//  (3578,20,Gladiator (2000),Action|Drama)
//  (1912,20,Out of Sight (1998),Action|Crime|Romance)
//  (1468,20,Booty Call (1997),Comedy|Romance)
//  (2858,20,American Beauty (1999),Comedy|Drama)
//  (1371,20,Star Trek: The Motion Picture (1979),Action|Adventure|Sci-Fi)
//  (3863,20,Cell, The (2000),Sci-Fi|Thriller)
//  (3753,20,Patriot, The (2000),Action|Drama|War)
//  (3527,20,Predator (1987),Action|Sci-Fi|Thriller)
//  (47,20,Seven (Se7en) (1995),Crime|Thriller)
//  (589,20,Terminator 2: Judgment Day (1991),Action|Sci-Fi|Thriller)
//  (2028,20,Saving Private Ryan (1998),Action|Drama|War)
//  (1240,20,Terminator, The (1984),Action|Sci-Fi|Thriller)
//  (2571,20,Matrix, The (1999),Action|Sci-Fi|Thriller)
//  (70,20,From Dusk Till Dawn (1996),Action|Comedy|Crime|Horror|Thriller)
//  (1617,20,L.A. Confidential (1997),Crime|Film-Noir|Mystery|Thriller)
//  (2641,20,Superman II (1980),Action|Adventure|Sci-Fi)
//  (1694,20,Apostle, The (1997),Drama)
//  (3717,20,Gone in 60 Seconds (2000),Action|Crime)
//  (648,20,Mission: Impossible (1996),Action|Adventure|Mystery)
//  (457,20,Fugitive, The (1993),Action|Thriller)
//  (110,20,Braveheart (1995),Action|Drama|War)
//  (1527,20,Fifth Element, The (1997),Action|Sci-Fi)
//  (1923,20,There's Something About Mary (1998),Comedy)


object MovieDemo2 {
  def main(args: Array[String]): Unit = {
    // 需要用到 ratings.dat 和 movies
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val filepath = "moive-data"
    val rating_rdd = sc.textFile(filepath + "/ratings.dat")
    val movies_rdd = sc.textFile(filepath + "/movies.dat")

    // "ratings.dat"：UserID::MovieID::Rating::Timestamp
    // "movies.dat"：MovieID::Title::Genres
    // 结果格式应为 MovieID,UserID,Title,Genres
    val user_basic = rating_rdd
      .map(_.split("::"))
      .map(rate => {
        (rate(1), rate(0))
      })
    val movie = movies_rdd
      .map(_.split("::"))
      .map(movie => {
        (movie(0), (movie(1), movie(2)))
      })
    val result = user_basic.join(movie)
    //  RDD[(String, (String, (String, String)))]
    val new_result = result
      .map(value => {
        (value._1, value._2._1, value._2._2._1, value._2._2._2)
      })
      // 过滤操作可以省略，该操作仅用于匹配示例
      .filter(value => {
          value._2=="20"
        })

    new_result.cache()
    val cnt = new_result.count()
    println("20 观看过的电影数: " + cnt)

    new_result.collect().foreach(println)



    sc.stop()

  }

}
