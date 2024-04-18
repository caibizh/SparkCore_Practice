package spark_code_git

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 分析最受不同年龄段人员欢迎的前 10 部电影
// 包括以下指标
// 1、某年龄段用户数量，各年龄段的人数,(年龄段，人数)
// 2、年龄段为:1喜爱的电影Top 10:(movieId, 电影名,平均分,总分数,观影次数)
// 原始数据集中的年龄段划分
//  * under 18: 1
//  * 18 - 24: 18
//  * 25 - 34: 25
//  * 35 - 44: 35
//  * 45 - 49: 45
//  * 50 - 55: 50
//  * 56 - + : 56
// 结果展示
// 年龄小于18的人的数量为：222
//  各年龄段的人数,(年龄段，人数)
//  (1,222)
//  (18,1103)
//  (25,2096)
//  (35,1193)
//  (45,550)
//  (50,496)
//  (56,380)
// 年龄段为0-18人群喜爱的电影Top 10:(movieId, 电影名,平均分,总分数,观影次数)
// (2762,Sixth Sense, The (1999),4.275229357798165,466.0,109)
// (2571,Matrix, The (1999),4.4646464646464645,442.0,99)
// (1,Toy Story (1995),3.919642857142857,439.0,112)
// (260,Star Wars: Episode IV - A New Hope (1977),4.267326732673268,431.0,101)
// (3114,Toy Story 2 (1999),4.202020202020202,416.0,99)
// (1210,Star Wars: Episode VI - Return of the Jedi (1983),4.13,413.0,100)
// (2858,American Beauty (1999),4.434782608695652,408.0,92)
// (1196,Star Wars: Episode V - The Empire Strikes Back (1980),4.184782608695652,385.0,92)
// (1580,Men in Black (1997),3.84,384.0,100)
// (356,Forrest Gump (1994),4.168539325842697,371.0,89)

object MovieDemo5 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    val filepath = "moive-data"
    //  1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
    //  2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
    //  3，"movies.dat"：MovieID::Title::Genres
    val movies_rdd = sc.textFile(filepath + "/movies.dat")
    val user_rdd = sc.textFile(filepath + "/users.dat")
    val ratings_rdd = sc.textFile(filepath + "/ratings.dat")
    // 需要得到 userid，age对应的段数
    def mapAgg(age : Int):String = {
              if (age < 18) {
                "1"

              }
              else if (age >= 18 && age <= 24) {
                "18"
              }
              else if (age >= 25 && age <= 34) {
                "25"
              }
              else if (age >= 35 && age <= 44) {
                "35"
              }
              else if (age >= 45 && age <= 49) {
                "45"
              }
              else if (age >= 50 && age <= 55) {
                val s="50"
                s
              }
              else if (age >= 56) {
                "56"
              }
              else "error"  // 必须写最后这个else
    }
    // userid,年龄段
    val user_basic = user_rdd
      .map(_.split("::"))
      .map(user => {
        (user(0), user(2).toInt)
      })
      .map(value => {
        (value._1,mapAgg(value._2))
      })
        .cache()
    // TODO 求年龄小于18的人的数量
    val cnt = user_basic
      .filter(value => {
        value._2 == "1"
      })
      .count()
    println("年龄小于18的人的数量为：" + cnt)

    // TODO 求各年龄段的人数,(年龄段，人数)
    println("各年龄段的人数,(年龄段，人数)")
    val count = user_basic.groupBy(_._2)

        .map(value => {
          (value._1,value._2.size)
        })
        // 按年龄升序排列
        .sortBy(_._1)
        .collect()
        .foreach(println)

    // TODO 年龄段为:1喜爱的电影Top 10:(movieId, 电影名,平均分,总分数,观影次数)
    val movie = movies_rdd
      .map(_.split("::"))
      .map(value => {
        (value(0), value(1))
      })
    val rate = ratings_rdd
      .map(_.split("::"))
      .map(value => {
        // userid,(movieid,rate)
        (value(0),( value(1), value(2).toDouble))
      })
      // 先关联rate和user_basic，然后修改key为movieid关联movie
      println("年龄段为0-18人群喜爱的电影Top 10:(movieId, 电影名,平均分,总分数,观影次数)")
      val result =
        rate
        .join(user_basic)
        // userid,(movie_id,rate),age
        .map(value => {
          // 修改格式为key:movieid,value: userid,age,rate
          (value._2._1._1, (value._1, value._2._2, value._2._1._2))
        })
        .join(movie)
        .map(value => {
          // movie_id,movie_name,rate,1,age
          (value._1,value._2._2,value._2._1._3,1,value._2._1._2)
        })
        // 计算年龄小于18的人群最喜欢电影top10
        .filter(_._5=="1")
        .map(value => {
          ((value._1,value._2),(value._3,value._4))
        })
          .reduceByKey(
            (x,y) => {
              (x._1+y._1,x._2+y._2)
            }
          )
          // 聚合后格式为（id,name）,(sum_score,count)
          .map(value => {
            // 需要的格式为 (movieId, 电影名,平均分,总分数,观影次数)
            (value._1._1,value._1._2,value._2._1/value._2._2,value._2._1,value._2._2)
          })
          // 按照总分进行降序排序
          .sortBy(_._4,false)
          .take(10)
          .foreach(println)
//    result

    sc.stop()

  }

}
