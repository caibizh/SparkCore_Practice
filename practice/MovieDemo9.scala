package spark_code_git

import org.apache.spark.{SparkConf, SparkContext}


// 需求
// 分析不同职业对观看电影类型的影响 (职业名,(电影类型,观影次数))
// 结果展示
// 分析不同职业对观看电影类型的影响 (职业名,(电影类型,观影次数))
// (K-12 student,(War,1385))
// (K-12 student,(Drama,6000))
// (K-12 student,(Comedy,9465))
// (K-12 student,(Documentary,98))
// (K-12 student,(Fantasy,1275))
// (K-12 student,(Crime,1355))
// (K-12 student,(Action,6067))
// (K-12 student,(Thriller,4212))
// (K-12 student,(Romance,2990))
// (K-12 student,(Film-Noir,235))

object MovieDemo9 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    val filepath = "moive-data"
    val occupations_rdd = sc.textFile(filepath + "/occupations.dat")
    val movies_rdd = sc.textFile(filepath + "/movies.dat")
    val user_rdd = sc.textFile(filepath + "/users.dat")
    val ratings_rdd = sc.textFile(filepath + "/ratings.dat")
    // "users.dat"：UserID::Gender::Age::OccupationID::Zip-code
    // "occupations.dat"：OccupationID::OccupationName   职业id和职业名称

    // TODO 1、首先关联user和occupation，得到 (user_id,职位)
    val user_basic = user_rdd
      .map(_.split("::"))
      .map(value => {
        // occ_id,user_id
        (value(3), value(0))
      })
    val occ_basic = occupations_rdd
      .map(_.split("::"))
      .map(value => {
        (value(0), value(1))
      })
    val user_occ = user_basic
      .join(occ_basic)
      .map(value => {
          // 得到user_id,职业类型
          (value._2._1,value._2._2)
        })
      .cache()
    //  "ratings.dat"：UserID::MovieID::Rating::Timestamp
    // "movies.dat"：MovieID::Title::Genres

    // TODO 2、关联rate和movie，得到 (user_id,电影id,电影类型)
    val rate_basic = ratings_rdd
      .map(_.split("::"))
      .map(value => {
        (value(1), value(0))
      })
    val movie_basic = movies_rdd
      .map(_.split("::"))
      .map(value => {
        (value(0), value(2))
      })
    val rate_movie = rate_basic
      .join(movie_basic)
      .map(value => {
        // 得到 user_id,(电影id,电影类型)
        (value._2._1, (value._1, value._2._2))
      }).cache()

    // TODO 3、关联1、2,得到 职位,电影类型
    val joinRDD = user_occ.join(rate_movie)
    // (String, (String, (String, String)))
    // user_id,(职位,  (movie_id,电影类型)  )
    val occ_movie_type = joinRDD.map(value => {
      // 得到 职位,电影类型
      (value._2._1, value._2._2._2)
    })

    // TODO 4、对于电影类型进行扁平化处理，FlatMapValues
    // 因为电影类型可能有多个，以 | 分隔，数据不适用,例如，上游算子输出数据为(student,(type_a,type_b))
    // 应该将其转化为 (student,type_a),(student,type_b)
    // 意义是，这个人看过一部电影（喜剧，动作片），最后计算时分别在喜剧，动作都要+1
    val flatMapRDD = occ_movie_type.flatMapValues(_.split("\\|"))

    // TODO 5、修改rdd结构为 (职业名,电影类型),1    然后进行聚合操作
    val resultRDD = flatMapRDD
      .map(value => {
        ((value._1, value._2), 1)
      })
      .reduceByKey(_ + _)
      .map(value => {
          (value._1._1,(value._1._2,value._2))
        })
//        .sortBy(_._2._2,false)
        .filter(_._1=="K-12 student")
    println("分析不同职业对观看电影类型的影响 (职业名,(电影类型,观影次数))")
    resultRDD.take(10).foreach(println)

    sc.stop()
  }

}
