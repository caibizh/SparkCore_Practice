package spark_code_git

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
*   需求
 1、读取信息,统计数据条数. 职业数, 电影数, 用户数, 评分条数
 2、显示每个职业下的用户详细信息，显示为 (职业编号,(人的编号,性别,年龄,邮编),职业名)
 */

/* 结果展示
1、    职业数:21
      电影数:3883
      用户数:6040
      评分条数:1000209
      
2、
    (4,((25,M,18,01609),college/grad student))
    (4,((38,F,18,02215),college/grad student))
    (4,((39,M,18,61820),college/grad student))
    (4,((41,F,18,15116),college/grad student))
    (4,((47,M,18,94305),college/grad student))
    (4,((48,M,25,92107),college/grad student))
    (4,((52,M,18,72212),college/grad student))
    (4,((63,M,18,54902),college/grad student))
    (4,((68,M,18,53706),college/grad student))
    (4,((70,M,18,53703),college/grad student))
* */

object MovieDemo01 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    // TODO 1、读取数据，并分别计算 职业数, 电影数, 用户数, 评分条数
    val filepath = "moive-data"
    val occupations_rdd = sc.textFile(filepath + "/occupations.dat")
    val movies_rdd = sc.textFile(filepath + "/movies.dat")
    val user_rdd = sc.textFile(filepath + "/users.dat")
    val ratings_rdd = sc.textFile(filepath + "/ratings.dat")
    occupations_rdd.cache()
    movies_rdd.cache()
    user_rdd.cache()
    ratings_rdd.cache()
    println("职业数:" + occupations_rdd.count())
    println("电影数:" + movies_rdd.count())
    println("用户数:" + user_rdd.count())
    println("评分条数:" + ratings_rdd.count())

    // TODO 2、显示每个职业下的用户详细信息，显示为 (职业编号,(人的编号,性别,年龄,邮编),职业名)
    // 需要使用到 user_rdd，occupations_rdd
    val user_basic = user_rdd
      .map(_.split("::"))
      .map(user => {
        (user(3), (user(0), user(1), user(2), user(4)))
      })
    // user_basic.collect().foreach(println);

    val occupation = occupations_rdd
      .map(_.split("::"))
      .map(op => {
        (op(0), op(1))
      })
    val result: RDD[(String, ((String, String, String, String), String))] = user_basic.join(occupation)
    result.take(10).foreach(println);

    sc.stop()
  }

}
