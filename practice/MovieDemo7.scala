package spark_code_git

import java.util.regex.Pattern

import org.apache.spark.{SparkConf, SparkContext}

// 需求
// 分析每年度生产的电影总数 输出格式 (年度,数量)
// 结果示例
// (1919,3)
// (1920,2)
// (1921,1)
// (1922,2)
// (1923,3)
// (1925,6)
// (1926,8)
// (1927,6)
// (1928,3)
// (1929,3)
// (1930,7)

object MovieDemo7 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    val filepath = "moive-data"
    val movies_rdd = sc.textFile(filepath + "/movies.dat")
    val pattern = Pattern.compile("(\\(\\d{4}\\))$")
    // (\d{4})  使用这个正则表达式会出问题，可以试一下，结果中会出现(1600,1）,实际这个1600不是年份
    // 问题数据如下  1422::Murder at 1600 (1997)::Mystery|Thriller
    // 因此需要再加上括号匹配
    // "movies.dat"：MovieID::Title::Genres
    val resultRDD = movies_rdd
      .map(_.split("::"))
      .map(movie => {
        movie(1)
      })
      // 这里要得到年度需要使用正则表达式
      .map(
        name => {
          var year = ""
          val matcher = pattern.matcher(name)
          if (matcher.find()) {
            year = matcher.group(1)
          }
          // (1996) => 1996
          year = year.substring(1,5)
          (year, 1)
        }
      )
      .reduceByKey(_ + _)
      // 按年份升序展示
      .sortBy(_._1)

    resultRDD.collect().foreach(println)


    sc.stop()
  }

}
