package imdb

case class TitleBasics(tconst: String, titleType: Option[String], primaryTitle: Option[String],
                      originalTitle: Option[String], isAdult: Int, startYear: Option[Int], endYear: Option[Int],
                      runtimeMinutes: Option[Int], genres: Option[List[String]])
case class TitleRatings(tconst: String, averageRating: Float, numVotes: Int)
case class TitleCrew(tconst: String, directors: Option[List[String]], writers: Option[List[String]])
case class NameBasics(nconst: String, primaryName: Option[String], birthYear: Option[Int], deathYear: Option[Int],
                      primaryProfession: Option[List[String]], knownForTitles: Option[List[String]])

object ImdbAnalysis {


  // Hint: use a combination of `ImdbData.titleBasicsPath` and `ImdbData.parseTitleBasics`
  val titleBasicsList: List[TitleBasics] = scala.io.Source.fromFile(ImdbData.titleBasicsPath).getLines().toList.map(ImdbData.parseTitleBasics)
 
  // Hint: use a combination of `ImdbData.titleRatingsPath` and `ImdbData.parseTitleRatings`
  val titleRatingsList: List[TitleRatings] = scala.io.Source.fromFile(ImdbData.titleRatingsPath).getLines().toList.map(ImdbData.parseTitleRatings)
  
  // // Hint: use a combination of `ImdbData.titleCrewPath` and `ImdbData.parseTitleCrew`
  val titleCrewList: List[TitleCrew] = scala.io.Source.fromFile(ImdbData.titleCrewPath).getLines().toList.map(ImdbData.parseTitleCrew)
  
  // // Hint: use a combination of `ImdbData.nameBasicsPath` and `ImdbData.parseNameBasics`
  val nameBasicsList: List[NameBasics] = scala.io.Source.fromFile(ImdbData.nameBasicsPath).getLines().toList.map(ImdbData.parseNameBasics)
  
  def task1(list: List[TitleBasics]): List[(String, Int)] = { //: List[(String, Int)]
    list.flatMap(_.genres).flatten.groupBy(identity).mapValues(_.size).toList.sortBy(-_._2)
    //.map(_.distinct)
  }


  // // Hint: There could be an input list that you do not really need in your implementation.
  def task2(l1: List[TitleBasics], l2: List[TitleCrew], l3: List[NameBasics]): List[(String, Int)] = { //: List[(String, Int)]
    val filteredTitles = l1.filter(_.titleType.exists(_.equals("movie"))).flatMap(tb => tb.startYear.filter(year => year >= 2010 && year <= 2022).map(_ => tb.tconst)).toSet

    l3.flatMap(nb => nb.primaryName.flatMap(pn => nb.knownForTitles.map(kft => kft.filter(filteredTitles.contains)).filter(_.size >= 3).map((pn,_)))).map{case (name, list) => (name, list.size)}
  }

  // : List[(String, Float)]
  def task3(l1: List[NameBasics],l2: List[TitleBasics] , l3: List[TitleCrew], l4: List[TitleRatings]): List[(String, Float)] = {
    val filteredTitles = l2.filter(_.titleType.exists(_.equals("movie"))).map(_.tconst).toSet

    val filteredRatings = l4.filter(tr => tr.numVotes >= 10000 && filteredTitles.contains(tr.tconst))
    val filteredRatingTitles = filteredRatings.map(_.tconst).toSet

    val filteredRatingTitlesMap = filteredRatings.map(tc => (tc.tconst, (tc.averageRating, tc.numVotes))).toMap

    val directorsListsMovies = l3.filter(tc => filteredRatingTitles.contains(tc.tconst)).flatMap(tc => tc.directors.map(directorList => (directorList, tc.tconst)))
    
    val directorMovies = directorsListsMovies.flatMap{
      case (directorList, tconst) => directorList.map(director => (director, tconst))
      }.groupBy(_._1).map{case (director, tconstList) => (director, tconstList.map(_._2))}.filter(_._2.size >= 2)

    val directorList = directorMovies.map(_._1).toSet

    val directorRatings = directorMovies.map{case (director, tconstList)=>
      val tmp = tconstList.flatMap(tcl => filteredRatingTitlesMap.get(tcl))
      // println(tmp)
      val rating = tmp.map{
        case (ar, nv) => ar * nv
        }.sum / tmp.map(_._2).sum
      (director, rating)
    }

    val ncNameMap = l1.flatMap(nb => nb.primaryName.flatMap(pn => if (directorList.contains(nb.nconst)) Some((nb.nconst, pn)) else None)).toMap
    
    val tmpList = directorRatings.flatMap{case (nc, ratings) => ncNameMap.get(nc).map(name => (name, ratings))}

    tmpList.toList.sortBy(-_._2)
    // .toList.sortBy(-_._2)
  }

  //: List[(Int, List[(String, Float)])]
  def task4(l1:List[TitleBasics], l2: List[TitleRatings]): List[(Int, List[(String, Float)])] = {
    val tmpTb = l1.filter(tb => tb.titleType.exists(_.equals("movie")) && tb.startYear.exists(sy => sy >= 1900 && sy <= 1999))
    val tconsts = tmpTb.map(_.tconst).toSet
    val movieGernesYears = tmpTb.flatMap(tb => tb.genres.flatMap(gn => tb.startYear.map(sy => (tb.tconst, gn, (sy - 1900) / 10))))
    val decadeGenresTcs = movieGernesYears.flatMap{
      case (tc, gns, dc) => 
       gns.map(gn => (dc, gn, tc))
    }.groupBy(_._1)
    
    val tmpList = decadeGenresTcs.map{
      case (dc, lists) => 
        val groupedGenre = lists.groupBy(_._2).map{case (genre, lists2) => (genre, lists2.map(_._3))}.toList
        (dc, groupedGenre)
    }.toList
    // println(tmpList)

    val ratingsMap = l2.filter(tr => tconsts.contains(tr.tconst)).map(tr => (tr.tconst, (tr.averageRating, tr.numVotes))).toMap

    val tmpList2 = tmpList.flatMap{
        case (dc, lists) => lists.map{
          case (genre, tclist) => 
          val tmp = tclist.map(tc => ratingsMap.get(tc)).flatten
          // println(tmp)
          val rating = tmp.map{
            case (ar, nv) => ar * nv
          }.sum / tmp.map(_._2).sum
          (genre, rating)
          // (genre, tclist)
        }.map((dc,_)).groupBy(_._1)
    }

    tmpList2.map{
      case (dc, list) => (dc, list.map(_._2).sortBy(-_._2).take(5))
    }.sortBy(_._1)
  }

  def main(args: Array[String]) {
    val genres = timed("Task 1", task1(titleBasicsList))
    val crews = timed("Task 2", task2(titleBasicsList, titleCrewList, nameBasicsList))
    val topDirectors = timed("Task 3", task3(nameBasicsList, titleBasicsList, titleCrewList, titleRatingsList))
    val topGenres = timed("Task 4", task4(titleBasicsList, titleRatingsList))

    println("---- Task 1 ----")
    println(genres)
    println("---- Task 2 ----")
    println(crews)
    println("---- Task 3 ----")
    println(topDirectors)
    println("---- Task 4 ----")
    println(topGenres)
    println(timing)
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
