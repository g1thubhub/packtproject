package packt.core

import scala.collection.mutable

object FunctionalProgramming {

  def main(args: Array[String]): Unit = {

    val tokens = List[String]("Settlements", "some", "centuries", "old", "and", "still", "no", "bigger", "than",
      "pinheads", "on", "the", "untouched", "expanse", "of", "their", "background")

    ////////////////////////////////////////////
    // map
    ////////////////////////////////////////////
    // non anonymous:
    def lengthMethod(token: String): Int = token.length // method argument is a String, return type an integer
    val lengthFunction: (String => Int) = token => token.length // function type is a function mapping a String to an integer
    val tokenLengths1 = tokens.map(lengthFunction)

    // anonymous functions, all are equivalent:
    val tokensLengthAnonymous1 = tokens.map(_.length) // method parameter is replaced with underscore
    val tokensLengthAnonymous2 = tokens.map(token => token.length) // method parameter explicitly mentioned
    val tokensLengthAnonymous3 = tokens.map((token: String) => token.length)

    val tokenLengths = tokens.map(lengthFunction)

    ////////////////////////////////////////////
    // flatMap
    ////////////////////////////////////////////
    def lowerLengthMethod(token: String): Option[Int] = {
      if(token.charAt(0).isUpper) // skip elements starting with uppercase letter
        None
      else // else return the length of the lowercase token
        Some(token.length)
    }

    val lowerTokenLengths = tokens.flatMap(lowerLengthMethod)

    ////////////////////////////////////////////
    // reduce
    ////////////////////////////////////////////
    def minLengthMethod(tokenLengths: (Int, Int)): Int = {
      if (tokenLengths._1 <= tokenLengths._2)
        tokenLengths._1
      else
        tokenLengths._2
    }

    val minLength: Int = tokens
      .map(lengthFunction)
      .reduce((length1: Int, length2: Int) => minLengthMethod(length1, length2)) // or shorter:  .reduce(minLengthMethod(_,_))

    ////////////////////////////////////////////
    // foldLeft
    ////////////////////////////////////////////

    def addInitialToSet(set: mutable.Set[String], nextToken: String): mutable.Set[String] = {
      set.add(nextToken.substring(0, 1))
      set
    }

    val distinctInitials = tokens
      .foldLeft(mutable.Set.empty[String])((accumulator: mutable.Set[String], nextToken: String) => addInitialToSet(accumulator, nextToken))
    // or shorter:  .foldLeft(mutable.Set.empty[String])(addInitialToSet)

  }
}
