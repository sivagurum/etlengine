package com.aero.core

object CustomFunc {
  
  val genArray = {
    actors: String => {
      actors.replace("[", " ").replace("]", " ").trim.split(",").toList
    }
  }
  
  def isShortMovie(duration: Int): Boolean = if (duration <= 90) true else false
  
  val isMovieShort = {
    duration: Int => {
      if (duration <= 90) "Yes" else "No"
    }
  }
}