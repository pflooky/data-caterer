package com.github.pflooky.datagen.core.util

import com.github.pflooky.datacaterer.api.model.Constants.{PASSWORD, USERNAME}

import java.util.Base64

object HttpUtil {

  def getAuthHeader(connectionConfig: Map[String, String]): Map[String, Seq[String]] = {
    if (connectionConfig.contains(USERNAME) && connectionConfig.contains(PASSWORD)) {
      val user = connectionConfig(USERNAME)
      val password = connectionConfig(PASSWORD)
      val encodedUserPassword = Base64.getEncoder.encodeToString(s"$user:$password".getBytes)
      Map("Authorization" -> Seq(s"Basic $encodedUserPassword"))
    } else {
      Map()
    }
  }
}
