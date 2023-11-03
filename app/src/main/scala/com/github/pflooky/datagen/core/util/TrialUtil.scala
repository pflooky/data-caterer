package com.github.pflooky.datagen.core.util

import com.github.pflooky.datacaterer.api.model.Constants.API_KEY
import com.github.pflooky.datagen.core.model.Constants.DATA_CATERER_SITE_PRICING
import org.apache.log4j.Logger
import org.joda.time.{DateTime, Days}

import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import scala.util.{Failure, Success, Try}

object TrialUtil {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val pattern = "^.+?random ([0-9]{4}-[0-9]{2}-[0-9]{2}) text.*$".r
  private val numberOfTrialDays = 31

  def checkApiKey(key: String = ""): Boolean = {
    val apiKeyEnv = if (key.nonEmpty) key else System.getenv(API_KEY)
    if (apiKeyEnv == null) {
      LOGGER.error(s"$API_KEY is not defined as an environment variable. Please define to run Data Caterer")
      false
    } else {
      val decryptedApiKey = new KeyEncryption().decrypt(apiKeyEnv)
      decryptedApiKey match {
        case Some(pattern(date)) =>
          val parsedDate = DateTime.parse(date)
          val today = DateTime.now()
          val daysBetween = Days.daysBetween(DateTime.now, parsedDate.plusDays(numberOfTrialDays)).getDays
          val daysLeft = Math.max(daysBetween, 0)
          if (daysLeft == 0) {
            LOGGER.error(s"Your trial period has expired. Please consider upgrading to the paid plan to help support development $DATA_CATERER_SITE_PRICING")
          } else {
            LOGGER.info(s"Your trial has $daysLeft days left. Go nuts and try everything out!")
          }
          daysLeft > 0
        case _ =>
          LOGGER.error("Failed to verify API key")
          false
      }
    }
  }

  class KeyEncryption {
    private val key = "getBytesgetStringgetInst"
    private val initVector = "getInstanceInitt"
    private val iv = new IvParameterSpec(initVector.getBytes("UTF-8"))
    private val skeySpec = new SecretKeySpec(key.getBytes("UTF-8"), "AES")
    private val cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING")

    def encrypt(string: String): String = {
      cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv)
      val encrypted = cipher.doFinal(string.getBytes)
      Base64.getEncoder.encodeToString(encrypted)
    }

    def decrypt(string: String): Option[String] = {
      cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv)
      val tryDecrypt = Try(cipher.doFinal(Base64.getDecoder.decode(string)))
      tryDecrypt match {
        case Failure(_) =>
          LOGGER.error("Invalid API_KEY. Please reach out to Peter Flook on Slack for assistance")
          None
        case Success(value) =>
          Some(new String(value))
      }
    }
  }
}

