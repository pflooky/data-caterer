package com.github.pflooky.datagen.core.util

import java.io.File
import scala.util.{Success, Try}

object FileUtil {

  def getFile(filePath: String): File = {
    val (directFile, classFile, classLoaderFile) = getDirectAndClassFiles(filePath)
    (directFile.exists(), classFile.map(_.exists), classLoaderFile.map(_.exists)) match {
      case (true, _, _) => directFile
      case (_, Success(true), _) => classFile.get
      case (_, _, Success(true)) => classLoaderFile.get
      case _ => throw new RuntimeException(s"Failed for find file, path=$filePath")
    }
  }

  def getDirectory(folderPath: String): File = {
    val (directFile, classFile, classLoaderFile) = getDirectAndClassFiles(folderPath)
    (directFile.isDirectory, classFile.map(_.isDirectory), classLoaderFile.map(_.isDirectory)) match {
      case (true, _, _) => directFile
      case (_, Success(true), _) => classFile.get
      case (_, _, Success(true)) => classLoaderFile.get
      case _ => throw new RuntimeException(s"Failed for find directory, path=$folderPath")
    }
  }

  private def getDirectAndClassFiles(filePath: String): (File, Try[File], Try[File]) = {
    val directFile = new File(filePath)
    val classFile = Try(new File(getClass.getResource(filePath).getPath))
    val classLoaderFile = Try(new File(getClass.getClassLoader.getResource(filePath).getPath))
    (directFile, classFile, classLoaderFile)
  }

}
