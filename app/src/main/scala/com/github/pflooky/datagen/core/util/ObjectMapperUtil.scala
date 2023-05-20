package com.github.pflooky.datagen.core.util

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object ObjectMapperUtil {

  val yamlObjectMapper = new ObjectMapper(new YAMLFactory())
  yamlObjectMapper.registerModule(DefaultScalaModule)
  yamlObjectMapper.setSerializationInclusion(Include.NON_ABSENT)

  val jsonObjectMapper = new ObjectMapper()
  jsonObjectMapper.registerModule(DefaultScalaModule)

}
