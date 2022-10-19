package com.github.pflooky.datagen.core.generator.provider

object OneOfDataGenerator {

  def getGenerator(options: Map[String, Any]): DataGenerator[_] = {
    new RandomOneOfDataGenerator(options)
  }

  class RandomOneOfDataGenerator(val options: Map[String, Any]) extends DataGenerator[Any] {
    private val oneOfList = options("oneOf").asInstanceOf[List[Any]]
    private val oneOfListSize = oneOfList.size

    override def generate: Any = {
      oneOfList(random.nextInt(oneOfListSize))
    }
  }

}
