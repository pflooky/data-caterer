package com.github.pflooky.datagen.core.model

import com.github.pflooky.datacaterer.api.model.Constants.FOREIGN_KEY_DELIMITER
import org.scalatest.funsuite.AnyFunSuite

class ForeignKeyRelationHelperTest extends AnyFunSuite {

  test("Can parse foreign key relation from string") {
    val result = ForeignKeyRelationHelper.fromString(s"my_postgres${FOREIGN_KEY_DELIMITER}public.categories${FOREIGN_KEY_DELIMITER}id")

    assert(result.dataSource == "my_postgres")
    assert(result.step == "public.categories")
    assert(result.columns == List("id"))
  }

  test("Can parse foreign key relation from string with multiple columns") {
    val result = ForeignKeyRelationHelper.fromString(s"my_postgres${FOREIGN_KEY_DELIMITER}public.categories${FOREIGN_KEY_DELIMITER}id,amount,description")

    assert(result.dataSource == "my_postgres")
    assert(result.step == "public.categories")
    assert(result.columns == List("id", "amount", "description"))
  }

}
