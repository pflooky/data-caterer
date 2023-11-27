package com.github.pflooky.datagen.core.model

import com.github.pflooky.datacaterer.api.model.Constants.FOREIGN_KEY_DELIMITER
import com.github.pflooky.datacaterer.api.model.SinkOptions
import com.github.pflooky.datagen.core.util.PlanImplicits.SinkOptionsOps
import org.scalatest.funsuite.AnyFunSuite

class PlanImplicitsTest extends AnyFunSuite {

  test("Can map foreign key relations to relationships without column names") {
    val sinkOptions = SinkOptions(foreignKeys =
      List(
        s"my_postgres${FOREIGN_KEY_DELIMITER}public.categories${FOREIGN_KEY_DELIMITER}id" ->
          List(s"my_csv${FOREIGN_KEY_DELIMITER}account${FOREIGN_KEY_DELIMITER}account_id")
      )
    )
    val result = sinkOptions.foreignKeysWithoutColumnNames

    assert(result == List(s"my_postgres${FOREIGN_KEY_DELIMITER}public.categories" -> List(s"my_csv${FOREIGN_KEY_DELIMITER}account")))
  }

}
