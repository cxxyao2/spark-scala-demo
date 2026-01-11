package de.xixi.sparkcourse

import org.scalatest.funsuite.AnyFunSuite

class FirstTest extends AnyFunSuite {
  test("add(2,5) returns 7") {
    val result = Main.add(2,5)
    assert(result == 7)
  }

}
