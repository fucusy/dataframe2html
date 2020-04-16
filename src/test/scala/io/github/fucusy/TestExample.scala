package io.github.fucusy

import org.scalatest.funsuite.AnyFunSuite

class TestExample extends AnyFunSuite {
  test("Test the Example") {
    assert(Example.returnA() == "A")
  }
}
