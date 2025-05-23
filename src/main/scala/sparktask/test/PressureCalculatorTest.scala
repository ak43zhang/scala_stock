//package spark.test
//
//class PressureCalculatorTest extends SparkSessionTestWrapper {
//
//  test("ATR计算逻辑验证") {
//    val testData = Seq(
//      RawData("TEST", "2023-01-01 09:30:00", 100.0, 105.0, 98.0, 102.0, 1000),
//      RawData("TEST", "2023-01-02 09:30:00", 102.0, 108.0, 101.0, 107.0, 2000)
//    )
//    val ds = spark.createDataset(testData)
//    val result = PressureSupportCalculator.calculateATR(ds).collect()
//    assert(result.last.atr > BigDecimal(0))
//  }
//
//  test("结果校验逻辑验证") {
//    val badResult = ResultData("TEST", "2023-01-01", 90.0, 95.0, 85.0, 90.0, 80.0, 85.0, 100.0, 95.0)
//    val ds = spark.createDataset(Seq(badResult))
//    intercept[IllegalStateException] {
//      PressureSupportCalculator.validateAndSaveResults(ds, "test_path")
//    }
//  }
//}