import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class Tests extends FunSuite with BeforeAndAfterEach {

  var session: SparkSession = _

  override def beforeEach() {
    session = SparkSession.builder()
      .appName("test app")
      .master("local[2]")
      .getOrCreate()
  }

  test("First test") {
    val testdate = session.range(1000)
    assert(testdate.count() === 1000)
  }

  override def afterEach() {
    session.stop()
  }
}