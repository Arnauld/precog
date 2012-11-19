package precog.util

import org.specs2.mutable._

/**
 * 
 * @author <a href="http://twitter.com/aloyer">@aloyer</a>
 */
class BytesComparatorSpec extends Specification {

  implicit def bytes(s:String) = s.getBytes("utf-8")

  "BytesComparator" should {
    "support equality" in {
      BytesComparator.compare("bouh", "bouh") must_== 0
    }
    "indicates longer string with identical prefix is greater than" in {
      BytesComparator.compare("bouhhh", "bouh") must be_>(0)
    }
    "indicates longer string with identical prefix is greater than (reflexive)" in {
      BytesComparator.compare("bouh", "bouhhhh") must be_<(0)
    }
  }
}
