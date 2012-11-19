package precog.util

import org.specs2.mutable._

/**
 * 
 * @author <a href="http://twitter.com/aloyer">@aloyer</a>
 */
class ArrayComparatorSpec extends Specification {

  implicit def bytes(s:String) = s.getBytes("utf-8")

  "ArrayComparator" should {
    "support equality" in {
      ArrayComparator.compare("bouh", "bouh") must_== 0
    }
    "indicates longer string with identical prefix is greater than" in {
      ArrayComparator.compare("bouhhh", "bouh") must be_>(0)
    }
    "indicates longer string with identical prefix is greater than (reflexive)" in {
      ArrayComparator.compare("bouh", "bouhhhh") must be_<(0)
    }
  }
}
