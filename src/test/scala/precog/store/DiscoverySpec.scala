package precog.store

import org.specs2.mutable._
import scala.Array
import annotation.tailrec
import precog.util.{RichComparator, BytesComparator}

/**
 *
 * @author <a href="http://twitter.com/aloyer">@aloyer</a>
 */
class DiscoverySpec extends Specification {

  implicit def bytes(s:String) = s.getBytes("utf-8")

  "A user" should {
    "have a basic understanding of how things are used" in {
      val store = new BasicStore(BytesComparator)
      store.put("areuhh", "what!?")
      store.put("argh0", "humpf")
      store.put("argh1", "erf")
      store.put("arghlll", "oops")
      store.put("bouhh", "doo")

      var q = store.traverse("argh", "beuarhhh")(countReader(0))
      q must_== 3
    }
  }

  def countReader(count:Int):Reader[Int] = More( _ match {
      case Some(v) =>
        countReader(count+1)
      case None =>
        Done(count)
    })

}

class BasicStore(comparator:RichComparator[Array[Byte]]) extends DiskStore {
  private var values: Map[Array[Byte], Array[Byte]] = Map()

  def put(key: Array[Byte], value: Array[Byte]) {
    values = values + (key -> value)
  }

  def get(key: Array[Byte]) = values.get(key)

  def flush() {}

  def traverse[A](start: Array[Byte], end: Array[Byte])(reader: Reader[A]) = {
    @tailrec
    def iter(values: Iterator[(Array[Byte], Array[Byte])], reader: Reader[A]): A =
      reader match {
        case Done(a) => a
        case More(r) =>
            if (values.hasNext) {
              val next = values.next()
              if (comparator.between(next._1, start, end)) {
                val nextReader = r(Some(next))
                iter(values, nextReader)
              }
              else
                iter(values, reader)
            }
            else {
              val nextReader = r(None)
              iter(values, nextReader)
            }

      }
    iter(values.iterator, reader)
  }
}
