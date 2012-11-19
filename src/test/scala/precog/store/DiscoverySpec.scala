package precog.store

import org.specs2.mutable._
import scala.Array
import java.util.Comparator
import annotation.tailrec

/**
 *
 * @author <a href="http://twitter.com/aloyer">@aloyer</a>
 */
class DiscoverySpec extends Specification {

  implicit def bytes(s:String) = s.getBytes("utf-8")

  "A user" should {
    "have a basic understanding of how things are used" in {
      val store = new BasicStore(ArrayComparator)
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

trait RichComparator[A] extends Comparator[A] {
  def between(value:A, min:A, max:A):Boolean = compare(value, min) >= 0 && compare(value, max) <= 0
}

object ArrayComparator extends RichComparator[Array[Byte]] {
  def compare(o1: Array[Byte], o2: Array[Byte]) = {
    val len1 = o1.length
    val len2 = o2.length
    @tailrec
    def compare0(idx:Int):Int = {
      if (idx < len1 && idx < len2) {
        val a = (o1(idx) & 0xff)
        val b = (o2(idx) & 0xff)
        if (a != b)
           a - b
        else
          compare0(idx+1)
      }
      else
        len1 - len2
    }
    compare0(0)
  }
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
