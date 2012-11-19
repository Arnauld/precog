package precog.util

import annotation.tailrec

/**
 *
 * @author <a href="http://twitter.com/aloyer">@aloyer</a>
 */
object BytesComparator extends RichComparator[Array[Byte]] {
  def compare(o1: Array[Byte], o2: Array[Byte]) = {
    val len1 = o1.length
    val len2 = o2.length
    @tailrec def compare0(idx:Int):Int = {
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
