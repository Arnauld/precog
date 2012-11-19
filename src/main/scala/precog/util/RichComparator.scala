package precog.util

import java.util.Comparator

/**
 *
 * @author <a href="http://twitter.com/aloyer">@aloyer</a>
 */
trait RichComparator[A] extends Comparator[A] {
  def between(value:A, min:A, max:A):Boolean =
    compare(value, min) >= 0 && compare(value, max) <= 0
}
