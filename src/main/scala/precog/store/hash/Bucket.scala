package precog.store.hash

import precog.store.Address

/**
 * 
 * @author <a href="http://twitter.com/aloyer">@aloyer</a>
 */
case class Bucket(pairs:List[HP], nextBucket:Option[Address]) {
  assert(!pairs.isEmpty, "Pairs cannot be empty")

  import scala.math.{min, max}

  def range:(Long,Long) = {
    val head = pairs.head // pairs is never empty
    pairs.tail.foldLeft((head.hash, head.hash))({ (minmax, hp) =>
      (min(minmax._1, hp.hash), max(minmax._2, hp.hash))
    })
  }
}

/**
 * Hash-Pointer tuple
 */
case class HP(hash: Long, address: Address)
