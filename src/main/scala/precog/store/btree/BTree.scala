package precog.store.btree

import precog.util.RichComparator
import annotation.tailrec
import precog.store.Address

/**
 * Inspired directly from:
 * <a href="https://github.com/Arnauld/mochusi/blob/master/src/mbtree.erl">mbtree.erl</a>
 *
 * @param blockStore where nodes are stored
 * @param comparator used to compare key for ordering
 * @param rootAddress tree's root address
 * @param n block size threshold
 *
 * @author <a href="http://twitter.com/aloyer">@aloyer</a>
 */
class BTree(blockStore:NodeStore,
            comparator:RichComparator[Array[Byte]],
            val rootAddress:Address,
            val n:Int) {

  // TODO replace by generics in the BTree definition
  type K = Array[Byte]
  type V = Address

  private def compare(k1: K, k2: K) = comparator.compare(k1, k2)

  /**
   * Retrieve a <code>value</code> based on its <code>key</code>
   */
  def find(key:K):Option[V] = {
    assert(key!=null, "Key cannot be null")

    find(rootAddress, key)
  }

  private def find(addr:Address, key:K):Option[V] =
    blockStore.readNode(addr) match {
      case KPNode(pairs) =>
        traverseKPToFind(key, pairs)
      case KVNode(pairs) =>
        traverseKVToFind(key, pairs)
    }

  private def traverseKVToFind(key:K, pairs:List[KV]) : Option[V] =
    pairs match {
      case Nil => None
      case head::tail =>
        val cmp = compare(head.key, key)
        if (cmp < 0)
          traverseKVToFind(key, tail)
        else if (cmp > 0)
          None
        else // hurray!!!
          Some(head.value)
    }

  private def traverseKPToFind(key:K, pairs:List[KP]) : Option[V] =
    pairs match {
      case last::Nil => find(last.address, key)
      case head::tail =>
        val cmp = compare(head.key, key)
        if (cmp < 0)
          traverseKPToFind(key, tail)
        else
          find(head.address, key)
      case Nil =>
        throw new IllegalStateException("Pairs must have at least one element to exist")
    }

  /**
   * Associates the specified <code>value</code> with the specified
   * <code>key</code> in this BTree. If the BTree previously contained
   * a mapping for the key, the old value is replaced by the specified
   * value.
   *
   * @return an new updated <code>BTree</code>
   */
  def insert(key:K, value:V):BTree = {

    assert(key!=null, "Key cannot be null")
    assert(value!=null, "Value cannot be null")

    insert_1(rootAddress, key, value, 0) match {
      case Ok(newAddr) =>
        new BTree(blockStore, comparator, newAddr, n)
      case Split(splitKey, left, right) =>
        val newNode   = KPNode(KP(splitKey, left), KP(null, right))
        new BTree(blockStore, comparator, blockStore.writeNode(newNode), n)
    }
  }

  private def createKPNode(pairs: List[KP], depth:Int): InsertR = {
    if (pairs.size > n) {
      // node must be splitted
      val (left, head::rightTail) = pairs.splitAt(pairs.size / 2)
      val leftPairs = left ++ List(KP(null, head.address))
      val newLeftNode = KPNode(leftPairs)
      val newRightNode = KPNode(rightTail)
      val newLeftAddr = blockStore.writeNode(newLeftNode)
      val newRightAddr = blockStore.writeNode(newRightNode)
      Split(head.key, newLeftAddr, newRightAddr)
    }
    else {
      val node = KPNode(pairs)
      val addr = blockStore.writeNode(node)
      Ok(addr)
    }
  }

  private def createKVNode(pairs: List[KV], depth:Int): InsertR = {
    if (pairs.size > n) {
      // leaf node must be splitted
      val (leftPairs, rightPairs) = pairs.splitAt((pairs.size / 2) + 1)
      val newLeftNode = KVNode(leftPairs)
      val newRightNode = KVNode(rightPairs)
      val newLeftAddr = blockStore.writeNode(newLeftNode)
      val newRightAddr = blockStore.writeNode(newRightNode)
      Split(rightPairs.head.key, newLeftAddr, newRightAddr)
    }
    else {
      val node = KVNode(pairs)
      val addr = blockStore.writeNode(node)
      Ok(addr)
    }
  }

  private def insert_1(addr:Address, key:K, value:V, depth:Int):InsertR =
    if (addr == null) { // empty tree
      val node = KVNode(KV(key, value))
      val nodeAddr = blockStore.writeNode(node)
      Ok(nodeAddr)
    }
    else {
      val block = blockStore.readNode(addr)
      block match {
        // non-leaf cases
        case KPNode(pairs) =>
          val (kp, reversedTaversed, tail) = traverseKPPairs(key, Nil, pairs)

          val newPairs:List[KP] = insert_1(kp.address, key, value, depth + 1) match {
            case Ok(newAddr) =>
              val reversed = (KP(kp.key, newAddr) :: reversedTaversed).reverse_:::(tail)
              reversed.reverse

            case Split(splitKey, left, right) =>
              val newKP = KP(splitKey, left)
              // leaf child has been splitted: generate a new bucket
              tail match {
                case Nil => // KP is the last, simply discard it
                  val lastKP = KP(null, right)
                  val reversed = (newKP :: reversedTaversed).reverse_:::(List(lastKP))
                  reversed.reverse
                case _ => // replace next addr
                  val newNext = KP(kp.key, right)
                  val reversed = (newNext :: newKP :: reversedTaversed).reverse_:::(tail)
                  reversed.reverse
              }
          }
          createKPNode(newPairs, depth)

        // leaf cases
        case KVNode(pairs) =>
          val newPairs = insertPair(key, value, Nil, pairs)
          createKVNode(newPairs, depth)
      }
    }

  @tailrec
  private def insertPair(key:K, value:V, traversedInReversedOrder:List[KV], pairs:List[KV]): List[KV] = {
    pairs match {
      case Nil =>
        (KV(key, value) :: traversedInReversedOrder).reverse
      case head::tail =>
        val cmp = compare(head.key, key)
        if(cmp < 0)
          insertPair(key, value, head::traversedInReversedOrder, tail)
        else if(cmp > 0) {
          val reversed = (head :: KV(key, value) :: traversedInReversedOrder).reverse_:::(tail)
          reversed.reverse
        }
        else {
          val reversed = (KV(key, value) :: traversedInReversedOrder).reverse_:::(tail)
          reversed.reverse
        }

    }
  }

  @tailrec
  private def traverseKPPairs(key:K, traversedInReversedOrder:List[KP], pairs:List[KP]): (KP,List[KP],List[KP]) = {
     pairs match {
       case last::Nil =>
           (last, traversedInReversedOrder, Nil)
       case head::tail =>
         if(compare(head.key, key) < 0)
           traverseKPPairs(key, head::traversedInReversedOrder, tail)
         else
           (head, traversedInReversedOrder, tail)
       case Nil =>
         throw new IllegalStateException("Pairs must have at least one element to exist")
     }
  }

  def traverseInOrder[A](f:(K,V,A) => A, arg:A):A = traverseInOrder_addr(rootAddress, f, arg)


  // TODO: investigate tailrec that involve 3 methods 'traverseInOrder_*'
  // or refactor to find a most suitable way to support an equivalent
  private def traverseInOrder_addr[A](addr:Address, f:(K,V,A) => A, arg:A):A = {
    blockStore.readNode(addr) match {
      case KPNode(pairs) =>
        traverseInOrder_kp(pairs, f, arg)
      case KVNode(pairs) =>
        traverseInOrder_kv(pairs, f, arg)
    }
  }

  @tailrec
  private def traverseInOrder_kp[A](pairs:List[KP], f:(K,V,A) => A, arg:A):A =
    pairs match {
      case Nil  => arg
      case h::tail =>
        val arg1 = traverseInOrder_addr(h.address, f, arg)
        traverseInOrder_kp(tail, f, arg1)
    }

  @tailrec
  private def traverseInOrder_kv[A](pairs:List[KV], f:(K,V,A) => A, arg:A):A =
    pairs match {
      case Nil  => arg
      case h::tail =>
        if(h.value == null)
          arg
        else {
          val arg1 = f(h.key, h.value, arg)
          traverseInOrder_kv(tail, f, arg1)
        }
    }

}

/**
 * Return of an insert: it is used to indicate - when the value was inserted - if the node had been
 * splitted or not.
 */
sealed trait InsertR

case class Ok(newNode:Address) extends InsertR

case class Split(splitKey:Array[Byte], left:Address, right:Address) extends InsertR



