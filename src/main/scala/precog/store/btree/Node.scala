package precog.store.btree

import precog.store.Address

// Last bucket of a KP node does not have any key defined, it is used
// to redirect to keys greater than K2.
//
//   [K1|K2|  ]
//   /    |   \
//  P     P    P
//


/**
 * Base class for BTree node (non-leaf)
 */
sealed trait Node

/**
 * Key-Pointer tuple:
 *
 * Key based indirection to a node (non-leaf).
 */
case class KP(key: Array[Byte], address: Address) {
  override def toString = "KP(" + (if (key == null)
    "n/a"
  else
    new String(key)) + ", " + address + ")"
}

/**
 * Node made of KPs.
 */
case class KPNode(pairs: List[KP]) extends Node {
  assert(pairs.head.key != null)
}

object KPNode {
  def apply(kp: KP): KPNode = KPNode(List(kp))
  def apply(kp1: KP, kp2: KP): KPNode = KPNode(List(kp1, kp2))
}

/**
 * Key-Value tuple:
 *
 * Key based indirection to a value (leaf)
 */
case class KV(key: Array[Byte], value: Address) {
  override def toString = "KV(" + new String(key) /*+ ", " + value */ + ")"
}

/**
 * Node made of KVs. It corresponds to the last node before values.
 */
case class KVNode(pairs: List[KV]) extends Node {
  def this(kv: KV) = this(List(kv))
}

object KVNode {
  def apply(kv: KV): KVNode = KVNode(List(kv))

  def apply(kv1: KV, kv2: KV): KVNode = KVNode(List(kv1, kv2))
}

