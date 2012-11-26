package precog.store.btree

import precog.store.Address

/**
 *
 * @author <a href="http://twitter.com/aloyer">@aloyer</a>
 */
trait BlockStore {
  def readBlock(address: Address): Node
  def writeBlock(node:Node): Address
}
