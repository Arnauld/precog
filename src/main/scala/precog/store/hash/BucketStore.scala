package precog.store.hash

import precog.store.Address

/**
 * 
 * @author <a href="http://twitter.com/aloyer">@aloyer</a>
 */
trait BucketStore {
  def readBucket(address: Address): Bucket
  def writeBucket(node:Bucket): Address

}
