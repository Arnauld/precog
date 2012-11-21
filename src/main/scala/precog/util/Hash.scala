package precog.util

import java.security.MessageDigest

/**
 * 
 * @author <a href="http://twitter.com/aloyer">@aloyer</a>
 */
trait Hash {
  def hash(bytes:Array[Byte]):Long
}

object Hash {
  val Ketama:Hash = new Hash {
    def hash(bytes: Array[Byte]) = {
      val bKey = md5(bytes)
      val hash = ((bKey(3) & 0xFF).asInstanceOf[Long] << 24) |
                 ((bKey(2) & 0xFF).asInstanceOf[Long] << 16) |
                 ((bKey(1) & 0xFF).asInstanceOf[Long] << 8)  |
                 ((bKey(0) & 0xFF).asInstanceOf[Long])
      hash
    }
  }

  private val md5Digest = MessageDigest.getInstance("MD5")

  def md5(bytes: Array[Byte]):Array[Byte] = {
    val md5 = md5Digest.clone().asInstanceOf[MessageDigest]
    md5.update(bytes)
    md5.digest()
  }
}
