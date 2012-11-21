package precog.util

import org.specs2.mutable.Specification

/**
 * 
 * @author <a href="http://twitter.com/aloyer">@aloyer</a>
 */
class BytesSpec extends Specification {
  "Bytes" should {
    "allow a long to be serialized in bytes and read back" in {
      val bytes = Array(1,2,3,4,5,6,7,8,9,0,1,2,3).map( _.asInstanceOf[Byte] )
      Bytes.writeLong(2, bytes, 1515102375657623789L)

      //remains unchanged
      bytes(0) must_== 1
      bytes(1) must_== 2

      // serialized form of the long
      bytes(2) must_== (0x15).asInstanceOf[Byte]
      bytes(3) must_== (0x06).asInstanceOf[Byte]
      bytes(4) must_== (0xb9).asInstanceOf[Byte]
      bytes(5) must_== (0x95).asInstanceOf[Byte]

      bytes(6) must_== (0x53).asInstanceOf[Byte]
      bytes(7) must_== (0x6c).asInstanceOf[Byte]
      bytes(8) must_== (0x50).asInstanceOf[Byte]
      bytes(9) must_== (0xed).asInstanceOf[Byte]

      //remains unchanged
      bytes(10) must_== 1
      bytes(11) must_== 2
      bytes(12) must_== 3

      // read back
      val actual = Bytes.readLong(2, bytes)
      actual must_== 1515102375657623789L
    }
  }
}
