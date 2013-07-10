import org.specs2._

class IndexManagerSpec extends Specification {
  def is = s2"""
    This is a specification to check the 'Hello world' string

    The 'Hello world' string should
      contain 11 characters                                         $e1
      start with 'Hello'                                            $e2
      end with 'world'                                              $e3
  """
  def e1 = success
  def e2 = success
  def e3 = 1 must_== 2
}