package utils

/**
 * @describe 隐式转换
 * @author 肖斌武.
 * @datetime 2020/6/16 17:03
 */
object Implicits {
	implicit def ttInt(str: String): Int = {
		if (str == "") 0
		else Integer.parseInt(str)
	}

	implicit def ttDouble(str: String): Double = {
		if (str == "") 0.0
		else java.lang.Double.parseDouble(str)
	}
}
