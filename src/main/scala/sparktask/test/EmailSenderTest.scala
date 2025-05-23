package sparktask.test

import sparktask.tools.Send163EmailTools

object EmailSenderTest {
  def main(args: Array[String]): Unit = {
    // 配置信息（需替换为实际值）
    val username = "m17600700886@163.com"  // 你的163邮箱
    val password = "CPPHA5yTAZQLDju5" // 邮箱SMTP授权码（非登录密码）
    val to = "m17600700886@163.com"      // 收件人邮箱

    val subject = "测试文本邮件"
    val text_content = "这是一封来自Scala的测试邮件\n换行测试"
    val html_content = """
        <html>
          <body>
            <h1>HTML内容测试</h1>
            <p style="color: red;">红色文字</p>
            <ul>
              <li>列表项1</li>
              <li>列表项2</li>
            </ul>
          </body>
        </html>
      """
    // 发送文本邮件
    Send163EmailTools.sendEmail(username, password, to, subject ,text_content)

    // 发送HTML邮件
    Send163EmailTools.sendEmail(username, password, to, subject , content = html_content, isHtml = true)
  }
}