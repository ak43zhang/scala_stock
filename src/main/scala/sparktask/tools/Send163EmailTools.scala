package sparktask.tools

import java.util.Properties

import javax.mail.internet.{InternetAddress, MimeBodyPart, MimeMessage, MimeMultipart}
import javax.mail._

object Send163EmailTools {

    // 配置163邮箱SMTP服务器参数
    private val host = "smtp.163.com"
    private val port = "465"
    private val sslEnabled = true

    // 创建邮件配置
    private def getSession(username: String, password: String): Session = {
      val props = new Properties()
      props.put("mail.smtp.host", host)
      props.put("mail.smtp.port", port)
      props.put("mail.smtp.auth", "true")
      props.put("mail.smtp.ssl.enable", sslEnabled.toString)
      props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory")

      Session.getInstance(props, new Authenticator {
        override def getPasswordAuthentication: PasswordAuthentication = {
          new PasswordAuthentication(username, password)
        }
      })
    }

    // 发送邮件方法
    def sendEmail(
                   username: String,
                   password: String,
                   to: String,
                   subject: String,
                   content: String,
                   isHtml: Boolean = false
                 ): Unit = {
      val session = getSession(username, password)
      val message = new MimeMessage(session)

      try {
        // 设置发件人/收件人/主题
        message.setFrom(new InternetAddress(username))
        message.setRecipient(Message.RecipientType.TO, new InternetAddress(to))
        message.setSubject(subject)

        // 设置邮件内容（支持HTML/文本）
        if (isHtml) {
          message.setContent(content, "text/html; charset=utf-8")
        } else {
          message.setText(content)
        }

        // 发送邮件
        Transport.send(message)
        println("邮件发送成功！")
      } catch {
        case ex: MessagingException =>
          throw new RuntimeException(s"邮件发送失败: ${ex.getMessage}", ex)
      }
    }

  /**
   * 添加附件支持
   * @param message
   * @param filePath
   */
  def addAttachment(message: MimeMessage, filePath: String,content:String): Unit = {
    val multipart = new MimeMultipart()
    val textPart = new MimeBodyPart()
    textPart.setText(content)
    multipart.addBodyPart(textPart)

    val attachmentPart = new MimeBodyPart()
    attachmentPart.attachFile(filePath)
    multipart.addBodyPart(attachmentPart)

    message.setContent(multipart)
  }
  }
