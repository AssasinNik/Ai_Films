package com.ai_services.user_management.application.service

import org.springframework.mail.javamail.JavaMailSender
import org.springframework.mail.javamail.MimeMessageHelper
import org.springframework.stereotype.Service

@Service
class EmailService(private val mailSender: JavaMailSender) {
    fun sendVerificationCode(to: String, subject: String, code: String) {
        val message = mailSender.createMimeMessage()
        val helper = MimeMessageHelper(message, true, "UTF-8")
        helper.setTo(to)
        helper.setSubject(subject)
        helper.setText(buildHtml(code), true)
        mailSender.send(message)
    }

    private fun buildHtml(code: String): String = """
        <html>
          <head>
            <meta charset='utf-8'/>
            <style>
              /* Унифицированный стиль для всех клиентов (темная/светлая темы не влияют) */
              body { margin:0; padding:0; background:#010104; }
              table { border-collapse:collapse; }
              .container { width:100%; background:#010104; padding:24px 0; }
              .card { width:100%; max-width:560px; margin:0 auto; background:#0b0b13; border:1px solid #1a1a25; border-radius:12px; }
              .inner { padding:24px; font-family: Arial, Helvetica, sans-serif; color:#e6e7ec; }
              .title { font-size:20px; font-weight:bold; line-height:1.35; }
              .muted { color:#a3a6b3; font-size:14px; line-height:1.6; }
              .code { font-size:28px; letter-spacing:6px; font-weight:800; background:#0e0e18; padding:16px 20px; border-radius:10px; border:1px solid #1a1a25; display:inline-block; margin:16px 0; color:#e6e7ec; }
              .footer { font-size:12px; color:#8b8fa1; margin-top:16px; }
            </style>
          </head>
          <body>
            <table class="container" role="presentation" width="100%" cellpadding="0" cellspacing="0">
              <tr>
                <td align="center">
                  <table class="card" role="presentation" cellpadding="0" cellspacing="0">
                    <tr>
                      <td>
                        <div style="height:6px; background:linear-gradient(90deg,#F7FF2D 0%,#B4FD64 100%);"></div>
                      </td>
                    </tr>
                    <tr>
                      <td class="inner">
                        <div class="title">Код подтверждения AI Films</div>
                        <div class="muted">Если это вы пытаетесь войти или зарегистрироваться — введите код ниже.</div>
                        <div class="code">$code</div>
                        <div class="muted">Код действует 10 минут. Если это были не вы — проигнорируйте письмо.</div>
                        <div class="footer">© AI Films</div>
                      </td>
                    </tr>
                    <tr>
                      <td>
                        <div style="height:6px; background:linear-gradient(90deg,#B4FD64 0%,#F7FF2D 100%);"></div>
                      </td>
                    </tr>
                  </table>
                </td>
              </tr>
            </table>
          </body>
        </html>
    """.trimIndent()
}


