# tools/emailer.py
import os, smtplib
from email.mime.text import MIMEText

SMTP_HOST=os.getenv("SMTP_HOST"); SMTP_PORT=int(os.getenv("SMTP_PORT","587"))
SMTP_USER=os.getenv("SMTP_USER"); SMTP_PASS=os.getenv("SMTP_PASS")
MAIL_FROM=os.getenv("MAIL_FROM", "StoreBot <bot@example.com>")

def send_mail(to_email: str, subject: str, body: str):
    msg = MIMEText(body, "plain")
    msg["Subject"]=subject; msg["From"]=MAIL_FROM; msg["To"]=to_email
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
        s.starttls(); s.login(SMTP_USER, SMTP_PASS); s.send_message(msg)

