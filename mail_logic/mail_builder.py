import datetime
import email
import os
import random
import re
import string
from dataclasses import dataclass
from email import policy
from email.header import decode_header
from email.message import Message
from sys import platform
from typing import List, Optional, Union, Type

import chardet
import imgkit
from PIL import Image

from common.config_controller import Config

@dataclass
class MailData:
    raw_data: Message
    id: str
    date: str
    body: str
    subject: str
    sender: str
    receiver: str
    folder: str

    attachments: List[str] = ()
    is_sent: bool = False
    converted_to_image: bool = False

    @classmethod
    def get_builder(cls):
        return MailBuilder

    def to_dict(self):
        return {
            "id": self.id,
            "date": self.date,
            "body": self.body,
            "subject": self.subject,
            "sender": self.sender,
            "receiver": self.receiver,
            "folder": self.folder,
            "is_sent": self.is_sent,
            "converted_to_image": self.converted_to_image,
            "attachments": self.attachments
        }


class MailBuilder:
    def __init__(self, raw_mail: Message, folder: str, mail_id: str, force_to_image: bool = False,
                 replacements: List[dict] = None):
        self.raw_data = raw_mail
        self.force_to_image = force_to_image
        self.folder = folder
        self.mail_id = mail_id
        self.replacements = replacements or []

        self.log = Config.get_common_logger()

    def build(self) -> MailData:
        return MailData(raw_data=self.raw_data,
                        folder=self.folder,
                        id=self.folder + self.mail_id,
                        subject=self._get_mail_subject(),
                        date=self._get_email_date(),
                        sender=self._get_mail_sender(),
                        receiver=self._get_mail_receiver(),
                        body=self._get_mail_body(),
                        )

    def _get_email_date(self) -> str:
        _date = self.raw_data["Date"]
        list_for_check_formats = [
            {'input_date': _date, "from_format": "%d %b %Y %H:%M:%S %z"},
            {'input_date': _date[:25], "from_format": "%a, %d %b %Y %H:%M:%S"},
            {'input_date': _date[:30], "from_format": "%a, %d %b %Y %H:%M:%S %z"},
            {'input_date': _date[:31], "from_format": "%a, %d %b %Y %H:%M:%S %z"},
        ]
        dt_email = None
        for case in list_for_check_formats:
            dt_email = dt_email or self._convert_date(case['input_date'], case['from_format'])

        if dt_email is None:
            raise Exception(f'Формат даты {_date} письма не соответствует ни одному указанному шаблону даты')
        self.log.debug(f"Дата письма {dt_email}")
        return dt_email

    @staticmethod
    def _convert_date(date_text, from_format) -> Optional[str]:
        try:
            return datetime.datetime.strptime(date_text, from_format).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError as e:
            return None

    def _get_mail_body(self) -> str:
        if self.force_to_image:
            content = self._get_mail_body_like_html(self.raw_data)
        else:
            content = self._get_mail_body_like_plaint_text(self.raw_data)

        content = self._decode_bytes(content)

        content = self._base_replacements(content)
        content = self.replace_re_substring(content, self.replacements)
        return content

    def _decode_bytes(self, content: Union[bytes, str]):
        try:
            if isinstance(content, bytes):
                enc = chardet.detect(content)
                encoding = enc["encoding"]
                if enc["confidence"] < 0.5:
                    encoding = "utf-8"
                content = content.decode(encoding=encoding)

            if content.find('\\u') != -1:
                content = content.encode('utf-8').decode('unicode-escape')
            return content

        except Exception as e:
            self.log.error(f"Не удалось определить верно кодировку {enc} для {content[:1000]} ", exc_info=e)

    @staticmethod
    def _get_mail_body_like_plaint_text(mail_data: Message) -> bytes:
        while mail_data.is_multipart():
            mail_data = mail_data.get_payload(0)
        return mail_data.get_payload(decode=True)

    @staticmethod
    def _get_mail_body_like_html(mail_data: Message) -> bytes:
        try:
            while mail_data.is_multipart():
                mail_data = mail_data.get_payload(1)
            return mail_data.get_payload(decode=True)
        except IndexError as e:
            Config.get_common_logger().warning("Отключите force_image.")
            return MailBuilder._get_mail_body_like_plaint_text(mail_data)


    def _get_mail_subject(self) -> str:
        email_data = self.raw_data
        current_subject = email_data.get("Subject") or ""
        encoded_subject = decode_header(current_subject)[0][0]
        return self._decode_bytes(encoded_subject)

    def _get_mail_sender(self):
        sender = self.raw_data.get("From") or ""
        self.log.debug("Mail sender is: " + str(sender))
        return sender

    def _get_mail_receiver(self):
        receiver = self.raw_data.get("To") or ""
        self.log.debug(f"Mail receiver is {receiver}")
        return receiver.lower()

    @staticmethod
    def _random_word(length):
        letters = string.ascii_lowercase
        return ''.join(random.choice(letters) for i in range(length))

    @staticmethod
    def _base_replacements(body: str) -> str:
        mail_body = body
        mail_body = str(email.message_from_string(mail_body, policy=policy.default))
        new_list = []
        count = 1
        row_list = mail_body.split('\n')
        for row in row_list:
            row.replace('\r', '')
            match_attach = re.search(r'\[cid:.*\]', row)
            if match_attach:
                row = '[Вложение ' + str(count) + ']'
                count += 1
            if row.find('*From:*') != -1 or row.find('From: ') != -1:
                break
            if row != '':
                new_list.append(row)
        body = '\n'.join(new_list)
        return body

    @staticmethod
    def replace_re_substring(text: str, list_dict_pattern_substr: List[dict]):
        for dict_pattern_substr in list_dict_pattern_substr:
            pattern = dict_pattern_substr["pattern"]
            substr = dict_pattern_substr["substr"]
            text = re.sub(pattern, substr, text)
        return text

    @classmethod
    def is_html(cls, data: MailData) -> bool:
        text = data.body
        return (text.find("<html") != -1 and text.find("</html>") != -1) or text.find("<br>") != -1

    @classmethod
    def html_message_to_image(cls, data: MailData, store_path, max_height: int = 1200, max_width: int = 600):
        text = data.body
        path = os.path.join(store_path, cls._random_word(10) + ".png")
        text = re.sub('<img[^>]*>', '', text)
        text = re.sub('<img>[^>]*</img>', '', text)
        text = text.replace('src="cid:', 'src="')
        text = f"<html><style> html{{width: {max_width}px !important;}}</style>" + text + "</html>"

        Config.get_common_logger().debug(f"HTML to image: {text}")
        options = {
            'width': max_width,
            'encoding': 'UTF-8',
            '--disable-smart-width': ""
        }
        if platform == "win32":
            config = imgkit.config(wkhtmltoimage=r'C:\Program Files\wkhtmltopdf\bin\wkhtmltoimage.exe')

            imgkit.from_string(text, path, config=config, options=options)
        else:
            imgkit.from_string(text, path, options=options)

        input_image = Image.open(path)
        _path = path
        image_width, image_height = input_image.size

        # Обрезание по высоте слишком длинных писем
        _new = False
        if image_height > max_height:
            for y in range(0, image_height, max_height):
                _new = True
                upper = y
                lower = y + max_height
                # Crop the tile from the original image
                tile = input_image.crop((0, upper, image_width, lower))

                # Save the tile as a separate image
                path = path.replace(".png", "")
                path = f'{path}_{y}.png'
                tile.save(path)
                data.attachments.append(path)
        else:
            data.attachments.insert(0, path)

        input_image.close()
        if _new:
            os.remove(_path)

        data.converted_to_image = True
        data.body = "Выгружено в изображение"

    @classmethod
    def save_attachment(cls, email_data: MailData, store_path):
        att_paths = []

        for part in email_data.raw_data.walk():
            if part.get_content_maintype() == 'multipart':
                continue
            if part.get('Content-Disposition') is None:
                continue

            filename = part.get_filename()
            if filename is None:
                continue
            if filename.find('=?utf-8?') != -1:
                continue
            att_path = os.path.join(store_path, filename)

            if os.path.isfile(att_path):
                att_path = os.path.join(store_path, f"{cls._random_word(10)}_{filename}")
            try:
                with open(att_path, 'wb') as fp:
                    fp.write(part.get_payload(decode=True))
                att_paths.append(att_path)
            except:
                pass

        email_data.attachments = att_paths
