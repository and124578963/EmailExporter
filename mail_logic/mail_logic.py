import re
from email.message import Message

import email
from imaplib import IMAP4_SSL
from common.config_controller import Config
from database.database import MongoDatabase
from .mail_builder import MailData, MailBuilder
from .profile import ConfigProfile


class EmailConnection(IMAP4_SSL):
    def __init__(self, imap_host, login, passw):
        super().__init__(host=imap_host)
        self.login(user=login, password=passw)

        self.log = Config.get_common_logger()
        self.log.info(f"The connection was established.")

    def get_last_id_in_folder(self, folder) -> bytes:
        self.log.info(f"Getting last letter id from folder {folder}...")
        status, last_id = self.select(folder, readonly=True)
        if status != "OK":
            raise Exception(f"Got status {status} while getting last id in {folder} folder")

        return last_id[0]

    def get_mail_by_id(self, mail_id: int, folder) -> Message:
        self.log.info(f"Getting mail data for id {mail_id} in {folder} folder ")
        status, data = self.fetch(message_set=str(mail_id), message_parts="(RFC822)")
        if status != "OK":
            raise Exception(f"Got status {status} while getting message by id {mail_id} in {folder} folder")
        raw_email = data[0][1]

        return email.message_from_bytes(raw_email)


class MailFacade:
    def __init__(self, profile: ConfigProfile):
        conf = Config()
        self.profile = profile
        self.folder = self.profile.folder
        self.log = Config.get_common_logger()
        self.attachments_path = conf.attachment_path
        self.db_conn = MongoDatabase()

        self.email_connect = EmailConnection(self.profile.imap_host, self.profile.login, self.profile.passw)
        last_id = int(self.email_connect.get_last_id_in_folder(self.folder))

        amount_offset_data = self.db_conn.table("offset_folder").count_documents({"folder": self.folder})
        if amount_offset_data > 1:
            raise Exception(f"Два оффсета у одной папки {self.folder} в БД")

        if amount_offset_data == 0:
            _offset_dict = {"_id": self.folder,
                            'folder': self.folder,
                            'offset': last_id
                            }
            self.db_conn.table("offset_folder").insert_one(_offset_dict)
            self.log.info(f'Инициирован оффсет для папки {self.folder}')

        offset_data = self.db_conn.table("offset_folder").find({"folder": self.folder})
        offset = int(offset_data[0]['offset'])
        if offset == last_id:
            self.log.info(f'Нет новых писем в папке {self.folder}')
            return

        list_actual_id = range(offset + 1, last_id + 1)

        for mail_id in list_actual_id:
            self.log.info(f"Обработка mail_id {mail_id} из папки {self.folder}")
            _data = self.email_connect.get_mail_by_id(mail_id, self.folder)
            mail_data = MailData.get_builder()(raw_mail=_data,
                                               folder=self.folder,
                                               mail_id=str(mail_id),
                                               force_to_image=self.profile.force_to_image,
                                               replacements=self.profile.replacements
                                               ).build()

            self.log.info(f"Тема письма {mail_data.subject}")

            # Исключаем получателей не нас
            if not re.search(self.profile.receiver_regex_mask, mail_data.receiver):
                self.log.info(f"Исключено по получателю {mail_data.receiver}")
                continue

            if mail_data.subject is None:
                self.log.info(f"Исключено из-за отсутствующей темы")
                continue

            is_skip = False
            for filter in self.profile.restricted_subjects_regex:
                if re.search(filter, mail_data.subject.lower()):
                    is_skip = True
            if is_skip:
                self.log.info(f"Исключено из-за restricted_subjects_regex")
                continue

            if self.mail_id_already_in_db(mail_id):
                self.log.info(f"Исключено из-за уже наличия в бд")
                continue

            MailBuilder.save_attachment(mail_data, store_path=self.attachments_path)
            if MailBuilder.is_html(mail_data):
                MailBuilder.html_message_to_image(mail_data, store_path=self.attachments_path,
                                                  max_height=self.profile.max_height_px,
                                                  max_width=self.profile.max_width_px)

            mails_conn = self.db_conn.table("mails")
            mail_db_data = mail_data.to_dict()
            mail_db_data.update(self.profile.ext_fields)
            mails_conn.insert_one(mail_db_data)
            self.log.debug(f"Inserted in 'mails' table: {mail_db_data}")

        self.db_conn.table("offset_folder").delete_many(filter={'folder': self.folder})
        self.db_conn.table("offset_folder").insert_one({'folder': self.folder,
                                                        'offset': last_id
                                                        })
        self.log.info(f"Записи офсета обновлены для папки {self.folder}")
        self.email_connect.close()

    def mail_id_already_in_db(self, mail_id) -> bool:
        mail_id = f"{self.folder}{mail_id}"
        db_data = self.db_conn.table("mails").count_documents({"mail_id": mail_id})
        return db_data > 0





