import logging
import os
import re
import datetime
import multiprocessing
import time
from multiprocessing import freeze_support
import email
from email import policy
from imaplib import IMAP4_SSL
from email.header import decode_header
import random
import string
import traceback
import imgkit
from sys import platform

from pymongo import MongoClient
import chardet

from log_config import *
from config import *


class MailUtil:
    def __init__(self, conf):
        self.conf = conf

    def create_mail_connection(self):
        """
        Создает соединение с почтовым сервером
        """
        logging.info("Creating connection with mail server...")
        mail_connection = IMAP4_SSL(host="imap.gmail.com")
        mail_connection.login(user=self.conf['USER_EMAIL'], password=self.conf['USER_PASSWORD'])
        logging.info(f"The connection was established. Details: {mail_connection}")
        return mail_connection

    def get_mail_ids(self, folder: str, mail_connection):
        """
        Получает все id из папки писем
        """
        logging.info("Getting letters ids from folder...")
        logging.debug(f"folder: {folder}")
        logging.debug(f"mail_connection: {mail_connection}")
        mail_connection.select(folder, readonly=True)
        result, data = mail_connection.search(None, "ALL")
        ids = data[0].decode("utf-8")
        mail_ids = ids.split()
        logging.info(f"Letters IDs gathered. Details: {mail_ids}")
        return mail_ids

    def filter_mails_id_by_date(self, mail_ids, mail_connection, start_dt, end_dt,
                                folder):
        """
        Фильтрует все id писем по дате
        """
        logging.info("Filtering letters ids by date...")
        logging.debug(f"mail_ids: {mail_ids}")
        logging.debug(f"start_dt: {start_dt}")
        logging.debug(f"end_dt: {end_dt}")
        logging.debug(f"folder: {folder}")

        mails_filter_by_data = []
        start_letter_id = self.find_id_letter(mail_ids=mail_ids,
                                              mail_connection=mail_connection,
                                              date=start_dt, folder=folder)
        end_letter_id = self.find_id_letter(mail_ids=mail_ids,
                                            mail_connection=mail_connection,
                                            date=end_dt, folder=folder, is_end=True)
        for i in range(start_letter_id + 1, end_letter_id + 1):
            mails_filter_by_data.append(str(i))

        logging.info(f"Filtering letters ids by date: {mails_filter_by_data}")

        return mails_filter_by_data

    def find_id_letter(self, mail_ids, mail_connection, date, folder,
                       is_end: bool = False):
        """
        Реализует алгоритм быстрого поиска писем по дате
        """
        logging.info("Process of find letter id started...")
        logging.debug(f"mail_ids: {mail_ids}")
        logging.debug(f"date: {date}")
        logging.debug(f"folder: {folder}")
        logging.debug(f"is_end: {is_end}")

        start = 0
        end = len(mail_ids)
        step = 0
        while start < end:
            step = step + 1
            mid = (start + end) // 2
            date_mid = self.get_mail_date(mail_id=mail_ids[mid],
                                          mail_connection=mail_connection,
                                          folder=folder)
            if date_mid == date:
                return mid
            if date < date_mid:
                end = mid - 1
            else:
                start = mid + 1
        if start >= end and not is_end:
            return end + 1

        logging.info("Process of find letter id is finished")

        return mid + 1

    def get_mail_date(self, mail_id, mail_connection, folder):
        """
        Получает дату письма
        """
        logging.info("Getting letter date...")
        logging.debug(f"folder: {folder}")
        logging.debug(f"mail_id: {mail_id}")

        mail_data = self.get_mail_data(mail_connection=mail_connection, mail_id=mail_id,
                                       folder=folder)

        list_for_check_formats = [
            {'input_date': mail_data["Date"][:30], "from_format": "%a, %d %b %Y %H:%M:%S %z"},
            {'input_date': mail_data["Date"][:31], "from_format": "%a, %d %b %Y %H:%M:%S %z"},
            {'input_date': mail_data["Date"], "from_format": "%d %b %Y %H:%M:%S %z"},
            {'input_date': mail_data["Date"][:25], "from_format": "%a, %d %b %Y %H:%M:%S"},
        ]
        for case in list_for_check_formats:
            chek, dt_email = self.convert_date(case['input_date'], case['from_format'])
            if chek:
                break

        if dt_email == None:
            raise Exception('Форат даты письма не соответствует ни одному указанному шаблону даты')

        logging.info(f"Letter date: {dt_email}")

        return dt_email

    def convert_date(self, date_text, from_format):
        """
        Получает строку даты из письма и пытается перевести ее из указанного формата в необходимый
        """
        try:
            return [True, datetime.datetime.strptime(date_text, from_format).strftime("%Y-%m-%d %H:%M:%S")]
        except:
            return [False, None]

    def get_mail_body(self, mail_data):
        """
        Получает текст писем
        """
        logging.info("Getting bodies of letters...")
        logging.debug(f"mail_data: {mail_data}")

        while mail_data.is_multipart():
            mail_data = mail_data.get_payload(0)
            logging.debug(mail_data)
        try:
            content = mail_data.get_payload(decode=True)

            if not isinstance(content, str):
                enc = chardet.detect(content)
                content = content.decode(encoding=enc['encoding'])
            return content
        except:
            logging.error('ERROR: Определитель кодировки определил ее неверно')
            traceback.print_exc()

    def get_mail_subject(self, mail_data):
        """
        Получает заголовок письма
        """
        logging.info("Getting subjects of letters...")
        logging.debug(f"mail_data: {mail_data}")

        try:
            current_subject = mail_data.get("Subject")
            encoded_subject = decode_header(current_subject)[
                0][0]


            if not isinstance(encoded_subject, str):
                enc = chardet.detect(encoded_subject)
                encoded_subject = encoded_subject.decode(encoding=enc['encoding'])
            logging.info("Mail subject is: " + str(encoded_subject))

            return encoded_subject
        except:
            logging.error('ERROR: Определитель кодировки определил ее неверно \n')
            traceback.print_exc()

        # try:
        #     current_subject = mail_data.get("Subject")
        #     encoded_subject = decode_header(current_subject)[
        #         0][0].decode(encoding="utf-8")
        #
        # except Exception:
        #     try:
        #         current_subject = mail_data.get("Subject")
        #         encoded_subject = decode_header(current_subject)[
        #             0][0].decode(encoding="win-1251")
        #         logging.info("Mail subject is: " + str(encoded_subject))
        #         return encoded_subject
        #     except Exception:
        #         try:
        #             current_subject = mail_data.get("Subject")
        #             encoded_subject = decode_header(current_subject)[
        #                 0][0].decode(encoding="KOI8-R")
        #             logging.info("Mail subject is: " + str(encoded_subject))
        #             return encoded_subject
        #         except Exception as e:
        #             logging.error(f"Problems getting letter subject. Details: {e}")

    def get_mail_data(self, mail_connection, mail_id, folder):
        """
        Получает отправителя и получателя письма
        """
        logging.info("Getting mail data...")
        logging.debug(f"mail_id: {mail_id}")
        logging.debug(f"folder: {folder}")

        mail_connection.select(mailbox=folder, readonly=True)
        typ, data = mail_connection.fetch(message_set=mail_id,
                                          message_parts="(RFC822)")
        raw_email = data[0][1]
        raw_email_string = raw_email.decode(encoding="utf-8")
        email_message = email.message_from_string(raw_email_string)

        logging.debug(f"Email date gathered: {email_message}")

        return email_message

    def get_mail_sender(self, mail_data):
        """
        Получает отправителя письма
        """
        logging.info("Getting mail sender...")
        logging.debug(f"mail_data: {mail_data}")

        sender = mail_data.get("From")

        logging.debug("Mail sender is: " + str(sender))

        return sender

    def get_mail_reciever(self, mail_data):
        """
        Получает получателя письма
        """
        logging.info("Getting mail reciever...")
        logging.debug(f"mail_data: {mail_data}")

        reciever = mail_data.get("To")

        logging.debug("Mail reciever is: " + str(reciever))

        return reciever

    def read_emails(self, mail_connection, id_list, folder, start_dt, end_dt):
        """
        Читает письма email
        """
        logging.info("Reading emails...")
        logging.debug(f"folder: {folder}")
        logging.debug(f"start_dt: {start_dt}")
        logging.debug(f"end_dt: {end_dt}")

        mails_id = self.filter_mails_id_by_date(mail_ids=id_list,
                                                mail_connection=mail_connection,
                                                start_dt=start_dt,
                                                end_dt=end_dt, folder=folder)
        mails = []
        for mail_id in mails_id:
            date = self.get_mail_date(mail_connection=mail_connection, mail_id=mail_id,
                                      folder=folder)
            mail_data = self.get_mail_data(mail_connection=mail_connection,
                                           mail_id=mail_id, folder=folder)

            body = self.get_mail_body(mail_data=mail_data)
            subject = self.get_mail_subject(mail_data=mail_data)
            reciever = self.get_mail_reciever(mail_data=mail_data)
            sender = self.get_mail_sender(mail_data=mail_data)

            if all([mail_id, body, subject, reciever, sender, date, mail_data]):
                mail = {"mail_id": mail_id, "body": body, "subject": subject,
                        "reciever": reciever, "sender": sender, "date": date, 'raw_data': mail_data, }
                mails.append(mail)

            elif mail_id is not None:
                mail = {"mail_id": mail_id,
                        "body": body or 'Ошибка в получении',
                        "subject": subject or 'Ошибка в получении',
                        "reciever": reciever or 'Ошибка в получении',
                        "sender": sender or 'Ошибка в получении',
                        "date": date or 'Ошибка в получении',
                        'raw_data': mail_data or None,
                        }
                mails.append(mail)

        mail_connection.close()

        logging.debug("Mails is: " + str(mails))

        return mails

    def save_attachment(self, msg, download_folder="/tmp"):
        att_paths = []
        if msg is None:
            return []

        for part in msg.walk():
            if part.get_content_maintype() == 'multipart':
                continue
            if part.get('Content-Disposition') is None:
                continue

            filename = part.get_filename()
            if filename is None:
                continue
            if filename.find('=?utf-8?') != -1:
                continue
            att_path = os.path.join(download_folder, filename)

            if os.path.isfile(att_path):
                att_path = os.path.join(download_folder, self.randomword(10) + '_' + filename)
            try:
                with open(att_path, 'wb') as fp:
                    fp.write(part.get_payload(decode=True))
                att_paths.append(att_path)
            except:
                pass
        return att_paths

    def randomword(self, length):
        letters = string.ascii_lowercase
        return ''.join(random.choice(letters) for i in range(length))

    def strip_text_email(self, mail, last_row_of_letter):
        mail_body = mail['body']
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
            match_end = re.search(last_row_of_letter, row)
            if match_end or row.find('*From:*') != -1 or row.find('From: ') != -1:
                break
            if row != '':
                new_list.append(row)
        mail['body'] = '\n'.join(new_list)
        return mail

    def get_database_conn(self):
        # Provide the mongodb atlas url to connect python to mongodb using pymongo
        # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
        client = MongoClient(MONGO_URI)
        db_conn = client[MONGO_DB_NAME]
        # Create the database for our example (we will use the same database throughout the tutorial
        return db_conn

    def check_html_structure(self, dict_result):
        text = dict_result['text']
        if text.find("<html>") != -1 and text.find("</html>") != -1:
            path = "attachments/" + self.randomword(10) + ".png"
            if platform == "win32":
                config = imgkit.config(wkhtmltoimage=r'C:\Program Files\wkhtmltopdf\bin\wkhtmltoimage.exe')
                imgkit.from_string(text, path, config=config)
            else:
                imgkit.from_string(text, path)
            # print(type(text))
            # print(path)

            dict_result['attachments'] = [path, ] + dict_result['attachments']
            dict_result['text'] = "Выгружено в изображение"
            # print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1")

        return dict_result

    def chek_subjects_in_db(self, list_mails_data, chek_in_db):
        db_conn = self.get_database_conn()
        collection_name = db_conn["mail_subjects"]

        set_subjects = set(map(lambda x: x['subject'], list_mails_data))

        # разделяем тему письма на jira код и название
        subject_dict = {}
        for subj in set_subjects:
            match = None
            if self.conf['REGULAR_MASK_KEY'] is not None:
                match = re.search(self.conf['REGULAR_MASK_KEY'], subj)
            subject_dict[subj] = {}
            if match is not None:
                subject_dict[subj]['key'] = match[0]
                them = subj.replace(match[0], '')
                them = them.replace(self.conf['DEL_FROM_SUBJECT1'], '')
                subject_dict[subj]['them'] = them.replace(self.conf['DEL_FROM_SUBJECT2'], '')
            else:
                subject_dict[subj]['key'] = subj
                subject_dict[subj]['them'] = ''

        if chek_in_db == 'Subjects':
            # проверяем key в бд
            new_set_subjects = set()
            for subj in set_subjects:
                key = subject_dict[subj]['key']
                item_details = collection_name.find({"key": key})
                try:
                    item_details.next()
                except StopIteration:
                    new_set_subjects.add(subj)
            dict_topics = {}
            list_result = []
            for i in new_set_subjects:
                dict_topics[i] = list(filter(lambda x: x['subject'] == i, list_mails_data))

                dict_result = {'key': subject_dict[i]['key'],
                               'subject': subject_dict[i]['them'],
                               'text': dict_topics[i][0]['body'],
                               'attachments': self.save_attachment(dict_topics[i][0]['raw_data'],
                                                                   download_folder=ATTACHMENTS_FILDER),
                               'sended': 0,
                               'assign': 1 if self.conf['ENABLE_ASSIGN_TO_PEOPLE_IN_CHAT'] else 0,
                               'topic': self.conf['EMAIL_FOLDER']
                               }

                dict_result = self.check_html_structure(dict_result)

                list_result.append(dict_result)
            return list_result

        elif chek_in_db == 'Text':
            list_mails_data = list(filter(lambda mail_, conn=collection_name:
                                          self.chek_text_db_for_filter(conn, mail_['body']), list_mails_data))
            list_result = []
            for mail in list_mails_data:
                dict_result = {
                    'key': subject_dict[mail['subject']]['key'],
                    'subject': '',
                    'text': mail['body'],
                    'attachments': self.save_attachment(mail['raw_data'], download_folder=ATTACHMENTS_FILDER),
                    'sended': 0,
                    'assign': 1 if self.conf['ENABLE_ASSIGN_TO_PEOPLE_IN_CHAT'] else 0,
                    'topic': self.conf['EMAIL_FOLDER']

                }
                dict_result = self.check_html_structure(dict_result)

                list_result.append(dict_result)
            return list_result

    def chek_text_db_for_filter(self, connection, text):
        # Проверяем наличие письма в БД по тексту письма
        item_details = connection.find({"text": text})
        try:
            item_details.next()
        except StopIteration:
            return True
        return False

    def main(self):
        db_conn = self.get_database_conn()
        offset_collection = db_conn["offset_folder"]

        mail_connection = self.create_mail_connection()
        id_list = self.get_mail_ids(folder=self.conf['EMAIL_FOLDER'], mail_connection=mail_connection)
        max_id = max(list(map(lambda x: int(x), id_list)))

        offset = offset_collection.find({"folder": self.conf['EMAIL_FOLDER']})
        try:
            if (int(offset[0]['offset']) == max_id):
                print(self.conf['EMAIL_FOLDER'] + ': Писем нет, закрываемся')
                exit(0)

        except Exception:
            offset_collection.insert_one({'folder': self.conf['EMAIL_FOLDER'],
                                          'offset': 0
                                          })
            print(self.conf['EMAIL_FOLDER'] + ': Оффсет поле для новой папки создано')

        # Берем текущее время
        time_now = datetime.datetime.now()
        start = str(time_now - datetime.timedelta(**PERIOD_OF_SCAN_LETTER))[:19]
        end = str(time_now)[:19]

        # Выгружаем письма за период
        list_mails_data = self.read_emails(mail_connection, id_list, self.conf['EMAIL_FOLDER'], start, end)
        print(len(list_mails_data))
        # Исключаем получателей не нас
        list_mails_data = list(filter(lambda x:
                                      x["reciever"].lower().find(self.conf['RECIEVER']) != -1 and x[
                                          "subject"] is not None,
                                      list_mails_data))

        # Исключаем письма с re:, чтобы брать только самые первые письма в теме
        if self.conf['ONLY_FIRST_MAIL']:
            list_mails_data = list(filter(lambda x:
                                          x['subject'].lower().find('re:') == -1,
                                          list_mails_data))
        print(len(list_mails_data))
        # Форматируем текст письма под норм вид
        list_mails_data = list(map(lambda mail: self.strip_text_email(mail, self.conf['LAST_ROW_OF_LETTER']),
                                   list_mails_data))
        print(len(list_mails_data))
        # Проверяем обрабатывали ли мы уже эти письма и генерируем лист объектов для вставки в БД
        if self.conf['ONLY_FIRST_MAIL']:
            list_result = self.chek_subjects_in_db(list_mails_data, 'Subjects')
        else:
            list_result = self.chek_subjects_in_db(list_mails_data, 'Text')
        print(len(list_mails_data))
        collection_name = db_conn["mail_subjects"]
        try:
            collection_name.insert_many(list_result)
            offset_collection.delete_one(filter={'folder': self.conf['EMAIL_FOLDER']
                                                 })
            offset_collection.insert_one({'folder': self.conf['EMAIL_FOLDER'],
                                          'offset': max_id
                                          })
            print( self.conf['EMAIL_FOLDER'] + ': Записи в БД обновлены')

        except TypeError:
            print(self.conf['EMAIL_FOLDER'] + ': Ошибка вставки в бд')
            traceback.print_exc()

def run_main(conf):
    MailUtil(conf).main()


if '__main__' == __name__:
    freeze_support()
    list_proc = []
    for i in CONFIG_LIST:
        list_proc.append(multiprocessing.Process(target=run_main, args=(i,)))
    for i, _ in enumerate(CONFIG_LIST):
        list_proc[i].start()
        time.sleep(5)

