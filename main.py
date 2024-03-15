import multiprocessing
import time
from multiprocessing import freeze_support

from common.config_controller import Config
from mail_logic.mail_logic import MailFacade
from mail_logic.profile import ConfigProfile

if '__main__' == __name__:
    conf = Config()
    log = conf.get_common_logger()
    log.info("Start email module")

    freeze_support()
    list_proc = []
    for p_data in conf.profiles:
        list_proc.append(multiprocessing.Process(target=MailFacade, args=(ConfigProfile(**p_data),)))

    for proc in list_proc:
        proc.start()
        time.sleep(5)

    log.info("Stop email module")
