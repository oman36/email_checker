from abc import ABCMeta, abstractmethod
from datetime import date
import email
from email.message import Message
import imaplib
from itertools import chain
from typing import Dict, List, Sequence


class IMAP4Server(metaclass=ABCMeta):
    port: int = 993

    @property
    @abstractmethod
    def host(self):
        raise NotImplementedError

    def __enter__(self) -> imaplib.IMAP4_SSL:
        self._server = imaplib.IMAP4_SSL(self.host, self.port)
        # self._server.debug = 1000
        return self._server

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._server.close()
        return


class YandexIMAP4ServerWrapper(IMAP4Server):
    host = 'imap.yandex.com'


class YandexLogginedServer:
    _DATE_FORMAT = '%d-%b-%Y'

    def __init__(self, username: str, password: str, folder: str = None):
        self._username = username
        self._password = password
        self._folder = folder
        self._server_wrapper = YandexIMAP4ServerWrapper()

    def __enter__(self):
        self._server = self._server_wrapper.__enter__()
        try:
            self._server.login(self._username, self._password)
            self._server.select(self._folder)
        except Exception:
            self.__exit__(None, None, None)
            raise
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._server_wrapper.__exit__(exc_type, exc_val, exc_tb)

    def _format_date(self, date_: date) -> str:
        return date_.strftime(self._DATE_FORMAT)

    def get_uids(
            self,
            since: date,
            before: date = None,
            uid_max: int = 0,
            criteria: Dict[str, str] = None,
    ) -> List[int]:
        criteria = criteria or {}
        search_dates = f'(SINCE {self._format_date(since)})'
        if before is not None:
            search_dates = search_dates[:-1] + f'BEFORE {self._format_date(before)})'
        search_string = self._build_search_string(uid_max, criteria)
        _, data = self._server.uid('search', search_dates, search_string)
        return list(map(int, data[0].decode('utf8').split()))

    def fetch(self, uid: int) -> Message:
        _, email_data = self._server.uid('fetch', str(uid), '(RFC822)')
        # _, email_data = svr.fetch(str(uid), '(RFC822)')
        return self._parse_message(email_data)

    @staticmethod
    def _parse_message(email_data: Sequence[Sequence[bytes]]) -> Message:
        return email.message_from_string(email_data[0][1].decode("utf-8"))

    @staticmethod
    def _build_search_string(uid_max: int, criteria: Dict[str, str]) -> str:
        criteria = {key: f'"{value}"' for key, value in criteria.items()}
        criteria = dict(criteria, UID=f'{uid_max + 1}:*')
        criteria_text = ' '.join(chain(*criteria.items()))
        return f'({criteria_text})'
