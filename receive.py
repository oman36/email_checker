from abc import ABCMeta, abstractmethod
from datetime import date, datetime
import email
from email.message import Message
import imaplib
from itertools import chain
import json
import logging.config
import os
import re
import typing
from typing import Type, List, Tuple, Generator, Any, Optional, Union, Dict, Sequence
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from config import Config


class DebugAndInfoFilter(logging.Filter):
    def filter(self, record: logging.LogRecord):
        return record.levelno <= logging.INFO


logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': True,
    'root': {
        'handlers': ['stdout', 'stderr'],
        'level': logging.DEBUG,
    },
    'loggers': {
        'receive': {
            'level': logging.DEBUG,
        }
    },
    'handlers': {
        'stdout': {
            'class': 'logging.StreamHandler',
            'formatter': 'default',
            'filters': ['debug-and-info'],
            'level': logging.DEBUG,
            'stream': 'ext://sys.stdout',
        },
        'stderr': {
            'class': 'logging.StreamHandler',
            'formatter': 'default',
            'level': logging.WARNING,
            'stream': 'ext://sys.stderr',
        },
    },
    'filters': {
        'debug-and-info': {'()': DebugAndInfoFilter},
    },
    'formatters': {
        'default': {
            'format': '[%(asctime)s] %(levelname)7s %(message)s',
        },
    }
})
logger = logging.getLogger('receive')


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


class UIDKeeper:
    _MAX_UID_FILENAME = os.path.join(os.path.dirname(__file__), 'max_uid.txt')

    @classmethod
    def get_uid(cls) -> int:
        try:
            with open(cls._MAX_UID_FILENAME) as f:
                return int(f.read())
        except Exception as e:
            logger.error('Invalid uid', exc_info=e)
        return 0

    @classmethod
    def save_uid(cls, uid: int):
        with open(cls._MAX_UID_FILENAME, 'w+') as f:
            f.write(str(uid))


class AlertGroup(metaclass=ABCMeta):
    _theme_re = re.compile('<b>Тема:</b> Re: omd-dwh \| (.+)\s*\(!(\d+)\).*')

    @staticmethod
    def _search_re(text: str, regexp: typing.re.Pattern[str]) \
            -> Optional[typing.re.Match[str]]:
        for line in text.split('\n'):
            match = regexp.match(line)
            if match:
                return match
        return None

    def _get_theme(self, text: str) -> Union[Tuple[str, str], Tuple[None, None]]:
        for line in text.split('\n'):
            match = self._theme_re.match(line)
            if match:
                return match.group(1), self._get_rm_link(match.group(2))
        return None, None

    @staticmethod
    def _get_rm_link(mr_id: str) -> str:
        return f'https://gitlab.omd.ru/dwh/omd-dwh/merge_requests/{mr_id}'

    @abstractmethod
    def get_result(self, text: str) -> Optional[str]:
        raise NotImplementedError


class AssigneeAlert(AlertGroup):
    _assignee_re = re.compile(
        '.*<p>Assignee changed '
        'from <strong>([^<]+)</strong> '
        'to <strong>([^<]+)</strong>.*'
    )
    _target = 'Petrov Vladimir'

    def get_result(self, text: str) -> Optional[str]:
        match = self._search_re(text, self._assignee_re)
        if match:
            if self._target not in match.groups():
                return None
            mr_name, mr_link = self._get_theme(text)
            return (
                f'{mr_name} {mr_link} Assignee changed from '
                f'*{match.group(1)}* to *{match.group(2)}*'
            )


class MergeDoneAlert(AlertGroup):
    _merge_done_re = re.compile('.+Merge Request !(\d+) was merged.+')

    def get_result(self, text: str) -> Optional[str]:
        match = self._search_re(text, self._merge_done_re)
        if match:
            mr_name, mr_link = self._get_theme(text)
            return f'Merged {mr_name} {mr_link}'


class MailParser:
    _MAILS: List[AlertGroup] = [
        AssigneeAlert(),
        MergeDoneAlert(),
    ]

    @classmethod
    def parse(cls, text: str) -> Optional[str]:
        for group in cls._MAILS:
            result = group.get_result(text)
            if result is not None:
                return result


class Slack:
    def __init__(self, slack_url: str):
        self._slack_url = slack_url

    def send(self, message: str) -> None:
        try:
            request = Request(
                url=self._slack_url,
                data=json.dumps({'text': message}).encode(),
                headers={'Content-type': 'application/json'},
            )
            urlopen(request).read().decode()
        except HTTPError as e:
            logger.exception('Error occurred while sending:', exc_info=e)


class TextBlocksParser:
    _DEFAULT_CHARSET = 'utf-8'

    def parse(self, msg: Message) -> Generator[Tuple[str, str], Any, None]:
        type_ = msg.get_content_maintype()
        payload = msg.get_payload()
        root_messages: List[Message] = [payload] if type_ != 'multipart' else payload
        parts: List[Tuple[str, Message]] = \
            [(str(i), m) for i, m in enumerate(root_messages, 1)]
        while parts:
            path, part = parts.pop(0)
            content_type = part.get_content_maintype()
            if content_type == 'text':
                string: bytes = part.get_payload(decode=True)
                charset = part.get_param('charset', self._DEFAULT_CHARSET)
                yield path, string.decode(charset, 'replace')
            elif content_type == 'multipart':
                messages: List[Message] = part.get_payload()
                parts.extend(
                    (f'{path}.{i}', part_) for i, part_ in enumerate(messages, 1)
                )


def main(config: Type[Config]) -> None:
    logger.info('Start')
    server_wrapper = YandexLogginedServer(
        username=config.Yandex.username,
        password=config.Yandex.password,
        folder=config.Yandex.folder,
    )
    max_uid = UIDKeeper.get_uid()
    today = datetime.today()
    slack = Slack(config.Slack.web_hook_url)
    parser = TextBlocksParser()
    with server_wrapper as server:
        logger.info(
            'Fetching since %s from uid=%d ...', today.strftime('%Y-%m-%d'), max_uid,
        )
        uids = server.get_uids(since=today, uid_max=max_uid)
        logger.info("Emails found by search criteria: %d", len(uids))
        for uid in uids:
            if uid <= max_uid:
                continue
            max_uid = uid
            try:
                message = server.fetch(uid)
            except Exception as e:
                logger.error('Unexpected error:', exc_info=e)
                continue

            for path, text in parser.parse(message):
                logger.debug('Text %s_%s ...', uid, path)
                if not text.startswith('<html'):
                    continue
                with open(f'{uid}_{path}.html', 'w+') as f:
                    f.write(text)
                result = MailParser.parse(text)
                if result:
                    slack.send(result)
            continue
    UIDKeeper.save_uid(max_uid)
    logger.info('Finish.')


if __name__ == '__main__':
    main(Config)
