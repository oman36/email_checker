from datetime import datetime
from email.message import Message
import json
import logging.config
import os
from typing import Any, Generator, List, Optional, Tuple, Type
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from alerts import AssigneeAlert, Alert, MergeDoneAlert
from config import Config
from servers import YandexLogginedServer


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


class MailParser:
    _MAILS: List[Alert] = [
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
            except Exception as error:
                logger.error('Unexpected error:', exc_info=error)
                continue
            for path, text in parser.parse(message):
                logger.debug('Text %s_%s ...', uid, path)
                result = MailParser.parse(text)
                if result:
                    slack.send(result)
            continue
    UIDKeeper.save_uid(max_uid)
    logger.info('Finish.')


if __name__ == '__main__':
    main(Config)
